"""CLEXER Music - the voice-chat engine (its own Railway service).

bot.py stays the only thing users talk to: it receives /play, /skip, ... and
the buttons on the now-playing card, and forwards them here over HTTP. This
process holds the ASSISTANT user account (MUSIC_SESSION_STRING) which joins
the group's voice chat and streams the audio, and it posts the now-playing
card through the Bot API with the bot's own token, so everything the group
sees still comes from the bot.

Songs are fetched per request from YouTube (yt-dlp resolves a direct audio
stream; ffmpeg decodes it on the fly) and nothing is kept.

Env: MUSIC_API_ID, MUSIC_API_HASH, MUSIC_SESSION_STRING, TELEGRAM_BOT_TOKEN,
     MUSIC_SECRET (shared with bot.py), PORT.
"""
import asyncio
import html as _html
import json as _json
import os
import subprocess
import tempfile
import time
from typing import Optional

import requests as _rq
from fastapi import FastAPI, Header, HTTPException, Request
from pyrogram import Client
from pyrogram.errors import RPCError
from pytgcalls import PyTgCalls, filters as fl
from pytgcalls.exceptions import NoActiveGroupCall, NotInCallError
from pytgcalls.types import AudioQuality, ChatUpdate, MediaStream, StreamEnded, VideoQuality
from yt_dlp import YoutubeDL

API_ID = int(os.getenv("MUSIC_API_ID", "0") or 0)
API_HASH = os.getenv("MUSIC_API_HASH", "").strip()
# whitespace / line breaks that ride along when the string is pasted would
# break the base64 decode ("Incorrect padding")
SESSION = "".join(os.getenv("MUSIC_SESSION_STRING", "").split())
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
SECRET = os.getenv("MUSIC_SECRET", "").strip()
MAX_QUEUE = 25
# Where the player state survives a restart / redeploy: the bot's central
# store when CLEXER_API_URL + PUSH_STATE_SECRET are set (same values as the
# bot service), else a local file (survives a crash-restart, not a redeploy).
CENTRAL_URL = (os.getenv("CLEXER_API_URL") or "").rstrip("/")
CENTRAL_SECRET = os.getenv("PUSH_STATE_SECRET", "").strip()
STATE_FILE = os.path.join(tempfile.gettempdir(), "clexer_music_state.json")
RESUME_MAX_AGE = 3 * 3600      # a saved state older than this is just cleaned up, not resumed
# Premium emoji: MUSIC_EMOJI_JSON = {"🎵": "5231200819986047254", ...}. Text gets
# <tg-emoji> wrappers, buttons get icon_custom_emoji_id (glyph dropped from the
# label, as bot.py does). Empty map = plain emoji everywhere.
_DEFAULT_EMOJI = {          # the admin's premium set (2026-09-16); MUSIC_EMOJI_JSON overrides/extends
    "🎵": "6026256492619895014", "⏸": "5947029782121155470", "🎙": "5377544228505134960",
    "⚠️": "5911295297736154925", "🔎": "5309965701241379366", "🔍": "5409298261754258920",
    "➕": "5359529383319084413", "👤": "5262742999678329061", "⏱": "5382194935057372936",
    "📜": "5264821604935804414", "🔊": "5260325873688518261", "🔉": "5264718164943446901",
    "⏹": "6282581974796211552", "▶️": "5269410150426356158", "▶": "5269410150426356158",
}
try:
    import json as _json
    EMOJI = {**_DEFAULT_EMOJI, **{k: str(v) for k, v in _json.loads(os.getenv("MUSIC_EMOJI_JSON", "") or "{}").items()}}
except Exception:
    EMOJI = dict(_DEFAULT_EMOJI)
MAX_SECONDS = 20 * 60          # mixes / hour-long uploads are skipped

app = FastAPI()
client: Client = None      # both are created inside main(), on the loop that runs everything
call: PyTgCalls = None

# chat_id -> {"queue": [track], "now": track|None, "paused": bool, "volume": int, "card": (chat, msg_id)}
_state: dict = {}
_lock = asyncio.Lock()
_me = {"id": 0, "username": ""}


# ── Bot API (the bot's token, sending only - polling stays in bot.py) ────────
def bot_api(method, payload=None, timeout=15):
    if not BOT_TOKEN:
        return {}
    try:
        return _rq.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=payload, timeout=timeout).json()
    except Exception as e:
        print(f"[MUSIC] bot {method}: {e}")
        return {}


def _esc(s):
    return _html.escape(str(s or ""), quote=False)


def _pe(text: str) -> str:
    """Wrap known glyphs in premium-emoji tags (longest glyph first)."""
    for g in sorted(EMOJI, key=len, reverse=True):
        text = text.replace(g, f'<tg-emoji emoji-id="{EMOJI[g]}">{g}</tg-emoji>')
    return text


def _btn(label, data, style=None):
    b = {"text": label, "callback_data": data}
    if style:
        b["style"] = style
    for g, eid in EMOJI.items():
        if label.startswith(g):
            b["icon_custom_emoji_id"] = eid
            rest = label[len(g):].strip()
            b["text"] = rest or label
            break
    return b


def _dur(sec):
    sec = int(sec or 0)
    if sec <= 0:
        return "🔴 LIVE"
    return f"{sec // 60}:{sec % 60:02d}"


# ── YouTube lookup ─────────────────────────────────────────────────────────
# Video + audio: a combined 480p file when YouTube has one, else the best
# 480p video track plus the best audio track (streamed together). Video is
# capped at 360p (MUSIC_VIDEO_HEIGHT) to keep the server's encoder comfortable.
VIDEO_H = int(os.getenv("MUSIC_VIDEO_HEIGHT", "360") or 360)
_VQ = {360: VideoQuality.SD_360p, 480: VideoQuality.SD_480p, 720: VideoQuality.HD_720p}
VQ = _VQ.get(VIDEO_H, VideoQuality.SD_360p)
_YDL = {"format": (f"b[height<={VIDEO_H}][vcodec^=avc1][acodec!=none]/bv*[height<={VIDEO_H}][vcodec^=avc1]+ba"
                   f"/b[height<={VIDEO_H}][acodec!=none][vcodec!=none]/bv*[height<={VIDEO_H}]+ba/b[height<={VIDEO_H}]/bestaudio/best"),
        "noplaylist": True, "quiet": True, "no_warnings": True, "default_search": "ytsearch1", "skip_download": True,
        "extract_flat": False}


def _track_from(e, fallback_title=""):
    """Track dict from a full yt-dlp info: audio url + optional video url."""
    audio = video = None
    rf = e.get("requested_formats") or []
    if rf:
        for f in rf:
            if f.get("vcodec") not in (None, "none") and not video:
                video = f.get("url")
            if f.get("acodec") not in (None, "none") and not audio:
                audio = f.get("url")
    else:
        u = e.get("url")
        if e.get("vcodec") not in (None, "none") and e.get("acodec") not in (None, "none"):
            video = u                       # one file carrying both
        else:
            audio = u
    if not audio and not video:
        return None
    live = bool(e.get("is_live"))
    if live and not video:
        video = audio                       # a live manifest carries both
    return {"title": e.get("title") or fallback_title, "duration": 0 if live else (e.get("duration") or 0), "live": live,
            "url": audio or video, "video": video, "id": e.get("id") or "",
            "page": e.get("webpage_url") or "", "thumb": e.get("thumbnail") or "",
            "uploader": e.get("uploader") or e.get("channel") or ""}


# Songs are fetched to disk first and played from the local file: YouTube's
# CDN is never in the playback path, so a slow moment there cannot stall the
# stream. The next song (queue head, or the similar song that autoplay will
# pick) is fetched while the current one plays, so there is no gap either.
DL_MAX = int(os.getenv("MUSIC_DOWNLOAD_SECONDS", "70") or 70)   # give up and stream from the URL after this


def _download(track):
    """Grab the track into one local .mkv (streams copied, no re-encode)."""
    out = tempfile.NamedTemporaryFile(prefix="clx_dl_", suffix=".mkv", delete=False).name
    v, a = track.get("video"), track["url"]
    if v and v != a:
        cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", v, "-i", a, "-map", "0:v:0", "-map", "1:a:0", "-c", "copy", out]
    elif v:
        cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", v, "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", out]
    else:
        cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", a, "-vn", "-c", "copy", out]
    try:
        subprocess.run(cmd, check=True, timeout=DL_MAX)
        if os.path.getsize(out) > 10_000:
            return out
    except Exception as e:
        print(f"[MUSIC] download {track.get('title', '')[:40]!r}: {e!r}")
    try:
        os.remove(out)
    except Exception:
        pass
    return None


_fetching: dict = {}          # id(track) -> running download, so two callers share one


async def _fetch(track):
    """Make sure the track has a local file (unless it is live)."""
    if track.get("live") or track.get("local"):
        return
    key = id(track)
    if key in _fetching:
        await _fetching[key]; return
    fut = asyncio.ensure_future(asyncio.to_thread(_download, track))
    _fetching[key] = fut
    try:
        track["local"] = await fut
    finally:
        _fetching.pop(key, None)
    if track["local"]:
        print(f"[MUSIC] fetched {track.get('title', '')[:40]!r} ({os.path.getsize(track['local']) // 1_000_000} MB)")


def _drop_track(t):
    """Delete a track's local file (the URL stays for a replay via Prev)."""
    f = (t or {}).get("local")
    if f:
        t["local"] = None
        try:
            os.remove(f)
        except Exception:
            pass


def _drop_all(s):
    """Everything on disk for this chat: encoded file, current song, queue, prefetched next."""
    _drop_file(s)
    _drop_track(s.get("now"))
    for t in s["queue"]:
        _drop_track(t)
    _drop_track(s.get("up_next")); s["up_next"] = None


_prefetch_locks: dict = {}


async def _prefetch(chat_id):
    """Runs in the background while a song plays: fetch what comes next."""
    lock = _prefetch_locks.setdefault(chat_id, asyncio.Lock())
    async with lock:
        await _prefetch_locked(chat_id)


async def _prefetch_locked(chat_id):
    s = _st(chat_id); now = s["now"]
    if not now:
        return
    try:
        if s["queue"]:
            t = s["queue"][0]
        else:
            up = s.get("up_next")
            if up and up.get("after") == now.get("id"):
                if up.get("local") or up.get("live"):
                    return
                t = up                                  # picked earlier, file still missing
            else:
                seed = next((x for x in [now] + s["history"][::-1] if x.get("by") != "autoplay"), now)
                t = await asyncio.to_thread(_related, seed if seed.get("id") else now, set(s["played"]))
                if not t or s["now"] is not now:
                    return
                t["after"] = now.get("id")
                _drop_track(s.get("up_next")); s["up_next"] = t
        await _fetch(t)
    except Exception as e:
        print(f"[MUSIC] prefetch {chat_id}: {e!r}")


def _stream(track, file=None, video_on=True):
    if not video_on:
        src = file or track.get("local") or track["url"]
        return MediaStream(src, audio_parameters=AudioQuality.HIGH, video_flags=MediaStream.Flags.IGNORE)
    if track.get("live") and not file:
        return MediaStream(track["url"], audio_parameters=AudioQuality.HIGH, video_parameters=VQ)
    file = file or track.get("local")
    if file:
        return MediaStream(file, audio_parameters=AudioQuality.HIGH, video_parameters=VQ)
    if track.get("video"):
        if track["video"] == track["url"]:
            return MediaStream(track["video"], audio_parameters=AudioQuality.HIGH, video_parameters=VQ)
        return MediaStream(track["video"], audio_path=track["url"], audio_parameters=AudioQuality.HIGH, video_parameters=VQ)
    return MediaStream(track["url"], audio_parameters=AudioQuality.HIGH, video_flags=MediaStream.Flags.IGNORE)
# YouTube sometimes refuses datacenter IPs ("Sign in to confirm you're not a
# bot"). MUSIC_COOKIES = the text of a Netscape cookies.txt exported from a
# logged-in browser; it is written to disk once and handed to yt-dlp.
_COOKIES = os.getenv("MUSIC_COOKIES", "")
_cookie_info = {"loaded": False, "problem": "", "lines": 0, "login": [], "expires": None}


def _load_cookies(raw):
    """Write the cookies.txt yt-dlp reads. Accepts the file as pasted (real
    newlines), with literal \n, or base64 (safest way through an env editor
    that mangles line breaks). Records what it found for /health."""
    import base64, re as _re
    txt = raw.strip()
    if not txt:
        return
    if "\n" in txt and chr(10) not in txt:
        txt = txt.replace("\n", chr(10))
    if chr(10) not in txt and not txt.startswith("#"):
        try:
            txt = base64.b64decode(txt).decode("utf-8")
        except Exception:
            pass
    if chr(10) not in txt and chr(9) in txt:
        # one long line: an editor ate the line breaks - each record starts with a domain
        txt = _re.sub(r"\s(?=(?:\.?[\w-]+\.)+[a-z]{2,}\t)", chr(10), txt)
    lines = [l for l in txt.splitlines() if l.strip() and not l.startswith("#")]
    recs = [l.split(chr(9)) for l in lines]
    good = [r for r in recs if len(r) >= 7]
    _cookie_info["lines"] = len(good)
    if not good:
        _cookie_info["problem"] = "not a Netscape cookies.txt (no tab-separated records) - export again"
        return
    names = {r[5]: r for r in good if "youtube" in r[0]}
    login = [n for n in ("SID", "HSID", "SSID", "__Secure-3PSID", "LOGIN_INFO") if n in names]
    _cookie_info["login"] = login
    if not login:
        _cookie_info["problem"] = "no login cookies (SID/LOGIN_INFO) - export while signed in to YouTube"
    exp = [int(names[n][4]) for n in login if names[n][4].isdigit() and int(names[n][4]) > 0]
    if exp:
        _cookie_info["expires"] = min(exp)
        if min(exp) < time.time():
            _cookie_info["problem"] = "login cookies expired - export a fresh file"
    path = os.path.join(tempfile.gettempdir(), "clexer_yt_cookies.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Netscape HTTP Cookie File" + chr(10) + chr(10).join(chr(9).join(r[:7]) for r in good) + chr(10))
    _YDL["cookiefile"] = path
    _cookie_info["loaded"] = True


_load_cookies(_COOKIES)
if _COOKIES.strip():
    print(f"[MUSIC] cookies: {_cookie_info}")
_last_error = {"text": ""}


_BAD = ("non stop", "nonstop", "non-stop", "10 minutes", "1 hour", "loop", "slowed", "reverb", "8d", "remix", "mashup",
        "cover", "karaoke", "instrumental", "ringtone", "status", "whatsapp", "shorts", "jukebox", "medley", "mix",
        "dj ", "bass boosted", "lofi", "lo-fi", "speed up", "sped up", "reaction", "tutorial", "lyrics video by", "live ",
        "re-create", "recreate", "recreated", "2.o", "2.0", "unplugged", "flute", "piano", "guitar", "sad version", "female version")
_GOOD = ("official", "lyrical", "full song", "full video", "video song", "title track", "audio")
_LABELS = ("t-series", "sony music", "zee music", "yrf", "tips", "saregama", "eros", "times music", "speed records",
           "- topic", "melodies", "vevo", "records", "universal", "warner", "pen movies", "goldmines", "aditya music", "lahari")


def _score(e, query=""):
    """Higher = more likely the real, original song - and the one asked for."""
    t = (e.get("title") or "").lower()
    ch = (e.get("channel") or e.get("uploader") or "").lower()
    dur = e.get("duration") or 0
    sc = 0
    words = [w for w in query.lower().split() if len(w) > 2]
    sc += 2 * min(3, sum(1 for w in words if w in t))
    sc -= 6 * sum(1 for w in _BAD if w in t)
    sc += 2 * sum(1 for w in _GOOD if w in t)
    sc += 5 if any(w in ch for w in _LABELS) else 0
    if dur:
        sc += 3 if 120 <= dur <= 480 else (-2 if dur < 60 else -4)
    return sc


def _flat_search(url, q=None):
    """Titles/ids only (fast). Returns candidate watch URLs, best first.
    `url` is a yt-dlp search prefix ("ytsearch6:") or a full search page."""
    with YoutubeDL({**_YDL, "extract_flat": "in_playlist", "playlistend": 6}) as y:
        info = y.extract_info(url + (q or ""), download=False)
    scored = []
    for e in (info or {}).get("entries") or []:
        if not e:
            continue
        dur = e.get("duration") or 0
        if dur and (dur > MAX_SECONDS or dur < 30):
            continue                       # hour-long mixes and shorts
        vid = e.get("id")
        u = e.get("url") or e.get("webpage_url") or (vid and f"https://www.youtube.com/watch?v={vid}")
        if not u:
            continue
        if not u.startswith("http"):
            u = f"https://www.youtube.com/watch?v={u}"
        if "watch" in u:
            scored.append((_score(e, q or ""), u))
    scored.sort(key=lambda x: -x[0])       # the real song outranks loops, remixes and mixes
    return [u for _, u in scored]


_PIPED = ["https://pipedapi.kavin.rocks", "https://api.piped.yt", "https://pipedapi.adminforge.de"]


def _piped_search(q):
    """Third option: a public Piped instance's search API (plain JSON)."""
    from urllib.parse import quote_plus
    for base in _PIPED:
        try:
            r = _rq.get(f"{base}/search?q={quote_plus(q + ' song')}&filter=videos", timeout=8)
            items = (r.json() or {}).get("items") or []
        except Exception:
            continue
        out = []
        for it in items:
            dur = it.get("duration") or 0
            if dur and (dur > MAX_SECONDS or dur < 30):
                continue
            u = it.get("url") or ""
            if "watch?v=" in u:
                out.append("https://www.youtube.com" + u if u.startswith("/") else u)
        if out:
            return out[:6]
    return []


def _lookup(query: str) -> Optional[dict]:
    """Title / duration / direct audio url for a search or a YouTube link.
    Search order: YouTube search, YouTube Music search, web search - the
    first one that answers wins; the full extraction runs on the first
    candidate that is a plausible song."""
    q = query.strip()
    _last_error["text"] = ""
    if q.startswith(("http://", "https://")):
        cands = [q]
    else:
        cands = []
        from urllib.parse import quote_plus
        for label, fn in (("ytsearch", lambda: _flat_search("ytsearch6:", q + " song")),
                          ("ytmusic", lambda: _flat_search("https://music.youtube.com/search?q=" + quote_plus(q))),
                          ("piped", lambda: _piped_search(q))):
            try:
                cands = fn()
            except Exception as ex:
                print(f"[MUSIC] {label} {q!r}: {str(ex)[:160]}")
                _last_error["text"] = str(ex)
                cands = []
            if cands:
                if label != "ytsearch":
                    print(f"[MUSIC] {label} answered for {q!r}")
                break
    for u in cands[:5]:
        try:
            with YoutubeDL(_YDL) as y:
                e = y.extract_info(u, download=False)
        except Exception as ex:
            msg = str(ex)
            print(f"[MUSIC] extract {u}: {msg[:200]}")
            _last_error["text"] = msg
            continue
        if not e:
            continue
        dur = e.get("duration") or 0
        if not e.get("is_live") and dur and (dur > MAX_SECONDS or dur < 30):
            continue
        t = _track_from(e, q)
        if t:
            return t
    return None


def _related(track, played_ids):
    """The next song for autoplay: YouTube's own Mix for the last song,
    skipping anything already played in this chat."""
    vid = track.get("id")
    if not vid:
        return None
    try:
        with YoutubeDL({**_YDL, "noplaylist": False, "extract_flat": "in_playlist", "playlistend": 12}) as y:
            info = y.extract_info(f"https://www.youtube.com/watch?v={vid}&list=RD{vid}", download=False)
    except Exception as e:
        print(f"[MUSIC] related {vid}: {str(e)[:120]}")
        return None
    cands = []
    for e in (info or {}).get("entries") or []:
        if not e or not e.get("id") or e["id"] in played_ids:
            continue
        dur = e.get("duration") or 0
        if dur and (dur > MAX_SECONDS or dur < 60):
            continue
        cands.append((_score(e), e["id"]))
    cands.sort(key=lambda x: -x[0])
    for _, cid in cands[:4]:
        try:
            with YoutubeDL(_YDL) as y:
                e = y.extract_info(f"https://www.youtube.com/watch?v={cid}", download=False)
        except Exception as ex:
            print(f"[MUSIC] related extract {cid}: {str(ex)[:120]}")
            continue
        t = _track_from(e or {}, "")
        if t:
            t["by"] = "autoplay"
            return t
    return None


# ── state persistence ─────────────────────────────────────────────────────
def _snapshot():
    """Everything needed to pick a chat's playback up again."""
    out = {}
    for chat_id, st in _state.items():
        if not st["now"]:
            continue
        pos = st.get("_pos", 0)
        out[str(chat_id)] = {"now": st["now"], "queue": st["queue"], "paused": st["paused"], "volume": st["volume"],
                             "card": st["card"], "position": st["offset"] + pos, "saved": time.time(),
                             "autoplay": st["autoplay"], "played": st["played"][-40:], "history": st["history"][-5:],
                             "video_on": st["video_on"]}
    return out


def _save_state(snap=None):
    snap = _snapshot() if snap is None else snap
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            _json.dump(snap, f)
    except Exception as e:
        print(f"[MUSIC] state file: {e}")
    if CENTRAL_URL and CENTRAL_SECRET:
        try:
            _rq.post(f"{CENTRAL_URL}/kv/music_state", json=snap, headers={"X-Push-Secret": CENTRAL_SECRET}, timeout=8)
        except Exception as e:
            print(f"[MUSIC] state push: {e}")


def _load_state():
    snap = None
    if CENTRAL_URL and CENTRAL_SECRET:
        try:
            r = _rq.get(f"{CENTRAL_URL}/kv/music_state", headers={"X-Push-Secret": CENTRAL_SECRET}, timeout=8)
            if r.ok and r.json().get("found"):
                snap = r.json().get("data")
        except Exception as e:
            print(f"[MUSIC] state pull: {e}")
    if snap is None:
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                snap = _json.load(f)
        except Exception:
            snap = None
    return snap if isinstance(snap, dict) else {}


async def _track_positions():
    """Once a second, remember how far each playing chat is - so a restart
    can resume near the same spot - and save the state every 10 s."""
    n = 0
    while True:
        await asyncio.sleep(1)
        for chat_id, st in list(_state.items()):
            if st["now"] and not st["paused"]:
                try:
                    st["_pos"] = await call.time(chat_id)
                except Exception:
                    pass
        n += 1
        if n % 10 == 0 and any(st["now"] for st in _state.values()):
            await asyncio.to_thread(_save_state)
        if n % 300 == 0:
            await asyncio.to_thread(_janitor)


def _janitor():
    """Delete temp media nobody refers to any more (older than 10 min), so a
    missed cleanup can never fill the disk."""
    import glob
    keep = set()
    for st in _state.values():
        for t in [st.get("now"), st.get("up_next")] + list(st["queue"]):
            if t and t.get("local"):
                keep.add(t["local"])
        if st.get("file"):
            keep.add(st["file"])
    for f in glob.glob(os.path.join(tempfile.gettempdir(), "clx_*")):
        try:
            if f not in keep and time.time() - os.path.getmtime(f) > 600:
                os.remove(f)
        except Exception:
            pass


def _card_alive(card):
    """True while the card message still exists. A no-op markup edit is the
    cheapest probe the Bot API offers: 'not modified' means it is there,
    'message to edit not found' means someone deleted it."""
    j = bot_api("editMessageReplyMarkup", {"chat_id": card[0], "message_id": card[1], "reply_markup": _card_kb(card[0])}, timeout=8)
    if j.get("ok"):
        return True
    d = str(j.get("description", "")).lower()
    return "not found" not in d and "message_id_invalid" not in d and "can't be edited" not in d


async def _card_watch():
    """Every 40 s: if a playing chat's card was deleted by someone, post it
    again so the controls never disappear mid-song."""
    while True:
        await asyncio.sleep(40)
        for chat_id, st in list(_state.items()):
            if not st["now"]:
                continue
            try:
                if st["card"] and not await asyncio.to_thread(_card_alive, st["card"]):
                    print(f"[MUSIC] card {chat_id} was deleted - posting again")
                    st["card"] = None
                if not st["card"]:
                    await asyncio.to_thread(_post_card, chat_id)
            except Exception as e:
                print(f"[MUSIC] card watch {chat_id}: {e!r}")


async def _resume_all():
    """After a restart: pick every chat up where it was, or tidy its card."""
    snap = _load_state()
    if not snap:
        return
    fresh = {}
    for cid, st in snap.items():
        chat_id = int(cid)
        too_old = time.time() - float(st.get("saved", 0)) > RESUME_MAX_AGE
        card = st.get("card")
        if too_old or not st.get("now"):
            if card:
                _card_note(card, "⏹ Playback ended while I was restarting - send /play to start again.")
            continue
        s = _st(chat_id)
        s.update(queue=st.get("queue", []), volume=int(st.get("volume", 100)), paused=False, card=tuple(card) if card else None,
                 autoplay=bool(st.get("autoplay", True)), played=st.get("played", []), history=st.get("history", []),
                 video_on=bool(st.get("video_on", True)))
        track = st["now"]
        start = float(st.get("position", 0))
        try:
            if track.get("live"):
                s["now"] = track; s["file"] = None; s["offset"] = 0
                await call.play(chat_id, _stream(track, None, s["video_on"]))
            else:
                if track.get("local") and not os.path.exists(track["local"]):
                    track["local"] = None
                await _fetch(track)
                f = await asyncio.to_thread(_encode, track, start, s["volume"], s["video_on"])
                s["now"] = track; s["file"] = f; s["offset"] = start
                await call.play(chat_id, _stream(track, f, s["video_on"]))
            await asyncio.to_thread(_post_card, chat_id)
            print(f"[MUSIC] resumed {track['title'][:40]!r} in {chat_id} at {start:.0f}s")
            fresh[cid] = True
        except Exception as e:
            print(f"[MUSIC] resume {chat_id}: {e!r}")
            s["now"] = None; s["queue"] = []; s["file"] = None
            if card:
                _card_note(tuple(card), "⏹ I restarted and could not rejoin the voice chat - send /play to start again.")
            s["card"] = None
    if not fresh:
        _save_state({})


def _card_note(card, text):
    try:
        chat_id, mid, kind = card
        m = "editMessageCaption" if kind == "photo" else "editMessageText"
        bot_api(m, {"chat_id": chat_id, "message_id": mid, ("caption" if kind == "photo" else "text"): _pe(text), "parse_mode": "HTML"})
    except Exception:
        pass


# ── player ─────────────────────────────────────────────────────────────────
def _st(chat_id):
    return _state.setdefault(chat_id, {"queue": [], "now": None, "paused": False, "volume": 100, "card": None,
                                       "offset": 0, "file": None, "history": [], "played": [], "autoplay": True, "msgs": [],
                                       "video_on": True, "up_next": None})


def _drop_file(s):
    f = s.get("file")
    s["file"] = None
    if f:
        try:
            os.remove(f)
        except Exception:
            pass


def _encode(track, start, volume, video=True):
    """The rest of the song from `start` seconds at `volume`% into a temp
    file. Volume cannot be set through Telegram for a plain member of the
    call, so it is baked into the audio; the video track is copied as is."""
    start = max(0, float(start)); vol = f"volume={max(0.05, volume / 100):.2f}"
    loc = track.get("local")
    if loc and not os.path.exists(loc):
        loc = track["local"] = None
    if track.get("video") and video:
        out = tempfile.NamedTemporaryFile(prefix="clx_", suffix=".mkv", delete=False).name
        if loc:
            cmd = ["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{start:.1f}", "-i", loc,
                   "-map", "0:v:0", "-map", "0:a:0", "-c:v", "copy", "-af", vol, "-c:a", "libopus", "-b:a", "128k", out]
        elif track["video"] == track["url"]:
            cmd = ["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{start:.1f}", "-i", track["video"],
                   "-map", "0:v:0", "-map", "0:a:0", "-c:v", "copy", "-af", vol, "-c:a", "libopus", "-b:a", "128k", out]
        else:
            cmd = ["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{start:.1f}", "-i", track["video"],
                   "-ss", f"{start:.1f}", "-i", track["url"], "-map", "0:v:0", "-map", "1:a:0",
                   "-c:v", "copy", "-af", vol, "-c:a", "libopus", "-b:a", "128k", "-shortest", out]
    else:
        out = tempfile.NamedTemporaryFile(prefix="clx_", suffix=".ogg", delete=False).name
        cmd = ["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{start:.1f}", "-i", loc or track["url"], "-vn",
               "-af", vol, "-c:a", "libopus", "-b:a", "128k", "-ar", "48000", out]
    subprocess.run(cmd, check=True, timeout=300)
    return out


def _card_text(chat_id):
    s = _st(chat_id); t = s["now"]
    if not t:
        return _pe("⏹ Nothing playing.")
    nxt = s["queue"][0]["title"] if s["queue"] else "🔁 a similar song"
    head = "⏸ <b>Paused</b>" if s["paused"] else "🎵 <b>Now playing</b>"
    state = "⏸ paused - press Resume to continue" if s["paused"] else "▶ playing"
    who = "🔁 Similar to the last request" if t.get("by") == "autoplay" else f"👤 Requested by {_esc(t['by'])}"
    return _pe(f"{head}\n\n"
               f"<b>{_esc(t['title'])}</b>\n"
               f"⏱ {_dur(t['duration'])}   ·   🔊 {s['volume']}%   ·   {'📺 video' if (t.get('video') and s['video_on']) else '🎧 audio only'}\n"
               f"{state}\n"
               f"{who}\n\n"
               f"⏭ Next: {_esc(nxt)}   ·   📜 {len(s['queue'])} in queue\n"
               f"➕ Send /play song name to add to the queue")


def _card_kb(chat_id):
    s = _st(chat_id)
    if s["paused"]:
        return {"inline_keyboard": [
            [_btn("▶ Resume", f"mu:resume:{chat_id}", "success"), _btn("⏹ Stop", f"mu:stop:{chat_id}", "danger")],
            [_btn("📜 Queue", f"mu:queue:{chat_id}")]]}
    return {"inline_keyboard": [
        [_btn("⏮ Prev", f"mu:prev:{chat_id}"), _btn("⏸ Pause", f"mu:pause:{chat_id}"), _btn("⏭ Next", f"mu:skip:{chat_id}")],
        [_btn("🔉", f"mu:vdown:{chat_id}"), _btn("🔊", f"mu:vup:{chat_id}"), _btn("📜 Queue", f"mu:queue:{chat_id}"), _btn("⏹ Stop", f"mu:stop:{chat_id}", "danger")],
        [_btn("🎧 Audio only" if s["video_on"] else "📺 Video on", f"mu:{'voff' if s['video_on'] else 'von'}:{chat_id}")]]}


def _remember(chat_id, mid):
    if mid:
        _st(chat_id)["msgs"] = (_st(chat_id)["msgs"] + [int(mid)])[-200:]


def _say(chat_id, text):
    """A music reply in the group, tracked so it can be swept away later."""
    j = bot_api("sendMessage", {"chat_id": chat_id, "text": _pe(text), "parse_mode": "HTML"}, timeout=10)
    if j.get("ok"):
        _remember(chat_id, j["result"]["message_id"])
    return bool(j.get("ok"))


def _sweep(chat_id, keep_card=False):
    """Delete every music message this chat has - replies, the users'
    /play lines and the card - once the music is over."""
    s = _st(chat_id)
    if s["card"] and not keep_card:
        bot_api("unpinChatMessage", {"chat_id": chat_id, "message_id": s["card"][1]}, timeout=8)
        bot_api("deleteMessage", {"chat_id": chat_id, "message_id": s["card"][1]}, timeout=8)
        s["card"] = None
    for mid in s["msgs"]:
        bot_api("deleteMessage", {"chat_id": chat_id, "message_id": mid}, timeout=8)
    s["msgs"] = []


def _post_card(chat_id):
    """A fresh now-playing card (the old one is removed so the card always
    sits at the bottom of the chat when a new song starts)."""
    s = _st(chat_id)
    if s["card"]:
        bot_api("unpinChatMessage", {"chat_id": chat_id, "message_id": s["card"][1]}, timeout=8)
        bot_api("deleteMessage", {"chat_id": chat_id, "message_id": s["card"][1]}, timeout=8)
        s["card"] = None
    t = s["now"]
    j = {}
    if t and t.get("thumb"):
        j = bot_api("sendPhoto", {"chat_id": chat_id, "photo": t["thumb"], "caption": _card_text(chat_id),
                                  "parse_mode": "HTML", "reply_markup": _card_kb(chat_id)})
        if j.get("ok"):
            s["card"] = (chat_id, j["result"]["message_id"], "photo")
    if not s["card"]:
        j = bot_api("sendMessage", {"chat_id": chat_id, "text": _card_text(chat_id), "parse_mode": "HTML", "reply_markup": _card_kb(chat_id)})
        if j.get("ok"):
            s["card"] = (chat_id, j["result"]["message_id"], "text")
    if s["card"]:
        # pinned so the controls stay one tap away; needs the bot's "pin
        # messages" right, silently skipped otherwise
        bot_api("pinChatMessage", {"chat_id": chat_id, "message_id": s["card"][1], "disable_notification": True}, timeout=8)


def _refresh_card(chat_id):
    """Edit the card in place after every button / command; if the edit is
    refused (old message, deleted), post a fresh card instead."""
    s = _st(chat_id)
    if not s["card"]:
        if s["now"]:
            _post_card(chat_id)
        return
    if s["card"][2] == "photo":
        j = bot_api("editMessageCaption", {"chat_id": chat_id, "message_id": s["card"][1], "caption": _card_text(chat_id),
                                           "parse_mode": "HTML", "reply_markup": _card_kb(chat_id)})
    else:
        j = bot_api("editMessageText", {"chat_id": chat_id, "message_id": s["card"][1], "text": _card_text(chat_id),
                                        "parse_mode": "HTML", "reply_markup": _card_kb(chat_id)})
    if not j.get("ok") and "not modified" not in str(j.get("description", "")):
        print(f"[MUSIC] card edit {chat_id}: {j.get('description')}")
        s["card"] = None
        if s["now"]:
            _post_card(chat_id)


async def _start(chat_id, track):
    s = _st(chat_id)
    if s["now"] and (not s["history"] or s["history"][-1] is not s["now"]):
        s["history"] = (s["history"] + [s["now"]])[-20:]
    if track.get("id"):
        s["played"] = (s["played"] + [track["id"]])[-40:]
    if s["now"] is not track and s["now"] not in s["queue"]:
        _drop_track(s["now"])                       # Prev keeps it in the queue, so keep its file too
    if s.get("up_next") is track:
        s["up_next"] = None
    s["now"] = track; s["paused"] = False; s["offset"] = 0
    _drop_file(s)
    if track.get("local") and not os.path.exists(track["local"]):
        track["local"] = None                       # a restart wiped the temp dir
    await _fetch(track)
    f = None
    if s["volume"] != 100 and not track.get("live"):
        f = await asyncio.to_thread(_encode, track, 0, s["volume"], s["video_on"])
        s["file"] = f
    await call.play(chat_id, _stream(track, f, s["video_on"]))
    s["_pos"] = 0
    await asyncio.to_thread(_post_card, chat_id)
    await asyncio.to_thread(_save_state)
    asyncio.create_task(_prefetch(chat_id))


async def _set_video(chat_id, on: bool):
    """Video on/off while playing: re-stream from the current position."""
    s = _st(chat_id); t = s["now"]
    s["video_on"] = on
    if not t:
        return
    if t.get("live"):
        await call.play(chat_id, _stream(t, None, on)); return
    try:
        pos = await call.time(chat_id)
    except Exception:
        pos = 0
    start = s["offset"] + (pos or 0)
    f = await asyncio.to_thread(_encode, t, start, s["volume"], on)
    old = s.get("file"); s["file"] = f; s["offset"] = start
    await call.play(chat_id, _stream(t, f, on))
    if s["paused"]:
        try:
            await call.pause(chat_id)
        except Exception:
            pass
    if old:
        try:
            os.remove(old)
        except Exception:
            pass


async def _set_volume(chat_id, volume):
    """Re-encode from the current position with the new gain and swap the
    stream; the listener hears a short gap, then the same song at the new
    level. Falls back to Telegram's participant volume when it is allowed."""
    s = _st(chat_id); t = s["now"]
    s["volume"] = volume
    if not t:
        return
    if t.get("live"):
        try:
            await call.change_volume_call(chat_id, volume)
        except Exception:
            pass
        return
    try:
        pos = await call.time(chat_id)
    except Exception:
        pos = 0
    start = s["offset"] + (pos or 0)
    try:
        f = await asyncio.to_thread(_encode, t, start, volume, s["video_on"])
    except Exception as e:
        print(f"[MUSIC] volume encode {chat_id}: {e}")
        try:
            await call.change_volume_call(chat_id, volume)
        except Exception:
            pass
        return
    old = s.get("file")
    s["file"] = f; s["offset"] = start
    await call.play(chat_id, _stream(t, f, s["video_on"]))
    if s["paused"]:
        try:
            await call.pause(chat_id)
        except Exception:
            pass
    if old:
        try:
            os.remove(old)
        except Exception:
            pass


async def _next(chat_id):
    s = _st(chat_id)
    _drop_file(s)
    if s["queue"]:
        await _start(chat_id, s["queue"].pop(0))
        return True
    if s["now"]:
        # nothing queued: keep the room going with a song like the last one
        up = s.get("up_next")
        if up and up.get("after") == s["now"].get("id") and (up.get("local") or up.get("live")):
            await _start(chat_id, up)
            return True
        seed = next((t for t in [s["now"]] + s["history"][::-1] if t.get("by") != "autoplay"), s["now"])
        rel = await asyncio.to_thread(_related, seed if seed.get("id") else s["now"], set(s["played"]))
        if rel:
            await _start(chat_id, rel)
            return True
    _drop_all(s)
    s["now"] = None
    try:
        await call.leave_call(chat_id)
    except Exception:
        pass
    await asyncio.to_thread(_save_state)
    await asyncio.to_thread(_sweep, chat_id)
    return False


async def _on_end(_, update: StreamEnded):
    async with _lock:
        await _next(update.chat_id)


async def _on_chat_update(_, update: ChatUpdate):
    """The voice chat was closed, or the assistant was kicked / removed."""
    chat_id = update.chat_id
    async with _lock:
        s = _state.get(chat_id)
        if not s or not s["now"]:
            return
        why = ("voice chat ended" if update.status & ChatUpdate.Status.CLOSED_VOICE_CHAT
               else "kicked" if update.status & ChatUpdate.Status.KICKED
               else "left the group" if update.status & ChatUpdate.Status.LEFT_GROUP else "removed from the call")
        print(f"[MUSIC] {chat_id}: {why} - cleaning up")
        s["queue"].clear(); _drop_all(s); s["now"] = None; s["paused"] = False
        await asyncio.to_thread(_sweep, chat_id)
        try:
            await call.leave_call(chat_id)
        except Exception:
            pass
        await asyncio.to_thread(_save_state)


async def _ensure_member(chat_id, invite_link):
    """The assistant must be in the group before it can join the call."""
    try:
        m = await client.get_chat_member(chat_id, "me")
        status = str(getattr(m, "status", "")).lower()
        if "banned" in status or "kicked" in status:
            return False, f"@{_me['username']} was removed from this group - an admin has to unban it (Group → Removed users) and add it back."
        if "left" not in status:
            return True, ""
    except RPCError:
        pass
    if not invite_link:
        return False, f"Add @{_me['username']} to this group first (or give me the invite-users right)."
    try:
        await client.join_chat(invite_link)
        return True, ""
    except RPCError as e:
        name = type(e).__name__
        if "Banned" in name or "Kicked" in name or "USER_BANNED" in str(e) or "KICKED" in str(e):
            return False, f"@{_me['username']} was removed from this group - an admin has to unban it (Group → Removed users) and add it back."
        return False, f"I could not join the group: {getattr(e, 'MESSAGE', e)}"


# ── HTTP ───────────────────────────────────────────────────────────────────
def _auth(secret):
    if not SECRET or secret != SECRET:
        raise HTTPException(403, "bad secret")


@app.get("/health")
async def health():
    return {"ok": True, "assistant": _me["username"], "chats": len([c for c, s in _state.items() if s["now"]]),
            "cookies": _cookie_info, "last_error": _last_error["text"][-300:], "video_height": VIDEO_H}


@app.post("/play")
async def play(req: Request, x_music_secret: str = Header(default="")):
    _auth(x_music_secret)
    body = await req.json()
    chat_id, query = int(body["chat_id"]), str(body.get("query", "")).strip()
    by = str(body.get("by", "someone"))
    for mid in (body.get("msg_ids") or []):
        _remember(chat_id, mid)                     # the user's /play line and the bot's "Searching…"
    def reply(text):
        _say(chat_id, text)
        return {"text": "", "sent": True}
    if not query:
        return reply("Usage: /play song name (or a YouTube link)")
    try:
        track = await asyncio.to_thread(_lookup, query)
    except Exception as e:
        print(f"[MUSIC] search {query!r}: {e}")
        _last_error["text"] = str(e); track = None
    if not track:
        err = _last_error["text"].lower()
        if "netscape" in err or "cookie" in err and ("parse" in err or "load" in err or "format" in err):
            return reply("⚠️ The YouTube cookies file on the server is broken. Tell the admin - /music shows what's wrong.")
        if "sign in" in err or "not a bot" in err or "cookies" in err:
            return reply("⚠️ YouTube is blocking this server right now (it wants a login). Tell the admin - "
                         + ("the cookies file is set but YouTube isn't accepting it - export a fresh one." if _cookie_info["loaded"] else "a cookies file fixes it."))
        if err:
            return reply("⚠️ YouTube didn't give me that song - try again in a moment, or paste a YouTube link.")
        return reply("🔍 Couldn't find that - try another name or paste a YouTube link.")
    track["by"] = by
    ok, why = await _ensure_member(chat_id, body.get("invite_link"))
    if not ok:
        return reply(f"⚠️ {why}")
    async with _lock:
        s = _st(chat_id)
        if s["now"]:
            if len(s["queue"]) >= MAX_QUEUE:
                return reply(f"📜 Queue is full ({MAX_QUEUE}).")
            s["queue"].append(track)
            if len(s["queue"]) == 1:
                asyncio.create_task(_fetch(track))
            await asyncio.to_thread(_refresh_card, chat_id)
            return reply(f"➕ Queued #{len(s['queue'])}: <b>{_esc(track['title'])}</b> ({_dur(track['duration'])})")
        try:
            await _start(chat_id, track)
        except NoActiveGroupCall:
            s["now"] = None
            return reply("🎙 Start a voice chat in this group first, then send /play again.")
        except Exception as e:
            s["now"] = None
            print(f"[MUSIC] play {chat_id}: {e!r}")
            return reply("⚠️ Couldn't join the voice chat - is it running, and am I allowed in?")
    print(f"[MUSIC] playing {track['title'][:50]!r} in {chat_id} video={bool(track.get('video'))}")
    return {"text": "", "started": True}


@app.post("/control")
async def control(req: Request, x_music_secret: str = Header(default="")):
    _auth(x_music_secret)
    body = await req.json()
    chat_id, act = int(body["chat_id"]), str(body.get("action", ""))
    for mid in (body.get("msg_ids") or []):
        _remember(chat_id, mid)
    res = await _control(chat_id, act, body)
    if act == "stop" and body.get("msg_ids"):
        return {"text": "", "sent": True}           # everything was just swept - leave nothing behind
    if body.get("msg_ids") and res.get("text"):
        # typed command: answer in the group ourselves, tracked for the sweep
        _say(chat_id, res["text"])
        return {"text": "", "sent": True}
    return res


async def _control(chat_id, act, body):
    async with _lock:
        s = _st(chat_id)
        if not s["now"] and act not in ("queue",):
            return {"text": "⏹ Nothing is playing."}
        try:
            if act == "pause":
                await call.pause(chat_id); s["paused"] = True; await asyncio.to_thread(_refresh_card, chat_id); return {"text": "⏸ Paused."}
            if act == "resume":
                await call.resume(chat_id); s["paused"] = False; await asyncio.to_thread(_refresh_card, chat_id); return {"text": "▶ Resumed."}
            if act == "skip":
                had = await _next(chat_id)
                return {"text": "⏭ Next song." if had else "⏭ Nothing similar found - queue is empty, left the voice chat."}
            if act in ("von", "voff", "video"):
                on = (act == "von") if act != "video" else str(body.get("value", "")).lower() in ("r", "on", "resume", "start", "1")
                if on == s["video_on"]:
                    return {"text": "📺 Video is already on." if on else "🎧 Already audio only."}
                await _set_video(chat_id, on)
                await asyncio.to_thread(_refresh_card, chat_id)
                return {"text": "📺 Video on." if on else "🎧 Audio only - video stopped."}
            if act == "prev":
                if not s["history"]:
                    return {"text": "⏮ No previous song."}
                prev = s["history"].pop()
                s["queue"].insert(0, s["now"])          # Next brings the current one back
                _drop_file(s)
                await _start(chat_id, prev)
                return {"text": "⏮ Previous song."}

            if act == "stop":
                s["queue"].clear(); _drop_all(s); s["now"] = None
                await _next(chat_id); return {"text": "⏹ Stopped and left the voice chat."}
            if act in ("vup", "vdown", "volume"):
                v = int(body.get("value") or (s["volume"] + (20 if act == "vup" else -20)))
                v = max(10, min(200, v))
                if v == s["volume"]:
                    return {"text": f"🔊 Volume already {v}%"}
                await _set_volume(chat_id, v)
                await asyncio.to_thread(_refresh_card, chat_id)
                return {"text": f"🔊 Volume {s['volume']}%"}
            if act == "queue":
                if not s["now"]:
                    return {"text": "📜 Queue is empty."}
                lines = [f"▶ <b>{_esc(s['now']['title'])}</b> ({_dur(s['now']['duration'])})"]
                lines += [f"{i + 1}. {_esc(t['title'])} ({_dur(t['duration'])}) · {_esc(t['by'])}" for i, t in enumerate(s["queue"][:15])]
                if len(s["queue"]) > 15:
                    lines.append(f"… and {len(s['queue']) - 15} more")
                return {"text": "📜 <b>Queue</b>\n\n" + "\n".join(lines)}
            if act == "now":
                return {"text": _card_text(chat_id)}
        except NotInCallError:
            s["now"] = None; s["queue"].clear()
            return {"text": "⏹ I'm not in the voice chat any more - send /play to start again."}
    return {"text": "Unknown action."}


async def main():
    """One asyncio loop for Pyrogram, PyTgCalls and the HTTP server - a client
    built on a different loop than the one serving requests fails at start
    ("attached to a different loop")."""
    global client, call
    import uvicorn, glob
    for f in glob.glob(os.path.join(tempfile.gettempdir(), "clx_*")):
        try:
            os.remove(f)
        except Exception:
            pass
    client = Client("clexer-music", api_id=API_ID, api_hash=API_HASH, session_string=SESSION, in_memory=True)
    call = PyTgCalls(client)
    call.on_update(fl.stream_end())(_on_end)
    call.on_update(fl.chat_update(ChatUpdate.Status.LEFT_CALL))(_on_chat_update)
    # One session, one place. If Telegram reports the key as duplicated or
    # revoked the string has to be regenerated with music/login.py.
    try:
        await client.start()
    except Exception as e:
        name = type(e).__name__
        if "AuthKey" in name or "SessionRevoked" in name or "Unauthorized" in name:
            print(f"[MUSIC] SESSION DEAD ({name}): the string was used from two places at once or was logged out. "
                  f"Run music/login.py again and replace MUSIC_SESSION_STRING.")
        raise
    me = await client.get_me()
    _me.update(id=me.id, username=me.username or "")
    await call.start()
    print(f"[MUSIC] assistant @{_me['username']} ready")
    try:
        await _resume_all()
    except Exception as e:
        print(f"[MUSIC] resume: {e!r}")
    asyncio.create_task(_track_positions())
    asyncio.create_task(_card_watch())
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), loop="none", lifespan="off"))
    try:
        await server.serve()
    finally:
        # going down (redeploy / restart): remember where every chat was so the
        # next process can carry on, and say so on the cards
        try:
            await asyncio.to_thread(_save_state)
            for chat_id, st in list(_state.items()):
                if st["now"] and st["card"]:
                    _card_note(st["card"], "🔄 Restarting - the music continues in a moment…")
        except Exception:
            pass
        try:
            await client.stop()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
