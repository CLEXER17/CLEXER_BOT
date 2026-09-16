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
from pytgcalls.types import AudioQuality, MediaStream, StreamEnded
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
    return f"{sec // 60}:{sec % 60:02d}"


# ── YouTube lookup ─────────────────────────────────────────────────────────
_YDL = {"format": "bestaudio[ext=m4a]/bestaudio/best", "noplaylist": True, "quiet": True, "no_warnings": True,
        "default_search": "ytsearch1", "skip_download": True, "extract_flat": False}
# YouTube sometimes refuses datacenter IPs ("Sign in to confirm you're not a
# bot"). MUSIC_COOKIES = the text of a Netscape cookies.txt exported from a
# logged-in browser; it is written to disk once and handed to yt-dlp.
_COOKIES = os.getenv("MUSIC_COOKIES", "")
if _COOKIES.strip():
    _cp = os.path.join(tempfile.gettempdir(), "clexer_yt_cookies.txt")
    with open(_cp, "w", encoding="utf-8") as _f:
        _f.write(_COOKIES.replace(chr(92) + "n", chr(10)) if (chr(92) + "n") in _COOKIES else _COOKIES)
    _YDL["cookiefile"] = _cp
_last_error = {"text": ""}


def _flat_search(url, q=None):
    """Titles/ids only (fast). Returns candidate watch URLs, best first.
    `url` is a yt-dlp search prefix ("ytsearch6:") or a full search page."""
    with YoutubeDL({**_YDL, "extract_flat": "in_playlist", "playlistend": 6}) as y:
        info = y.extract_info(url + (q or ""), download=False)
    out = []
    for e in (info or {}).get("entries") or []:
        if not e:
            continue
        dur = e.get("duration") or 0
        if dur and (dur > MAX_SECONDS or dur < 30):
            continue                       # hour-long mixes and shorts
        vid = e.get("id")
        u = e.get("url") or e.get("webpage_url") or (vid and f"https://www.youtube.com/watch?v={vid}")
        if u and "watch" in u or (u and len(u) == 11):
            out.append(u if u.startswith("http") else f"https://www.youtube.com/watch?v={u}")
    return out


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
        if not e or not e.get("url"):
            continue
        dur = e.get("duration") or 0
        if dur and (dur > MAX_SECONDS or dur < 30):
            continue
        return {"title": e.get("title") or q, "duration": dur, "url": e["url"],
                "page": e.get("webpage_url") or "", "thumb": e.get("thumbnail") or "",
                "uploader": e.get("uploader") or e.get("channel") or ""}
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
                             "card": st["card"], "position": st["offset"] + pos, "saved": time.time()}
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
        s.update(queue=st.get("queue", []), volume=int(st.get("volume", 100)), paused=False, card=tuple(card) if card else None)
        track = st["now"]
        start = float(st.get("position", 0))
        try:
            f = await asyncio.to_thread(_encode, track["url"], start, s["volume"])
            s["now"] = track; s["file"] = f; s["offset"] = start
            await call.play(chat_id, MediaStream(f, audio_parameters=AudioQuality.HIGH, video_flags=MediaStream.Flags.IGNORE))
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
                                       "offset": 0, "file": None})


def _drop_file(s):
    f = s.get("file")
    s["file"] = None
    if f:
        try:
            os.remove(f)
        except Exception:
            pass


def _encode(src, start, volume):
    """The rest of the song from `start` seconds at `volume`% into a temp file.
    Volume cannot be set through Telegram for a plain member of the call, so it
    is baked into the audio instead."""
    out = tempfile.NamedTemporaryFile(prefix="clx_", suffix=".ogg", delete=False).name
    cmd = ["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{max(0, start):.1f}", "-i", src, "-vn",
           "-af", f"volume={max(0.05, volume / 100):.2f}", "-c:a", "libopus", "-b:a", "128k", "-ar", "48000", out]
    subprocess.run(cmd, check=True, timeout=180)
    return out


def _card_text(chat_id):
    s = _st(chat_id); t = s["now"]
    if not t:
        return _pe("⏹ Nothing playing.")
    nxt = s["queue"][0]["title"] if s["queue"] else "—"
    head = "⏸ <b>Paused</b>" if s["paused"] else "🎵 <b>Now playing</b>"
    state = "⏸ paused - press Resume to continue" if s["paused"] else "▶ playing"
    return _pe(f"{head}\n\n"
               f"<b>{_esc(t['title'])}</b>\n"
               f"⏱ {_dur(t['duration'])}   ·   🔊 {s['volume']}%\n"
               f"{state}\n"
               f"👤 Requested by {_esc(t['by'])}\n\n"
               f"⏭ Next: {_esc(nxt)}   ·   📜 {len(s['queue'])} in queue")


def _card_kb(chat_id):
    s = _st(chat_id)
    if s["paused"]:
        return {"inline_keyboard": [
            [_btn("▶ Resume", f"mu:resume:{chat_id}", "success"), _btn("⏹ Stop", f"mu:stop:{chat_id}", "danger")],
            [_btn("📜 Queue", f"mu:queue:{chat_id}")]]}
    return {"inline_keyboard": [
        [_btn("⏸ Pause", f"mu:pause:{chat_id}"), _btn("⏭ Skip", f"mu:skip:{chat_id}"), _btn("⏹ Stop", f"mu:stop:{chat_id}", "danger")],
        [_btn("🔉", f"mu:vdown:{chat_id}"), _btn("🔊", f"mu:vup:{chat_id}"), _btn("📜 Queue", f"mu:queue:{chat_id}")]]}


def _post_card(chat_id):
    """A fresh now-playing card (the old one is removed so the card always
    sits at the bottom of the chat when a new song starts)."""
    s = _st(chat_id)
    if s["card"]:
        bot_api("deleteMessage", {"chat_id": chat_id, "message_id": s["card"][1]}, timeout=8)
        s["card"] = None
    t = s["now"]
    if t and t.get("thumb"):
        j = bot_api("sendPhoto", {"chat_id": chat_id, "photo": t["thumb"], "caption": _card_text(chat_id),
                                  "parse_mode": "HTML", "reply_markup": _card_kb(chat_id)})
        if j.get("ok"):
            s["card"] = (chat_id, j["result"]["message_id"], "photo"); return
    j = bot_api("sendMessage", {"chat_id": chat_id, "text": _card_text(chat_id), "parse_mode": "HTML", "reply_markup": _card_kb(chat_id)})
    if j.get("ok"):
        s["card"] = (chat_id, j["result"]["message_id"], "text")


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
    s["now"] = track; s["paused"] = False; s["offset"] = 0
    _drop_file(s)
    src = track["url"]
    if s["volume"] != 100:
        src = await asyncio.to_thread(_encode, track["url"], 0, s["volume"])
        s["file"] = src
    await call.play(chat_id, MediaStream(src, audio_parameters=AudioQuality.HIGH, video_flags=MediaStream.Flags.IGNORE))
    s["_pos"] = 0
    await asyncio.to_thread(_post_card, chat_id)
    await asyncio.to_thread(_save_state)


async def _set_volume(chat_id, volume):
    """Re-encode from the current position with the new gain and swap the
    stream; the listener hears a short gap, then the same song at the new
    level. Falls back to Telegram's participant volume when it is allowed."""
    s = _st(chat_id); t = s["now"]
    s["volume"] = volume
    if not t:
        return
    try:
        pos = await call.time(chat_id)
    except Exception:
        pos = 0
    start = s["offset"] + (pos or 0)
    try:
        f = await asyncio.to_thread(_encode, t["url"], start, volume)
    except Exception as e:
        print(f"[MUSIC] volume encode {chat_id}: {e}")
        try:
            await call.change_volume_call(chat_id, volume)
        except Exception:
            pass
        return
    old = s.get("file")
    s["file"] = f; s["offset"] = start
    await call.play(chat_id, MediaStream(f, audio_parameters=AudioQuality.HIGH, video_flags=MediaStream.Flags.IGNORE))
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
    s["now"] = None
    try:
        await call.leave_call(chat_id)
    except Exception:
        pass
    await asyncio.to_thread(_save_state)
    if s["card"]:
        done = _pe("⏹ Queue finished - left the voice chat.")
        if s["card"][2] == "photo":
            bot_api("editMessageCaption", {"chat_id": chat_id, "message_id": s["card"][1], "caption": done, "parse_mode": "HTML"})
        else:
            bot_api("editMessageText", {"chat_id": chat_id, "message_id": s["card"][1], "text": done, "parse_mode": "HTML"})
        s["card"] = None
    return False


async def _on_end(_, update: StreamEnded):
    async with _lock:
        await _next(update.chat_id)


async def _ensure_member(chat_id, invite_link):
    """The assistant must be in the group before it can join the call."""
    try:
        await client.get_chat_member(chat_id, "me")
        return True, ""
    except RPCError:
        pass
    if not invite_link:
        return False, f"Add @{_me['username']} to this group first (or give me the invite-users right)."
    try:
        await client.join_chat(invite_link)
        return True, ""
    except RPCError as e:
        return False, f"I could not join the group: {e.MESSAGE if hasattr(e, 'MESSAGE') else e}"


# ── HTTP ───────────────────────────────────────────────────────────────────
def _auth(secret):
    if not SECRET or secret != SECRET:
        raise HTTPException(403, "bad secret")


@app.get("/health")
async def health():
    return {"ok": True, "assistant": _me["username"], "chats": len([c for c, s in _state.items() if s["now"]])}


@app.post("/play")
async def play(req: Request, x_music_secret: str = Header(default="")):
    _auth(x_music_secret)
    body = await req.json()
    chat_id, query = int(body["chat_id"]), str(body.get("query", "")).strip()
    by = str(body.get("by", "someone"))
    if not query:
        return {"text": "Usage: /play song name (or a YouTube link)"}
    try:
        track = await asyncio.to_thread(_lookup, query)
    except Exception as e:
        print(f"[MUSIC] search {query!r}: {e}")
        _last_error["text"] = str(e); track = None
    if not track:
        err = _last_error["text"].lower()
        if "sign in" in err or "not a bot" in err or "cookies" in err:
            return {"text": "⚠️ YouTube is blocking this server right now (it wants a login). Tell the admin - a cookies file fixes it."}
        if err:
            return {"text": "⚠️ YouTube didn't give me that song - try again in a moment, or paste a YouTube link."}
        return {"text": "🔍 Couldn't find that - try another name or paste a YouTube link."}
    track["by"] = by
    ok, why = await _ensure_member(chat_id, body.get("invite_link"))
    if not ok:
        return {"text": f"⚠️ {why}"}
    async with _lock:
        s = _st(chat_id)
        if s["now"]:
            if len(s["queue"]) >= MAX_QUEUE:
                return {"text": f"📜 Queue is full ({MAX_QUEUE})."}
            s["queue"].append(track)
            await asyncio.to_thread(_refresh_card, chat_id)
            return {"text": f"➕ Queued #{len(s['queue'])}: <b>{_esc(track['title'])}</b> ({_dur(track['duration'])})"}
        try:
            await _start(chat_id, track)
        except NoActiveGroupCall:
            s["now"] = None
            return {"text": "🎙 Start a voice chat in this group first, then send /play again."}
        except Exception as e:
            s["now"] = None
            print(f"[MUSIC] play {chat_id}: {e!r}")
            return {"text": "⚠️ Couldn't join the voice chat - is it running, and am I allowed in?"}
    return {"text": "", "started": True}


@app.post("/control")
async def control(req: Request, x_music_secret: str = Header(default="")):
    _auth(x_music_secret)
    body = await req.json()
    chat_id, act = int(body["chat_id"]), str(body.get("action", ""))
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
                if not s["queue"]:
                    return {"text": "📜 Nothing queued after this song - add one with /play, or press ⏹ Stop."}
                await _next(chat_id)
                return {"text": "⏭ Skipped."}
            if act == "stop":
                s["queue"].clear(); await _next(chat_id); return {"text": "⏹ Stopped and left the voice chat."}
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
    import uvicorn
    client = Client("clexer-music", api_id=API_ID, api_hash=API_HASH, session_string=SESSION, in_memory=True)
    call = PyTgCalls(client)
    call.on_update(fl.stream_end())(_on_end)
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
