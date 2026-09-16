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
        "default_search": "ytsearch1", "skip_download": True, "extract_flat": False,
        "extractor_args": {"youtube": {"player_client": ["android", "web"]}}}


def _lookup(query: str) -> Optional[dict]:
    """Title / duration / direct audio url for a search or a YouTube link.
    Search is done flat (titles + durations only, fast); the full extraction
    runs once, on the first result that is a plausible song."""
    q = query.strip()
    if q.startswith(("http://", "https://")):
        cands = [q]
    else:
        with YoutubeDL({**_YDL, "extract_flat": "in_playlist"}) as y:
            info = y.extract_info(f"ytsearch6:{q} song", download=False)
        cands = []
        for e in (info or {}).get("entries") or []:
            dur = (e or {}).get("duration") or 0
            if dur and (dur > MAX_SECONDS or dur < 30):
                continue                   # hour-long mixes and shorts
            u = e.get("url") or e.get("webpage_url") or (e.get("id") and f"https://www.youtube.com/watch?v={e['id']}")
            if u:
                cands.append(u)
    for u in cands[:3]:
        try:
            with YoutubeDL(_YDL) as y:
                e = y.extract_info(u, download=False)
        except Exception as ex:
            print(f"[MUSIC] extract {u}: {ex}")
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
    await asyncio.to_thread(_post_card, chat_id)


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
    track = await asyncio.to_thread(_lookup, query)
    if not track:
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
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), loop="none", lifespan="off"))
    try:
        await server.serve()
    finally:
        try:
            await client.stop()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
