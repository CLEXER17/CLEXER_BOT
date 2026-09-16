"""Group join requests and welcome cards (admin 2026-09-16).

Any group or channel where the bot is an admin with "invite users":

* a join request -> the requester gets one DM inside Telegram's 5-minute
  window (request received + Open Bot / Get VIP), and every human admin who
  can reach the bot gets an Approve / Decline card. One tap decides; all the
  cards update with who decided.
* an approved request, or a plain join where approval is off -> a welcome
  card (the member's photo in a gold ring, name, group, @username, id, date)
  posted in the group, with a text welcome whose name and username are
  clickable.

The VIP channel keeps its own auto-approve rule in bot.py; this module only
sees the chats bot.py hands it. Nothing is stored: open cards live in memory
and expire with the request.
"""
import html as _html
import io
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
from PIL import Image, ImageDraw, ImageFont, ImageOps

_TOKEN = ""
_BOT_USERNAME = lambda: ""
_ADMIN_ID = ""
_IS_VIP = lambda uid: False      # set by bot.py: does this user hold VIP?
IST = timedelta(hours=5, minutes=30)

# (chat_id, user_id) -> {"cards": [(admin_chat_id, message_id)], "name", "uname", "title", "at"}
_open: dict = {}
_lock = threading.Lock()
_fonts: dict = {}
_admins_cache: dict = {}      # chat_id -> (fetched_at, [admin user dicts])


def init(token: str, bot_username_getter, admin_id="", is_vip=None):
    global _TOKEN, _BOT_USERNAME, _ADMIN_ID, _IS_VIP
    _TOKEN, _BOT_USERNAME, _ADMIN_ID = token, bot_username_getter, str(admin_id or "")
    if is_vip:
        _IS_VIP = is_vip


def _api(method, payload=None, files=None, timeout=15):
    if not _TOKEN:
        return {}
    try:
        url = f"https://api.telegram.org/bot{_TOKEN}/{method}"
        if files:
            r = requests.post(url, data=payload, files=files, timeout=max(timeout, 30))
        else:
            r = requests.post(url, json=payload, timeout=timeout)
        return r.json()
    except Exception as e:
        print(f"  [JOIN] {method} failed: {e}")
        return {}


def _esc(s):
    return _html.escape(str(s or ""), quote=False)


def _dyn(s):
    return f"\u2063{s}\u2063"


def _mention(user):
    name = _esc(user.get("first_name") or user.get("username") or "member")
    return f'<a href="tg://user?id={user.get("id")}">{name}</a>'


def _stamp():
    return (datetime.now(timezone.utc) + IST).strftime("%H:%M · %d %b %Y")


# ── admins of a chat (cached 10 min) ────────────────────────────────────────
def _admins(chat_id):
    now = time.time()
    hit = _admins_cache.get(chat_id)
    if hit and now - hit[0] < 600:
        return hit[1]
    out = []
    j = _api("getChatAdministrators", {"chat_id": chat_id}, timeout=10)
    for a in (j.get("result") or []):
        u = a.get("user") or {}
        if u.get("is_bot"):
            continue
        if a.get("status") == "creator" or a.get("can_invite_users") or a.get("can_restrict_members"):
            out.append(u)
    _admins_cache[chat_id] = (now, out)
    return out


# ── join request ───────────────────────────────────────────────────────────
def on_join_request(jr: dict):
    """Groups only - channels keep bot.py's own VIP auto-approve."""
    chat, user = jr.get("chat") or {}, jr.get("from") or {}
    chat_id, uid = chat.get("id"), user.get("id")
    if not chat_id or not uid or chat.get("type") not in ("group", "supergroup"):
        return
    title = chat.get("title") or "the group"
    name = user.get("first_name") or user.get("username") or "there"
    uname = user.get("username")
    # 1. the requester - Telegram lets the bot DM them for 5 minutes
    bot_u = _BOT_USERNAME() or ""
    btns = []
    if bot_u:
        btns = [[{"text": "🎮 Games", "url": f"https://t.me/{bot_u}?start=games", "style": "primary"},
                 {"text": "🎵 Music", "url": f"https://t.me/{bot_u}?start=music", "style": "primary"}]]
    _api("sendMessage", {
        "chat_id": jr.get("user_chat_id") or uid, "parse_mode": "HTML",
        "text": (f"📩 <b>Request received</b>\n\n"
                 f"Hey {_dyn(_esc(name))} 👋\n"
                 f"Your request to join {_dyn('<b>' + _esc(title) + '</b>')} has been received - an admin will look at it shortly.\n\n"
                 f"Meanwhile, CLEXER is right here: live BTC and altcoin signals, copy trading and virtual trading."),
        **({"reply_markup": {"inline_keyboard": btns}} if btns else {})}, timeout=10)
    # 2. the admins - one card each, all edited together on a decision
    text = (f"👑 <b>ENTRY JOIN REQUEST</b>\n\n"
            f"<blockquote>👤 <b>User:</b> {_mention(user)}\n"
            f"💬 <b>Username:</b> {('@' + _esc(uname)) if uname else '-'}\n"
            f"🪪 <b>User ID:</b> <code>{uid}</code>\n"
            f"🏠 <b>Group:</b> {_esc(title)}\n"
            f"🕐 <b>Time:</b> {_stamp()}\n"
            f"✅ <b>Status:</b> pending decision</blockquote>\n\n"
            f"Awaiting admin approval…")
    kb = {"inline_keyboard": [[{"text": "👑 Approve", "callback_data": f"jn:a:{chat_id}:{uid}", "style": "success"},
                               {"text": "😡 Decline", "callback_data": f"jn:d:{chat_id}:{uid}", "style": "danger"}]]}
    # 2. the card goes into the group itself; only that group's admins can
    #    press the buttons (checked on tap)
    cards = []
    j = _api("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "reply_markup": kb}, timeout=10)
    if j.get("ok"):
        cards.append((chat_id, j["result"]["message_id"]))
    else:
        print(f"  [JOIN] card in {title} ({chat_id}) failed: {j.get('description')}")
    with _lock:
        _open[(str(chat_id), str(uid))] = {"cards": cards, "user": user, "title": title, "at": time.time(), "text": text}


def on_callback(data: str, admin_uid, admin_name: str) -> str:
    """jn:a|d:<chat>:<user> -> popup text."""
    try:
        _, act, chat_id, uid = data.split(":", 3)
    except ValueError:
        return "Bad button."
    with _lock:
        rec = _open.get((chat_id, uid))
    if not rec:
        return "This request was already handled or has expired."
    ok_admin = str(admin_uid) == _ADMIN_ID or any(str(a.get("id")) == str(admin_uid) for a in _admins(int(chat_id)))
    if not ok_admin:
        return "Only an admin of that group can decide."
    method = "approveChatJoinRequest" if act == "a" else "declineChatJoinRequest"
    j = _api(method, {"chat_id": int(chat_id), "user_id": int(uid)}, timeout=10)
    if not j.get("ok"):
        desc = str(j.get("description", ""))
        if "HIDE_REQUESTER_MISSING" in desc or "USER_ALREADY_PARTICIPANT" in desc:
            with _lock:
                _open.pop((chat_id, uid), None)
            return "That request is no longer open."
        return f"Telegram refused: {desc[:120] or 'unknown error'}"
    verdict = "approved" if act == "a" else "declined"
    final = rec["text"].replace("✅ <b>Status:</b> pending decision", f"{'✅' if act == 'a' else '❌'} <b>Status:</b> {verdict}") \
                       .replace("Awaiting admin approval…", f"{verdict.capitalize()} by {_esc(admin_name)} · {_stamp()}")
    for aid, mid in rec["cards"]:
        _api("editMessageText", {"chat_id": aid, "message_id": mid, "text": final, "parse_mode": "HTML"}, timeout=10)
    with _lock:
        _open.pop((chat_id, uid), None)
    if act == "a":
        _api("sendMessage", {"chat_id": int(uid), "parse_mode": "HTML",
                             "text": f"✅ <b>Approved</b> - welcome to {_dyn(_esc(rec['title']))}! Say hi in the group."}, timeout=8)
        threading.Thread(target=welcome, args=(int(chat_id), rec["user"], rec["title"]), daemon=True).start()
    else:
        _api("sendMessage", {"chat_id": int(uid), "text": f"❌ Your request to join {_dyn(_esc(rec['title']))} was declined."}, timeout=8)
    return "Approved." if act == "a" else "Declined."


def on_new_members(msg: dict):
    """A plain join (approval off) or an admin adding someone."""
    chat = msg.get("chat") or {}
    if chat.get("type") not in ("group", "supergroup"):
        return
    for u in msg.get("new_chat_members") or []:
        if u.get("is_bot"):
            continue
        threading.Thread(target=welcome, args=(chat.get("id"), u, chat.get("title") or "the group"), daemon=True).start()


def on_left_member(msg: dict):
    """Someone left (or was removed) - the goodbye twin of the welcome card."""
    chat = msg.get("chat") or {}
    u = msg.get("left_chat_member") or {}
    if chat.get("type") not in ("group", "supergroup") or not u or u.get("is_bot"):
        return
    threading.Thread(target=goodbye, args=(chat.get("id"), u, chat.get("title") or "the group"), daemon=True).start()


def goodbye(chat_id, user: dict, title: str):
    try:
        data = render_card(user, title, left=True)
    except Exception as e:
        print(f"  [JOIN] leave card render: {e}")
        data = None
    uname = user.get("username")
    caption = (f"👋 {_mention(user)} left <b>{_esc(title)}</b>" + chr(10) + chr(10)
               + "<blockquote>📄 <b>Their info</b>" + chr(10)
               + f"👤 Name » {_mention(user)}" + chr(10)
               + f"💬 Username » {('@' + _esc(uname)) if uname else '-'}" + chr(10)
               + f"🪪 ID » <code>{user.get('id')}</code></blockquote>" + chr(10) + chr(10)
               + "The door stays open - see you around. 🤝")
    if data:
        j = _api("sendPhoto", {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
                 files={"photo": ("goodbye.jpg", data, "image/jpeg")})
        if j.get("ok"):
            return
    _api("sendMessage", {"chat_id": chat_id, "text": caption, "parse_mode": "HTML"}, timeout=10)


def expire(max_age=48 * 3600):
    """Drop cards nobody decided on (join requests stay open on Telegram's side)."""
    now = time.time()
    with _lock:
        for k in [k for k, v in _open.items() if now - v["at"] > max_age]:
            _open.pop(k, None)


# ── welcome card ───────────────────────────────────────────────────────────
S = 2   # render at 2x, downsample - crisp text like the game boards


def _font(size, bold=True):
    key = (size, bold)
    if key in _fonts:
        return _fonts[key]
    cands = []
    try:
        import matplotlib
        cands.append(os.path.join(matplotlib.get_data_path(), "fonts", "ttf", "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"))
    except Exception:
        pass
    cands += ["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
              "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf"]
    f = None
    for p in cands:
        try:
            f = ImageFont.truetype(p, size * S); break
        except Exception:
            continue
    if f is None:
        f = ImageFont.load_default()
    _fonts[key] = f
    return f


def _avatar(uid, size):
    """The member's profile photo as a circle, or a lettered disc."""
    img = None
    try:
        j = _api("getUserProfilePhotos", {"user_id": uid, "limit": 1}, timeout=10)
        photos = (j.get("result") or {}).get("photos") or []
        if photos:
            fid = photos[0][-1]["file_id"]
            f = _api("getFile", {"file_id": fid}, timeout=10)
            path = (f.get("result") or {}).get("file_path")
            if path:
                r = requests.get(f"https://api.telegram.org/file/bot{_TOKEN}/{path}", timeout=15)
                img = Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception as e:
        print(f"  [JOIN] avatar {uid}: {e}")
    if img is None:
        return None
    img = ImageOps.fit(img, (size, size), Image.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(img, (0, 0), mask)
    return out


def render_card(user: dict, title: str, left: bool = False) -> bytes:
    W, H = 1000 * S, 520 * S
    NAVY, PANEL, GOLD, GOLD2, IVORY, MUTED, JADE = (10, 13, 20), (17, 24, 39), (201, 169, 106), (233, 207, 151), (242, 236, 223), (154, 162, 182), (87, 185, 142)
    im = Image.new("RGB", (W, H), NAVY)
    d = ImageDraw.Draw(im)
    # soft vignette panel
    d.rounded_rectangle((28 * S, 28 * S, W - 28 * S, H - 28 * S), radius=26 * S, fill=PANEL, outline=(201, 169, 106, 90), width=2)
    # gold corner ticks (the site's frame motif)
    for (x, y, dx, dy) in ((44 * S, 44 * S, 1, 1), (W - 44 * S, 44 * S, -1, 1), (44 * S, H - 44 * S, 1, -1), (W - 44 * S, H - 44 * S, -1, -1)):
        d.line((x, y, x + 26 * S * dx, y), fill=GOLD, width=3 * S)
        d.line((x, y, x, y + 26 * S * dy), fill=GOLD, width=3 * S)
    # avatar ring
    cx, cy, R = 190 * S, H // 2, 118 * S
    d.ellipse((cx - R - 10 * S, cy - R - 10 * S, cx + R + 10 * S, cy + R + 10 * S), outline=(192, 91, 108) if left else GOLD, width=4 * S)
    av = _avatar(user.get("id"), 2 * R)
    if av is not None:
        im.paste(av, (cx - R, cy - R), av)
    else:
        d.ellipse((cx - R, cy - R, cx + R, cy + R), fill=(30, 39, 64))
        letter = (user.get("first_name") or "?")[:1].upper()
        f = _font(96)
        tw = d.textlength(letter, font=f)
        d.text((cx - tw / 2, cy - 62 * S), letter, font=f, fill=GOLD2)
    # text block
    x0 = 360 * S
    CRIMSON = (192, 91, 108)
    d.text((x0, 86 * S), "LEFT THE COMMUNITY" if left else "WELCOME TO THE COMMUNITY", font=_font(15), fill=CRIMSON if left else GOLD)
    name = (user.get("first_name") or "") + (" " + user["last_name"] if user.get("last_name") else "")
    name = name.strip() or (user.get("username") or "New member")
    f = _font(44)
    while d.textlength(name, font=f) > (W - x0 - 60 * S) and len(name) > 6:
        name = name[:-2] + "…"
    d.text((x0, 118 * S), name, font=f, fill=IVORY)
    d.text((x0, 178 * S), f"GROUP: {title[:40].upper()}", font=_font(15), fill=MUTED)
    # info box
    bx, by, bw, bh = x0, 222 * S, W - x0 - 60 * S, 190 * S
    d.rounded_rectangle((bx, by, bx + bw, by + bh), radius=14 * S, outline=(99, 185, 199), width=2 * S)
    rows = [("USERNAME", ("@" + user["username"]) if user.get("username") else "-", IVORY),
            ("USER ID", str(user.get("id", "")), IVORY),
            ("STATUS", "LEFT THE GROUP" if left else "VERIFIED MEMBER", CRIMSON if left else JADE)]
    yy = by + 24 * S
    for k, v, col in rows:
        d.text((bx + 26 * S, yy), k, font=_font(13, False), fill=MUTED)
        d.text((bx + 200 * S, yy - 2 * S), v, font=_font(19), fill=col)
        yy += 46 * S
    d.text((bx + 26 * S, by + bh - 34 * S), f"{'LEFT' if left else 'VERIFIED'} · {(datetime.now(timezone.utc) + IST).strftime('%d-%b-%Y').upper()}", font=_font(11, False), fill=MUTED)
    d.text((W - 300 * S, H - 70 * S), "CLEXER  ·  CLEX™ BOT", font=_font(13), fill=GOLD)
    im = im.resize((W // S, H // S), Image.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, "JPEG", quality=88)
    return buf.getvalue()


_welcomed: dict = {}     # (chat_id, user_id) -> time; an approval and the join notice both arrive


def welcome(chat_id, user: dict, title: str):
    key = (str(chat_id), str(user.get("id")))
    now = time.time()
    with _lock:
        for k in [k for k, t in _welcomed.items() if now - t > 900]:
            _welcomed.pop(k, None)
        if key in _welcomed:
            return                              # already welcomed in the last 15 minutes
        _welcomed[key] = now
    try:
        data = render_card(user, title)
    except Exception as e:
        print(f"  [JOIN] card render: {e}")
        data = None
    uname = user.get("username")
    caption = (f"🎉 {_mention(user)} joined <b>{_esc(title)}</b>\n\n"
               f"<blockquote>📄 <b>Your info</b>\n"
               f"👤 Name » {_mention(user)}\n"
               f"💬 Username » {('@' + _esc(uname)) if uname else '-'}\n"
               f"🪪 ID » <code>{user.get('id')}</code></blockquote>\n\n"
               f"Welcome aboard, {_esc(user.get('first_name') or 'friend')}! Glad to have you with us - say hi and enjoy your stay. 🤝")
    if data:
        j = _api("sendPhoto", {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
                 files={"photo": ("welcome.jpg", data, "image/jpeg")})
        if j.get("ok"):
            return
    _api("sendMessage", {"chat_id": chat_id, "text": caption, "parse_mode": "HTML"}, timeout=10)
