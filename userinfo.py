#!/usr/bin/env python3
"""userinfo.py - /info: a profile card for the person whose message you
reply to (or yourself when /info is sent on its own).

Image: a dark "USER IDENTITY INTEL" card (avatar, name, role, id, username,
chat role, DC, premium, bio) rendered with Pillow. Caption: the same facts
plus the CLEXER side (rank, VIP, copy trading) as one quote block, with the
id in monospace and the name / username / permanent link clickable.

Facts the Bot API cannot see (DC, premium of a third person, verified,
scam / fake flags) come from the music assistant's account when the music
service is set up; otherwise those lines read "-".
"""

import io
from datetime import datetime, timezone

import requests
from PIL import Image, ImageDraw

from groupjoin import _api, _avatar, _esc, _font, S, IST

_MUSIC = {"url": "", "secret": ""}
_RANK = lambda uid: "👤 Regular Member"      # set by bot.py
_VIP = lambda uid: (False, "")               # set by bot.py: (is_vip, vip_end)
_COPY = lambda uid: False                    # set by bot.py: copy trading connected?
_BLOCKED = lambda uid: False                 # set by bot.py: has the user blocked the bot?


def init(music_url="", music_secret="", rank=None, vip=None, copy=None, blocked=None):
    global _RANK, _VIP, _COPY, _BLOCKED
    _MUSIC.update(url=music_url or "", secret=music_secret or "")
    if rank:
        _RANK = rank
    if vip:
        _VIP = vip
    if copy:
        _COPY = copy
    if blocked:
        _BLOCKED = blocked


# ── facts ──────────────────────────────────────────────────────────────────
def _telegram_facts(uid, chat_id, user):
    f = {"bio": "", "photos": 0, "status": "-", "premium": bool(user.get("is_premium")), "bot": bool(user.get("is_bot"))}
    j = _api("getChat", {"chat_id": uid}, timeout=8)
    r = j.get("result") or {}
    f["bio"] = r.get("bio") or ""
    if r.get("username") and not user.get("username"):
        user["username"] = r["username"]
    j = _api("getUserProfilePhotos", {"user_id": uid, "limit": 1}, timeout=8)
    f["photos"] = int((j.get("result") or {}).get("total_count") or 0)
    if str(chat_id).startswith("-"):
        j = _api("getChatMember", {"chat_id": chat_id, "user_id": uid}, timeout=8)
        st = (j.get("result") or {}).get("status") or ""
        f["status"] = {"creator": "Owner", "administrator": "Admin", "member": "Member", "restricted": "Restricted",
                       "left": "Not in chat", "kicked": "Banned"}.get(st, "-")
    else:
        f["status"] = "Private chat"
    return f


def _assistant_facts(uid=None, username=None):
    """DC, premium, verified, scam / fake - through the music account. Given a
    username it also resolves who that is (the Bot API cannot)."""
    if not _MUSIC["url"] or not _MUSIC["secret"]:
        return {}
    try:
        r = requests.post(f"{_MUSIC['url']}/user", json={"user_id": uid, "username": username},
                          headers={"X-Music-Secret": _MUSIC["secret"]}, timeout=8)
        j = r.json() if r.ok else {}
        return j if isinstance(j, dict) and not j.get("error") else {}
    except Exception:
        return {}


# ── image ──────────────────────────────────────────────────────────────────
def render(user, facts, rank_label, dc, premium):
    W, H = 1280 * S, 720 * S
    BG, PANEL, CYAN, CYAN2, WHITE, MUTED, LINE = (11, 15, 26), (14, 20, 34), (0, 214, 242), (70, 160, 220), (245, 247, 250), (150, 160, 180), (40, 56, 84)
    VIOLET, GREEN = (170, 110, 255), (60, 220, 140)
    im = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(im)
    # a soft glow top-right and a few sparkles, like the reference card
    glow = Image.new("RGB", (W, H), BG)
    gd = ImageDraw.Draw(glow)
    for i, r in enumerate((260, 200, 140, 90)):
        c = 14 + i * 4
        gd.ellipse((W - 240 * S - r * S, 40 * S - r * S + 160 * S, W - 240 * S + r * S, 40 * S + r * S + 160 * S), fill=(c, c + 6, c + 18))
    im = Image.blend(im, glow, 0.55)
    d = ImageDraw.Draw(im)
    for (x, y, r) in ((1180, 80, 7), (1200, 200, 6), (440, 630, 6)):
        x, y, r = x * S, y * S, r * S
        d.polygon([(x, y - r), (x + r // 3, y - r // 3), (x + r, y), (x + r // 3, y + r // 3), (x, y + r), (x - r // 3, y + r // 3), (x - r, y), (x - r // 3, y - r // 3)], fill=WHITE)
    # avatar with a cyan ring
    cx, cy, R = 250 * S, 360 * S, 150 * S
    d.ellipse((cx - R - 8 * S, cy - R - 8 * S, cx + R + 8 * S, cy + R + 8 * S), outline=CYAN, width=5 * S)
    av = _avatar(user.get("id"), 2 * R)
    if av is not None:
        im.paste(av, (cx - R, cy - R), av)
    else:
        d.ellipse((cx - R, cy - R, cx + R, cy + R), fill=(28, 38, 60))
        letter = (user.get("first_name") or "?")[:1].upper()
        f = _font(110)
        tw = d.textlength(letter, font=f)
        d.text((cx - tw / 2, cy - 72 * S), letter, font=f, fill=CYAN)
    # header pill
    x0 = 480 * S
    pill = "USER IDENTITY INTEL"
    f = _font(20)
    pw = d.textlength(pill, font=f) + 70 * S
    d.rounded_rectangle((x0, 95 * S, x0 + pw, 143 * S), radius=24 * S, fill=(12, 34, 52), outline=CYAN, width=2 * S)
    d.ellipse((x0 + 22 * S, 112 * S, x0 + 36 * S, 126 * S), fill=CYAN)
    d.text((x0 + 48 * S, 106 * S), pill, font=f, fill=WHITE)
    # name + role
    name = ((user.get("first_name") or "") + (" " + user["last_name"] if user.get("last_name") else "")).strip() or (user.get("username") or "User")
    f = _font(46)
    while d.textlength(name, font=f) > (W - x0 - 80 * S) and len(name) > 6:
        name = name[:-2] + "…"
    d.text((x0, 172 * S), name, font=f, fill=WHITE)
    role = rank_label.lstrip("👤👑⭐🛡🤝 ").strip() or rank_label
    f = _font(21)
    rw = d.textlength(role, font=f) + 36 * S
    d.rounded_rectangle((x0, 232 * S, x0 + rw, 270 * S), radius=8 * S, fill=(16, 40, 64), outline=CYAN2, width=2 * S)
    d.text((x0 + 18 * S, 240 * S), role, font=f, fill=(90, 190, 240))
    # info box
    bx, by, bw, bh = x0, 288 * S, W - x0 - 70 * S, 236 * S
    d.rounded_rectangle((bx, by, bx + bw, by + bh), radius=18 * S, fill=PANEL, outline=CYAN, width=2 * S)
    lf, vf = _font(19), _font(23)
    left = [("USER ID", str(user.get("id", "")), WHITE),
            ("USERNAME", ("@" + user["username"]) if user.get("username") else "-", WHITE),
            ("CHAT ROLE", facts.get("status") or "-", WHITE)]
    right = [("DC SERVER", dc or "-", CYAN),
             ("PREMIUM", "YES" if premium else "NO", VIOLET if premium else (255, 120, 120)) if premium is not None else ("PREMIUM", "-", MUTED)]
    yy = by + 28 * S
    for k, v, col in left:
        d.text((bx + 30 * S, yy), k, font=lf, fill=MUTED)
        d.text((bx + 185 * S, yy - 4 * S), v[:28], font=vf, fill=col)
        yy += 44 * S
    yy = by + 28 * S
    for k, v, col in right:
        d.text((bx + 480 * S, yy), k, font=lf, fill=MUTED)
        d.text((bx + 620 * S, yy - 4 * S), v, font=vf, fill=col)
        yy += 44 * S
    d.line((bx + 24 * S, by + 154 * S, bx + bw - 24 * S, by + 154 * S), fill=LINE, width=2 * S)
    bio = (facts.get("bio") or "No bio set").replace("\n", " ")
    f = _font(18, False)
    while d.textlength("BIO : " + bio, font=f) > bw - 60 * S and len(bio) > 8:
        bio = bio[:-2] + "…"
    d.text((bx + 26 * S, by + 172 * S), "BIO : " + bio, font=f, fill=MUTED)
    d.text((W - 330 * S, H - 60 * S), "CLEXER  ·  CLEX™ BOT", font=_font(14), fill=CYAN)
    im = im.resize((W // S, H // S), Image.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, "JPEG", quality=90)
    return buf.getvalue()


# ── caption ────────────────────────────────────────────────────────────────
def _yn(v):
    return "-" if v is None else ("Yes" if v else "No")


def caption(user, facts, extra, rank_label):
    uid = user.get("id")
    uname = user.get("username")
    name = _esc(((user.get("first_name") or "") + (" " + user["last_name"] if user.get("last_name") else "")).strip() or uname or "User")
    vip, vip_end = _VIP(uid)
    dc = extra.get("dc_id")
    premium = extra.get("is_premium", bool(user.get("is_premium")))
    lines = [
        f"👤 Name           : <a href=\"tg://user?id={uid}\">{name}</a>",
        f"🆔 User ID        : <code>{uid}</code>",
        f"💬 Username       : " + (f"<a href=\"https://t.me/{_esc(uname)}\">@{_esc(uname)}</a>" if uname else "-"),
        f"🌐 DC Server      : " + (f"DC {dc}" if dc else "-"),
        f"🖼 Profile photos : {facts.get('photos', 0)}",
        "",
        f"👑 Global rank    : {rank_label}",
        f"👮 Chat status    : {facts.get('status') or '-'}",
        f"⚡ Premium        : {_yn(premium)}",
        f"⭐ VIP status     : " + (("Yes" + (f" · till {_esc(vip_end)}" if vip_end else "")) if vip else "No"),
        f"🔄 Copy trading   : {'Connected' if _COPY(uid) else 'No'}",
        f"🛡 Verified       : {_yn(extra.get('is_verified'))}",
        f"🤖 Bot            : {_yn(extra.get('is_bot', facts.get('bot')))}",
        f"⚠️ Scam / Fake    : {_yn(extra.get('is_scam'))} / {_yn(extra.get('is_fake'))}",
        "",
        f"📝 Bio            : {_esc(facts.get('bio') or 'No bio set')}",
        f"🔗 Permanent link : <a href=\"tg://user?id={uid}\">Open profile</a>",
    ]
    stamp = (datetime.now(timezone.utc) + IST).strftime("%d %b %Y · %H:%M")
    return ("🪪 <b>USER TELEMETRY</b>\n"
            "<blockquote>" + "\n".join(lines) + "</blockquote>\n"
            f"⚡ Checked {stamp} IST")


# ── command ────────────────────────────────────────────────────────────────
def cmd_info(chat_id, message, arg=""):
    """/info as a reply -> that person; /info @username or /info 123456 -> that
    person (resolved through the music account); plain /info -> the sender."""
    msg = message or {}
    arg = (arg or "").strip()
    target = None
    if arg:
        if arg.lstrip("@").isdigit():
            found = _assistant_facts(uid=int(arg.lstrip("@")))
        else:
            found = _assistant_facts(username=arg.lstrip("@"))
        if not found.get("id"):
            _api("sendMessage", {"chat_id": chat_id, "parse_mode": "HTML",
                                 "text": f"Couldn't find <b>{_esc(arg)}</b>. Reply to one of their messages with /info instead."}, timeout=10)
            return
        target = {"id": found["id"], "first_name": found.get("first_name") or "", "last_name": found.get("last_name") or "",
                  "username": found.get("username") or "", "is_premium": found.get("is_premium"), "is_bot": found.get("is_bot")}
    if target is None:
        target = (msg.get("reply_to_message") or {}).get("from") or msg.get("from") or {}
    if not target.get("id"):
        _api("sendMessage", {"chat_id": chat_id, "text": "Reply to someone's message with /info to see their card.", "parse_mode": "HTML"}, timeout=10)
        return
    user = dict(target)
    uid = user["id"]
    facts = _telegram_facts(uid, chat_id, user)
    extra = _assistant_facts(uid)
    rank_label = _RANK(uid)
    premium = extra.get("is_premium", bool(user.get("is_premium")))
    dc = f"DC {extra['dc_id']}" if extra.get("dc_id") else ""
    text = caption(user, facts, extra, rank_label)
    reply_to = msg.get("message_id")
    try:
        data = render(user, facts, rank_label, dc, premium)
    except Exception as e:
        print(f"  [INFO] card render: {e}")
        data = None
    if data:
        j = _api("sendPhoto", {"chat_id": chat_id, "caption": text, "parse_mode": "HTML",
                               **({"reply_to_message_id": reply_to} if reply_to else {})},
                 files={"photo": ("info.jpg", data, "image/jpeg")})
        if j.get("ok"):
            return
        print(f"  [INFO] sendPhoto: {j.get('description')}")
    _api("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                         **({"reply_to_message_id": reply_to} if reply_to else {})}, timeout=10)
