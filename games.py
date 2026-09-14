#!/usr/bin/env python3
"""games.py - chat games for CLEXER BOT.

Everything a game needs lives here; bot.py only forwards two things:
    /games                         -> cmd_games(chat_id, chat_type)
    callback data "game:..."       -> on_callback(...)  (returns popup text or None)

How a game runs
    /games -> pick a game -> pick how many players (2/3/4/6)
    DM     : every other seat is a robot, the game starts at once
    group  : a lobby photo with Join / Add robot / Cancel; starts when full
    Each turn the board is REDRAWN (Pillow, no AI, no cost) and swapped into the
    same message with editMessageMedia - one message per game, never a stream.
    Only the seated players can press anything; everyone else gets a popup.
    Quit asks "are you sure?" first; the game carries on for the others.

State is in memory: one game per chat, keyed by chat id. Robots take their
turns from a small background thread with a pause between moves so a human
can follow. A watchdog auto-rolls for a human who idles 90 s in a group,
cancels a lobby nobody joined after 5 min, and clears finished boards.

Buttons are deliberately NOT run through bot.py's _style_keyboard: the admin
wants game buttons plain, with only Quit / Cancel in red.
"""

import copy
import io
import json
import math
import os
import random
import threading
import time
import html as _html

import requests
from PIL import Image, ImageDraw, ImageFont

_TOKEN = None
_STORE = None              # hidden chat that boards are uploaded to ahead of time
PRELOAD = True             # upload-first-swap-second (see _prepare)
_lock = threading.RLock()
_games: dict = {}          # chat id (str) -> game dict
_fonts: dict = {}
_watch_started = False

SIZES = (2, 3, 4, 6)
TURN_SECS = 90             # group only: idle this long and the bot rolls for you
LOBBY_SECS = 300           # lobby with empty seats is cancelled after this
ROBOT_DELAY = 1.6          # pause before each robot move, so the board can be read
FINISHED_KEEP = 3600       # a finished board can be replayed for an hour

# emoji, name, RGB - seat colours in order
COLORS = [("🔴", "Red",    (226, 60, 60)),
          ("🟢", "Green",  (52, 168, 83)),
          ("🔵", "Blue",   (52, 120, 230)),
          ("🟡", "Yellow", (240, 190, 30)),
          ("🟣", "Purple", (150, 80, 210)),
          ("🟠", "Orange", (245, 130, 40))]

GAMES = {
    "snl": {"title": "🐍 Snake & Ladder", "short": "Snake & Ladder", "min": 2, "max": 6},
}

# Classic board layout (bottom of a ladder -> top, head of a snake -> tail)
LADDERS = {4: 14, 9: 31, 20: 38, 28: 84, 40: 59, 51: 67, 63: 81, 71: 91}
SNAKES = {17: 7, 54: 34, 62: 19, 64: 60, 87: 24, 93: 73, 95: 75, 99: 78}


# ── wiring ─────────────────────────────────────────────────────────────────

def init(token: str, store_chat=None):
    """Called once from bot.py after the token is known. store_chat is where
    boards are uploaded ahead of a move (a private channel the bot is admin
    of, or the admin's DM - the upload is deleted the moment its file_id is
    known, so nothing stays visible there)."""
    global _TOKEN, _watch_started
    _TOKEN = token
    set_store(store_chat)
    if not _watch_started:
        _watch_started = True
        threading.Thread(target=_watchdog, daemon=True, name="games-watchdog").start()


def set_store(chat):
    """Where boards are parked ahead of a move. None switches the
    upload-first path off (moves upload inside the edit, as before)."""
    global _STORE
    _STORE = str(chat).strip() if chat and str(chat).strip() else None


def _api(method, payload=None, files=None, timeout=15):
    if not _TOKEN:
        return {}
    url = f"https://api.telegram.org/bot{_TOKEN}/{method}"
    try:
        if files:
            r = requests.post(url, data=payload, files=files, timeout=max(timeout, 25))
        else:
            r = requests.post(url, json=payload, timeout=timeout)
        j = r.json()
        if not j.get("ok"):
            print(f"  [GAMES] {method}: {j.get('description')}")
        return j
    except Exception as e:
        print(f"  [GAMES] {method} failed: {e}")
        return {}


def _esc(s):
    return _html.escape(str(s or ""))


# ── /games menu ────────────────────────────────────────────────────────────

def _menu_kb():
    rows = [[{"text": v["title"], "callback_data": f"game:pick:{k}"}] for k, v in GAMES.items()]
    return {"inline_keyboard": rows}


def _menu_text(chat_type):
    where = ("In a DM you play against robots 🤖 - add the bot to a group to play with friends."
             if chat_type == "private" else
             "Anyone here can host. The host picks the number of players, the rest tap Join.")
    return f"🎮 <b>Games</b>\n\n{where}\n\nPick a game:"


def cmd_games(chat_id, chat_type="private", message_id=None):
    """Send the games menu - or, from a help-menu tap, edit that message into it
    so one tap gives one screen, not a "Done" plus a new message."""
    if chat_type is None:
        chat_type = "private" if int(str(chat_id).lstrip("-") or 0) == int(str(chat_id)) else "group"
    if message_id:
        j = _api("editMessageText", {"chat_id": chat_id, "message_id": message_id,
                                     "text": _menu_text(chat_type), "parse_mode": "HTML",
                                     "reply_markup": _menu_kb()})
        if j.get("ok"):
            return
    _api("sendMessage", {"chat_id": chat_id, "text": _menu_text(chat_type),
                         "parse_mode": "HTML", "reply_markup": _menu_kb()})


def _count_kb(kind):
    spec = GAMES[kind]
    row = [{"text": str(n), "callback_data": f"game:n:{kind}:{n}"}
           for n in SIZES if spec["min"] <= n <= spec["max"]]
    return {"inline_keyboard": [row, [{"text": "◀️ Back", "callback_data": "game:menu"}]]}


# ── game state ─────────────────────────────────────────────────────────────

def _new_game(kind, chat, chat_type, host_id, host_name):
    return {"kind": kind, "chat": str(chat), "chat_type": chat_type, "host": str(host_id),
            "msg_id": None, "phase": "lobby", "want": 0, "players": [], "turn": 0,
            "last": None, "log": [], "confirm": None, "winner": None, "busy": False,
            "ver": 0, "pre": None,
            "created": time.time(), "updated": time.time(), "turn_at": time.time()}


def _seat(g, uid, name, bot=False):
    idx = len(g["players"])
    g["players"].append({"id": str(uid), "name": str(name)[:20], "pos": 0, "c": idx, "bot": bot})
    g["ver"] += 1


def _find(g, uid):
    for p in g["players"]:
        if p["id"] == str(uid):
            return p
    return None


def _current(g):
    return g["players"][g["turn"]] if g["players"] else None


def _tag(p):
    return f"{COLORS[p['c']][0]} {_esc(p['name'])}"


# ── the move ───────────────────────────────────────────────────────────────

def _do_roll(g, auto=False, d=None):
    """d is normally decided here; _prepare passes one in that it already
    rolled in secret so the board for it could be drawn and uploaded early."""
    p = _current(g)
    d = d or random.randint(1, 6)
    frm = p["pos"]
    to = frm + d
    ev = None
    if to > 100:
        to, ev = frm, "stay"
    elif to in LADDERS:
        ev, to = "ladder", LADDERS[to]
    elif to in SNAKES:
        ev, to = "snake", SNAKES[to]
    p["pos"] = to
    again = (d == 6 and to != 100)
    g["last"] = {"c": p["c"], "roll": d, "frm": frm, "to": to, "ev": ev}
    line = f"{_tag(p)} rolled <b>{d}</b>"
    if ev == "stay":
        line += f" - needs exactly {100 - frm}, stays on {frm}"
    else:
        line += f" · {frm} → <b>{to}</b>"
        if ev == "ladder":
            line += " 🪜 ladder up!"
        elif ev == "snake":
            line += " 🐍 snake bite!"
    if auto:
        line += " ⏱ (auto)"
    if again:
        line += " · rolls again"
    g["log"] = (g["log"] + [line])[-3:]
    if to == 100:
        g["phase"] = "over"
        g["winner"] = p
    elif not again:
        g["turn"] = (g["turn"] + 1) % len(g["players"])
    g["turn_at"] = g["updated"] = time.time()
    g["ver"] += 1


def _remove_player(g, uid, why="left the game"):
    p = _find(g, uid)
    if not p:
        return
    i = g["players"].index(p)
    g["players"].remove(p)
    g["log"] = (g["log"] + [f"{_tag(p)} {why}"])[-3:]
    if g["phase"] == "play":
        if i < g["turn"]:
            g["turn"] -= 1
        if g["players"]:
            g["turn"] %= len(g["players"])
        if len(g["players"]) == 1:
            g["phase"] = "over"
            g["winner"] = g["players"][0]
            g["log"] = (g["log"] + [f"{_tag(g['players'][0])} wins - everyone else left"])[-3:]
    g["turn_at"] = g["updated"] = time.time()
    g["ver"] += 1


# ── captions & keyboards ───────────────────────────────────────────────────

def _caption(g):
    spec = GAMES[g["kind"]]
    out = [f"{_esc(spec['title'])}"]
    if g["phase"] == "lobby":
        out.append(f"Host: {_tag(g['players'][0])}")
        out.append(f"\nPlayers {len(g['players'])}/{g['want']}:")
        for p in g["players"]:
            out.append(_tag(p) + (" 🤖" if p["bot"] else ""))
        for _ in range(g["want"] - len(g["players"])):
            out.append("⚪ waiting…")
        out.append("\nTap <b>Join</b> to sit down. The host can add robots or cancel.")
        return "\n".join(out)
    out.append("")
    for p in sorted(g["players"], key=lambda q: -q["pos"]):
        out.append(f"{_tag(p)}{' 🤖' if p['bot'] else ''} · <b>{p['pos']}</b>")
    if g["log"]:
        out.append("")
        out.extend(g["log"])
    out.append("")
    if g["phase"] == "over":
        w = g["winner"]
        out.append(f"🏆 <b>{_esc(w['name'])} wins!</b>")
    else:
        cur = _current(g)
        if cur["bot"]:
            out.append(f"🤖 {_tag(cur)} is rolling…")
        elif g["confirm"] and g["confirm"] == cur["id"]:
            out.append(f"👉 Turn: {_tag(cur)}")
        else:
            out.append(f"👉 Turn: {_tag(cur)} - tap 🎲 Roll")
    return "\n".join(out)[:1000]


def _keyboard(g):
    if g["phase"] == "lobby":
        rows = [[{"text": "Join", "callback_data": "game:join"}]]
        rows.append([{"text": "🤖 Add robot", "callback_data": "game:addbot"},
                     {"text": "Cancel", "callback_data": "game:cancel", "style": "danger"}])
        return {"inline_keyboard": rows}
    if g["phase"] == "over":
        return {"inline_keyboard": [[{"text": "🔁 Play again", "callback_data": "game:again"},
                                     {"text": "🎮 Games", "callback_data": "game:menu"}]]}
    if g["confirm"]:
        return {"inline_keyboard": [[{"text": "Yes, quit", "callback_data": "game:quit_yes", "style": "danger"},
                                     {"text": "No, keep playing", "callback_data": "game:quit_no"}]]}
    return {"inline_keyboard": [[{"text": "🎲 Roll", "callback_data": "game:roll"}],
                                [{"text": "Quit", "callback_data": "game:quit", "style": "danger"}]]}


# ── drawing ────────────────────────────────────────────────────────────────
# Everything is drawn at 2x (S) and downsampled with Lanczos at the end -
# Pillow has no anti-aliasing of its own, so this is what turns jagged
# circles and lines into smooth ones. The static board (frame, squares,
# ladders, snakes) is drawn once and cached; a move only paints the title
# strip, the dice, the landing glow and the tokens onto a copy.

S = 2                                  # supersample factor
OUT_W = 800                            # delivered width in px
CELL, MARGIN, TOP, BOTTOM = 72, 28, 96, 28
W = MARGIN * 2 + CELL * 10             # 776 logical px
H = TOP + CELL * 10 + BOTTOM

FELT = (24, 84, 58)                    # table felt behind the board
FRAME = (92, 58, 30)                   # wooden frame
CELL_A, CELL_B = (252, 246, 232), (232, 219, 190)
INK = (98, 84, 70)


def _font(size, bold=True):
    key = (size, bold)
    if key in _fonts:
        return _fonts[key]
    f = None
    cands = []
    try:
        import matplotlib
        cands.append(os.path.join(matplotlib.get_data_path(), "fonts", "ttf",
                                  "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"))
    except Exception:
        pass
    cands += ["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
              "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf"]
    for p in cands:
        try:
            f = ImageFont.truetype(p, size * S)
            break
        except Exception:
            continue
    if f is None:
        f = ImageFont.load_default()
    _fonts[key] = f
    return f


def _cell_xy(n):
    """Centre of square n (1..100) in logical px. Row 0 is the bottom."""
    i = n - 1
    row, col = divmod(i, 10)
    if row % 2 == 1:
        col = 9 - col
    return (MARGIN + col * CELL + CELL / 2, TOP + (9 - row) * CELL + CELL / 2)


def _sx(v):
    return v * S


def _shade(rgb, k):
    """Lighten (k>0) or darken (k<0) a colour."""
    if k >= 0:
        return tuple(int(c + (255 - c) * k) for c in rgb)
    return tuple(int(c * (1 + k)) for c in rgb)


def _cubic(p0, p3, k=0.28, n=40):
    dx, dy = p3[0] - p0[0], p3[1] - p0[1]
    L = math.hypot(dx, dy) or 1
    px, py = -dy / L, dx / L
    c1 = (p0[0] + dx / 3 + px * L * k, p0[1] + dy / 3 + py * L * k)
    c2 = (p0[0] + 2 * dx / 3 - px * L * k, p0[1] + 2 * dy / 3 - py * L * k)
    pts = []
    for i in range(n + 1):
        t = i / n
        u = 1 - t
        x = u ** 3 * p0[0] + 3 * u * u * t * c1[0] + 3 * u * t * t * c2[0] + t ** 3 * p3[0]
        y = u ** 3 * p0[1] + 3 * u * u * t * c1[1] + 3 * u * t * t * c2[1] + t ** 3 * p3[1]
        pts.append((x, y))
    return pts


def _draw_ladder(d, a, b):
    (x1, y1), (x2, y2) = _cell_xy(a), _cell_xy(b)
    x1, y1, x2, y2 = _sx(x1), _sx(y1), _sx(x2), _sx(y2)
    L = math.hypot(x2 - x1, y2 - y1) or 1
    px, py = -(y2 - y1) / L * 9 * S, (x2 - x1) / L * 9 * S
    dark, mid, hi = (96, 58, 26), (160, 104, 48), (214, 160, 92)
    # shadow first, then rails with a highlight edge
    for s in (1, -1):
        d.line([(x1 + px * s + 3 * S, y1 + py * s + 3 * S), (x2 + px * s + 3 * S, y2 + py * s + 3 * S)],
               fill=(0, 0, 0, 70), width=7 * S)
    for s in (1, -1):
        d.line([(x1 + px * s, y1 + py * s), (x2 + px * s, y2 + py * s)], fill=dark, width=7 * S)
        d.line([(x1 + px * s, y1 + py * s), (x2 + px * s, y2 + py * s)], fill=mid, width=4 * S)
        d.line([(x1 + px * s - 1 * S, y1 + py * s - 1 * S), (x2 + px * s - 1 * S, y2 + py * s - 1 * S)],
               fill=hi, width=1 * S)
    steps = max(2, int(L / (20 * S)))
    for i in range(1, steps):
        t = i / steps
        cx, cy = x1 + (x2 - x1) * t, y1 + (y2 - y1) * t
        d.line([(cx + px, cy + py), (cx - px, cy - py)], fill=dark, width=5 * S)
        d.line([(cx + px, cy + py), (cx - px, cy - py)], fill=mid, width=3 * S)


def _draw_snake(d, head, tail):
    (hx0, hy0), (tx0, ty0) = _cell_xy(head), _cell_xy(tail)
    pts = _cubic((hx0, hy0), (tx0, ty0), n=max(60, int(math.hypot(tx0 - hx0, ty0 - hy0) / 1.5)))
    pts = [(_sx(x), _sx(y)) for x, y in pts]
    n = len(pts)
    body_dark, body, belly, spot = (26, 92, 48), (72, 168, 92), (150, 214, 120), (20, 70, 36)
    # tapering body: fat at the head, thin at the tail. Each layer is drawn
    # as round dots along the curve so the taper is smooth and the shadow
    # narrows with the body instead of peeking out beside the tail.
    def _dots(dx, dy, base, shrink, color):
        for i in range(n):
            r = (base - shrink * (i / n)) * S / 2
            x, y = pts[i][0] + dx, pts[i][1] + dy
            d.ellipse([x - r, y - r, x + r, y + r], fill=color)
    _dots(4 * S, 5 * S, 17, 12, (0, 0, 0, 55))
    _dots(0, 0, 17, 12, body_dark)
    _dots(0, 0, 13, 9, body)
    _dots(-1 * S, -2 * S, 6, 4, belly)
    # scale spots along the back
    for i in range(6, n - 6, max(4, n // 12)):
        x, y = pts[i]
        r = (4 - 2 * (i / n)) * S
        d.ellipse([x - r, y - r, x + r, y + r], fill=spot)
    # head
    hx, hy = pts[0]
    nx, ny = pts[1]
    ang = math.atan2(ny - hy, nx - hx)
    r = 13 * S
    d.ellipse([hx - r - 2 * S, hy - r - 2 * S, hx + r + 2 * S, hy + r + 2 * S], fill=body_dark)
    d.ellipse([hx - r, hy - r, hx + r, hy + r], fill=body)
    d.ellipse([hx - r + 3 * S, hy - r + 3 * S, hx + r - 5 * S, hy - 1 * S], fill=_shade(body, 0.25))
    # eyes sit across the direction of travel
    ex, ey = -math.sin(ang) * 6 * S, math.cos(ang) * 6 * S
    for s in (1, -1):
        cx, cy = hx + ex * s - math.cos(ang) * 2 * S, hy + ey * s - math.sin(ang) * 2 * S
        d.ellipse([cx - 3.2 * S, cy - 3.2 * S, cx + 3.2 * S, cy + 3.2 * S], fill=(250, 250, 240))
        d.ellipse([cx - 1.6 * S, cy - 1.6 * S, cx + 1.6 * S, cy + 1.6 * S], fill=(10, 10, 10))
    # forked tongue pointing away from the body
    tx, ty = hx - math.cos(ang) * r, hy - math.sin(ang) * r
    fx, fy = hx - math.cos(ang) * (r + 12 * S), hy - math.sin(ang) * (r + 12 * S)
    d.line([(tx, ty), (fx, fy)], fill=(214, 40, 60), width=2 * S)
    for s in (1, -1):
        d.line([(fx, fy), (fx - math.cos(ang) * 5 * S - math.sin(ang) * 4 * S * s,
                           fy - math.sin(ang) * 5 * S + math.cos(ang) * 4 * S * s)],
               fill=(214, 40, 60), width=2 * S)
    # tail tip
    tx, ty = pts[-1]
    d.ellipse([tx - 3 * S, ty - 3 * S, tx + 3 * S, ty + 3 * S], fill=body_dark)


def _noise(size, sigma=14):
    n = Image.effect_noise(size, sigma).convert("L")
    return n


_snl_base = None


def _snl_board() -> Image.Image:
    """Static part - felt, wooden frame, squares, ladders, snakes - at 2x,
    drawn once and cached."""
    global _snl_base
    if _snl_base is not None:
        return _snl_base.copy()
    img = Image.new("RGB", (W * S, H * S), FELT)
    # felt texture
    tex = _noise((W * S, H * S), 12)
    img = Image.composite(img, Image.new("RGB", img.size, _shade(FELT, -0.25)), tex.point(lambda v: 140 + v // 3))
    d = ImageDraw.Draw(img, "RGBA")
    # wooden frame with a drop shadow
    bx0, by0 = _sx(MARGIN - 12), _sx(TOP - 12)
    bx1, by1 = _sx(MARGIN + CELL * 10 + 12), _sx(TOP + CELL * 10 + 12)
    d.rounded_rectangle([bx0 + 8 * S, by0 + 10 * S, bx1 + 8 * S, by1 + 10 * S], radius=10 * S, fill=(0, 0, 0, 110))
    d.rounded_rectangle([bx0, by0, bx1, by1], radius=10 * S, fill=FRAME)
    for i in range(0, 12 * S, 3):
        d.rounded_rectangle([bx0 + i, by0 + i, bx1 - i, by1 - i], radius=10 * S,
                            outline=_shade(FRAME, 0.10 + 0.02 * (i % 4)), width=1)
    d.rounded_rectangle([bx0, by0, bx1, by1], radius=10 * S, outline=_shade(FRAME, 0.35), width=2 * S)
    # squares
    f13 = _font(13)
    for n in range(1, 101):
        cx, cy = _cell_xy(n)
        x0, y0 = _sx(cx - CELL / 2), _sx(cy - CELL / 2)
        x1, y1 = x0 + CELL * S, y0 + CELL * S
        row, col = divmod(n - 1, 10)
        fill = CELL_A if (row + col) % 2 == 0 else CELL_B
        if n == 100:
            fill = (255, 208, 84)
        elif n == 1:
            fill = (188, 226, 196)
        d.rectangle([x0, y0, x1, y1], fill=fill)
        # soft bevel: light top-left, dark bottom-right
        d.line([(x0, y1), (x0, y0), (x1, y0)], fill=_shade(fill, 0.55), width=2 * S)
        d.line([(x1, y0), (x1, y1), (x0, y1)], fill=_shade(fill, -0.14), width=2 * S)
        d.text((x0 + 6 * S, y0 + 4 * S), str(n), font=f13, fill=INK)
        if n == 100:
            d.text((x0 + 13 * S, y0 + 42 * S), "FINISH", font=_font(14), fill=(122, 78, 0))
        elif n == 1:
            d.text((x0 + 16 * S, y0 + 42 * S), "START", font=_font(14), fill=(28, 100, 52))
    for a, b in LADDERS.items():
        _draw_ladder(d, a, b)
    for a, b in SNAKES.items():
        _draw_snake(d, a, b)
    _snl_base = img
    return img.copy()


def _token(d, x, y, rgb, label, r=15):
    """A 3D-looking pawn: shadow, dark rim, radial highlight, label."""
    x, y, r = _sx(x), _sx(y), r * S
    d.ellipse([x - r + 2 * S, y - r + 5 * S, x + r + 2 * S, y + r + 5 * S], fill=(0, 0, 0, 90))
    d.ellipse([x - r - 2 * S, y - r - 2 * S, x + r + 2 * S, y + r + 2 * S], fill=(255, 255, 255))
    d.ellipse([x - r, y - r, x + r, y + r], fill=_shade(rgb, -0.35))
    steps = 8
    for i in range(steps):
        k = i / steps
        rr = r * (1 - k * 0.55)
        ox, oy = -r * 0.25 * k, -r * 0.3 * k
        d.ellipse([x + ox - rr, y + oy - rr, x + ox + rr, y + oy + rr], fill=_shade(rgb, -0.3 + 0.75 * k))
    f = _font(13)
    tw = d.textlength(label, font=f)
    d.text((x - tw / 2 + 1 * S, y - 8 * S + 1 * S), label, font=f, fill=(0, 0, 0, 120))
    d.text((x - tw / 2, y - 8 * S), label, font=f, fill=(255, 255, 255))


def _die(d, x, y, n, rgb, size=40):
    """Rounded die showing n pips, tinted in the roller's colour."""
    x, y, s = _sx(x), _sx(y), size * S
    d.rounded_rectangle([x + 3 * S, y + 4 * S, x + s + 3 * S, y + s + 4 * S], radius=8 * S, fill=(0, 0, 0, 100))
    d.rounded_rectangle([x, y, x + s, y + s], radius=8 * S, fill=(250, 250, 246), outline=_shade(rgb, -0.2), width=2 * S)
    d.rounded_rectangle([x + 2 * S, y + 2 * S, x + s - 2 * S, y + s * 0.45], radius=6 * S, fill=(255, 255, 255))
    pips = {1: [(0.5, 0.5)], 2: [(0.25, 0.25), (0.75, 0.75)], 3: [(0.25, 0.25), (0.5, 0.5), (0.75, 0.75)],
            4: [(0.25, 0.25), (0.75, 0.25), (0.25, 0.75), (0.75, 0.75)],
            5: [(0.25, 0.25), (0.75, 0.25), (0.5, 0.5), (0.25, 0.75), (0.75, 0.75)],
            6: [(0.25, 0.22), (0.75, 0.22), (0.25, 0.5), (0.75, 0.5), (0.25, 0.78), (0.75, 0.78)]}
    pr = s * 0.085
    for px, py in pips.get(n, []):
        cx, cy = x + s * px, y + s * py
        d.ellipse([cx - pr, cy - pr, cx + pr, cy + pr], fill=_shade(rgb, -0.25))
        d.ellipse([cx - pr * 0.5, cy - pr * 0.6, cx + pr * 0.1, cy - pr * 0.05], fill=_shade(rgb, 0.45))


def render_snl(g) -> bytes:
    img = _snl_board()
    d = ImageDraw.Draw(img, "RGBA")
    # title strip: dark gradient with a gold rule
    for i in range(TOP * S - 14 * S):
        k = i / (TOP * S)
        d.line([(0, i), (W * S, i)], fill=(int(22 + 14 * k), int(26 + 14 * k), int(36 + 18 * k)))
    d.line([(0, TOP * S - 14 * S), (W * S, TOP * S - 14 * S)], fill=(214, 172, 80), width=2 * S)
    d.text((_sx(MARGIN) + 2 * S, 22 * S + 2 * S), "SNAKE & LADDER", font=_font(30), fill=(0, 0, 0, 140))
    d.text((_sx(MARGIN), 22 * S), "SNAKE & LADDER", font=_font(30), fill=(255, 226, 150))
    if g["phase"] == "lobby":
        status = f"Waiting {len(g['players'])}/{g['want']}"
    elif g["phase"] == "over":
        status = f"{g['winner']['name']} wins!"
    else:
        status = f"Turn: {_current(g)['name']}"
    f16 = _font(17)
    tw = d.textlength(status, font=f16)
    d.text((_sx(W - MARGIN) - tw, 10 * S), status, font=f16, fill=(255, 255, 255))
    # dice for the last roll, in the roller's colour
    last = g.get("last")
    if last and last.get("roll"):
        _die(d, W / 2 - 24, 12, last["roll"], COLORS[last["c"]][2], size=48)
    # landing glow
    if last and last["to"] > 0:
        cx, cy = _cell_xy(last["to"])
        for i in range(6, 0, -1):
            a = int(28 + 18 * (6 - i))
            d.rounded_rectangle([_sx(cx - CELL / 2) - i * S, _sx(cy - CELL / 2) - i * S,
                                 _sx(cx + CELL / 2) + i * S, _sx(cy + CELL / 2) + i * S],
                                radius=4 * S, outline=(255, 190, 40, a), width=2 * S)
        d.rectangle([_sx(cx - CELL / 2), _sx(cy - CELL / 2), _sx(cx + CELL / 2), _sx(cy + CELL / 2)],
                    outline=(255, 170, 0), width=3 * S)
    # tokens
    offs = [(-15, -15), (15, -15), (-15, 15), (15, 15), (0, -17), (0, 17)]
    bench_x = W - MARGIN - 16
    for p in g["players"]:
        rgb = COLORS[p["c"]][2]
        if p["pos"] <= 0:
            x, y = bench_x, TOP - 36
            bench_x -= 34
        else:
            cx, cy = _cell_xy(p["pos"])
            ox, oy = offs[p["c"] % len(offs)]
            x, y = cx + ox, cy + oy
        label = p["name"].split()[-1] if p["bot"] else (p["name"][:1] or "?").upper()
        _token(d, x, y, rgb, label)
    if g["phase"] == "lobby":
        f = _font(32)
        msg = "Waiting for players"
        tw = d.textlength(msg, font=f)
        bx, by = W * S / 2 - tw / 2 - 28 * S, _sx(TOP + CELL * 4.2)
        d.rounded_rectangle([bx + 4 * S, by + 6 * S, bx + tw + 56 * S + 4 * S, by + 70 * S + 6 * S],
                            radius=16 * S, fill=(0, 0, 0, 120))
        d.rounded_rectangle([bx, by, bx + tw + 56 * S, by + 70 * S], radius=16 * S, fill=(31, 36, 48),
                            outline=(214, 172, 80), width=2 * S)
        d.text((W * S / 2 - tw / 2, by + 16 * S), msg, font=f, fill=(255, 226, 150))
    out = img.resize((OUT_W, int(H * OUT_W / W)), Image.LANCZOS)
    buf = io.BytesIO()
    out.save(buf, "JPEG", quality=84)
    return buf.getvalue()


RENDER = {"snl": render_snl}


# ── pushing the board to Telegram ──────────────────────────────────────────

def _upload(png):
    """Park a board in the store chat and return its file_id (or None). The
    parked message is deleted at once; the file stays on Telegram's servers
    and the id keeps working, so the later edit has nothing to upload."""
    if not (_STORE and png):
        return None
    j = _api("sendPhoto", {"chat_id": _STORE, "disable_notification": True},
             files={"photo": ("board.jpg", png)})
    res = j.get("result") or {}
    sizes = res.get("photo") or []
    fid = sizes[-1].get("file_id") if sizes else None
    if res.get("message_id"):
        _api("deleteMessage", {"chat_id": _STORE, "message_id": res["message_id"]})
    return fid


def _preview(g, d):
    """Board as it will look after the current player rolls d - drawn on a
    copy so the real game is untouched until the tap actually happens."""
    with _lock:
        g2 = copy.deepcopy({k: v for k, v in g.items() if k != "pre"})
    g2["pre"] = None
    _do_roll(g2, d=d)
    return RENDER[g2["kind"]](g2)


def _prepare(g):
    """Upload first, swap second. As soon as it is a human's turn, roll their
    dice in secret, draw the board that roll produces and upload it. When they
    tap Roll the edit only references the file_id - the server-side upload
    and Telegram's photo processing are already done. The version counter
    makes sure a board prepared for a state that has since changed (someone
    quit, a robot moved) is thrown away instead of used."""
    if not (PRELOAD and _STORE):
        return
    with _lock:
        if g["phase"] != "play":
            return
        cur = _current(g)
        if not cur or cur["bot"]:
            return
        ver = g["ver"]
        if g["pre"] and g["pre"]["ver"] == ver:
            return
        g["pre"] = {"ver": ver, "roll": None, "file_id": None, "png": None}   # claimed

    def run():
        d = random.randint(1, 6)
        try:
            png = _preview(g, d)
            fid = _upload(png)
        except Exception as e:
            print(f"  [GAMES] prepare: {e}")
            png = fid = None
        with _lock:
            if g["ver"] == ver:
                g["pre"] = {"ver": ver, "roll": d, "file_id": fid, "png": png}
    threading.Thread(target=run, daemon=True, name="games-prepare").start()


def _take_pre(g):
    """The prepared roll for the current state, if one finished in time."""
    pre = g.get("pre")
    g["pre"] = None
    if pre and pre["ver"] == g["ver"] and pre["roll"]:
        return pre
    return None


def _push(g, kb_only=False, file_id=None, png=None):
    """Redraw and swap the board into the game's message. One message per game.
    With file_id the edit carries no bytes at all; png (if given) is the
    already-rendered board used as the fallback."""
    with _lock:
        cap = _caption(g)
        kb = _keyboard(g)
        if not kb_only and png is None and not file_id:
            png = RENDER[g["kind"]](g)
        chat, mid = g["chat"], g["msg_id"]
    if kb_only:
        if mid:
            _api("editMessageReplyMarkup", {"chat_id": chat, "message_id": mid, "reply_markup": kb})
        return
    if mid and file_id:
        j = _api("editMessageMedia",
                 {"chat_id": chat, "message_id": mid,
                  "media": {"type": "photo", "media": file_id,
                            "caption": cap, "parse_mode": "HTML"},
                  "reply_markup": kb})
        if j.get("ok"):
            _prepare(g)
            return
        if "retry after" in str(j.get("description", "")):
            return
        # bad/expired file id - fall back to a normal upload
        if png is None:
            with _lock:
                png = RENDER[g["kind"]](g)
    if mid:
        j = _api("editMessageMedia",
                 {"chat_id": chat, "message_id": mid,
                  "media": json.dumps({"type": "photo", "media": "attach://board",
                                       "caption": cap, "parse_mode": "HTML"}),
                  "reply_markup": json.dumps(kb)},
                 files={"board": ("board.jpg", png)})
        if j.get("ok"):
            _prepare(g)
            return
        desc = str(j.get("description", ""))
        if "not modified" in desc or "retry after" in desc:
            return
        # message gone (user deleted it) - fall through and post a fresh one
    j = _api("sendPhoto", {"chat_id": chat, "caption": cap, "parse_mode": "HTML",
                           "reply_markup": json.dumps(kb)},
             files={"photo": ("board.jpg", png)})
    new_id = (j.get("result") or {}).get("message_id")
    if new_id:
        with _lock:
            g["msg_id"] = new_id
    _prepare(g)


def _robots_go(chat):
    """Let robots take their turns, one every ROBOT_DELAY seconds."""
    with _lock:
        g = _games.get(str(chat))
        if not g or g["phase"] != "play" or g["busy"]:
            return
        cur = _current(g)
        if not cur or not cur["bot"]:
            return
        g["busy"] = True

    def run():
        try:
            while True:
                # The robot's roll is decided now; its board is drawn and
                # uploaded during the pause, so the visible edit at the end
                # of the pause has nothing left to send.
                t0 = time.time()
                with _lock:
                    g2 = _games.get(str(chat))
                    if not g2 or g2 is not g or g2["phase"] != "play":
                        break
                    cur2 = _current(g2)
                    if not cur2 or not cur2["bot"]:
                        break
                    ver = g["ver"]
                d = random.randint(1, 6)
                fid = png = None
                if PRELOAD and _STORE:
                    try:
                        png = _preview(g, d)
                        fid = _upload(png)
                    except Exception as e:
                        print(f"  [GAMES] robot prepare: {e}")
                        fid = png = None
                time.sleep(max(0.0, ROBOT_DELAY - (time.time() - t0)))
                with _lock:
                    g2 = _games.get(str(chat))
                    if not g2 or g2 is not g or g2["phase"] != "play":
                        break
                    cur2 = _current(g2)
                    if not cur2 or not cur2["bot"]:
                        break
                    if g["ver"] != ver:          # someone quit meanwhile - redo
                        fid = png = None
                    _do_roll(g2, d=d)
                _push(g, file_id=fid, png=png)
                if g["phase"] != "play":
                    break
        finally:
            g["busy"] = False
    threading.Thread(target=run, daemon=True, name=f"games-robot-{chat}").start()


# ── callbacks ──────────────────────────────────────────────────────────────

def on_callback(data, uid, name, chat_id, msg_id, chat_type="private"):
    """Returns popup text for answerCallbackQuery (or None for a silent ack)."""
    parts = data.split(":")
    act = parts[1] if len(parts) > 1 else ""
    chat = str(chat_id)
    uid = str(uid)
    name = (name or "Player")[:20]

    if act == "menu":
        j = _api("editMessageText", {"chat_id": chat, "message_id": msg_id,
                                     "text": _menu_text(chat_type), "parse_mode": "HTML",
                                     "reply_markup": _menu_kb()})
        if not j.get("ok"):          # came from a photo (finished board) - send a fresh menu
            cmd_games(chat, chat_type)
        return None

    if act == "pick":
        kind = parts[2] if len(parts) > 2 else ""
        if kind not in GAMES:
            return "That game isn't ready yet."
        with _lock:
            g = _games.get(chat)
            if g and g["phase"] in ("lobby", "play"):
                return "A game is already running in this chat. Finish or cancel it first."
        spec = GAMES[kind]
        hint = ("Every other seat will be a robot 🤖" if chat_type == "private"
                else "Friends join by tapping Join on the board.")
        _api("editMessageText", {"chat_id": chat, "message_id": msg_id,
                                 "text": f"{_esc(spec['title'])}\n\nHow many players?\n{hint}",
                                 "parse_mode": "HTML", "reply_markup": _count_kb(kind)})
        return None

    if act == "n":
        kind = parts[2] if len(parts) > 2 else ""
        try:
            n = int(parts[3])
        except Exception:
            n = 0
        if kind not in GAMES or n not in SIZES:
            return "Pick a number from the buttons."
        with _lock:
            g = _games.get(chat)
            if g and g["phase"] in ("lobby", "play"):
                return "A game is already running in this chat."
            g = _new_game(kind, chat, chat_type, uid, name)
            g["want"] = n
            _seat(g, uid, name)
            if chat_type == "private":
                for i in range(1, n):
                    _seat(g, f"bot{i}", f"Robo {i}", bot=True)
                g["phase"] = "play"
            _games[chat] = g
        _api("deleteMessage", {"chat_id": chat, "message_id": msg_id})
        _push(g)
        _robots_go(chat)
        return None

    with _lock:
        g = _games.get(chat)
    if not g:
        return "No game here right now. Send /games to start one."
    if msg_id and g["msg_id"] and int(msg_id) != int(g["msg_id"]):
        return "That board is old - use the latest one below."
    me = _find(g, uid)

    if act == "join":
        if g["phase"] != "lobby":
            return "This game already started."
        if me:
            return "You're already in."
        with _lock:
            _seat(g, uid, name)
            full = len(g["players"]) >= g["want"]
            if full:
                g["phase"] = "play"
                g["turn_at"] = time.time()
        _push(g)
        if full:
            _robots_go(chat)
        return None

    if act == "addbot":
        if g["phase"] != "lobby":
            return "This game already started."
        if uid != g["host"]:
            return "Only the host can add robots."
        with _lock:
            k = sum(1 for p in g["players"] if p["bot"]) + 1
            _seat(g, f"bot{k}", f"Robo {k}", bot=True)
            full = len(g["players"]) >= g["want"]
            if full:
                g["phase"] = "play"
                g["turn_at"] = time.time()
        _push(g)
        if full:
            _robots_go(chat)
        return None

    if act == "cancel":
        if g["phase"] != "lobby":
            return "The game already started - use Quit."
        if uid != g["host"]:
            return "Only the host can cancel."
        with _lock:
            _games.pop(chat, None)
        _api("editMessageCaption", {"chat_id": chat, "message_id": g["msg_id"],
                                    "caption": f"{_esc(GAMES[g['kind']]['title'])}\n\n❌ Cancelled by the host.",
                                    "parse_mode": "HTML"})
        return None

    if not me:
        return "You're not in this game."

    if act == "roll":
        if g["phase"] != "play":
            return "The game is over - tap Play again."
        if g["confirm"]:
            return "Answer the quit question first."
        cur = _current(g)
        if cur["id"] != uid:
            return f"Not your turn - it's {cur['name']}'s."
        with _lock:
            pre = _take_pre(g)
            _do_roll(g, d=pre["roll"] if pre else None)
        _push(g, file_id=pre["file_id"] if pre else None, png=pre["png"] if pre else None)
        _robots_go(chat)
        return None

    if act == "quit":
        if g["phase"] != "play":
            return "The game is already over."
        if g["confirm"]:
            return "Someone is already deciding whether to quit."
        with _lock:
            g["confirm"] = uid
        _push(g, kb_only=True)
        return "Are you sure you want to leave? Tap Yes to quit."

    if act == "quit_no":
        if g["confirm"] != uid:
            return "That question isn't for you."
        with _lock:
            g["confirm"] = None
        _push(g, kb_only=True)
        return None

    if act == "quit_yes":
        if g["confirm"] != uid:
            return "That question isn't for you."
        with _lock:
            g["confirm"] = None
            _remove_player(g, uid)
            humans = [p for p in g["players"] if not p["bot"]]
            if not humans:
                # robots don't play each other - the board is done
                _games.pop(chat, None)
        if not humans:
            _api("editMessageCaption", {"chat_id": chat, "message_id": g["msg_id"],
                                        "caption": f"{_esc(GAMES[g['kind']]['title'])}\n\n"
                                                   "Everyone left - game closed. Send /games to play again.",
                                        "parse_mode": "HTML"})
            return "You left the game."
        _push(g)
        _robots_go(chat)
        return "You left the game."

    if act == "again":
        if g["phase"] != "over":
            return "The game is still going."
        with _lock:
            for p in g["players"]:
                p["pos"] = 0
            g.update({"phase": "play", "turn": 0, "last": None, "log": [], "confirm": None,
                      "winner": None, "pre": None, "ver": g["ver"] + 1,
                      "created": time.time(), "updated": time.time(),
                      "turn_at": time.time()})
        _push(g)
        _robots_go(chat)
        return None

    return None


# ── watchdog ───────────────────────────────────────────────────────────────

def _watchdog():
    while True:
        time.sleep(15)
        try:
            now = time.time()
            with _lock:
                items = list(_games.items())
            for chat, g in items:
                if g["phase"] == "lobby" and now - g["created"] > LOBBY_SECS:
                    with _lock:
                        _games.pop(chat, None)
                    _api("editMessageCaption", {"chat_id": chat, "message_id": g["msg_id"],
                                                "caption": f"{_esc(GAMES[g['kind']]['title'])}\n\n"
                                                           "⌛ Nobody joined - lobby closed.",
                                                "parse_mode": "HTML"})
                elif g["phase"] == "play":
                    if now - g["updated"] > 6 * 3600:
                        with _lock:
                            _games.pop(chat, None)
                        continue
                    cur = _current(g)
                    if (g["chat_type"] != "private" and cur and not cur["bot"] and not g["confirm"]
                            and now - g["turn_at"] > TURN_SECS):
                        with _lock:
                            pre = _take_pre(g)
                            _do_roll(g, auto=True, d=pre["roll"] if pre else None)
                        _push(g, file_id=pre["file_id"] if pre else None, png=pre["png"] if pre else None)
                        _robots_go(chat)
                elif g["phase"] == "over" and now - g["updated"] > FINISHED_KEEP:
                    with _lock:
                        _games.pop(chat, None)
        except Exception as e:
            print(f"  [GAMES] watchdog: {e}")
