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

def init(token: str):
    """Called once from bot.py after the token is known."""
    global _TOKEN, _watch_started
    _TOKEN = token
    if not _watch_started:
        _watch_started = True
        threading.Thread(target=_watchdog, daemon=True, name="games-watchdog").start()


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
            "created": time.time(), "updated": time.time(), "turn_at": time.time()}


def _seat(g, uid, name, bot=False):
    idx = len(g["players"])
    g["players"].append({"id": str(uid), "name": str(name)[:20], "pos": 0, "c": idx, "bot": bot})


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

def _do_roll(g, auto=False):
    p = _current(g)
    d = random.randint(1, 6)
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

CELL, MARGIN, TOP = 64, 20, 72
W = MARGIN * 2 + CELL * 10
H = TOP + CELL * 10 + MARGIN


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
            f = ImageFont.truetype(p, size)
            break
        except Exception:
            continue
    if f is None:
        f = ImageFont.load_default()
    _fonts[key] = f
    return f


def _cell_xy(n):
    """Centre of square n (1..100). Row 0 is the bottom, numbering snakes."""
    i = n - 1
    row, col = divmod(i, 10)
    if row % 2 == 1:
        col = 9 - col
    return (MARGIN + col * CELL + CELL / 2, TOP + (9 - row) * CELL + CELL / 2)


def _cubic(p0, p3, k=0.28, n=28):
    """S-shaped curve between two points - a snake."""
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
    L = math.hypot(x2 - x1, y2 - y1) or 1
    px, py = -(y2 - y1) / L * 8, (x2 - x1) / L * 8
    rail = (140, 92, 46)
    for s in (1, -1):
        d.line([(x1 + px * s, y1 + py * s), (x2 + px * s, y2 + py * s)], fill=rail, width=5)
    steps = max(2, int(L / 18))
    for i in range(1, steps):
        t = i / steps
        cx, cy = x1 + (x2 - x1) * t, y1 + (y2 - y1) * t
        d.line([(cx + px, cy + py), (cx - px, cy - py)], fill=(190, 135, 70), width=4)


def _draw_snake(d, head, tail):
    pts = _cubic(_cell_xy(head), _cell_xy(tail))
    d.line(pts, fill=(28, 96, 52), width=11, joint="curve")
    d.line(pts, fill=(92, 184, 104), width=6, joint="curve")
    hx, hy = pts[0]
    d.ellipse([hx - 11, hy - 11, hx + 11, hy + 11], fill=(28, 96, 52))
    d.ellipse([hx - 9, hy - 9, hx + 9, hy + 9], fill=(92, 184, 104))
    for ex in (-4, 4):
        d.ellipse([hx + ex - 2, hy - 3, hx + ex + 2, hy + 1], fill=(20, 20, 20))
    tx, ty = pts[-1]
    d.ellipse([tx - 4, ty - 4, tx + 4, ty + 4], fill=(28, 96, 52))


_snl_base = None


def _snl_board() -> Image.Image:
    """The static part - squares, ladders, snakes - drawn once and reused.
    Each turn only the title strip, highlight and tokens are painted on a copy,
    which keeps a move at ~15 ms instead of ~100 ms."""
    global _snl_base
    if _snl_base is not None:
        return _snl_base.copy()
    img = Image.new("RGB", (W, H), (246, 241, 231))
    d = ImageDraw.Draw(img)
    f12 = _font(12, bold=False)
    for n in range(1, 101):
        cx, cy = _cell_xy(n)
        x0, y0 = cx - CELL / 2, cy - CELL / 2
        row = (n - 1) // 10
        col = (n - 1) % 10
        light = (row + col) % 2 == 0
        fill = (255, 250, 240) if light else (233, 223, 203)
        if n == 100:
            fill = (255, 214, 92)
        elif n == 1:
            fill = (200, 232, 205)
        d.rectangle([x0, y0, x0 + CELL, y0 + CELL], fill=fill, outline=(205, 196, 180))
        d.text((x0 + 4, y0 + 2), str(n), font=f12, fill=(110, 100, 90))
        if n == 100:
            d.text((x0 + 12, y0 + 36), "FINISH", font=_font(13), fill=(120, 80, 0))
        elif n == 1:
            d.text((x0 + 14, y0 + 36), "START", font=_font(13), fill=(30, 100, 50))
    for a, b in LADDERS.items():
        _draw_ladder(d, a, b)
    for a, b in SNAKES.items():
        _draw_snake(d, a, b)
    _snl_base = img
    return img.copy()


def render_snl(g) -> bytes:
    img = _snl_board()
    d = ImageDraw.Draw(img)
    # title strip
    d.rectangle([0, 0, W, TOP - 8], fill=(31, 36, 48))
    d.text((MARGIN, 18), "SNAKE & LADDER", font=_font(26), fill=(255, 255, 255))
    if g["phase"] == "lobby":
        status = f"Waiting {len(g['players'])}/{g['want']}"
    elif g["phase"] == "over":
        status = f"{g['winner']['name']} wins!"
    else:
        status = f"Turn: {_current(g)['name']}"
    f15 = _font(16)
    tw = d.textlength(status, font=f15)
    d.text((W - MARGIN - tw, 8), status, font=f15, fill=(255, 220, 120))
    # highlight where the last move landed
    last = g.get("last")
    if last and last["to"] > 0:
        cx, cy = _cell_xy(last["to"])
        d.rectangle([cx - CELL / 2 + 1, cy - CELL / 2 + 1, cx + CELL / 2 - 1, cy + CELL / 2 - 1],
                    outline=(255, 170, 0), width=3)
    # tokens
    offs = [(-13, -13), (13, -13), (-13, 13), (13, 13), (0, -13), (0, 13)]
    f11 = _font(11)
    bench_x = W - MARGIN - 14
    for p in g["players"]:
        rgb = COLORS[p["c"]][2]
        if p["pos"] <= 0:
            # not on the board yet: sits on the bench in the title strip
            x, y = bench_x, TOP - 22
            bench_x -= 28
        else:
            cx, cy = _cell_xy(p["pos"])
            ox, oy = offs[p["c"] % len(offs)]
            x, y = cx + ox, cy + oy
        r = 11
        d.ellipse([x - r - 2, y - r - 2, x + r + 2, y + r + 2], fill=(255, 255, 255))
        d.ellipse([x - r, y - r, x + r, y + r], fill=rgb)
        ch = p["name"].split()[-1] if p["bot"] else (p["name"][:1] or "?").upper()
        tw = d.textlength(ch, font=f11)
        d.text((x - tw / 2, y - 7), ch, font=f11, fill=(255, 255, 255))
    if g["phase"] == "lobby":
        f = _font(30)
        msg = "Waiting for players"
        tw = d.textlength(msg, font=f)
        bx, by = W / 2 - tw / 2 - 24, TOP + CELL * 4.2
        d.rounded_rectangle([bx, by, bx + tw + 48, by + 60], radius=14, fill=(31, 36, 48))
        d.text((W / 2 - tw / 2, by + 13), msg, font=f, fill=(255, 220, 120))
    buf = io.BytesIO()
    img.save(buf, "PNG", compress_level=6)
    return buf.getvalue()


RENDER = {"snl": render_snl}


# ── pushing the board to Telegram ──────────────────────────────────────────

def _push(g, kb_only=False):
    """Redraw and swap the board into the game's message. One message per game."""
    with _lock:
        cap = _caption(g)
        kb = _keyboard(g)
        png = None if kb_only else RENDER[g["kind"]](g)
        chat, mid = g["chat"], g["msg_id"]
    if kb_only and mid:
        _api("editMessageReplyMarkup", {"chat_id": chat, "message_id": mid, "reply_markup": kb})
        return
    if mid:
        j = _api("editMessageMedia",
                 {"chat_id": chat, "message_id": mid,
                  "media": json.dumps({"type": "photo", "media": "attach://board",
                                       "caption": cap, "parse_mode": "HTML"}),
                  "reply_markup": json.dumps(kb)},
                 files={"board": ("board.png", png)})
        if j.get("ok"):
            return
        desc = str(j.get("description", ""))
        if "not modified" in desc or "retry after" in desc:
            return
        # message gone (user deleted it) - fall through and post a fresh one
    j = _api("sendPhoto", {"chat_id": chat, "caption": cap, "parse_mode": "HTML",
                           "reply_markup": json.dumps(kb)},
             files={"photo": ("board.png", png)})
    new_id = (j.get("result") or {}).get("message_id")
    if new_id:
        with _lock:
            g["msg_id"] = new_id


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
                time.sleep(ROBOT_DELAY)
                with _lock:
                    g2 = _games.get(str(chat))
                    if not g2 or g2 is not g or g2["phase"] != "play":
                        break
                    cur2 = _current(g2)
                    if not cur2 or not cur2["bot"]:
                        break
                    _do_roll(g2)
                _push(g)
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
            _do_roll(g)
        _push(g)
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
                      "winner": None, "created": time.time(), "updated": time.time(),
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
                            _do_roll(g, auto=True)
                        _push(g)
                        _robots_go(chat)
                elif g["phase"] == "over" and now - g["updated"] > FINISHED_KEEP:
                    with _lock:
                        _games.pop(chat, None)
        except Exception as e:
            print(f"  [GAMES] watchdog: {e}")
