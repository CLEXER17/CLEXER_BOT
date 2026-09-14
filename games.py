#!/usr/bin/env python3
"""games.py - chat games for CLEXER BOT.

Everything a game needs lives here; bot.py only forwards three things:
    /games                         -> cmd_games(chat_id, chat_type)
    callback data "game:..."       -> on_callback(...)  (returns popup text or None)
    typed text in a chat with a game that wants it -> on_text(...)

How a game runs
    /games -> pick a game -> pick how many players (2/3/4/6)
    DM     : every other seat is a robot, the game starts at once
    group  : a lobby photo with Join / Add robot / Cancel; starts when full
    Each turn the board is REDRAWN (Pillow, no AI, no cost) and swapped into the
    same message with editMessageMedia - one message per game, never a stream.
    Only the seated players can press anything; everyone else gets a popup.
    Quit asks "are you sure?" first; the game carries on for the others.

Adding a game = one Spec subclass (see the ones at the bottom): it says how
many can play, draws its keyboard and board, applies one action, and picks a
move for a robot. The framework does lobbies, turns, quitting, robots,
timeouts, upload-first preloading and the menu.

State is in memory: one game per chat, keyed by chat id. Robots take their
turns from a small background thread with a pause between moves so a human
can follow. A watchdog moves for a human who idles 90 s in a group, cancels
a lobby nobody joined after 5 min, and clears finished boards.

Upload first, swap second: when it is a human's turn in a dice game the
dice are rolled in secret, the resulting board drawn and parked in the store
chat (GAMES_STORE_CHAT); the tap then only references that file_id. Robots
do the same during their pause. Off until a store chat is set.

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
TURN_SECS = 90             # group only: idle this long and the bot moves for you
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

GAMES: dict = {}           # key -> Spec instance, in menu order


# ── wiring ─────────────────────────────────────────────────────────────────

def init(token: str, store_chat=None):
    """Called once from bot.py after the token is known."""
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
    keys = list(GAMES)
    rows = []
    for i in range(0, len(keys), 2):
        rows.append([{"text": GAMES[k].title, "callback_data": f"game:pick:{k}"} for k in keys[i:i + 2]])
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
        chat_type = "private" if not str(chat_id).startswith("-") else "group"
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
           for n in SIZES if spec.min <= n <= spec.max]
    return {"inline_keyboard": [row, [{"text": "◀️ Back", "callback_data": "game:menu"}]]}


# ── game state ─────────────────────────────────────────────────────────────

def _new_game(kind, chat, chat_type, host_id, host_name):
    return {"kind": kind, "chat": str(chat), "chat_type": chat_type, "host": str(host_id),
            "msg_id": None, "phase": "lobby", "want": 0, "players": [], "turn": 0,
            "log": [], "confirm": None, "winner": None, "draw": False, "busy": False,
            "ver": 0, "pre": None, "st": {},
            "created": time.time(), "updated": time.time(), "turn_at": time.time()}


def _seat(g, uid, name, bot=False):
    idx = len(g["players"])
    g["players"].append({"id": str(uid), "name": str(name)[:20], "pos": 0, "c": idx,
                         "bot": bot, "out": False})
    _bump(g)


def _find(g, uid):
    for p in g["players"]:
        if p["id"] == str(uid):
            return p
    return None


def _current(g):
    return g["players"][g["turn"]] if g["players"] else None


def _alive(g):
    return [p for p in g["players"] if not p["out"]]


def _tag(p):
    return f"{COLORS[p['c']][0]} {_esc(p['name'])}"


def _bump(g):
    g["ver"] += 1
    g["updated"] = g["turn_at"] = time.time()


def _log(g, line):
    g["log"] = (g["log"] + [line])[-3:]


def _advance(g):
    """Next player who is still in."""
    n = len(g["players"])
    for _ in range(n):
        g["turn"] = (g["turn"] + 1) % n
        if not g["players"][g["turn"]]["out"]:
            break


def _win(g, p):
    g["phase"] = "over"
    g["winner"] = p
    g["draw"] = False


def _draw_game(g):
    g["phase"] = "over"
    g["winner"] = None
    g["draw"] = True


def _spec(g):
    return GAMES[g["kind"]]


def _start(g):
    for p in g["players"]:
        p["pos"] = 0
        p["out"] = False
    g.update({"phase": "play", "turn": 0, "log": [], "confirm": None, "winner": None,
              "draw": False, "pre": None, "st": {}})
    _spec(g).start(g)
    _bump(g)


def _remove_player(g, uid, why="left the game"):
    p = _find(g, uid)
    if not p:
        return
    i = g["players"].index(p)
    g["players"].remove(p)
    _log(g, f"{_tag(p)} {why}")
    if g["phase"] == "play":
        if i < g["turn"]:
            g["turn"] -= 1
        if g["players"]:
            g["turn"] %= len(g["players"])
            if g["players"][g["turn"]]["out"]:
                _advance(g)
        _spec(g).left(g, p)
        alive = _alive(g)
        if len(alive) == 1:
            _win(g, alive[0])
            _log(g, f"{_tag(alive[0])} wins - everyone else left")
    _bump(g)


# ── captions & keyboards ───────────────────────────────────────────────────

def _caption(g):
    spec = _spec(g)
    out = [f"{_esc(spec.title)}"]
    if g["phase"] == "lobby":
        out.append(f"Host: {_tag(g['players'][0])}")
        out.append(f"\nPlayers {len(g['players'])}/{g['want']}:")
        for p in g["players"]:
            out.append(_tag(p) + (" 🤖" if p["bot"] else ""))
        for _ in range(g["want"] - len(g["players"])):
            out.append("⚪ waiting…")
        out.append("\nTap <b>Join</b> to sit down. The host can add robots or cancel.")
        return "\n".join(out)
    body = spec.caption(g)
    if body:
        out.append("")
        out.extend(body)
    if g["log"]:
        out.append("")
        out.extend(g["log"])
    out.append("")
    if g["phase"] == "over":
        if g["winner"]:
            out.append(f"🏆 <b>{_esc(g['winner']['name'])} wins!</b>")
        else:
            out.append("🤝 <b>Draw - nobody wins.</b>")
    else:
        out.append(spec.turn_text(g))
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
    rows = [[{"text": t, "callback_data": f"game:a:{a}"} for t, a in row] for row in _spec(g).keyboard(g)]
    rows.append([{"text": "Quit", "callback_data": "game:quit", "style": "danger"}])
    return {"inline_keyboard": rows}


# ── drawing toolkit ────────────────────────────────────────────────────────
# Everything is drawn at 2x (S) and downsampled with Lanczos at the end -
# Pillow has no anti-aliasing of its own, so this is what turns jagged
# circles and lines into smooth ones.

S = 2
OUT_W = 800
TOP = 96
FELT = (24, 84, 58)
FRAME = (92, 58, 30)
INK = (98, 84, 70)
CELL_A, CELL_B = (252, 246, 232), (232, 219, 190)
GOLD = (255, 226, 150)


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


def _sx(v):
    return v * S


def _shade(rgb, k):
    if k >= 0:
        return tuple(int(c + (255 - c) * k) for c in rgb)
    return tuple(int(c * (1 + k)) for c in rgb)


_noise_cache: dict = {}


def _canvas(w, h, title, status):
    """Felt table + dark title strip, at 2x. Returns (img, draw)."""
    img = Image.new("RGB", (w * S, h * S), FELT)
    key = (w, h)
    if key not in _noise_cache:
        _noise_cache[key] = Image.effect_noise((w * S, h * S), 12).convert("L").point(lambda v: 140 + v // 3)
    img = Image.composite(img, Image.new("RGB", img.size, _shade(FELT, -0.25)), _noise_cache[key])
    d = ImageDraw.Draw(img, "RGBA")
    for i in range(TOP * S - 14 * S):
        k = i / (TOP * S)
        d.line([(0, i), (w * S, i)], fill=(int(22 + 14 * k), int(26 + 14 * k), int(36 + 18 * k)))
    d.line([(0, TOP * S - 14 * S), (w * S, TOP * S - 14 * S)], fill=(214, 172, 80), width=2 * S)
    d.text((_sx(28) + 2 * S, 22 * S + 2 * S), title, font=_font(30), fill=(0, 0, 0, 140))
    d.text((_sx(28), 22 * S), title, font=_font(30), fill=GOLD)
    f16 = _font(17)
    tw = d.textlength(status, font=f16)
    d.text((_sx(w - 28) - tw, 10 * S), status, font=f16, fill=(255, 255, 255))
    return img, d


def _finish(img, w, h):
    out = img.resize((OUT_W, int(h * OUT_W / w)), Image.LANCZOS)
    buf = io.BytesIO()
    out.save(buf, "JPEG", quality=84)
    return buf.getvalue()


def _frame(d, x0, y0, x1, y1, pad=12):
    """Wooden frame around a board (logical coords)."""
    bx0, by0, bx1, by1 = _sx(x0 - pad), _sx(y0 - pad), _sx(x1 + pad), _sx(y1 + pad)
    d.rounded_rectangle([bx0 + 8 * S, by0 + 10 * S, bx1 + 8 * S, by1 + 10 * S], radius=10 * S, fill=(0, 0, 0, 110))
    d.rounded_rectangle([bx0, by0, bx1, by1], radius=10 * S, fill=FRAME)
    for i in range(0, pad * S, 3):
        d.rounded_rectangle([bx0 + i, by0 + i, bx1 - i, by1 - i], radius=10 * S,
                            outline=_shade(FRAME, 0.10 + 0.02 * (i % 4)), width=1)
    d.rounded_rectangle([bx0, by0, bx1, by1], radius=10 * S, outline=_shade(FRAME, 0.35), width=2 * S)


def _panel(d, x0, y0, x1, y1, fill=(31, 36, 48), radius=14, outline=(214, 172, 80)):
    d.rounded_rectangle([_sx(x0) + 4 * S, _sx(y0) + 6 * S, _sx(x1) + 4 * S, _sx(y1) + 6 * S],
                        radius=radius * S, fill=(0, 0, 0, 120))
    d.rounded_rectangle([_sx(x0), _sx(y0), _sx(x1), _sx(y1)], radius=radius * S, fill=fill,
                        outline=outline, width=2 * S if outline else 0)


def _cell(d, x0, y0, size, fill, bevel=True):
    x0, y0 = _sx(x0), _sx(y0)
    x1, y1 = x0 + size * S, y0 + size * S
    d.rectangle([x0, y0, x1, y1], fill=fill)
    if bevel:
        d.line([(x0, y1), (x0, y0), (x1, y0)], fill=_shade(fill, 0.55), width=2 * S)
        d.line([(x1, y0), (x1, y1), (x0, y1)], fill=_shade(fill, -0.14), width=2 * S)


def _txt(d, x, y, text, size=16, fill=(255, 255, 255), anchor="mm", bold=True, shadow=False):
    f = _font(size, bold)
    if shadow:
        d.text((_sx(x) + 2 * S, _sx(y) + 2 * S), text, font=f, fill=(0, 0, 0, 140), anchor=anchor)
    d.text((_sx(x), _sx(y)), text, font=f, fill=fill, anchor=anchor)


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
    f = _font(int(r / S * 0.85))
    d.text((x + 1 * S, y + 1 * S), label, font=f, fill=(0, 0, 0, 120), anchor="mm")
    d.text((x, y), label, font=f, fill=(255, 255, 255), anchor="mm")


def _plabel(p):
    return p["name"].split()[-1] if p["bot"] else (p["name"][:1] or "?").upper()


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


def _bar(d, x0, y0, w, h, frac, rgb):
    d.rounded_rectangle([_sx(x0), _sx(y0), _sx(x0 + w), _sx(y0 + h)], radius=6 * S, fill=(20, 24, 30))
    if frac > 0:
        d.rounded_rectangle([_sx(x0) + 2 * S, _sx(y0) + 2 * S, _sx(x0 + 2 + (w - 4) * frac), _sx(y0 + h) - 2 * S],
                            radius=5 * S, fill=rgb)
        d.rounded_rectangle([_sx(x0) + 2 * S, _sx(y0) + 2 * S, _sx(x0 + 2 + (w - 4) * frac), _sx(y0) + h * S * 0.45],
                            radius=5 * S, fill=_shade(rgb, 0.35))


def _score_rows(d, g, x, y, w, extra=None, rowh=44):
    """Player list panel: token, name, and an optional right-hand value."""
    n = len(g["players"])
    _panel(d, x, y, x + w, y + 14 + rowh * n)
    for i, p in enumerate(g["players"]):
        cy = y + 14 + rowh * i + rowh / 2
        _token(d, x + 26, cy, COLORS[p["c"]][2], _plabel(p), r=13)
        name = p["name"] + ("  (out)" if p["out"] else "")
        col = (150, 150, 150) if p["out"] else (255, 255, 255)
        if g["phase"] == "play" and _current(g) is p and not _spec(g).simultaneous:
            col = GOLD
        _txt(d, x + 50, cy, name, 17, col, anchor="lm")
        if extra:
            _txt(d, x + w - 16, cy, str(extra(p)), 18, GOLD, anchor="rm")


# ── pushing the board to Telegram ──────────────────────────────────────────

def _upload(png):
    """Park a board in the store chat and return its file_id (or None)."""
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


def _preview(g, pid, action, secret):
    """Board as it will look after player pid does action - on a copy."""
    with _lock:
        g2 = copy.deepcopy({k: v for k, v in g.items() if k != "pre"})
    g2["pre"] = None
    p2 = _find(g2, pid)
    _spec(g2).act(g2, p2, action, secret)
    return _spec(g2).render(g2)


def _prepare(g):
    """Roll a human's dice in secret and park the board before they tap."""
    if not (PRELOAD and _STORE):
        return
    with _lock:
        spec = _spec(g)
        if g["phase"] != "play" or spec.simultaneous:
            return
        cur = _current(g)
        if not cur or cur["bot"]:
            return
        secret = spec.secret(g, cur)
        if secret is None:
            return
        ver = g["ver"]
        if g["pre"] and g["pre"]["ver"] == ver:
            return
        action = spec.default_action
        g["pre"] = {"ver": ver, "action": action, "secret": None, "file_id": None, "png": None}
        pid = cur["id"]

    def run():
        try:
            png = _preview(g, pid, action, secret)
            fid = _upload(png)
        except Exception as e:
            print(f"  [GAMES] prepare: {e}")
            png = fid = None
        with _lock:
            if g["ver"] == ver:
                g["pre"] = {"ver": ver, "action": action, "secret": secret, "file_id": fid, "png": png}
    threading.Thread(target=run, daemon=True, name="games-prepare").start()


def _take_pre(g, action):
    pre = g.get("pre")
    g["pre"] = None
    if pre and pre["ver"] == g["ver"] and pre["secret"] is not None and pre["action"] == action:
        return pre
    return None


def _render_lobby(g):
    spec = _spec(g)
    img, d = _canvas(spec.W, 420, spec.title.split(" ", 1)[-1].upper(), f"Waiting {len(g['players'])}/{g['want']}")
    _panel(d, 60, TOP + 30, spec.W - 60, TOP + 110)
    _txt(d, spec.W / 2, TOP + 70, "Waiting for players", 32, GOLD)
    for i, p in enumerate(g["players"]):
        _token(d, 100, TOP + 160 + i * 42, COLORS[p["c"]][2], _plabel(p), r=15)
        _txt(d, 130, TOP + 160 + i * 42, p["name"] + ("  (robot)" if p["bot"] else ""), 18, (255, 255, 255), anchor="lm")
    for i in range(len(g["players"]), g["want"]):
        d.ellipse([_sx(85), _sx(TOP + 145 + i * 42), _sx(115), _sx(TOP + 175 + i * 42)], outline=(150, 150, 160), width=3 * S)
        _txt(d, 130, TOP + 160 + i * 42, "waiting…", 18, (150, 150, 160), anchor="lm")
    return _finish(img, spec.W, 420)


def _render(g):
    return _render_lobby(g) if g["phase"] == "lobby" else _spec(g).render(g)


def _push(g, kb_only=False, file_id=None, png=None):
    """Redraw and swap the board into the game's message."""
    with _lock:
        cap = _caption(g)
        kb = _keyboard(g)
        if not kb_only and png is None and not file_id:
            png = _render(g)
        chat, mid = g["chat"], g["msg_id"]
    if kb_only:
        if mid:
            _api("editMessageReplyMarkup", {"chat_id": chat, "message_id": mid, "reply_markup": kb})
        return
    if mid and file_id:
        j = _api("editMessageMedia",
                 {"chat_id": chat, "message_id": mid,
                  "media": {"type": "photo", "media": file_id, "caption": cap, "parse_mode": "HTML"},
                  "reply_markup": kb})
        if j.get("ok"):
            _prepare(g)
            return
        if "retry after" in str(j.get("description", "")):
            return
        if png is None:
            with _lock:
                png = _render(g)
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
    j = _api("sendPhoto", {"chat_id": chat, "caption": cap, "parse_mode": "HTML",
                           "reply_markup": json.dumps(kb)},
             files={"photo": ("board.jpg", png)})
    new_id = (j.get("result") or {}).get("message_id")
    if new_id:
        with _lock:
            g["msg_id"] = new_id
    _prepare(g)


def _robots_go(chat):
    """Let robots move, one every ROBOT_DELAY seconds (all at once in
    simultaneous games such as Rock Paper Scissors)."""
    with _lock:
        g = _games.get(str(chat))
        if not g or g["phase"] != "play" or g["busy"]:
            return
        spec = _spec(g)
        if not any(p["bot"] and spec.can_act(g, p) for p in g["players"]):
            return
        g["busy"] = True

    def run():
        try:
            while True:
                t0 = time.time()
                with _lock:
                    g2 = _games.get(str(chat))
                    if not g2 or g2 is not g or g2["phase"] != "play":
                        break
                    spec = _spec(g)
                    bots = [p for p in g["players"] if p["bot"] and spec.can_act(g, p)]
                    if not bots:
                        break
                    ver = g["ver"]
                    if spec.simultaneous:
                        for p in bots:
                            spec.act(g, p, spec.robot(g, p), spec.secret(g, p))
                        _bump(g)
                        moved = True
                    else:
                        moved = False
                        p = bots[0]
                        action = spec.robot(g, p)
                        secret = spec.secret(g, p)
                        pid = p["id"]
                if moved:
                    time.sleep(max(0.0, ROBOT_DELAY / 2 - (time.time() - t0)))
                    with _lock:
                        if _games.get(str(chat)) is not g:
                            break
                    _push(g)
                else:
                    fid = png = None
                    if PRELOAD and _STORE:
                        try:
                            png = _preview(g, pid, action, secret)
                            fid = _upload(png)
                        except Exception as e:
                            print(f"  [GAMES] robot prepare: {e}")
                            fid = png = None
                    time.sleep(max(0.0, ROBOT_DELAY - (time.time() - t0)))
                    with _lock:
                        g2 = _games.get(str(chat))
                        if not g2 or g2 is not g or g2["phase"] != "play":
                            break
                        p = _find(g, pid)
                        if not p or not spec.can_act(g, p):
                            continue
                        if g["ver"] != ver:              # state changed under us - redo the choice
                            fid = png = None
                            action = spec.robot(g, p)
                            secret = spec.secret(g, p)
                        spec.act(g, p, action, secret)
                        _bump(g)
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
        if not j.get("ok"):
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
                                 "text": f"{_esc(spec.title)}\n{_esc(spec.blurb)}\n\nHow many players?\n{hint}",
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
        spec = GAMES[kind]
        if not (spec.min <= n <= spec.max):
            return f"{spec.title} takes {spec.min}-{spec.max} players."
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
                _start(g)
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
    spec = _spec(g)

    if act == "join":
        if g["phase"] != "lobby":
            return "This game already started."
        if me:
            return "You're already in."
        with _lock:
            _seat(g, uid, name)
            full = len(g["players"]) >= g["want"]
            if full:
                _start(g)
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
                _start(g)
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
                                    "caption": f"{_esc(spec.title)}\n\n❌ Cancelled by the host.",
                                    "parse_mode": "HTML"})
        return None

    if not me:
        return "You're not in this game."

    if act == "a":
        action = ":".join(parts[2:])
        if g["phase"] != "play":
            return "The game is over - tap Play again."
        if g["confirm"]:
            return "Answer the quit question first."
        if me["out"]:
            return "You're out of this round."
        if not spec.can_act(g, me):
            return spec.deny(g, me)
        with _lock:
            pre = _take_pre(g, action)
            secret = pre["secret"] if pre else spec.secret(g, me)
            popup = spec.act(g, me, action, secret)
            if popup:
                return popup
            _bump(g)
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
                _games.pop(chat, None)
        if not humans:
            _api("editMessageCaption", {"chat_id": chat, "message_id": g["msg_id"],
                                        "caption": f"{_esc(spec.title)}\n\n"
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
            _start(g)
            g["created"] = time.time()
        _push(g)
        _robots_go(chat)
        return None

    return None


# ── typed input (number guessing etc.) ─────────────────────────────────────

def wants_text(chat_id) -> bool:
    g = _games.get(str(chat_id))
    return bool(g and g["phase"] == "play" and _spec(g).uses_text)


def on_text(chat_id, uid, name, text) -> bool:
    """True when the text was consumed by the game."""
    chat = str(chat_id)
    with _lock:
        g = _games.get(chat)
        if not g or g["phase"] != "play":
            return False
        me = _find(g, str(uid))
        if not me or me["out"] or g["confirm"]:
            return False
        spec = _spec(g)
        if not spec.can_act(g, me):
            return False
        changed = spec.on_text(g, me, text.strip())
        if changed:
            _bump(g)
    if changed:
        _push(g)
        _robots_go(chat)
    return bool(changed)


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
                                                "caption": f"{_esc(_spec(g).title)}\n\n"
                                                           "⌛ Nobody joined - lobby closed.",
                                                "parse_mode": "HTML"})
                elif g["phase"] == "play":
                    if now - g["updated"] > 6 * 3600:
                        with _lock:
                            _games.pop(chat, None)
                        continue
                    if g["chat_type"] == "private" or g["confirm"] or now - g["turn_at"] <= TURN_SECS:
                        continue
                    spec = _spec(g)
                    idle = [p for p in g["players"] if not p["bot"] and spec.can_act(g, p)]
                    if not idle:
                        continue
                    with _lock:
                        for p in idle:
                            spec.act(g, p, spec.robot(g, p), spec.secret(g, p))
                            _log(g, f"⏱ {_tag(p)} idle - moved for them")
                        _bump(g)
                    _push(g)
                    _robots_go(chat)
                elif g["phase"] == "over" and now - g["updated"] > FINISHED_KEEP:
                    with _lock:
                        _games.pop(chat, None)
        except Exception as e:
            print(f"  [GAMES] watchdog: {e}")


# ═══════════════════════════════════════════════════════════════════════════
#  THE GAMES
# ═══════════════════════════════════════════════════════════════════════════

class Spec:
    key = ""
    title = ""
    blurb = ""
    min = 2
    max = 6
    W = 776                 # logical canvas size
    H = 700
    simultaneous = False    # everyone acts once per round (RPS) vs one at a time
    uses_text = False       # accepts typed messages (number guessing)
    default_action = ""     # the action a secret is prepared for (dice games)
    hint = "make your move"

    def start(self, g): pass
    def caption(self, g): return []
    def keyboard(self, g): return []
    def can_act(self, g, p): return g["phase"] == "play" and _current(g) is p
    def deny(self, g, p):
        cur = _current(g)
        return f"Not your turn - it's {cur['name']}'s." if cur else "Not now."
    def act(self, g, p, action, secret=None): return "Nothing happened."
    def robot(self, g, p): return self.default_action
    def secret(self, g, p): return None
    def on_text(self, g, p, text): return False
    def left(self, g, p): pass
    def turn_text(self, g):
        if self.simultaneous:
            w = [p for p in g["players"] if self.can_act(g, p)]
            return "👉 Waiting for: " + ", ".join(_tag(p) for p in w)
        cur = _current(g)
        if cur["bot"]:
            return f"🤖 {_tag(cur)} is thinking…"
        return f"👉 Turn: {_tag(cur)} - {self.hint}"
    def render(self, g): return b""


def _register(cls):
    GAMES[cls.key] = cls()
    return cls


def _scores(g):
    return g["st"].setdefault("score", {p["id"]: 0 for p in g["players"]})


# ── 1. Snake & Ladder ───────────────────────────────────────────────────────

LADDERS = {4: 14, 9: 31, 20: 38, 28: 84, 40: 59, 51: 67, 63: 81, 71: 91}
SNAKES = {17: 7, 54: 34, 62: 19, 64: 60, 87: 24, 93: 73, 95: 75, 99: 78}
_SNL_CELL, _SNL_M = 72, 28
_snl_base = None


def _snl_xy(n):
    i = n - 1
    row, col = divmod(i, 10)
    if row % 2 == 1:
        col = 9 - col
    return (_SNL_M + col * _SNL_CELL + _SNL_CELL / 2, TOP + (9 - row) * _SNL_CELL + _SNL_CELL / 2)


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
        pts.append((u ** 3 * p0[0] + 3 * u * u * t * c1[0] + 3 * u * t * t * c2[0] + t ** 3 * p3[0],
                    u ** 3 * p0[1] + 3 * u * u * t * c1[1] + 3 * u * t * t * c2[1] + t ** 3 * p3[1]))
    return pts


def _draw_ladder(d, a, b):
    (x1, y1), (x2, y2) = _snl_xy(a), _snl_xy(b)
    x1, y1, x2, y2 = _sx(x1), _sx(y1), _sx(x2), _sx(y2)
    L = math.hypot(x2 - x1, y2 - y1) or 1
    px, py = -(y2 - y1) / L * 9 * S, (x2 - x1) / L * 9 * S
    dark, mid, hi = (96, 58, 26), (160, 104, 48), (214, 160, 92)
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
    (hx0, hy0), (tx0, ty0) = _snl_xy(head), _snl_xy(tail)
    pts = _cubic((hx0, hy0), (tx0, ty0), n=max(60, int(math.hypot(tx0 - hx0, ty0 - hy0) / 1.5)))
    pts = [(_sx(x), _sx(y)) for x, y in pts]
    n = len(pts)
    body_dark, body, belly, spot = (26, 92, 48), (72, 168, 92), (150, 214, 120), (20, 70, 36)

    def _dots(dx, dy, base, shrink, color):
        for i in range(n):
            r = (base - shrink * (i / n)) * S / 2
            x, y = pts[i][0] + dx, pts[i][1] + dy
            d.ellipse([x - r, y - r, x + r, y + r], fill=color)
    _dots(4 * S, 5 * S, 17, 12, (0, 0, 0, 55))
    _dots(0, 0, 17, 12, body_dark)
    _dots(0, 0, 13, 9, body)
    _dots(-1 * S, -2 * S, 6, 4, belly)
    for i in range(6, n - 6, max(4, n // 12)):
        x, y = pts[i]
        r = (4 - 2 * (i / n)) * S
        d.ellipse([x - r, y - r, x + r, y + r], fill=spot)
    hx, hy = pts[0]
    nx, ny = pts[1]
    ang = math.atan2(ny - hy, nx - hx)
    r = 13 * S
    d.ellipse([hx - r - 2 * S, hy - r - 2 * S, hx + r + 2 * S, hy + r + 2 * S], fill=body_dark)
    d.ellipse([hx - r, hy - r, hx + r, hy + r], fill=body)
    d.ellipse([hx - r + 3 * S, hy - r + 3 * S, hx + r - 5 * S, hy - 1 * S], fill=_shade(body, 0.25))
    ex, ey = -math.sin(ang) * 6 * S, math.cos(ang) * 6 * S
    for s in (1, -1):
        cx, cy = hx + ex * s - math.cos(ang) * 2 * S, hy + ey * s - math.sin(ang) * 2 * S
        d.ellipse([cx - 3.2 * S, cy - 3.2 * S, cx + 3.2 * S, cy + 3.2 * S], fill=(250, 250, 240))
        d.ellipse([cx - 1.6 * S, cy - 1.6 * S, cx + 1.6 * S, cy + 1.6 * S], fill=(10, 10, 10))
    tx, ty = hx - math.cos(ang) * r, hy - math.sin(ang) * r
    fx, fy = hx - math.cos(ang) * (r + 12 * S), hy - math.sin(ang) * (r + 12 * S)
    d.line([(tx, ty), (fx, fy)], fill=(214, 40, 60), width=2 * S)
    for s in (1, -1):
        d.line([(fx, fy), (fx - math.cos(ang) * 5 * S - math.sin(ang) * 4 * S * s,
                           fy - math.sin(ang) * 5 * S + math.cos(ang) * 4 * S * s)],
               fill=(214, 40, 60), width=2 * S)
    tx, ty = pts[-1]
    d.ellipse([tx - 3 * S, ty - 3 * S, tx + 3 * S, ty + 3 * S], fill=body_dark)


def _snl_board():
    global _snl_base
    if _snl_base is not None:
        return _snl_base.copy()
    img, d = _canvas(776, TOP + _SNL_CELL * 10 + 28, "", "")
    _frame(d, _SNL_M, TOP, _SNL_M + _SNL_CELL * 10, TOP + _SNL_CELL * 10)
    for n in range(1, 101):
        cx, cy = _snl_xy(n)
        row, col = divmod(n - 1, 10)
        fill = CELL_A if (row + col) % 2 == 0 else CELL_B
        if n == 100:
            fill = (255, 208, 84)
        elif n == 1:
            fill = (188, 226, 196)
        _cell(d, cx - _SNL_CELL / 2, cy - _SNL_CELL / 2, _SNL_CELL, fill)
        _txt(d, cx - _SNL_CELL / 2 + 6, cy - _SNL_CELL / 2 + 4, str(n), 13, INK, anchor="la")
        if n == 100:
            _txt(d, cx, cy + 16, "FINISH", 14, (122, 78, 0))
        elif n == 1:
            _txt(d, cx, cy + 16, "START", 14, (28, 100, 52))
    for a, b in LADDERS.items():
        _draw_ladder(d, a, b)
    for a, b in SNAKES.items():
        _draw_snake(d, a, b)
    _snl_base = img
    return img.copy()


@_register
class SnakeLadder(Spec):
    key = "snl"
    title = "🐍 Snake & Ladder"
    blurb = "Roll, climb ladders, dodge snakes, first to 100 wins. 6 = roll again."
    min, max = 2, 6
    W, H = 776, TOP + _SNL_CELL * 10 + 28
    default_action = "roll"
    hint = "tap 🎲 Roll"

    def keyboard(self, g):
        return [[("🎲 Roll", "roll")]]

    def caption(self, g):
        return [f"{_tag(p)}{' 🤖' if p['bot'] else ''} · <b>{p['pos']}</b>"
                for p in sorted(g["players"], key=lambda q: -q["pos"])]

    def secret(self, g, p):
        return random.randint(1, 6)

    def act(self, g, p, action, secret=None):
        d = secret or random.randint(1, 6)
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
        g["st"]["last"] = {"c": p["c"], "roll": d, "to": to}
        line = f"{_tag(p)} rolled <b>{d}</b>"
        if ev == "stay":
            line += f" - needs exactly {100 - frm}, stays on {frm}"
        else:
            line += f" · {frm} → <b>{to}</b>"
            if ev == "ladder":
                line += " 🪜 ladder up!"
            elif ev == "snake":
                line += " 🐍 snake bite!"
        if again:
            line += " · rolls again"
        _log(g, line)
        if to == 100:
            _win(g, p)
        elif not again:
            _advance(g)
        return None

    def render(self, g):
        img = _snl_board()
        d = ImageDraw.Draw(img, "RGBA")
        W, H = self.W, self.H
        for i in range(TOP * S - 14 * S):
            k = i / (TOP * S)
            d.line([(0, i), (W * S, i)], fill=(int(22 + 14 * k), int(26 + 14 * k), int(36 + 18 * k)))
        d.line([(0, TOP * S - 14 * S), (W * S, TOP * S - 14 * S)], fill=(214, 172, 80), width=2 * S)
        _txt(d, 28, 22, "SNAKE & LADDER", 30, GOLD, anchor="la", shadow=True)
        status = (f"Waiting {len(g['players'])}/{g['want']}" if g["phase"] == "lobby" else
                  f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  f"Turn: {_current(g)['name']}" if g["phase"] == "play" else "")
        _txt(d, W - 28, 10, status, 17, (255, 255, 255), anchor="ra")
        last = g["st"].get("last")
        if last:
            _die(d, W / 2 - 24, 12, last["roll"], COLORS[last["c"]][2], size=48)
            cx, cy = _snl_xy(last["to"])
            for i in range(6, 0, -1):
                a = int(28 + 18 * (6 - i))
                d.rounded_rectangle([_sx(cx - _SNL_CELL / 2) - i * S, _sx(cy - _SNL_CELL / 2) - i * S,
                                     _sx(cx + _SNL_CELL / 2) + i * S, _sx(cy + _SNL_CELL / 2) + i * S],
                                    radius=4 * S, outline=(255, 190, 40, a), width=2 * S)
            d.rectangle([_sx(cx - _SNL_CELL / 2), _sx(cy - _SNL_CELL / 2), _sx(cx + _SNL_CELL / 2),
                         _sx(cy + _SNL_CELL / 2)], outline=(255, 170, 0), width=3 * S)
        offs = [(-15, -15), (15, -15), (-15, 15), (15, 15), (0, -17), (0, 17)]
        bench_x = W - 28 - 16
        for p in g["players"]:
            if p["pos"] <= 0:
                x, y = bench_x, TOP - 36
                bench_x -= 34
            else:
                cx, cy = _snl_xy(p["pos"])
                ox, oy = offs[p["c"] % 6]
                x, y = cx + ox, cy + oy
            _token(d, x, y, COLORS[p["c"]][2], _plabel(p))
        if g["phase"] == "lobby":
            _panel(d, W / 2 - 190, TOP + 300, W / 2 + 190, TOP + 370)
            _txt(d, W / 2, TOP + 335, "Waiting for players", 32, GOLD)
        return _finish(img, W, H)


# ── 2. Tic-Tac-Toe ─────────────────────────────────────────────────────────

_TTT_LINES = [(0, 1, 2), (3, 4, 5), (6, 7, 8), (0, 3, 6), (1, 4, 7), (2, 5, 8), (0, 4, 8), (2, 4, 6)]


@_register
class TicTacToe(Spec):
    key = "ttt"
    title = "❌⭕ Tic-Tac-Toe"
    blurb = "Three in a row wins. Red is ❌, green is ⭕."
    min = max = 2
    W, H = 640, 640
    hint = "tap a square"

    def start(self, g):
        g["st"] = {"b": [""] * 9, "line": None}

    def mark(self, g, p):
        return "X" if g["players"].index(p) == 0 else "O"

    def keyboard(self, g):
        b = g["st"]["b"]
        lab = {"X": "❌", "O": "⭕", "": "·"}
        return [[(lab[b[r * 3 + c]], f"c{r * 3 + c}") for c in range(3)] for r in range(3)]

    def caption(self, g):
        return [f"{_tag(p)} · {'❌' if i == 0 else '⭕'}" for i, p in enumerate(g["players"])]

    def act(self, g, p, action, secret=None):
        try:
            i = int(action[1:])
        except Exception:
            return "Tap a square."
        b = g["st"]["b"]
        if b[i]:
            return "That square is taken."
        m = self.mark(g, p)
        b[i] = m
        for ln in _TTT_LINES:
            if all(b[k] == m for k in ln):
                g["st"]["line"] = ln
                _log(g, f"{_tag(p)} makes three in a row!")
                _win(g, p)
                return None
        if all(b):
            _log(g, "Board full.")
            _draw_game(g)
            return None
        _advance(g)
        return None

    def robot(self, g, p):
        b = g["st"]["b"]
        me = self.mark(g, p)
        other = "O" if me == "X" else "X"
        for m in (me, other):
            for ln in _TTT_LINES:
                vals = [b[k] for k in ln]
                if vals.count(m) == 2 and "" in vals:
                    return f"c{ln[vals.index('')]}"
        for i in [4, 0, 2, 6, 8, 1, 3, 5, 7]:
            if not b[i] and random.random() < 0.8:
                return f"c{i}"
        return f"c{random.choice([i for i in range(9) if not b[i]])}"

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "TIC-TAC-TOE", status)
        size, x0, y0 = 150, 95, TOP + 30
        _frame(d, x0, y0, x0 + size * 3, y0 + size * 3)
        for i in range(9):
            r, c = divmod(i, 3)
            cx, cy = x0 + c * size + size / 2, y0 + r * size + size / 2
            fill = CELL_A if (r + c) % 2 == 0 else CELL_B
            if st["line"] and i in st["line"]:
                fill = (255, 214, 92)
            _cell(d, x0 + c * size, y0 + r * size, size, fill)
            m = st["b"][i]
            if m == "X":
                col = COLORS[0][2]
                for s in (1, -1):
                    d.line([(_sx(cx - 40), _sx(cy - 40 * s)), (_sx(cx + 40), _sx(cy + 40 * s))],
                           fill=_shade(col, -0.3), width=22 * S)
                for s in (1, -1):
                    d.line([(_sx(cx - 40), _sx(cy - 40 * s)), (_sx(cx + 40), _sx(cy + 40 * s))],
                           fill=col, width=14 * S)
            elif m == "O":
                col = COLORS[1][2]
                d.ellipse([_sx(cx - 44), _sx(cy - 44), _sx(cx + 44), _sx(cy + 44)], outline=_shade(col, -0.3), width=22 * S)
                d.ellipse([_sx(cx - 44), _sx(cy - 44), _sx(cx + 44), _sx(cy + 44)], outline=col, width=14 * S)
        return _finish(img, self.W, self.H)


# ── 3. Connect 4 ───────────────────────────────────────────────────────────

@_register
class Connect4(Spec):
    key = "c4"
    title = "🔴🟢 Connect 4"
    blurb = "Drop a disc into a column. Four in a row - across, down or diagonal - wins."
    min = max = 2
    W, H = 776, 760
    hint = "pick a column"

    def start(self, g):
        g["st"] = {"b": [[0] * 7 for _ in range(6)], "last": None, "win": []}

    def keyboard(self, g):
        return [[(str(c + 1), f"d{c}") for c in range(7)]]

    def caption(self, g):
        return [f"{_tag(p)}" for p in g["players"]]

    def _drop(self, b, c, v):
        for r in range(6):
            if b[r][c] == 0:
                b[r][c] = v
                return r
        return None

    def _four(self, b, r, c):
        v = b[r][c]
        for dr, dc in ((0, 1), (1, 0), (1, 1), (1, -1)):
            cells = [(r, c)]
            for s in (1, -1):
                rr, cc = r + dr * s, c + dc * s
                while 0 <= rr < 6 and 0 <= cc < 7 and b[rr][cc] == v:
                    cells.append((rr, cc))
                    rr += dr * s
                    cc += dc * s
            if len(cells) >= 4:
                return cells
        return None

    def act(self, g, p, action, secret=None):
        try:
            c = int(action[1:])
        except Exception:
            return "Pick a column."
        b = g["st"]["b"]
        v = g["players"].index(p) + 1
        r = self._drop(b, c, v)
        if r is None:
            return "That column is full."
        g["st"]["last"] = (r, c)
        four = self._four(b, r, c)
        if four:
            g["st"]["win"] = four
            _log(g, f"{_tag(p)} connects four!")
            _win(g, p)
            return None
        if all(b[5][cc] for cc in range(7)):
            _draw_game(g)
            return None
        _advance(g)
        return None

    def robot(self, g, p):
        b = g["st"]["b"]
        me = g["players"].index(p) + 1
        for v in (me, 3 - me):
            for c in range(7):
                bb = copy.deepcopy(b)
                r = self._drop(bb, c, v)
                if r is not None and self._four(bb, r, c):
                    return f"d{c}"
        opts = [c for c in range(7) if b[5][c] == 0]
        weights = [4 - abs(3 - c) for c in opts]
        return f"d{random.choices(opts, weights)[0]}"

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "CONNECT 4", status)
        cs, x0, y0 = 92, 66, TOP + 24
        _panel(d, x0 - 14, y0 - 14, x0 + cs * 7 + 14, y0 + cs * 6 + 14, fill=(30, 70, 160), radius=18, outline=(20, 45, 110))
        for r in range(6):
            for c in range(7):
                cx, cy = x0 + c * cs + cs / 2, y0 + (5 - r) * cs + cs / 2
                v = st["b"][r][c]
                d.ellipse([_sx(cx - 38), _sx(cy - 38), _sx(cx + 38), _sx(cy + 38)], fill=(16, 40, 100))
                if v:
                    col = COLORS[v - 1][2]
                    d.ellipse([_sx(cx - 36), _sx(cy - 36), _sx(cx + 36), _sx(cy + 36)], fill=_shade(col, -0.35))
                    d.ellipse([_sx(cx - 33), _sx(cy - 33), _sx(cx + 33), _sx(cy + 33)], fill=col)
                    d.ellipse([_sx(cx - 26), _sx(cy - 30), _sx(cx + 10), _sx(cy - 4)], fill=_shade(col, 0.35))
                    if (r, c) in st["win"] or st["last"] == (r, c):
                        d.ellipse([_sx(cx - 38), _sx(cy - 38), _sx(cx + 38), _sx(cy + 38)],
                                  outline=(255, 214, 92) if (r, c) in st["win"] else (255, 255, 255), width=4 * S)
        for c in range(7):
            _txt(d, x0 + c * cs + cs / 2, y0 + cs * 6 + 34, str(c + 1), 20, GOLD)
        return _finish(img, self.W, self.H)


# ── 4. Dice Battle ─────────────────────────────────────────────────────────

@_register
class DiceBattle(Spec):
    key = "dice"
    title = "🎲 Dice Battle"
    blurb = "Everyone rolls once a round; highest takes the point. First to 3 points wins."
    min, max = 2, 6
    W, H = 700, 580
    default_action = "roll"
    hint = "tap 🎲 Roll"

    def start(self, g):
        g["st"] = {"round": 1, "rolls": {}, "score": {p["id"]: 0 for p in g["players"]}}

    def keyboard(self, g):
        return [[("🎲 Roll", "roll")]]

    def caption(self, g):
        sc = _scores(g)
        return [f"Round {g['st']['round']} · first to 3"] + \
               [f"{_tag(p)} · {sc[p['id']]} pt" for p in g["players"]]

    def secret(self, g, p):
        return random.randint(1, 6)

    def act(self, g, p, action, secret=None):
        st = g["st"]
        d = secret or random.randint(1, 6)
        st["rolls"][p["id"]] = d
        _log(g, f"{_tag(p)} rolled <b>{d}</b>")
        alive = _alive(g)
        if all(q["id"] in st["rolls"] for q in alive):
            best = max(st["rolls"][q["id"]] for q in alive)
            tops = [q for q in alive if st["rolls"][q["id"]] == best]
            if len(tops) == 1:
                st["score"][tops[0]["id"]] += 1
                _log(g, f"🏅 Round {st['round']} to {_tag(tops[0])} with a {best}")
                if st["score"][tops[0]["id"]] >= 3:
                    _win(g, tops[0])
                    return None
            else:
                _log(g, f"Round {st['round']}: tie on {best} - no point")
            st["round"] += 1
            st["last_rolls"] = dict(st["rolls"])
            st["rolls"] = {}
            g["turn"] = g["players"].index(alive[0])
        else:
            _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  f"Round {st['round']} · Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "DICE BATTLE", status)
        n = len(g["players"])
        rowh = 64
        y = TOP + 20
        _panel(d, 40, y, self.W - 40, y + 20 + rowh * n)
        shown = st["rolls"] if st["rolls"] else st.get("last_rolls", {})
        for i, p in enumerate(g["players"]):
            cy = y + 20 + rowh * i + rowh / 2
            _token(d, 80, cy, COLORS[p["c"]][2], _plabel(p), r=16)
            col = GOLD if (g["phase"] == "play" and cur is p) else (255, 255, 255)
            _txt(d, 112, cy, p["name"], 19, col, anchor="lm")
            r = shown.get(p["id"])
            if r:
                _die(d, self.W - 250, cy - 22, r, COLORS[p["c"]][2], size=44)
            for k in range(3):
                cx = self.W - 150 + k * 34
                filled = k < st["score"][p["id"]]
                d.ellipse([_sx(cx - 11), _sx(cy - 11), _sx(cx + 11), _sx(cy + 11)],
                          fill=(255, 200, 60) if filled else (60, 66, 80), outline=(214, 172, 80), width=2 * S)
        return _finish(img, self.W, self.H)


# ── 5. Rock Paper Scissors ─────────────────────────────────────────────────

_RPS_BEATS = {"r": "s", "s": "p", "p": "r"}
_RPS_NAME = {"r": "✊ Rock", "p": "✋ Paper", "s": "✌️ Scissors"}


@_register
class RPS(Spec):
    key = "rps"
    title = "✊✋✌️ Rock Paper Scissors"
    blurb = "Everyone picks in secret, then all hands are revealed. First to 3 points wins."
    min, max = 2, 6
    W, H = 700, 600
    simultaneous = True

    def start(self, g):
        g["st"] = {"round": 1, "picks": {}, "shown": {}, "score": {p["id"]: 0 for p in g["players"]}}

    def keyboard(self, g):
        return [[("✊ Rock", "r"), ("✋ Paper", "p"), ("✌️ Scissors", "s")]]

    def can_act(self, g, p):
        return g["phase"] == "play" and not p["out"] and p["id"] not in g["st"]["picks"]

    def deny(self, g, p):
        return "You already picked - waiting for the others."

    def caption(self, g):
        sc = _scores(g)
        return [f"Round {g['st']['round']} · first to 3"] + \
               [f"{_tag(p)} · {sc[p['id']]} pt" + (" ✅" if p["id"] in g["st"]["picks"] else "")
                for p in g["players"]]

    def robot(self, g, p):
        return random.choice("rps")

    def act(self, g, p, action, secret=None):
        st = g["st"]
        if action not in _RPS_BEATS:
            return "Pick rock, paper or scissors."
        st["picks"][p["id"]] = action
        alive = _alive(g)
        if not all(q["id"] in st["picks"] for q in alive):
            return None
        st["shown"] = dict(st["picks"])
        wins = {q["id"]: 0 for q in alive}
        for a in alive:
            for b in alive:
                if a is not b and _RPS_BEATS[st["picks"][a["id"]]] == st["picks"][b["id"]]:
                    wins[a["id"]] += 1
        best = max(wins.values())
        _log(g, "  ".join(f"{COLORS[q['c']][0]}{_RPS_NAME[st['picks'][q['id']]].split()[0]}" for q in alive))
        if best == 0:
            _log(g, f"Round {st['round']}: nobody wins")
        else:
            tops = [q for q in alive if wins[q["id"]] == best]
            for q in tops:
                st["score"][q["id"]] += 1
            _log(g, f"🏅 Round {st['round']} to " + ", ".join(_tag(q) for q in tops))
            top = max(st["score"][q["id"]] for q in alive)
            leaders = [q for q in alive if st["score"][q["id"]] == top]
            if top >= 3 and len(leaders) == 1:      # a shared lead plays on until someone pulls clear
                st["picks"] = {}
                _win(g, leaders[0])
                return None
        st["round"] += 1
        st["picks"] = {}
        return None

    def _hand(self, d, cx, cy, kind, rgb):
        if kind == "r":
            d.ellipse([_sx(cx - 26), _sx(cy - 24), _sx(cx + 26), _sx(cy + 24)], fill=(120, 120, 130), outline=(60, 60, 70), width=3 * S)
            d.ellipse([_sx(cx - 16), _sx(cy - 18), _sx(cx + 4), _sx(cy - 4)], fill=(170, 170, 180))
        elif kind == "p":
            d.rounded_rectangle([_sx(cx - 22), _sx(cy - 28), _sx(cx + 22), _sx(cy + 28)], radius=4 * S,
                                fill=(250, 250, 245), outline=(120, 120, 120), width=3 * S)
            for k in range(4):
                d.line([(_sx(cx - 14), _sx(cy - 16 + k * 10)), (_sx(cx + 14), _sx(cy - 16 + k * 10))], fill=(170, 170, 170), width=2 * S)
        elif kind == "s":
            for s in (1, -1):
                d.line([(_sx(cx - 20 * s), _sx(cy - 26)), (_sx(cx + 20 * s), _sx(cy + 20))], fill=(190, 190, 200), width=8 * S)
            for s in (1, -1):
                d.ellipse([_sx(cx + 12 * s - 10), _sx(cy + 14), _sx(cx + 12 * s + 10), _sx(cy + 34)], outline=rgb, width=5 * S)
        else:
            _txt(d, cx, cy, "?", 34, (150, 150, 160))

    def render(self, g):
        st = g["st"]
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Round {st['round']}")
        img, d = _canvas(self.W, self.H, "ROCK PAPER SCISSORS", status)
        n = len(g["players"])
        rowh = 70
        y = TOP + 20
        _panel(d, 40, y, self.W - 40, y + 20 + rowh * n)
        for i, p in enumerate(g["players"]):
            cy = y + 20 + rowh * i + rowh / 2
            _token(d, 80, cy, COLORS[p["c"]][2], _plabel(p), r=16)
            _txt(d, 112, cy, p["name"], 19, (255, 255, 255), anchor="lm")
            kind = st["shown"].get(p["id"]) if not st["picks"] else ("✓" if p["id"] in st["picks"] else None)
            if kind == "✓":
                _txt(d, self.W - 250, cy, "picked", 16, (120, 220, 140))
            else:
                self._hand(d, self.W - 250, cy, kind, COLORS[p["c"]][2])
            for k in range(3):
                cx = self.W - 150 + k * 34
                filled = k < st["score"][p["id"]]
                d.ellipse([_sx(cx - 11), _sx(cy - 11), _sx(cx + 11), _sx(cy + 11)],
                          fill=(255, 200, 60) if filled else (60, 66, 80), outline=(214, 172, 80), width=2 * S)
        return _finish(img, self.W, self.H)


# ── 6. Coin Flip ───────────────────────────────────────────────────────────

@_register
class CoinFlip(Spec):
    key = "coin"
    title = "🪙 Coin Flip"
    blurb = "Call heads or tails. Right call = your point, wrong = theirs. First to 3."
    min = max = 2
    W, H = 640, 570
    hint = "call it"

    def start(self, g):
        g["st"] = {"round": 1, "result": None, "score": {p["id"]: 0 for p in g["players"]}}

    def keyboard(self, g):
        return [[("Heads", "h"), ("Tails", "t")]]

    def caption(self, g):
        sc = _scores(g)
        return [f"Round {g['st']['round']} · first to 3"] + [f"{_tag(p)} · {sc[p['id']]} pt" for p in g["players"]]

    def robot(self, g, p):
        return random.choice("ht")

    def act(self, g, p, action, secret=None):
        if action not in "ht":
            return "Heads or tails?"
        st = g["st"]
        res = random.choice("ht")
        st["result"] = res
        other = [q for q in g["players"] if q is not p][0]
        name = {"h": "Heads", "t": "Tails"}
        if res == action:
            st["score"][p["id"]] += 1
            _log(g, f"{_tag(p)} called {name[action]} - it's <b>{name[res]}</b> ✅")
            w = p
        else:
            st["score"][other["id"]] += 1
            _log(g, f"{_tag(p)} called {name[action]} - it's <b>{name[res]}</b> ❌ point to {_tag(other)}")
            w = other
        if st["score"][w["id"]] >= 3:
            _win(g, w)
            return None
        st["round"] += 1
        _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Round {st['round']} · {cur['name']} calls")
        img, d = _canvas(self.W, self.H, "COIN FLIP", status)
        cx, cy, r = self.W / 2, TOP + 150, 100
        d.ellipse([_sx(cx - r + 6), _sx(cy - r + 12), _sx(cx + r + 6), _sx(cy + r + 12)], fill=(0, 0, 0, 110))
        d.ellipse([_sx(cx - r), _sx(cy - r), _sx(cx + r), _sx(cy + r)], fill=(190, 140, 30))
        d.ellipse([_sx(cx - r + 10), _sx(cy - r + 10), _sx(cx + r - 10), _sx(cy + r - 10)], fill=(240, 196, 60))
        d.ellipse([_sx(cx - r + 26), _sx(cy - r + 18), _sx(cx + 10), _sx(cy - 6)], fill=(255, 230, 140))
        if st["result"]:
            _txt(d, cx, cy, "H" if st["result"] == "h" else "T", 90, (150, 100, 10))
            _txt(d, cx, cy + r + 34, "HEADS" if st["result"] == "h" else "TAILS", 24, GOLD)
        else:
            _txt(d, cx, cy, "?", 90, (150, 100, 10))
        _score_rows(d, g, 40, TOP + 320, self.W - 80, extra=lambda p: f"{st['score'][p['id']]} pt")
        return _finish(img, self.W, self.H)


# ── 7. Hangman ─────────────────────────────────────────────────────────────

_HANG_WORDS = {
    "Animal": "ELEPHANT GIRAFFE DOLPHIN PENGUIN KANGAROO CROCODILE LEOPARD OCTOPUS BUTTERFLY SQUIRREL HEDGEHOG FLAMINGO".split(),
    "Country": "INDIA BRAZIL JAPAN CANADA GERMANY NIGERIA MEXICO AUSTRALIA PORTUGAL THAILAND EGYPT NORWAY".split(),
    "Fruit": "MANGO BANANA PINEAPPLE STRAWBERRY WATERMELON COCONUT PAPAYA CHERRY ORANGE POMEGRANATE".split(),
    "Crypto": "BITCOIN ETHEREUM WALLET LEVERAGE LIQUIDITY BREAKOUT CANDLE STOPLOSS ALTCOIN BLOCKCHAIN SOLANA FUTURES".split(),
    "Sport": "CRICKET FOOTBALL TENNIS HOCKEY BASKETBALL SWIMMING CYCLING BOXING ARCHERY VOLLEYBALL".split(),
    "Food": "BIRYANI PIZZA SAMOSA BURGER NOODLES PANCAKE CHOCOLATE SANDWICH DOSA LASAGNA".split(),
}
_HANG_MAX = 6


@_register
class Hangman(Spec):
    key = "hang"
    title = "🔤 Hangman"
    blurb = "Guess the word a letter at a time. Right letter = points and another go. 6 misses and the man hangs."
    min, max = 2, 6
    W, H = 776, 620
    hint = "pick a letter"

    def start(self, g):
        cat = random.choice(list(_HANG_WORDS))
        g["st"] = {"cat": cat, "word": random.choice(_HANG_WORDS[cat]), "used": [], "miss": 0,
                   "score": {p["id"]: 0 for p in g["players"]}}

    def keyboard(self, g):
        used = set(g["st"]["used"])
        letters = [c for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" if c not in used]
        return [[(c, f"l{c}") for c in letters[i:i + 7]] for i in range(0, len(letters), 7)]

    def caption(self, g):
        st = g["st"]
        shown = " ".join(c if c in st["used"] else "_" for c in st["word"])
        sc = _scores(g)
        return [f"Category: <b>{st['cat']}</b>", f"<code>{shown}</code>", f"Misses: {st['miss']}/{_HANG_MAX}"] + \
               [f"{_tag(p)} · {sc[p['id']]} pt" for p in g["players"]]

    def robot(self, g, p):
        used = set(g["st"]["used"])
        for c in "ETAOINSRHLDCUMFPGWYBVKXJQZ":
            if c not in used:
                return f"l{c}"
        return "lA"

    def act(self, g, p, action, secret=None):
        st = g["st"]
        c = action[1:2].upper()
        if not c.isalpha() or c in st["used"]:
            return "Pick a letter that hasn't been used."
        st["used"].append(c)
        hits = st["word"].count(c)
        if hits:
            st["score"][p["id"]] += hits
            _log(g, f"{_tag(p)} · <b>{c}</b> ✅ ×{hits} - goes again")
            if all(ch in st["used"] for ch in st["word"]):
                _log(g, f"Word: <b>{st['word']}</b>")
                best = max(st["score"].values())
                tops = [q for q in g["players"] if st["score"][q["id"]] == best]
                _win(g, p if p in tops else tops[0])
        else:
            st["miss"] += 1
            _log(g, f"{_tag(p)} · <b>{c}</b> ❌")
            if st["miss"] >= _HANG_MAX:
                _log(g, f"The man hangs. Word: <b>{st['word']}</b>")
                _draw_game(g)
            else:
                _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Hanged!" if g["phase"] == "over" else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "HANGMAN", status)
        # gallows
        gx, gy = 70, TOP + 40
        wood = (150, 100, 50)
        d.line([(_sx(gx), _sx(gy + 300)), (_sx(gx + 160), _sx(gy + 300))], fill=wood, width=10 * S)
        d.line([(_sx(gx + 40), _sx(gy + 300)), (_sx(gx + 40), _sx(gy))], fill=wood, width=10 * S)
        d.line([(_sx(gx + 40), _sx(gy)), (_sx(gx + 160), _sx(gy))], fill=wood, width=10 * S)
        d.line([(_sx(gx + 160), _sx(gy)), (_sx(gx + 160), _sx(gy + 40))], fill=(200, 180, 120), width=4 * S)
        hx, hy = gx + 160, gy + 70
        skin = (240, 210, 170)
        parts = st["miss"]
        if parts >= 1:
            d.ellipse([_sx(hx - 28), _sx(hy - 28), _sx(hx + 28), _sx(hy + 28)], fill=skin, outline=(120, 90, 60), width=3 * S)
            if g["phase"] == "over" and not g["winner"]:
                for ex in (-10, 10):
                    _txt(d, hx + ex, hy - 6, "x", 18, (120, 60, 40))
            else:
                for ex in (-10, 10):
                    d.ellipse([_sx(hx + ex - 3), _sx(hy - 9), _sx(hx + ex + 3), _sx(hy - 3)], fill=(40, 40, 40))
        if parts >= 2:
            d.line([(_sx(hx), _sx(hy + 28)), (_sx(hx), _sx(hy + 130))], fill=(60, 90, 160), width=12 * S)
        if parts >= 3:
            d.line([(_sx(hx), _sx(hy + 45)), (_sx(hx - 50), _sx(hy + 100))], fill=skin, width=9 * S)
        if parts >= 4:
            d.line([(_sx(hx), _sx(hy + 45)), (_sx(hx + 50), _sx(hy + 100))], fill=skin, width=9 * S)
        if parts >= 5:
            d.line([(_sx(hx), _sx(hy + 130)), (_sx(hx - 40), _sx(hy + 200))], fill=(50, 50, 60), width=10 * S)
        if parts >= 6:
            d.line([(_sx(hx), _sx(hy + 130)), (_sx(hx + 40), _sx(hy + 200))], fill=(50, 50, 60), width=10 * S)
        # word
        word = st["word"]
        n = len(word)
        bw = min(46, int(440 / n))
        x0 = 300 + (440 - bw * n) / 2
        _panel(d, 290, TOP + 30, self.W - 40, TOP + 130)
        _txt(d, 306, TOP + 48, st["cat"].upper(), 14, (170, 180, 200), anchor="la")
        for i, ch in enumerate(word):
            cx = x0 + i * bw + bw / 2
            d.line([(_sx(cx - bw / 2 + 5), _sx(TOP + 112)), (_sx(cx + bw / 2 - 5), _sx(TOP + 112))], fill=GOLD, width=3 * S)
            if ch in st["used"] or g["phase"] == "over":
                _txt(d, cx, TOP + 88, ch, int(bw * 0.75), (255, 255, 255) if ch in st["used"] else (255, 120, 120))
        # misses
        wrong = [c for c in st["used"] if c not in word]
        _panel(d, 290, TOP + 150, self.W - 40, TOP + 215, fill=(60, 30, 30), outline=(160, 60, 60))
        _txt(d, 306, TOP + 182, "MISSES  " + " ".join(wrong), 20, (255, 150, 150), anchor="lm")
        _score_rows(d, g, 290, TOP + 235, self.W - 330, extra=lambda p: f"{st['score'][p['id']]} pt", rowh=38)
        return _finish(img, self.W, self.H)


# ── 8. Number Guessing ─────────────────────────────────────────────────────

@_register
class NumberGuess(Spec):
    key = "guess"
    title = "🔢 Number Guessing"
    blurb = "I'm thinking of a number from 1 to 100. Type your guess on your turn - I'll say higher or lower."
    min, max = 2, 6
    W, H = 700, 520
    uses_text = True
    hint = "type a number 1-100"
    H = 600

    def start(self, g):
        g["st"] = {"n": random.randint(1, 100), "lo": 1, "hi": 100, "guesses": []}

    def keyboard(self, g):
        return []

    def caption(self, g):
        st = g["st"]
        return [f"Somewhere between <b>{st['lo']}</b> and <b>{st['hi']}</b>"]

    def robot(self, g, p):
        st = g["st"]
        return str((st["lo"] + st["hi"]) // 2 if random.random() < 0.7 else random.randint(st["lo"], st["hi"]))

    def on_text(self, g, p, text):
        if not text.isdigit():
            return False
        return self.act(g, p, text) is None

    def act(self, g, p, action, secret=None):
        st = g["st"]
        try:
            n = int(action)
        except Exception:
            return "Type a number."
        if not 1 <= n <= 100:
            return "1 to 100 only."
        st["guesses"].append((p["c"], n))
        if n == st["n"]:
            _log(g, f"{_tag(p)} guessed <b>{n}</b> - exactly right! 🎯")
            _win(g, p)
            return None
        if n < st["n"]:
            st["lo"] = max(st["lo"], n + 1)
            _log(g, f"{_tag(p)} · {n} → <b>higher</b> ⬆️")
        else:
            st["hi"] = min(st["hi"], n - 1)
            _log(g, f"{_tag(p)} · {n} → <b>lower</b> ⬇️")
        _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "NUMBER GUESSING", status)
        x0, x1, y = 60, self.W - 60, TOP + 90
        _panel(d, 40, TOP + 20, self.W - 40, TOP + 200)
        d.rounded_rectangle([_sx(x0), _sx(y - 10), _sx(x1), _sx(y + 10)], radius=10 * S, fill=(50, 56, 70))
        lo, hi = st["lo"], st["hi"]
        if g["phase"] == "over":
            lo = hi = st["n"]
        fx = lambda v: x0 + (x1 - x0) * (v - 1) / 99
        d.rounded_rectangle([_sx(fx(lo)) - 6 * S, _sx(y - 10), _sx(fx(hi)) + 6 * S, _sx(y + 10)], radius=10 * S, fill=(52, 168, 83))
        for v in (1, 25, 50, 75, 100):
            _txt(d, fx(v), y + 30, str(v), 14, (170, 180, 200))
        for c, n in st["guesses"][-12:]:
            _token(d, fx(n), y - 34, COLORS[c][2], str(n), r=15)
        _txt(d, self.W / 2, TOP + 160, f"{lo}  –  {hi}" if lo != hi else f"It was {st['n']}", 30, GOLD)
        _score_rows(d, g, 40, TOP + 220, self.W - 80, rowh=38)
        return _finish(img, self.W, self.H)


# ── 9. Minesweeper ─────────────────────────────────────────────────────────

_MS_N, _MS_MINES = 8, 10


@_register
class Minesweeper(Spec):
    key = "mines"
    title = "💣 Minesweeper"
    blurb = "Take turns opening squares. Hit a mine and you're out. Most squares opened when the field is clear wins."
    min, max = 2, 4
    W, H = 700, 760
    hint = "open a square"

    def start(self, g):
        g["st"] = {"mines": None, "open": {}, "score": {p["id"]: 0 for p in g["players"]}, "boom": [], "last": None}

    def _place(self, st, avoid):
        cells = [i for i in range(_MS_N * _MS_N) if i != avoid]
        st["mines"] = set(random.sample(cells, _MS_MINES))

    def _adj(self, i):
        r, c = divmod(i, _MS_N)
        for dr in (-1, 0, 1):
            for dc in (-1, 0, 1):
                rr, cc = r + dr, c + dc
                if (dr or dc) and 0 <= rr < _MS_N and 0 <= cc < _MS_N:
                    yield rr * _MS_N + cc

    def keyboard(self, g):
        st = g["st"]
        rows = []
        for r in range(_MS_N):
            row = []
            for c in range(_MS_N):
                i = r * _MS_N + c
                if i in st["boom"]:
                    t = "💥"
                elif i in st["open"]:
                    t = str(st["open"][i]) if st["open"][i] else "▫️"
                else:
                    t = "▪️"
                row.append((t, f"o{i}"))
            rows.append(row)
        return rows

    def caption(self, g):
        sc = _scores(g)
        return [f"{_tag(p)} · {sc[p['id']]} opened" + (" 💥 out" if p["out"] else "") for p in g["players"]]

    def robot(self, g, p):
        st = g["st"]
        closed = [i for i in range(_MS_N * _MS_N) if i not in st["open"] and i not in st["boom"]]
        # prefer squares next to an open 0, then random
        safe = [i for i in closed if any(st["open"].get(j) == 0 for j in self._adj(i))]
        return f"o{random.choice(safe or closed)}"

    def act(self, g, p, action, secret=None):
        st = g["st"]
        try:
            i = int(action[1:])
        except Exception:
            return "Tap a square."
        if i in st["open"] or i in st["boom"]:
            return "Already open."
        if st["mines"] is None:
            self._place(st, i)
        st["last"] = i
        if i in st["mines"]:
            st["boom"].append(i)
            p["out"] = True
            _log(g, f"💥 {_tag(p)} hit a mine!")
            alive = _alive(g)
            if len(alive) == 1:
                _win(g, alive[0])
                return None
            if not alive:
                _draw_game(g)
                return None
            _advance(g)
            return None
        stack = [i]
        n = 0
        while stack:
            k = stack.pop()
            if k in st["open"]:
                continue
            cnt = sum(1 for j in self._adj(k) if j in st["mines"])
            st["open"][k] = cnt
            n += 1
            if cnt == 0:
                stack.extend(j for j in self._adj(k) if j not in st["open"] and j not in st["mines"])
        st["score"][p["id"]] += n
        _log(g, f"{_tag(p)} opened {n} square{'s' if n != 1 else ''}")
        if len(st["open"]) >= _MS_N * _MS_N - _MS_MINES:
            best = max(st["score"][q["id"]] for q in _alive(g))
            tops = [q for q in _alive(g) if st["score"][q["id"]] == best]
            _log(g, "Field cleared!")
            _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
            return None
        _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "MINESWEEPER", status)
        cs, x0, y0 = 70, 70, TOP + 30
        _frame(d, x0, y0, x0 + cs * _MS_N, y0 + cs * _MS_N)
        numcol = {1: (40, 90, 220), 2: (30, 140, 60), 3: (210, 50, 50), 4: (90, 40, 160), 5: (150, 80, 20), 6: (30, 150, 150), 7: (40, 40, 40), 8: (120, 120, 120)}
        over = g["phase"] == "over"
        for i in range(_MS_N * _MS_N):
            r, c = divmod(i, _MS_N)
            cx, cy = x0 + c * cs + cs / 2, y0 + r * cs + cs / 2
            if i in st["boom"]:
                _cell(d, x0 + c * cs, y0 + r * cs, cs, (240, 90, 90), bevel=False)
                d.ellipse([_sx(cx - 16), _sx(cy - 16), _sx(cx + 16), _sx(cy + 16)], fill=(30, 30, 30))
                _txt(d, cx, cy, "✱", 22, (255, 200, 60))
            elif i in st["open"]:
                _cell(d, x0 + c * cs, y0 + r * cs, cs, (232, 226, 210), bevel=False)
                d.rectangle([_sx(x0 + c * cs), _sx(y0 + r * cs), _sx(x0 + c * cs + cs), _sx(y0 + r * cs + cs)], outline=(200, 190, 170), width=1)
                if st["open"][i]:
                    _txt(d, cx, cy, str(st["open"][i]), 30, numcol[st["open"][i]])
            else:
                fill = (150, 168, 190) if (r + c) % 2 == 0 else (136, 154, 176)
                _cell(d, x0 + c * cs, y0 + r * cs, cs, fill)
                if over and st["mines"] and i in st["mines"]:
                    d.ellipse([_sx(cx - 14), _sx(cy - 14), _sx(cx + 14), _sx(cy + 14)], fill=(30, 30, 30))
            if st["last"] == i:
                d.rectangle([_sx(x0 + c * cs), _sx(y0 + r * cs), _sx(x0 + c * cs + cs), _sx(y0 + r * cs + cs)], outline=(255, 170, 0), width=3 * S)
        _txt(d, self.W / 2, y0 + cs * _MS_N + 40, f"{_MS_MINES} mines · {len(st['open'])}/{_MS_N * _MS_N - _MS_MINES} opened", 18, GOLD)
        return _finish(img, self.W, self.H)


# ── 10. Memory Match ───────────────────────────────────────────────────────

_MEM_SYMS = [("A", (226, 60, 60)), ("B", (52, 168, 83)), ("C", (52, 120, 230)), ("D", (240, 190, 30)),
             ("E", (150, 80, 210)), ("F", (245, 130, 40)), ("G", (30, 170, 170)), ("H", (230, 80, 160))]


@_register
class Memory(Spec):
    key = "memory"
    title = "🃏 Memory Match"
    blurb = "Flip two cards. A pair is yours and you go again. Most pairs wins."
    min, max = 2, 4
    W, H = 640, 850
    hint = "flip a card"

    def start(self, g):
        deck = list(range(8)) * 2
        random.shuffle(deck)
        g["st"] = {"deck": deck, "found": {}, "open": [], "hide": False, "seen": {},
                   "score": {p["id"]: 0 for p in g["players"]}}

    def keyboard(self, g):
        st = g["st"]
        rows = []
        for r in range(4):
            row = []
            for c in range(4):
                i = r * 4 + c
                if i in st["found"] or i in st["open"]:
                    t = _MEM_SYMS[st["deck"][i]][0]
                else:
                    t = "❔"
                row.append((t, f"f{i}"))
            rows.append(row)
        return rows

    def caption(self, g):
        sc = _scores(g)
        return [f"{_tag(p)} · {sc[p['id']]} pair{'s' if sc[p['id']] != 1 else ''}" for p in g["players"]]

    def robot(self, g, p):
        st = g["st"]
        closed = [i for i in range(16) if i not in st["found"] and i not in st["open"]]
        seen = {i: s for i, s in st["seen"].items() if i in closed}
        if st["open"]:
            want = st["deck"][st["open"][0]]
            for i, s in seen.items():
                if s == want:
                    return f"f{i}"
        else:
            by = {}
            for i, s in seen.items():
                by.setdefault(s, []).append(i)
            for s, idx in by.items():
                if len(idx) >= 2:
                    return f"f{idx[0]}"
        unseen = [i for i in closed if i not in seen]
        return f"f{random.choice(unseen or closed)}"

    def act(self, g, p, action, secret=None):
        st = g["st"]
        try:
            i = int(action[1:])
        except Exception:
            return "Tap a card."
        if st["hide"]:
            st["open"] = []
            st["hide"] = False
        if i in st["found"] or i in st["open"]:
            return "That card is already face up."
        st["open"].append(i)
        st["seen"][i] = st["deck"][i]
        if len(st["open"]) < 2:
            return None
        a, b = st["open"]
        if st["deck"][a] == st["deck"][b]:
            st["found"][a] = st["found"][b] = p["c"]
            st["score"][p["id"]] += 1
            st["open"] = []
            _log(g, f"{_tag(p)} found a pair - <b>{_MEM_SYMS[st['deck'][a]][0]}</b> - goes again")
            if len(st["found"]) == 16:
                best = max(st["score"].values())
                tops = [q for q in g["players"] if st["score"][q["id"]] == best]
                _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
        else:
            st["hide"] = True
            _log(g, f"{_tag(p)} · {_MEM_SYMS[st['deck'][a]][0]} and {_MEM_SYMS[st['deck'][b]][0]} - no match")
            _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "MEMORY MATCH", status)
        cs, gap, x0, y0 = 118, 14, 60, TOP + 30
        for i in range(16):
            r, c = divmod(i, 4)
            x, y = x0 + c * (cs + gap), y0 + r * (cs + gap)
            d.rounded_rectangle([_sx(x) + 4 * S, _sx(y) + 6 * S, _sx(x + cs) + 4 * S, _sx(y + cs) + 6 * S], radius=12 * S, fill=(0, 0, 0, 110))
            if i in st["found"] or i in st["open"]:
                sym, col = _MEM_SYMS[st["deck"][i]]
                d.rounded_rectangle([_sx(x), _sx(y), _sx(x + cs), _sx(y + cs)], radius=12 * S, fill=(250, 247, 238),
                                    outline=COLORS[st["found"][i]][2] if i in st["found"] else (255, 255, 255), width=4 * S)
                d.ellipse([_sx(x + cs / 2 - 36), _sx(y + cs / 2 - 36), _sx(x + cs / 2 + 36), _sx(y + cs / 2 + 36)], fill=col)
                _txt(d, x + cs / 2, y + cs / 2, sym, 40, (255, 255, 255))
            else:
                d.rounded_rectangle([_sx(x), _sx(y), _sx(x + cs), _sx(y + cs)], radius=12 * S, fill=(120, 70, 40), outline=(190, 135, 70), width=3 * S)
                d.rounded_rectangle([_sx(x + 14), _sx(y + 14), _sx(x + cs - 14), _sx(y + cs - 14)], radius=8 * S, outline=(160, 105, 55), width=2 * S)
                _txt(d, x + cs / 2, y + cs / 2, "?", 44, (190, 135, 70))
        _score_rows(d, g, 60, y0 + 4 * (cs + gap) + 6, self.W - 120, extra=lambda p: st["score"][p["id"]], rowh=36)
        return _finish(img, self.W, self.H)


# ── 11. Racing ─────────────────────────────────────────────────────────────

_RACE_LEN = 30
_RACE_BOOST = {5, 13, 21}
_RACE_OIL = {9, 17, 25}


@_register
class Racing(Spec):
    key = "race"
    title = "🏎️ Racing"
    blurb = "Roll to move your car down the track. Turbo squares push you 3 ahead, oil slicks 2 back. First across the line wins."
    min, max = 2, 6
    W, H = 776, 560
    default_action = "roll"
    hint = "tap 🎲 Roll"

    def keyboard(self, g):
        return [[("🎲 Roll", "roll")]]

    def caption(self, g):
        return [f"{_tag(p)} · {min(p['pos'], _RACE_LEN)}/{_RACE_LEN}" for p in sorted(g["players"], key=lambda q: -q["pos"])]

    def secret(self, g, p):
        return random.randint(1, 6)

    def act(self, g, p, action, secret=None):
        d = secret or random.randint(1, 6)
        frm = p["pos"]
        to = frm + d
        line = f"{_tag(p)} rolled <b>{d}</b> · {frm} → {to}"
        if to < _RACE_LEN and to in _RACE_BOOST:
            to += 3
            line += f" 🚀 turbo → <b>{to}</b>"
        elif to < _RACE_LEN and to in _RACE_OIL:
            to = max(0, to - 2)
            line += f" 🛢 oil slick → <b>{to}</b>"
        p["pos"] = to
        g["st"]["last"] = {"c": p["c"], "roll": d}
        _log(g, line)
        if to >= _RACE_LEN:
            _log(g, f"🏁 {_tag(p)} crosses the line!")
            _win(g, p)
            return None
        _advance(g)
        return None

    def render(self, g):
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Turn: {cur['name']}")
        n = len(g["players"])
        h = TOP + 30 + 56 * n + 60 + 14 + 34 * n + 30
        img, d = _canvas(self.W, h, "RACING", status)
        x0, x1 = 60, self.W - 60
        laneh = 56
        y0 = TOP + 30
        d.rounded_rectangle([_sx(x0 - 20), _sx(y0 - 12), _sx(x1 + 20), _sx(y0 + laneh * n + 12)], radius=12 * S, fill=(70, 70, 76))
        step = (x1 - x0) / _RACE_LEN
        for i, p in enumerate(g["players"]):
            ly = y0 + i * laneh
            if i:
                for k in range(0, _RACE_LEN, 2):
                    d.line([(_sx(x0 + k * step), _sx(ly)), (_sx(x0 + (k + 1) * step), _sx(ly))], fill=(230, 230, 230), width=3 * S)
            for k in _RACE_BOOST:
                d.rectangle([_sx(x0 + k * step), _sx(ly + 6), _sx(x0 + (k + 1) * step), _sx(ly + laneh - 6)], fill=(60, 150, 90))
            for k in _RACE_OIL:
                d.ellipse([_sx(x0 + k * step + 2), _sx(ly + 14), _sx(x0 + (k + 1) * step - 2), _sx(ly + laneh - 14)], fill=(20, 20, 24))
            cx = x0 + min(p["pos"], _RACE_LEN) * step
            cy = ly + laneh / 2
            col = COLORS[p["c"]][2]
            d.rounded_rectangle([_sx(cx - 22) + 3 * S, _sx(cy - 12) + 4 * S, _sx(cx + 22) + 3 * S, _sx(cy + 12) + 4 * S], radius=6 * S, fill=(0, 0, 0, 110))
            d.rounded_rectangle([_sx(cx - 22), _sx(cy - 12), _sx(cx + 22), _sx(cy + 12)], radius=6 * S, fill=_shade(col, -0.3))
            d.rounded_rectangle([_sx(cx - 20), _sx(cy - 10), _sx(cx + 20), _sx(cy + 10)], radius=5 * S, fill=col)
            d.rounded_rectangle([_sx(cx - 8), _sx(cy - 8), _sx(cx + 8), _sx(cy + 8)], radius=3 * S, fill=(40, 60, 90))
            _txt(d, cx, cy, _plabel(p), 10, (255, 255, 255))
        for k in range(0, _RACE_LEN + 1, 5):
            _txt(d, x0 + k * step, y0 + laneh * n + 30, str(k), 13, (200, 200, 210))
        # finish line
        for k in range(int(laneh * n / 10)):
            d.rectangle([_sx(x1), _sx(y0 + k * 10), _sx(x1 + 10), _sx(y0 + (k + 1) * 10)], fill=(255, 255, 255) if k % 2 == 0 else (20, 20, 20))
        last = g["st"].get("last")
        if last:
            _die(d, self.W / 2 - 24, 12, last["roll"], COLORS[last["c"]][2], size=48)
        _score_rows(d, g, 60, y0 + laneh * n + 60, self.W - 120, extra=lambda p: f"{min(p['pos'], _RACE_LEN)}", rowh=34)
        return _finish(img, self.W, h)


# ── 12. Player Duel ────────────────────────────────────────────────────────

@_register
class Duel(Spec):
    key = "duel"
    title = "⚔️ Player Duel"
    blurb = "100 HP each. Attack, defend (halves the next hit) or heal. Bring the other one to zero."
    min = max = 2
    W, H = 700, 560
    hint = "attack, defend or heal"

    def start(self, g):
        g["st"] = {"hp": {p["id"]: 100 for p in g["players"]}, "guard": {p["id"]: False for p in g["players"]}, "last": ""}

    def keyboard(self, g):
        return [[("⚔️ Attack", "atk"), ("🛡 Defend", "def"), ("❤️ Heal", "heal")]]

    def caption(self, g):
        st = g["st"]
        return [f"{_tag(p)} · ❤️ {st['hp'][p['id']]}" + (" 🛡" if st["guard"][p["id"]] else "") for p in g["players"]]

    def robot(self, g, p):
        hp = g["st"]["hp"][p["id"]]
        if hp < 35 and random.random() < 0.6:
            return "heal"
        return random.choices(["atk", "def"], [7, 3])[0]

    def act(self, g, p, action, secret=None):
        st = g["st"]
        other = [q for q in g["players"] if q is not p][0]
        if action == "atk":
            dmg = random.randint(12, 26)
            crit = random.random() < 0.15
            if crit:
                dmg = int(dmg * 1.5)
            if st["guard"][other["id"]]:
                dmg //= 2
                st["guard"][other["id"]] = False
                note = " (blocked half)"
            else:
                note = " 💥 critical!" if crit else ""
            st["hp"][other["id"]] = max(0, st["hp"][other["id"]] - dmg)
            st["last"] = f"{p['name']} hits {other['name']} for {dmg}"
            _log(g, f"{_tag(p)} ⚔️ hits {_tag(other)} for <b>{dmg}</b>{note}")
            if st["hp"][other["id"]] <= 0:
                _win(g, p)
                return None
        elif action == "def":
            st["guard"][p["id"]] = True
            st["last"] = f"{p['name']} raises the shield"
            _log(g, f"{_tag(p)} 🛡 defends - next hit halved")
        elif action == "heal":
            h = random.randint(10, 22)
            st["hp"][p["id"]] = min(100, st["hp"][p["id"]] + h)
            st["last"] = f"{p['name']} heals {h}"
            _log(g, f"{_tag(p)} ❤️ heals <b>{h}</b>")
        else:
            return "Attack, defend or heal."
        _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "PLAYER DUEL", status)
        a, b = g["players"]
        for p, cx in ((a, 180), (b, self.W - 180)):
            col = COLORS[p["c"]][2]
            cy = TOP + 150
            d.ellipse([_sx(cx - 70) + 6 * S, _sx(cy - 70) + 10 * S, _sx(cx + 70) + 6 * S, _sx(cy + 70) + 10 * S], fill=(0, 0, 0, 110))
            _token(d, cx, cy, col, _plabel(p), r=66)
            if st["guard"][p["id"]]:
                d.ellipse([_sx(cx - 80), _sx(cy - 80), _sx(cx + 80), _sx(cy + 80)], outline=(120, 200, 255), width=5 * S)
            _txt(d, cx, cy + 100, p["name"], 20, (255, 255, 255))
            _bar(d, cx - 110, cy + 120, 220, 22, st["hp"][p["id"]] / 100, (220, 60, 60) if st["hp"][p["id"]] < 35 else (60, 190, 90))
            _txt(d, cx, cy + 131, f"{st['hp'][p['id']]} / 100", 13, (255, 255, 255))
        _txt(d, self.W / 2, TOP + 150, "VS", 44, GOLD, shadow=True)
        if st["last"]:
            _panel(d, 60, TOP + 330, self.W - 60, TOP + 390)
            _txt(d, self.W / 2, TOP + 360, st["last"], 18, (255, 255, 255))
        return _finish(img, self.W, self.H)


# ── 13. Penalty Shootout ───────────────────────────────────────────────────

_PEN_NAME = {"l": "left", "c": "centre", "r": "right"}


@_register
class Penalty(Spec):
    key = "penalty"
    title = "⚽ Penalty Shootout"
    blurb = "Striker picks a corner, keeper picks a side to dive. Same side = saved. 5 kicks each, then sudden death."
    min = max = 2
    W, H = 700, 560
    simultaneous = True

    def start(self, g):
        g["st"] = {"kick": 1, "striker": 0, "picks": {}, "shown": None, "score": {p["id"]: 0 for p in g["players"]},
                   "kicks": {p["id"]: 0 for p in g["players"]}}

    def keyboard(self, g):
        return [[("⬅️ Left", "l"), ("⬆️ Centre", "c"), ("➡️ Right", "r")]]

    def can_act(self, g, p):
        return g["phase"] == "play" and p["id"] not in g["st"]["picks"]

    def deny(self, g, p):
        return "You already picked - waiting for the other player."

    def caption(self, g):
        st = g["st"]
        if len(g["players"]) < 2:
            return []
        s = g["players"][st["striker"]]
        k = g["players"][1 - st["striker"]]
        sc = _scores(g)
        return [f"Kick {st['kick']} · ⚽ {_tag(s)} shoots · 🧤 {_tag(k)} in goal"] + \
               [f"{_tag(p)} · {sc[p['id']]} goal{'s' if sc[p['id']] != 1 else ''}" + (" ✅" if p["id"] in st["picks"] else "") for p in g["players"]]

    def turn_text(self, g):
        w = [p for p in g["players"] if self.can_act(g, p)]
        return "👉 Waiting for: " + ", ".join(_tag(p) for p in w) + " - pick a side"

    def robot(self, g, p):
        return random.choice("lcr")

    def act(self, g, p, action, secret=None):
        st = g["st"]
        if action not in "lcr":
            return "Left, centre or right."
        st["picks"][p["id"]] = action
        if len(st["picks"]) < 2:
            return None
        s = g["players"][st["striker"]]
        k = g["players"][1 - st["striker"]]
        shot, dive = st["picks"][s["id"]], st["picks"][k["id"]]
        st["shown"] = {"shot": shot, "dive": dive, "goal": shot != dive}
        st["kicks"][s["id"]] += 1
        if shot != dive:
            st["score"][s["id"]] += 1
            _log(g, f"⚽ {_tag(s)} shoots {_PEN_NAME[shot]}, keeper dives {_PEN_NAME[dive]} - <b>GOAL!</b>")
        else:
            _log(g, f"🧤 {_tag(s)} shoots {_PEN_NAME[shot]} - <b>SAVED</b> by {_tag(k)}")
        st["picks"] = {}
        st["kick"] += 1
        st["striker"] = 1 - st["striker"]
        a, b = g["players"]
        ka, kb = st["kicks"][a["id"]], st["kicks"][b["id"]]
        sa, sb = st["score"][a["id"]], st["score"][b["id"]]
        # decided early, or after 5 each and not level, or sudden death
        if (ka >= 5 and kb >= 5 and ka == kb and sa != sb) or \
           (ka <= 5 and kb <= 5 and (sa > sb + (5 - kb) or sb > sa + (5 - ka))):
            _win(g, a if sa > sb else b)
        return None

    def render(self, g):
        st = g["st"]
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Kick {st['kick']}")
        img, d = _canvas(self.W, self.H, "PENALTY SHOOTOUT", status)
        gx0, gx1, gy0, gy1 = 110, self.W - 110, TOP + 40, TOP + 220
        d.rectangle([_sx(40), _sx(gy1), _sx(self.W - 40), _sx(gy1 + 130)], fill=(60, 150, 70))
        for k in range(6):
            d.rectangle([_sx(40), _sx(gy1 + k * 22), _sx(self.W - 40), _sx(gy1 + k * 22 + 11)], fill=(70, 165, 80))
        d.rectangle([_sx(gx0), _sx(gy0), _sx(gx1), _sx(gy1)], fill=(35, 60, 45))
        for x in range(gx0, gx1, 24):
            d.line([(_sx(x), _sx(gy0)), (_sx(x), _sx(gy1))], fill=(200, 200, 200, 90), width=1 * S)
        for y in range(gy0, gy1, 24):
            d.line([(_sx(gx0), _sx(y)), (_sx(gx1), _sx(y))], fill=(200, 200, 200, 90), width=1 * S)
        d.line([(_sx(gx0), _sx(gy1)), (_sx(gx0), _sx(gy0)), (_sx(gx1), _sx(gy0)), (_sx(gx1), _sx(gy1))], fill=(255, 255, 255), width=8 * S)
        lanes = {"l": gx0 + 80, "c": (gx0 + gx1) / 2, "r": gx1 - 80}
        keeper = g["players"][1 - st["striker"]] if not st["shown"] else g["players"][st["striker"]]
        if st["shown"]:
            kx = lanes[st["shown"]["dive"]]
            _token(d, kx, gy1 - 60, COLORS[keeper["c"]][2], _plabel(keeper), r=30)
            bx = lanes[st["shown"]["shot"]]
            by = gy0 + 50 if st["shown"]["goal"] else gy1 - 60
            d.ellipse([_sx(bx - 18), _sx(by - 18), _sx(bx + 18), _sx(by + 18)], fill=(250, 250, 250), outline=(30, 30, 30), width=3 * S)
            _txt(d, bx, by, "●", 12, (30, 30, 30))
            _txt(d, self.W / 2, gy1 + 70, "GOAL!" if st["shown"]["goal"] else "SAVED!", 40, GOLD if st["shown"]["goal"] else (255, 120, 120), shadow=True)
        else:
            _token(d, lanes["c"], gy1 - 60, COLORS[keeper["c"]][2], _plabel(keeper), r=30)
            d.ellipse([_sx(self.W / 2 - 18), _sx(gy1 + 60), _sx(self.W / 2 + 18), _sx(gy1 + 96)], fill=(250, 250, 250), outline=(30, 30, 30), width=3 * S)
        _score_rows(d, g, 60, gy1 + 150, self.W - 120, extra=lambda p: f"{st['score'][p['id']]}  ({st['kicks'][p['id']]}/5)", rowh=36)
        return _finish(img, self.W, self.H)


# ── 14. Treasure Hunt ──────────────────────────────────────────────────────

_TH_N = 6


@_register
class Treasure(Spec):
    key = "treasure"
    title = "🏴‍☠️ Treasure Hunt"
    blurb = "A chest is buried in the 6×6 grid. Dig a square: the colour tells you how close you are. First to find it wins."
    min, max = 2, 6
    W, H = 700, 700
    hint = "dig a square"

    def start(self, g):
        g["st"] = {"t": random.randrange(_TH_N * _TH_N), "dug": {}, "last": None}

    def _dist(self, a, b):
        ra, ca = divmod(a, _TH_N)
        rb, cb = divmod(b, _TH_N)
        return max(abs(ra - rb), abs(ca - cb))

    def keyboard(self, g):
        st = g["st"]
        heat = {0: "💰", 1: "🔥", 2: "♨️", 3: "🌤"}
        rows = []
        for r in range(_TH_N):
            row = []
            for c in range(_TH_N):
                i = r * _TH_N + c
                t = heat.get(st["dug"][i], "❄️") if i in st["dug"] else "·"
                row.append((t, f"g{i}"))
            rows.append(row)
        return rows

    def caption(self, g):
        return ["🔥 burning · ♨️ warm · 🌤 cool · ❄️ cold"]

    def robot(self, g, p):
        st = g["st"]
        closed = [i for i in range(_TH_N * _TH_N) if i not in st["dug"]]
        # squares consistent with every clue so far
        ok = [i for i in closed if all(self._dist(i, j) == dd for j, dd in st["dug"].items())]
        # a robot that always reasons perfectly finds it in three digs - let it
        # guess blindly half the time so humans stay in the hunt
        return f"g{random.choice(ok if ok and random.random() < 0.5 else closed)}"

    def act(self, g, p, action, secret=None):
        st = g["st"]
        try:
            i = int(action[1:])
        except Exception:
            return "Tap a square."
        if i in st["dug"]:
            return "Already dug there."
        dd = self._dist(i, st["t"])
        st["dug"][i] = dd
        st["last"] = i
        if dd == 0:
            _log(g, f"💰 {_tag(p)} digs up the treasure!")
            _win(g, p)
            return None
        word = {1: "🔥 burning hot", 2: "♨️ warm", 3: "🌤 cool"}.get(dd, "❄️ ice cold")
        _log(g, f"{_tag(p)} digs - {word}")
        _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "TREASURE HUNT", status)
        cs, x0, y0 = 88, 86, TOP + 30
        _frame(d, x0, y0, x0 + cs * _TH_N, y0 + cs * _TH_N)
        heat = {1: (235, 80, 60), 2: (240, 150, 60), 3: (240, 210, 110)}
        for i in range(_TH_N * _TH_N):
            r, c = divmod(i, _TH_N)
            x, y = x0 + c * cs, y0 + r * cs
            if i in st["dug"]:
                dd = st["dug"][i]
                if dd == 0:
                    _cell(d, x, y, cs, (255, 214, 92))
                    d.rounded_rectangle([_sx(x + 18), _sx(y + 30), _sx(x + cs - 18), _sx(y + cs - 16)], radius=6 * S, fill=(120, 70, 30), outline=(80, 45, 20), width=3 * S)
                    d.rectangle([_sx(x + 18), _sx(y + 30), _sx(x + cs - 18), _sx(y + 46)], fill=(160, 100, 40))
                    d.ellipse([_sx(x + cs / 2 - 6), _sx(y + 42), _sx(x + cs / 2 + 6), _sx(y + 54)], fill=(255, 220, 90))
                else:
                    _cell(d, x, y, cs, heat.get(dd, (150, 190, 230)))
                    d.ellipse([_sx(x + 20), _sx(y + 26), _sx(x + cs - 20), _sx(y + cs - 14)], fill=(110, 80, 50))
            else:
                _cell(d, x, y, cs, (196, 168, 110) if (r + c) % 2 == 0 else (182, 152, 96))
            if st["last"] == i:
                d.rectangle([_sx(x), _sx(y), _sx(x + cs), _sx(y + cs)], outline=(255, 255, 255), width=3 * S)
        return _finish(img, self.W, self.H)


# ── 15. Slot Machine ───────────────────────────────────────────────────────

_SLOT_SYMS = [("7", (230, 50, 50), 1), ("BAR", (40, 40, 40), 2), ("★", (240, 190, 30), 3), ("◆", (60, 120, 230), 4), ("●", (52, 168, 83), 6)]
_SLOT_PAY = {"7": 100, "BAR": 50, "★": 30, "◆": 20, "●": 10}


@_register
class Slots(Spec):
    key = "slot"
    title = "🎰 Slot Machine"
    blurb = "Everyone spins 3 times. Three of a kind pays big, a pair pays a little. Highest total wins."
    min, max = 2, 6
    W, H = 700, 620
    default_action = "spin"
    hint = "tap 🎰 Spin"

    def start(self, g):
        g["st"] = {"round": 1, "reels": {}, "total": {p["id"]: 0 for p in g["players"]}, "spun": set()}

    def keyboard(self, g):
        return [[("🎰 Spin", "spin")]]

    def caption(self, g):
        st = g["st"]
        return [f"Spin {st['round']} of 3"] + [f"{_tag(p)} · {st['total'][p['id']]} pts" for p in g["players"]]

    def secret(self, g, p):
        names = [s[0] for s in _SLOT_SYMS]
        w = [s[2] for s in _SLOT_SYMS]
        return [random.choices(names, w)[0] for _ in range(3)]

    def act(self, g, p, action, secret=None):
        st = g["st"]
        reels = secret or self.secret(g, p)
        st["reels"][p["id"]] = reels
        st["last_id"] = p["id"]
        st["spun"].add(p["id"])
        if reels[0] == reels[1] == reels[2]:
            pay = _SLOT_PAY[reels[0]]
            note = "JACKPOT!" if reels[0] == "7" else "three of a kind!"
        elif len(set(reels)) == 2:
            pay = 5
            note = "a pair"
        else:
            pay = 0
            note = "nothing"
        st["total"][p["id"]] += pay
        _log(g, f"{_tag(p)} · {' '.join(reels)} · {note} <b>+{pay}</b>")
        alive = _alive(g)
        if all(q["id"] in st["spun"] for q in alive):
            if st["round"] >= 3:
                best = max(st["total"][q["id"]] for q in alive)
                tops = [q for q in alive if st["total"][q["id"]] == best]
                _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
                return None
            st["round"] += 1
            st["spun"] = set()
            g["turn"] = g["players"].index(alive[0])
        else:
            _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Spin {st['round']}/3 · {cur['name']}")
        img, d = _canvas(self.W, self.H, "SLOT MACHINE", status)
        shown = st["reels"].get(st.get("last_id"))
        _panel(d, 70, TOP + 20, self.W - 70, TOP + 200, fill=(120, 30, 40), outline=(240, 190, 30))
        for k in range(3):
            x = 110 + k * 170
            d.rounded_rectangle([_sx(x), _sx(TOP + 45), _sx(x + 140), _sx(TOP + 175)], radius=10 * S, fill=(250, 248, 240), outline=(200, 190, 170), width=3 * S)
            if shown:
                sym = shown[k]
                col = dict((s[0], s[1]) for s in _SLOT_SYMS)[sym]
                _txt(d, x + 70, TOP + 110, sym, 54 if len(sym) == 1 else 34, col)
            else:
                _txt(d, x + 70, TOP + 110, "?", 54, (180, 180, 180))
        q = _find(g, st.get("last_id")) if shown else None
        if q:
            _txt(d, self.W / 2, TOP + 225, f"{q['name']}'s spin", 16, GOLD)
        _score_rows(d, g, 70, TOP + 250, self.W - 140, extra=lambda p: f"{st['total'][p['id']]} pts", rowh=40)
        return _finish(img, self.W, self.H)
