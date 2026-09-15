#!/usr/bin/env python3
"""games_board.py - the board-game half of the second batch: Ludo, Chess,
Checkers, Battleship, Blackjack. Imported by games.py; each class registers
itself with the shared engine there. See games.Spec for the contract."""

import copy
import random

from PIL import ImageDraw

from games import (Spec, _register, COLORS, TOP, S, GOLD, INK, CELL_A, CELL_B,
                   _canvas, _finish, _frame, _panel, _cell, _txt, _token, _plabel, _die,
                   _score_rows, _shade, _sx, _font, _log, _advance, _win, _draw_game,
                   _current, _alive, _find, _tag, _tags, _scores, _esc, _dyn)

try:
    import chess as _chess
except Exception:            # the game simply stays off the menu without the library
    _chess = None


# ── Ludo ───────────────────────────────────────────────────────────────────

_LP = ([(6, c) for c in range(1, 6)] + [(r, 6) for r in range(5, -1, -1)] + [(0, 7)] +
       [(r, 8) for r in range(0, 6)] + [(6, c) for c in range(9, 15)] + [(7, 14)] +
       [(8, c) for c in range(14, 8, -1)] + [(r, 8) for r in range(9, 15)] + [(14, 7)] +
       [(r, 6) for r in range(14, 8, -1)] + [(8, c) for c in range(5, -1, -1)] + [(7, 0)] + [(6, 0)])
assert len(_LP) == 52
_L_OFF = [0, 13, 26, 39]
_L_HOMECOL = {0: [(7, c) for c in range(1, 6)], 1: [(r, 7) for r in range(1, 6)],
              2: [(7, c) for c in range(13, 8, -1)], 3: [(r, 7) for r in range(13, 8, -1)]}
_L_BASE = {0: (0, 0), 1: (0, 9), 2: (9, 9), 3: (9, 0)}
_L_HOME = {0: (7, 5.6), 1: (5.6, 7), 2: (7, 8.4), 3: (8.4, 7)}
_L_SAFE = {0, 8, 13, 21, 26, 34, 39, 47}
_L_CORNERS = {2: [0, 2], 3: [0, 1, 2], 4: [0, 1, 2, 3]}
_L_CELL = 44
_L_M = (776 - _L_CELL * 15) / 2


def _l_cell(corner, prog):
    if 1 <= prog <= 51:
        return _LP[(_L_OFF[corner] + prog - 1) % 52]
    if 52 <= prog <= 56:
        return _L_HOMECOL[corner][prog - 52]
    return _L_HOME[corner]


def _l_xy(rc):
    r, c = rc
    return (_L_M + c * _L_CELL + _L_CELL / 2, TOP + r * _L_CELL + _L_CELL / 2)


@_register
class Ludo(Spec):
    key = "ludo"
    title = "🎲 Ludo"
    blurb = "Two tokens each (chat-length game). Roll a 6 to leave base, land on a rival to send it home, exact roll into home. 6 = roll again. First with both tokens home wins."
    min, max = 2, 4
    W, H = 776, TOP + _L_CELL * 15 + 28
    default_action = "roll"
    hint = "tap 🎲 Roll"

    def start(self, g):
        corners = _L_CORNERS[min(4, max(2, len(g["players"])))]
        g["st"] = {"corner": {p["id"]: corners[i] for i, p in enumerate(g["players"])},
                   "tok": {p["id"]: [0, 0] for p in g["players"]}, "roll": None, "last": None}

    def _movable(self, g, p, d):
        return [i for i, t in enumerate(g["st"]["tok"][p["id"]]) if (t == 0 and d == 6) or (1 <= t <= 56 and t + d <= 57)]

    def keyboard(self, g):
        st = g["st"]
        if st["roll"] is None:
            return [[("🎲 Roll", "roll")]]
        cur = _current(g)
        return [[(f"Token {i + 1}", f"t{i}") for i in self._movable(g, cur, st["roll"])]]

    def caption(self, g):
        st = g["st"]
        out = []
        for p in g["players"]:
            t = st["tok"][p["id"]]
            out.append(f"{_tag(p)} · 🏠 {sum(1 for x in t if x == 57)}/{len(t)} home · {sum(1 for x in t if x == 0)} in base")
        return out

    def turn_text(self, g):
        cur = _current(g)
        if cur["bot"]:
            return f"🤖 {_tag(cur)} is thinking…"
        if g["st"]["roll"] is not None:
            return f"👉 {_tag(cur)} rolled <b>{g['st']['roll']}</b> - pick a token"
        return f"👉 Turn: {_tag(cur)} - tap 🎲 Roll"

    def secret(self, g, p):
        return None if g["st"]["roll"] is not None else random.randint(1, 6)

    def robot(self, g, p):
        st = g["st"]
        if st["roll"] is None:
            return "roll"
        d = st["roll"]
        mv = self._movable(g, p, d)
        best, score = mv[0], -1
        for i in mv:
            t = st["tok"][p["id"]][i]
            new = 1 if t == 0 else t + d
            s = 0
            if new == 57:
                s = 90
            elif t == 0:
                s = 60
            elif 1 <= new <= 51:
                idx = (_L_OFF[st["corner"][p["id"]]] + new - 1) % 52
                if idx not in _L_SAFE:
                    for q in g["players"]:
                        if q is p:
                            continue
                        for tq in st["tok"][q["id"]]:
                            if 1 <= tq <= 51 and (_L_OFF[st["corner"][q["id"]]] + tq - 1) % 52 == idx:
                                s = 100
                if idx in _L_SAFE:
                    s = max(s, 40)
            s += new / 10
            if s > score:
                best, score = i, s
        return f"t{best}"

    def _move(self, g, p, i, d):
        st = g["st"]
        t = st["tok"][p["id"]][i]
        new = 1 if t == 0 else t + d
        st["tok"][p["id"]][i] = new
        line = f"{_tag(p)} moves token {i + 1}" + (" out of base" if t == 0 else f" to {new}")
        if 1 <= new <= 51:
            idx = (_L_OFF[st["corner"][p["id"]]] + new - 1) % 52
            if idx not in _L_SAFE:
                for q in g["players"]:
                    if q is p:
                        continue
                    for j, tq in enumerate(st["tok"][q["id"]]):
                        if 1 <= tq <= 51 and (_L_OFF[st["corner"][q["id"]]] + tq - 1) % 52 == idx:
                            st["tok"][q["id"]][j] = 0
                            line += f" · 💥 sends {_tag(q)}'s token {j + 1} home!"
        elif new == 57:
            line += " 🏠 home!"
        st["last"] = {"c": p["c"], "roll": d, "cell": _l_cell(st["corner"][p["id"]], new)}
        _log(g, line)
        if all(x == 57 for x in st["tok"][p["id"]]):
            _win(g, p)
            return
        st["roll"] = None
        if d != 6:
            _advance(g)

    def act(self, g, p, action, secret=None):
        st = g["st"]
        if action == "roll":
            if st["roll"] is not None:
                return "Pick a token first."
            d = secret or random.randint(1, 6)
            mv = self._movable(g, p, d)
            if not mv:
                _log(g, f"{_tag(p)} rolled <b>{d}</b> - no move" + (" · rolls again" if d == 6 else ""))
                st["last"] = {"c": p["c"], "roll": d, "cell": None}
                if d != 6:
                    _advance(g)
                return None
            if len(mv) == 1:
                _log(g, f"{_tag(p)} rolled <b>{d}</b>")
                self._move(g, p, mv[0], d)
                return None
            st["roll"] = d
            st["last"] = {"c": p["c"], "roll": d, "cell": None}
            return None
        if action.startswith("t"):
            if st["roll"] is None:
                return "Roll first."
            try:
                i = int(action[1:])
            except Exception:
                return "Pick a token."
            if i not in self._movable(g, p, st["roll"]):
                return "That token can't move."
            self._move(g, p, i, st["roll"])
            return None
        return "Tap Roll."

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "LUDO", status)
        cs = _L_CELL
        _frame(d, _L_M, TOP, _L_M + cs * 15, TOP + cs * 15, pad=10)
        seat_col = {}
        for p in g["players"]:
            seat_col[st["corner"][p["id"]]] = COLORS[p["c"]][2]
        d.rectangle([_sx(_L_M), _sx(TOP), _sx(_L_M + cs * 15), _sx(TOP + cs * 15)], fill=(250, 246, 236))
        # bases
        for corner, (r0, c0) in _L_BASE.items():
            col = seat_col.get(corner, (200, 200, 200))
            x0, y0 = _L_M + c0 * cs, TOP + r0 * cs
            d.rectangle([_sx(x0), _sx(y0), _sx(x0 + cs * 6), _sx(y0 + cs * 6)], fill=col)
            d.rounded_rectangle([_sx(x0 + 30), _sx(y0 + 30), _sx(x0 + cs * 6 - 30), _sx(y0 + cs * 6 - 30)], radius=12 * S, fill=(252, 250, 244))
            for k in range(2):
                sx_, sy_ = x0 + cs * 6 / 2 + (-45 if k % 2 == 0 else 45), y0 + cs * 6 / 2
                d.ellipse([_sx(sx_ - 20), _sx(sy_ - 20), _sx(sx_ + 20), _sx(sy_ + 20)], fill=_shade(col, 0.55), outline=col, width=3 * S)
        # path cells
        for idx, (r, c) in enumerate(_LP):
            x0, y0 = _L_M + c * cs, TOP + r * cs
            fill = (255, 255, 255)
            for corner, off in enumerate(_L_OFF):
                if idx == off:
                    fill = _shade(seat_col.get(corner, (200, 200, 200)), 0.25)
            d.rectangle([_sx(x0), _sx(y0), _sx(x0 + cs), _sx(y0 + cs)], fill=fill, outline=(200, 190, 175), width=1 * S)
            if idx in _L_SAFE:
                _txt(d, x0 + cs / 2, y0 + cs / 2, "★", 20, (200, 180, 120))
        for corner, cells in _L_HOMECOL.items():
            col = seat_col.get(corner, (200, 200, 200))
            for (r, c) in cells:
                x0, y0 = _L_M + c * cs, TOP + r * cs
                d.rectangle([_sx(x0), _sx(y0), _sx(x0 + cs), _sx(y0 + cs)], fill=_shade(col, 0.35), outline=(200, 190, 175), width=1 * S)
        # centre home
        cx, cy = _L_M + cs * 7.5, TOP + cs * 7.5
        h = cs * 1.5
        tri = {0: [(cx - h, cy - h), (cx - h, cy + h)], 1: [(cx - h, cy - h), (cx + h, cy - h)],
               2: [(cx + h, cy - h), (cx + h, cy + h)], 3: [(cx - h, cy + h), (cx + h, cy + h)]}
        for corner, pts in tri.items():
            d.polygon([(_sx(cx), _sx(cy))] + [(_sx(x), _sx(y)) for x, y in pts], fill=seat_col.get(corner, (215, 215, 215)))
        # last move highlight
        if st["last"]:
            _die(d, self.W / 2 - 24, 12, st["last"]["roll"], COLORS[st["last"]["c"]][2], size=48)
            if st["last"]["cell"] and st["last"]["cell"] not in _L_HOME.values():
                x, y = _l_xy(st["last"]["cell"])
                d.rectangle([_sx(x - cs / 2), _sx(y - cs / 2), _sx(x + cs / 2), _sx(y + cs / 2)], outline=(255, 170, 0), width=3 * S)
        # tokens
        counts = {}
        for p in g["players"]:
            corner = st["corner"][p["id"]]
            col = COLORS[p["c"]][2]
            r0, c0 = _L_BASE[corner]
            for i, t in enumerate(st["tok"][p["id"]]):
                if t == 0:
                    x = _L_M + c0 * cs + cs * 3 + (-45 if i % 2 == 0 else 45)
                    y = TOP + r0 * cs + cs * 3
                else:
                    cell = _l_cell(corner, t)
                    x, y = _l_xy(cell)
                    k = counts.get(cell, 0)
                    counts[cell] = k + 1
                    x += (k % 2) * 12 - 6
                    y += (k // 2) * 12 - 6
                _token(d, x, y, col, str(i + 1), r=14)
        return _finish(img, self.W, self.H)


# ── Chess ──────────────────────────────────────────────────────────────────

_PV = {1: 1, 2: 3, 3: 3, 4: 5, 5: 9, 6: 0}
_GLYPH = {"P": "♟", "N": "♞", "B": "♝", "R": "♜", "Q": "♛", "K": "♚"}


if _chess:
    @_register
    class Chess(Spec):
        key = "chess"
        title = "♟️ Chess"
        blurb = "Full rules - castling, en passant, promotion to queen. Tap a piece, then where it goes."
        min = max = 2
        W, H = 700, 800
        hint = "pick a piece"

        def start(self, g):
            g["st"] = {"fen": _chess.Board().fen(), "sel": None, "last": None, "moves": 0}

        def _board(self, g):
            return _chess.Board(g["st"]["fen"])

        def _legal(self, b):
            return [m for m in b.legal_moves if m.promotion in (None, _chess.QUEEN)]

        def keyboard(self, g):
            st = g["st"]
            b = self._board(g)
            moves = self._legal(b)
            if st["sel"] is None:
                sqs = sorted({m.from_square for m in moves})
                btn = [(f"{_GLYPH[b.piece_at(s).symbol().upper()]}{_chess.square_name(s)}", f"p{s}") for s in sqs]
            else:
                tos = sorted({m.to_square for m in moves if m.from_square == st["sel"]})
                btn = [(_chess.square_name(s), f"m{s}") for s in tos] + [("↩ Back", "back")]
            return [btn[i:i + 4] for i in range(0, len(btn), 4)]

        def caption(self, g):
            b = self._board(g)
            out = [f"{_tag(g['players'][0])} · White", f"{_tag(g['players'][1])} · Black"]
            if b.is_check() and g["phase"] == "play":
                out.append("⚠️ Check!")
            return out

        def turn_text(self, g):
            cur = _current(g)
            if cur["bot"]:
                return f"🤖 {_tag(cur)} is thinking…"
            st = g["st"]
            if st["sel"] is not None:
                return f"👉 {_tag(cur)} · {_GLYPH[self._board(g).piece_at(st['sel']).symbol().upper()]}{_chess.square_name(st['sel'])} - where to?"
            return f"👉 Turn: {_tag(cur)} - pick a piece"

        def _eval(self, b, colour):
            if b.is_checkmate():
                return -1000 if b.turn == colour else 1000
            s = 0
            for sq, pc in b.piece_map().items():
                s += _PV[pc.piece_type] * (1 if pc.color == colour else -1)
            return s

        def robot(self, g, p):
            st = g["st"]
            b = self._board(g)
            if st["sel"] is not None:
                return "back"
            me = b.turn
            best, bests = None, -10 ** 9
            for m in self._legal(b):
                b.push(m)
                if b.is_checkmate():
                    b.pop()
                    return f"m{m.to_square}:{m.from_square}"
                worst = 10 ** 9
                replies = self._legal(b)
                if not replies:
                    worst = self._eval(b, me)
                for r in replies[:40]:
                    b.push(r)
                    worst = min(worst, self._eval(b, me))
                    b.pop()
                b.pop()
                sc = worst + random.random() * 0.3 + (0.2 if b.is_capture(m) else 0)
                if sc > bests:
                    best, bests = m, sc
            return f"m{best.to_square}:{best.from_square}"

        def act(self, g, p, action, secret=None):
            st = g["st"]
            b = self._board(g)
            if action == "back":
                st["sel"] = None
                return None
            if action.startswith("p"):
                try:
                    s = int(action[1:])
                except Exception:
                    return "Pick a piece."
                if not any(m.from_square == s for m in self._legal(b)):
                    return "That piece has no legal move."
                st["sel"] = s
                return None
            if action.startswith("m"):
                body = action[1:]
                if ":" in body:                      # robot passes from-square too
                    to, frm = (int(x) for x in body.split(":"))
                else:
                    to, frm = int(body), st["sel"]
                if frm is None:
                    return "Pick a piece first."
                mv = None
                for m in self._legal(b):
                    if m.from_square == frm and m.to_square == to:
                        mv = m
                        break
                if mv is None:
                    return "Not a legal move."
                san = b.san(mv)
                b.push(mv)
                st["fen"] = b.fen()
                st["sel"] = None
                st["last"] = (frm, to)
                st["moves"] += 1
                _log(g, f"{_tag(p)} · <b>{_dyn(_esc(san))}</b>")
                if b.is_checkmate():
                    _log(g, "Checkmate!")
                    _win(g, p)
                elif b.is_stalemate() or b.is_insufficient_material() or b.can_claim_draw():
                    _log(g, "Stalemate" if b.is_stalemate() else "Draw")
                    _draw_game(g)
                else:
                    _advance(g)
                return None
            return "Tap a piece."

        def render(self, g):
            st = g["st"]
            b = self._board(g)
            cur = _current(g)
            status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                      "Draw" if g["phase"] == "over" else f"Turn: {cur['name']}")
            img, d = _canvas(self.W, self.H, "CHESS", status)
            cs, x0, y0 = 76, 46, TOP + 30
            _frame(d, x0, y0, x0 + cs * 8, y0 + cs * 8)
            legal_to = {m.to_square for m in self._legal(b) if m.from_square == st["sel"]} if st["sel"] is not None else set()
            king_sq = b.king(b.turn) if b.is_check() else None
            for sq in range(64):
                f, r = _chess.square_file(sq), _chess.square_rank(sq)
                x, y = x0 + f * cs, y0 + (7 - r) * cs
                light = (f + r) % 2 == 1
                fill = (238, 224, 196) if light else (150, 108, 72)
                if st["last"] and sq in st["last"]:
                    fill = (232, 214, 120) if light else (176, 150, 70)
                if sq == st["sel"]:
                    fill = (150, 210, 130)
                if sq == king_sq:
                    fill = (230, 90, 80)
                d.rectangle([_sx(x), _sx(y), _sx(x + cs), _sx(y + cs)], fill=fill)
                if sq in legal_to:
                    d.ellipse([_sx(x + cs / 2 - 9), _sx(y + cs / 2 - 9), _sx(x + cs / 2 + 9), _sx(y + cs / 2 + 9)], fill=(60, 140, 70, 170))
                pc = b.piece_at(sq)
                if pc:
                    glyph = _GLYPH[pc.symbol().upper()]
                    fill_c = (250, 250, 250) if pc.color == _chess.WHITE else (30, 30, 30)
                    edge = (30, 30, 30) if pc.color == _chess.WHITE else (220, 220, 220)
                    fnt = _font(52, bold=False)
                    cx, cy = _sx(x + cs / 2), _sx(y + cs / 2 + 2)
                    for dx, dy in ((-2, 0), (2, 0), (0, -2), (0, 2), (-2, -2), (2, 2), (-2, 2), (2, -2)):
                        d.text((cx + dx * S, cy + dy * S), glyph, font=fnt, fill=edge, anchor="mm")
                    d.text((cx, cy), glyph, font=fnt, fill=fill_c, anchor="mm")
            for f in range(8):
                _txt(d, x0 + f * cs + cs / 2, y0 + cs * 8 + 26, "abcdefgh"[f], 15, GOLD)
            for r in range(8):
                _txt(d, x0 - 26, y0 + (7 - r) * cs + cs / 2, str(r + 1), 15, GOLD)
            return _finish(img, self.W, self.H)


# ── Checkers ───────────────────────────────────────────────────────────────

def _ck_name(sq):
    r, c = divmod(sq, 8)
    return "abcdefgh"[c] + str(8 - r)


@_register
class Checkers(Spec):
    key = "checkers"
    title = "⚫⚪ Checkers"
    blurb = "Diagonal moves, jump to capture - and jumps are compulsory. Reach the far side to crown a king."
    min = max = 2
    W, H = 700, 800
    hint = "pick a piece"

    def start(self, g):
        b = {}
        for r in range(8):
            for c in range(8):
                if (r + c) % 2 == 1:
                    if r < 3:
                        b[r * 8 + c] = [1, False]      # player index 1 (top) moving down
                    elif r > 4:
                        b[r * 8 + c] = [0, False]      # player 0 (bottom) moving up
        g["st"] = {"b": b, "sel": None, "chain": None, "last": None}

    def _moves(self, st, owner, only_from=None):
        b = st["b"]
        simple, jumps = [], []
        for sq, (o, king) in b.items():
            if o != owner or (only_from is not None and sq != only_from):
                continue
            r, c = divmod(sq, 8)
            dirs = [(-1, -1), (-1, 1), (1, -1), (1, 1)] if king else ([(-1, -1), (-1, 1)] if owner == 0 else [(1, -1), (1, 1)])
            for dr, dc in dirs:
                r1, c1 = r + dr, c + dc
                if 0 <= r1 < 8 and 0 <= c1 < 8:
                    t = r1 * 8 + c1
                    if t not in b:
                        simple.append((sq, t, None))
                    elif b[t][0] != owner:
                        r2, c2 = r1 + dr, c1 + dc
                        if 0 <= r2 < 8 and 0 <= c2 < 8 and (r2 * 8 + c2) not in b:
                            jumps.append((sq, r2 * 8 + c2, t))
        return jumps if jumps else simple

    def keyboard(self, g):
        st = g["st"]
        cur = _current(g)
        owner = g["players"].index(cur)
        moves = self._moves(st, owner, st["chain"])
        if st["sel"] is None:
            frm = sorted({m[0] for m in moves})
            btn = [(("♛" if st["b"][s][1] else "●") + _ck_name(s), f"p{s}") for s in frm]
        else:
            btn = [(_ck_name(m[1]), f"m{m[1]}") for m in moves if m[0] == st["sel"]]
            if st["chain"] is None:
                btn.append(("↩ Back", "back"))
        return [btn[i:i + 4] for i in range(0, len(btn), 4)]

    def caption(self, g):
        b = g["st"]["b"]
        return [f"{_tag(p)} · {sum(1 for v in b.values() if v[0] == i)} pieces" for i, p in enumerate(g["players"])]

    def turn_text(self, g):
        cur = _current(g)
        st = g["st"]
        if cur["bot"]:
            return f"🤖 {_tag(cur)} is thinking…"
        if st["chain"] is not None:
            return f"👉 {_tag(cur)} - jump again!"
        if st["sel"] is not None:
            return f"👉 {_tag(cur)} · {_ck_name(st['sel'])} - where to?"
        return f"👉 Turn: {_tag(cur)} - pick a piece"

    def robot(self, g, p):
        st = g["st"]
        owner = g["players"].index(p)
        if st["sel"] is not None and st["chain"] is None:
            return "back"
        moves = self._moves(st, owner, st["chain"])
        def score(m):
            frm, to, cap = m
            s = 10 if cap else 0
            r = to // 8
            if not st["b"][frm][1] and ((owner == 0 and r == 0) or (owner == 1 and r == 7)):
                s += 5
            # avoid landing where it can be taken
            st2 = copy.deepcopy(st)
            self._apply(st2, m, owner)
            if any(mm[2] == to for mm in self._moves(st2, 1 - owner)):
                s -= 6
            return s + random.random()
        best = max(moves, key=score)
        return f"m{best[1]}:{best[0]}"

    def _apply(self, st, m, owner):
        frm, to, cap = m
        piece = st["b"].pop(frm)
        if cap is not None:
            st["b"].pop(cap, None)
        r = to // 8
        crowned = False
        if not piece[1] and ((owner == 0 and r == 0) or (owner == 1 and r == 7)):
            piece[1] = True
            crowned = True
        st["b"][to] = piece
        return crowned

    def act(self, g, p, action, secret=None):
        st = g["st"]
        owner = g["players"].index(p)
        if action == "back":
            if st["chain"] is not None:
                return "You must finish the jump."
            st["sel"] = None
            return None
        if action.startswith("p"):
            s = int(action[1:])
            if s not in {m[0] for m in self._moves(st, owner, st["chain"])}:
                return "That piece can't move (jumps are compulsory)."
            st["sel"] = s
            return None
        if action.startswith("m"):
            body = action[1:]
            if ":" in body:
                to, frm = (int(x) for x in body.split(":"))
            else:
                to, frm = int(body), st["sel"]
            if frm is None:
                return "Pick a piece first."
            mv = next((m for m in self._moves(st, owner, st["chain"]) if m[0] == frm and m[1] == to), None)
            if mv is None:
                return "Not a legal move."
            crowned = self._apply(st, mv, owner)
            st["sel"] = None
            st["last"] = (frm, to)
            line = f"{_tag(p)} · {_dyn(_ck_name(frm))} → {_dyn(_ck_name(to))}" + (" ✂️ captures" if mv[2] is not None else "") + (" 👑 king!" if crowned else "")
            _log(g, line)
            other = 1 - owner
            if not any(v[0] == other for v in st["b"].values()):
                _win(g, p)
                return None
            if mv[2] is not None and not crowned:
                again = [m for m in self._moves(st, owner, to) if m[2] is not None]
                if again:
                    st["chain"] = to
                    st["sel"] = to
                    return None
            st["chain"] = None
            if not self._moves(st, other):
                _log(g, f"{_tag(g['players'][other])} has no moves")
                _win(g, p)
                return None
            _advance(g)
            return None
        return "Pick a piece."

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "CHECKERS", status)
        cs, x0, y0 = 76, 46, TOP + 30
        _frame(d, x0, y0, x0 + cs * 8, y0 + cs * 8)
        owner = g["players"].index(cur) if g["phase"] == "play" else 0
        dests = {m[1] for m in self._moves(st, owner, st["chain"]) if m[0] == st["sel"]} if st["sel"] is not None else set()
        for sq in range(64):
            r, c = divmod(sq, 8)
            x, y = x0 + c * cs, y0 + r * cs
            dark = (r + c) % 2 == 1
            fill = (120, 84, 56) if dark else (238, 224, 196)
            if st["last"] and sq in st["last"]:
                fill = (176, 150, 70)
            if sq == st["sel"]:
                fill = (110, 170, 100)
            d.rectangle([_sx(x), _sx(y), _sx(x + cs), _sx(y + cs)], fill=fill)
            if sq in dests:
                d.ellipse([_sx(x + cs / 2 - 9), _sx(y + cs / 2 - 9), _sx(x + cs / 2 + 9), _sx(y + cs / 2 + 9)], fill=(60, 200, 90, 200))
            if sq in st["b"]:
                o, king = st["b"][sq]
                col = COLORS[g["players"][o]["c"]][2]
                _token(d, x + cs / 2, y + cs / 2, col, "♛" if king else "", r=26)
        for f in range(8):
            _txt(d, x0 + f * cs + cs / 2, y0 + cs * 8 + 26, "abcdefgh"[f], 15, GOLD)
        for r in range(8):
            _txt(d, x0 - 26, y0 + r * cs + cs / 2, str(8 - r), 15, GOLD)
        return _finish(img, self.W, self.H)


# ── Battleship ─────────────────────────────────────────────────────────────

_BS_N = 8
_BS_SHIPS = [5, 4, 3, 3, 2]


@_register
class Battleship(Spec):
    key = "ships"
    title = "🚢 Battleship"
    blurb = "Fleets are placed for you in secret. Take turns firing at the 8×8 grid; sink all five enemy ships to win."
    min = max = 2
    W, H = 776, 560
    hint = "fire at a square"

    def _place(self):
        fleet = []
        taken = set()
        for size in _BS_SHIPS:
            for _ in range(500):
                horiz = random.random() < 0.5
                r, c = random.randrange(_BS_N), random.randrange(_BS_N)
                cells = [(r * _BS_N + c + i) if horiz else ((r + i) * _BS_N + c) for i in range(size)]
                if (horiz and c + size > _BS_N) or (not horiz and r + size > _BS_N):
                    continue
                if any(x in taken for x in cells):
                    continue
                fleet.append(cells)
                taken.update(cells)
                break
        return fleet

    def start(self, g):
        g["st"] = {"fleet": {p["id"]: self._place() for p in g["players"]},
                   "shots": {p["id"]: {} for p in g["players"]}, "last": None}

    def _enemy(self, g, p):
        return [q for q in g["players"] if q is not p][0]

    def _sunk(self, st, victim, shooter):
        shots = st["shots"][shooter]
        return [s for s in st["fleet"][victim] if all(c in shots for c in s)]

    def keyboard(self, g):
        cur = _current(g)
        shots = g["st"]["shots"][cur["id"]]
        rows = []
        for r in range(_BS_N):
            rows.append([(("💥" if shots[i] else "🌊") if i in shots else "·", f"s{i}")
                         for i in (r * _BS_N + c for c in range(_BS_N))])
        return rows

    def caption(self, g):
        st = g["st"]
        out = []
        for p in g["players"]:
            e = self._enemy(g, p)
            out.append(f"{_tag(p)} · sunk {len(self._sunk(st, e['id'], p['id']))}/5 enemy ships")
        return out

    def robot(self, g, p):
        st = g["st"]
        shots = st["shots"][p["id"]]
        e = self._enemy(g, p)
        sunk_cells = {c for s in self._sunk(st, e["id"], p["id"]) for c in s}
        hits = [c for c, h in shots.items() if h and c not in sunk_cells]
        cands = []
        for h in hits:
            r, c = divmod(h, _BS_N)
            for dr, dc in ((0, 1), (1, 0), (0, -1), (-1, 0)):
                rr, cc = r + dr, c + dc
                if 0 <= rr < _BS_N and 0 <= cc < _BS_N and (rr * _BS_N + cc) not in shots:
                    cands.append(rr * _BS_N + cc)
        if cands:
            return f"s{random.choice(cands)}"
        free = [i for i in range(_BS_N * _BS_N) if i not in shots]
        parity = [i for i in free if (i // _BS_N + i % _BS_N) % 2 == 0]
        return f"s{random.choice(parity or free)}"

    def act(self, g, p, action, secret=None):
        st = g["st"]
        try:
            i = int(action[1:])
        except Exception:
            return "Tap a square."
        shots = st["shots"][p["id"]]
        if i in shots:
            return "Already fired there."
        e = self._enemy(g, p)
        hit = any(i in s for s in st["fleet"][e["id"]])
        shots[i] = hit
        st["last"] = (p["id"], i)
        name = _dyn("abcdefgh"[i % _BS_N] + str(i // _BS_N + 1))
        if hit:
            ship = next(s for s in st["fleet"][e["id"]] if i in s)
            if all(c in shots for c in ship):
                _log(g, f"{_tag(p)} fires {name} - 💥 <b>hit and sunk</b> a {len(ship)}-ship!")
            else:
                _log(g, f"{_tag(p)} fires {name} - 💥 <b>hit!</b>")
            if len(self._sunk(st, e["id"], p["id"])) == len(_BS_SHIPS):
                _win(g, p)
                return None
        else:
            _log(g, f"{_tag(p)} fires {name} - 🌊 miss")
        _advance(g)
        return None

    def _grid(self, d, g, x0, y0, cs, shooter, reveal):
        st = g["st"]
        e = self._enemy(g, shooter)
        shots = st["shots"][shooter["id"]]
        sunk = {c for s in self._sunk(st, e["id"], shooter["id"]) for c in s}
        fleet = {c for s in st["fleet"][e["id"]] for c in s}
        for i in range(_BS_N * _BS_N):
            r, c = divmod(i, _BS_N)
            x, y = x0 + c * cs, y0 + r * cs
            fill = (58, 110, 170) if (r + c) % 2 == 0 else (52, 100, 158)
            if i in sunk:
                fill = (90, 60, 60)
            elif reveal and i in fleet:
                fill = (110, 110, 120)
            d.rectangle([_sx(x), _sx(y), _sx(x + cs), _sx(y + cs)], fill=fill, outline=(40, 80, 130), width=1 * S)
            if i in shots:
                if shots[i]:
                    d.ellipse([_sx(x + 8), _sx(y + 8), _sx(x + cs - 8), _sx(y + cs - 8)], fill=(240, 90, 60))
                    _txt(d, x + cs / 2, y + cs / 2, "✕", 16, (255, 240, 200))
                else:
                    d.ellipse([_sx(x + cs / 2 - 5), _sx(y + cs / 2 - 5), _sx(x + cs / 2 + 5), _sx(y + cs / 2 + 5)], fill=(200, 225, 245))
            if st["last"] and st["last"] == (shooter["id"], i):
                d.rectangle([_sx(x), _sx(y), _sx(x + cs), _sx(y + cs)], outline=(255, 200, 60), width=3 * S)
        for c in range(_BS_N):
            _txt(d, x0 + c * cs + cs / 2, y0 - 12, "abcdefgh"[c], 12, GOLD)
        for r in range(_BS_N):
            _txt(d, x0 - 12, y0 + r * cs + cs / 2, str(r + 1), 12, GOLD)

    def render(self, g):
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "BATTLESHIP", status)
        cs = 42
        over = g["phase"] == "over"
        for k, p in enumerate(g["players"]):
            x0 = 50 + k * 390
            y0 = TOP + 50
            _panel(d, x0 - 20, y0 - 34, x0 + cs * _BS_N + 12, y0 + cs * _BS_N + 12, fill=(28, 44, 70), outline=(214, 172, 80))
            _txt(d, x0 + cs * _BS_N / 2, y0 - 26, f"{p['name']}'s shots", 14, (255, 255, 255))
            self._grid(d, g, x0, y0, cs, p, reveal=over)
            e = self._enemy(g, p)
            left = len(_BS_SHIPS) - len(self._sunk(g["st"], e["id"], p["id"]))
            _txt(d, x0 + cs * _BS_N / 2, y0 + cs * _BS_N + 34, f"enemy ships left: {left}", 14, GOLD)
        return _finish(img, self.W, self.H)


# ── Blackjack ──────────────────────────────────────────────────────────────

_RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
_SUITS = ["♠", "♥", "♦", "♣"]


def _bj_total(hand):
    t = 0
    aces = 0
    for r, s in hand:
        if r == "A":
            t += 11
            aces += 1
        elif r in ("J", "Q", "K"):
            t += 10
        else:
            t += int(r)
    while t > 21 and aces:
        t -= 10
        aces -= 1
    return t


def _card(d, x, y, r, s, w=54, h=76, down=False):
    d.rounded_rectangle([_sx(x) + 3 * S, _sx(y) + 4 * S, _sx(x + w) + 3 * S, _sx(y + h) + 4 * S], radius=6 * S, fill=(0, 0, 0, 110))
    if down:
        d.rounded_rectangle([_sx(x), _sx(y), _sx(x + w), _sx(y + h)], radius=6 * S, fill=(120, 40, 50), outline=(240, 240, 240), width=2 * S)
        d.rounded_rectangle([_sx(x + 8), _sx(y + 8), _sx(x + w - 8), _sx(y + h - 8)], radius=4 * S, outline=(200, 120, 130), width=2 * S)
        return
    d.rounded_rectangle([_sx(x), _sx(y), _sx(x + w), _sx(y + h)], radius=6 * S, fill=(252, 252, 248), outline=(150, 150, 150), width=2 * S)
    col = (210, 40, 50) if s in "♥♦" else (30, 30, 30)
    _txt(d, x + 8, y + 6, r, 16, col, anchor="la")
    _txt(d, x + w / 2, y + h / 2 + 6, s, 30, col)


@_register
class Blackjack(Spec):
    key = "bj"
    title = "🃏 Blackjack"
    blurb = "Beat the dealer without going over 21. Hit or stand; the dealer draws to 17. Three rounds, most wins takes it."
    min, max = 2, 6
    W, H = 776, 640
    hint = "hit or stand"

    def start(self, g):
        g["st"] = {"round": 0, "score": {p["id"]: 0 for p in g["players"]}}
        self._deal(g)

    def _deal(self, g):
        st = g["st"]
        deck = [(r, s) for r in _RANKS for s in _SUITS]
        random.shuffle(deck)
        st["round"] += 1
        st["deck"] = deck
        st["hands"] = {p["id"]: [deck.pop(), deck.pop()] for p in g["players"]}
        st["dealer"] = [deck.pop(), deck.pop()]
        st["done"] = set()
        st["stage"] = "play"
        st["result"] = {}
        g["turn"] = g["players"].index(_alive(g)[0])

    def keyboard(self, g):
        return [[("🃏 Hit", "hit"), ("✋ Stand", "stand")]]

    def caption(self, g):
        st = g["st"]
        out = [f"Round {st['round']} of 3"]
        for p in g["players"]:
            h = st["hands"][p["id"]]
            t = _bj_total(h)
            res = st["result"].get(p["id"], "")
            out.append(f"{_tag(p)} · {t}{' bust' if t > 21 else ''} · {st['score'][p['id']]} won {res}")
        return out

    def robot(self, g, p):
        return "hit" if _bj_total(g["st"]["hands"][p["id"]]) < 17 else "stand"

    def _finish_round(self, g):
        st = g["st"]
        while _bj_total(st["dealer"]) < 17:
            st["dealer"].append(st["deck"].pop())
        dt = _bj_total(st["dealer"])
        st["stage"] = "result"
        wins = []
        for p in _alive(g):
            t = _bj_total(st["hands"][p["id"]])
            if t > 21:
                st["result"][p["id"]] = "❌"
            elif dt > 21 or t > dt:
                st["result"][p["id"]] = "✅"
                st["score"][p["id"]] += 1
                wins.append(p)
            elif t == dt:
                st["result"][p["id"]] = "🤝"
            else:
                st["result"][p["id"]] = "❌"
        _log(g, f"Dealer shows <b>{dt}</b>{' - bust!' if dt > 21 else ''} · " + (_tags(wins) + " win" if wins else "dealer takes the round"))
        if st["round"] >= 3:
            best = max(st["score"][p["id"]] for p in _alive(g))
            tops = [p for p in _alive(g) if st["score"][p["id"]] == best]
            _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
            return
        self._deal(g)

    def act(self, g, p, action, secret=None):
        st = g["st"]
        h = st["hands"][p["id"]]
        if action == "hit":
            h.append(st["deck"].pop())
            t = _bj_total(h)
            if t > 21:
                _log(g, f"{_tag(p)} hits · {_dyn(h[-1][0] + h[-1][1])} · <b>{t} bust</b>")
                st["done"].add(p["id"])
            else:
                _log(g, f"{_tag(p)} hits · {_dyn(h[-1][0] + h[-1][1])} · {t}")
                if t == 21:
                    st["done"].add(p["id"])
        elif action == "stand":
            _log(g, f"{_tag(p)} stands on <b>{_bj_total(h)}</b>")
            st["done"].add(p["id"])
        else:
            return "Hit or stand?"
        if p["id"] in st["done"]:
            if all(q["id"] in st["done"] for q in _alive(g)):
                self._finish_round(g)
            else:
                _advance(g)
                while _current(g)["id"] in st["done"]:
                    _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Round {st['round']} · {cur['name']}")
        n = len(g["players"])
        h = TOP + 150 + 100 * n + 30
        img, d = _canvas(self.W, h, "BLACKJACK", status)
        # dealer
        _panel(d, 40, TOP + 16, self.W - 40, TOP + 128, fill=(40, 30, 30), outline=(214, 172, 80))
        _txt(d, 56, TOP + 34, "DEALER", 14, GOLD, anchor="la")
        hidden = st["stage"] == "play" and g["phase"] == "play"
        for k, (r, s) in enumerate(st["dealer"]):
            _card(d, 140 + k * 62, TOP + 30, r, s, down=(hidden and k == 1))
        if not hidden:
            _txt(d, self.W - 60, TOP + 72, str(_bj_total(st["dealer"])), 30, GOLD, anchor="rm")
        # players
        for i, p in enumerate(g["players"]):
            y = TOP + 150 + i * 100
            active = g["phase"] == "play" and cur is p
            _panel(d, 40, y, self.W - 40, y + 90, outline=GOLD if active else (90, 90, 100))
            _token(d, 70, y + 45, COLORS[p["c"]][2], _plabel(p), r=15)
            _txt(d, 96, y + 26, p["name"], 15, GOLD if active else (255, 255, 255), anchor="lm")
            _txt(d, 96, y + 64, f"{st['score'][p['id']]} won", 12, (170, 180, 200), anchor="lm")
            hand = st["hands"][p["id"]]
            for k, (r, s) in enumerate(hand[:8]):
                _card(d, 250 + k * 50, y + 8, r, s, w=46, h=66)
            t = _bj_total(hand)
            col = (255, 110, 110) if t > 21 else (255, 255, 255)
            word = {"✅": "WIN", "❌": "LOSE", "🤝": "PUSH"}.get(st["result"].get(p["id"]), "")
            _txt(d, self.W - 60, y + 45, f"{t}  {word}".rstrip(), 26, col, anchor="rm")
        return _finish(img, self.W, h)
