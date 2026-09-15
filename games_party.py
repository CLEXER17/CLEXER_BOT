#!/usr/bin/env python3
"""games_party.py - the party half of the second batch: Trivia, Uno-style
cards, 2048, Simon Says, Word Scramble, Archery, Target, Boxing, Zombie
Survival, Number Puzzle. Imported by games.py; see games.Spec."""

import heapq
import html as _html
import random

import requests

from games import (Spec, _register, COLORS, TOP, S, GOLD, INK, CELL_A, CELL_B,
                   _canvas, _finish, _frame, _panel, _cell, _txt, _token, _plabel, _die, _bar,
                   _score_rows, _shade, _sx, _font, _log, _advance, _win, _draw_game,
                   _current, _alive, _find, _tag, _tags, _scores, _esc, _dyn, _HANG_WORDS)


def _wrap(d, text, font, maxw):
    words, lines, cur = text.split(), [], ""
    for w in words:
        t = (cur + " " + w).strip()
        if d.textlength(t, font=font) <= maxw:
            cur = t
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


# ── Trivia ─────────────────────────────────────────────────────────────────

_TRIVIA_BANK = [
    ("Crypto", "What is the maximum number of bitcoins that will ever exist?", ["21 million", "100 million", "1 billion", "42 million"], 0),
    ("Crypto", "Which blockchain did Ethereum's Vitalik Buterin co-found?", ["Solana", "Ethereum", "Cardano", "Ripple"], 1),
    ("Crypto", "A 'stop loss' order is placed to…", ["Take profit", "Limit a losing trade", "Add leverage", "Stake coins"], 1),
    ("Crypto", "What does 'HODL' mean in crypto slang?", ["Hold your coins", "High order daily limit", "Sell fast", "Hedge on decline"], 0),
    ("Crypto", "Which coin uses the ticker SOL?", ["Solar", "Solana", "Solidus", "Solvent"], 1),
    ("Trading", "In candlestick charts a green candle usually means the price…", ["Fell", "Rose", "Stayed flat", "Was halted"], 1),
    ("Trading", "Leverage of 10x on $100 controls a position worth…", ["$10", "$100", "$1,000", "$10,000"], 2),
    ("Trading", "What does 'TP' stand for in a trade signal?", ["Trend point", "Take profit", "Trade plan", "Top price"], 1),
    ("Science", "What planet is known as the Red Planet?", ["Venus", "Jupiter", "Mars", "Saturn"], 2),
    ("Science", "Water boils at what temperature at sea level?", ["90°C", "100°C", "110°C", "120°C"], 1),
    ("Science", "What gas do plants absorb from the air?", ["Oxygen", "Nitrogen", "Carbon dioxide", "Helium"], 2),
    ("Science", "How many bones are in the adult human body?", ["106", "206", "306", "406"], 1),
    ("Science", "What is the chemical symbol for gold?", ["Ag", "Gd", "Au", "Go"], 2),
    ("Geography", "What is the capital of Australia?", ["Sydney", "Melbourne", "Canberra", "Perth"], 2),
    ("Geography", "Which is the longest river in the world?", ["Amazon", "Nile", "Yangtze", "Mississippi"], 1),
    ("Geography", "Mount Everest lies on the border of Nepal and…", ["India", "Bhutan", "China", "Pakistan"], 2),
    ("Geography", "Which country has the most people?", ["India", "USA", "Indonesia", "Brazil"], 0),
    ("Geography", "The Sahara desert is on which continent?", ["Asia", "Africa", "Australia", "South America"], 1),
    ("History", "In which year did World War II end?", ["1943", "1945", "1947", "1950"], 1),
    ("History", "Who was the first person to walk on the Moon?", ["Buzz Aldrin", "Yuri Gagarin", "Neil Armstrong", "John Glenn"], 2),
    ("History", "The Taj Mahal was built by which emperor?", ["Akbar", "Shah Jahan", "Aurangzeb", "Babur"], 1),
    ("History", "Which ancient wonder stood in Alexandria?", ["Colossus", "Hanging Gardens", "Lighthouse", "Great Pyramid"], 2),
    ("Sport", "How many players are on a football (soccer) team on the pitch?", ["9", "10", "11", "12"], 2),
    ("Sport", "In cricket, how many balls make an over?", ["4", "5", "6", "8"], 2),
    ("Sport", "The Olympic Games are held every…", ["2 years", "3 years", "4 years", "5 years"], 2),
    ("Sport", "Which sport uses a shuttlecock?", ["Tennis", "Badminton", "Squash", "Hockey"], 1),
    ("Movies", "Who directed the film 'Inception'?", ["Steven Spielberg", "Christopher Nolan", "James Cameron", "Ridley Scott"], 1),
    ("Movies", "In 'The Lion King', what is Simba's father called?", ["Scar", "Mufasa", "Rafiki", "Zazu"], 1),
    ("Movies", "Which superhero is from the planet Krypton?", ["Batman", "Thor", "Superman", "Hulk"], 2),
    ("Tech", "What does CPU stand for?", ["Central Processing Unit", "Computer Power Unit", "Core Program Utility", "Central Print Unit"], 0),
    ("Tech", "Which company makes the iPhone?", ["Samsung", "Google", "Apple", "Nokia"], 2),
    ("Tech", "HTML is used to…", ["Style pages", "Structure web pages", "Query databases", "Send email"], 1),
    ("Tech", "How many bits are in a byte?", ["4", "8", "16", "32"], 1),
    ("Nature", "What is the largest animal on Earth?", ["Elephant", "Blue whale", "Giraffe", "Great white shark"], 1),
    ("Nature", "How many legs does a spider have?", ["6", "8", "10", "12"], 1),
    ("Nature", "Which bird is known for mimicking human speech?", ["Eagle", "Parrot", "Penguin", "Owl"], 1),
    ("Maths", "What is 12 × 12?", ["124", "144", "154", "164"], 1),
    ("Maths", "What is the square root of 81?", ["7", "8", "9", "11"], 2),
    ("Maths", "How many degrees are in a right angle?", ["45", "60", "90", "180"], 2),
    ("Maths", "What is 15% of 200?", ["20", "25", "30", "35"], 2),
    ("General", "How many days are in a leap year?", ["364", "365", "366", "367"], 2),
    ("General", "Which language has the most native speakers?", ["English", "Hindi", "Mandarin", "Spanish"], 2),
    ("General", "What colour do you get by mixing blue and yellow?", ["Purple", "Green", "Orange", "Brown"], 1),
    ("General", "How many continents are there?", ["5", "6", "7", "8"], 2),
    ("General", "What currency is used in Japan?", ["Yuan", "Won", "Yen", "Ringgit"], 2),
]
_TRIVIA_Q = 8


@_register
class Trivia(Spec):
    key = "trivia"
    title = "🧠 Trivia Quiz"
    blurb = "8 questions, four options each. Everyone answers in secret; a point per correct answer."
    min, max = 2, 6
    W, H = 776, 620
    simultaneous = True

    def start(self, g):
        qs = None
        try:
            r = requests.get("https://opentdb.com/api.php", params={"amount": _TRIVIA_Q, "type": "multiple"}, timeout=2.5)
            data = r.json().get("results") or []
            qs = []
            for it in data:
                opts = [_html.unescape(x) for x in it["incorrect_answers"]] + [_html.unescape(it["correct_answer"])]
                random.shuffle(opts)
                qs.append((_html.unescape(it["category"]).split(":")[-1].strip(), _html.unescape(it["question"]),
                           opts, opts.index(_html.unescape(it["correct_answer"]))))
            if len(qs) < _TRIVIA_Q:
                qs = None
        except Exception:
            qs = None
        if not qs:
            qs = random.sample(_TRIVIA_BANK, _TRIVIA_Q)
        g["st"] = {"qs": qs, "i": 0, "ans": {}, "shown": None, "score": {p["id"]: 0 for p in g["players"]}}

    def keyboard(self, g):
        return [[("A", "0"), ("B", "1"), ("C", "2"), ("D", "3")]]

    def can_act(self, g, p):
        return g["phase"] == "play" and not p["out"] and p["id"] not in g["st"]["ans"]

    def deny(self, g, p):
        return "You already answered - waiting for the others."

    def caption(self, g):
        st = g["st"]
        sc = _scores(g)
        out = []
        if g["phase"] == "play":
            cat, q, opts, _ = st["qs"][st["i"]]
            out += [f"Q{st['i'] + 1}/{_TRIVIA_Q} · <b>{_dyn(_esc(cat))}</b>", _dyn(_esc(q)), ""]
            out += [f"{'ABCD'[k]}. {_dyn(_esc(o))}" for k, o in enumerate(opts)]
            out.append("")
        out += [f"{_tag(p)} · {sc[p['id']]} pt" + (" ✅" if p["id"] in st["ans"] else "") for p in g["players"]]
        return out

    def robot(self, g, p):
        st = g["st"]
        c = st["qs"][st["i"]][3]
        return str(c if random.random() < 0.55 else random.choice([k for k in range(4) if k != c]))

    def act(self, g, p, action, secret=None):
        st = g["st"]
        if action not in "0123":
            return "Tap A, B, C or D."
        st["ans"][p["id"]] = int(action)
        alive = _alive(g)
        if not all(q["id"] in st["ans"] for q in alive):
            return None
        cat, q, opts, c = st["qs"][st["i"]]
        right = [q_ for q_ in alive if st["ans"][q_["id"]] == c]
        for q_ in right:
            st["score"][q_["id"]] += 1
        st["shown"] = {"i": st["i"], "ans": dict(st["ans"])}
        _log(g, f"Q{st['i'] + 1}: <b>{_dyn('ABCD'[c])}. {_dyn(_esc(opts[c]))}</b> · " + (_tags(right) + " ✅" if right else "nobody got it"))
        st["ans"] = {}
        st["i"] += 1
        if st["i"] >= len(st["qs"]):
            best = max(st["score"][x["id"]] for x in alive)
            tops = [x for x in alive if st["score"][x["id"]] == best]
            _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
        return None

    def render(self, g):
        st = g["st"]
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Question {st['i'] + 1}/{_TRIVIA_Q}")
        img, d = _canvas(self.W, self.H, "TRIVIA QUIZ", status)
        if g["phase"] == "play":
            cat, q, opts, c = st["qs"][st["i"]]
            reveal = None
        else:
            cat, q, opts, c = st["qs"][-1]
            reveal = c
        # if the last question was just revealed and we're still playing, show the previous one's answer briefly? keep simple
        _panel(d, 40, TOP + 16, self.W - 40, TOP + 150)
        _txt(d, 58, TOP + 34, cat.upper(), 13, (170, 180, 200), anchor="la")
        f = _font(19)
        lines = _wrap(d, q, f, (self.W - 120) * S)[:4]
        for k, ln in enumerate(lines):
            d.text((_sx(58), _sx(TOP + 58 + k * 24)), ln, font=f, fill=(255, 255, 255))
        for k, o in enumerate(opts):
            y = TOP + 170 + k * 62
            fill = (31, 36, 48)
            if reveal is not None and k == reveal:
                fill = (40, 110, 60)
            _panel(d, 40, y, self.W - 40, y + 50, fill=fill, outline=(120, 130, 150))
            _txt(d, 66, y + 25, "ABCD"[k], 20, GOLD)
            fo = _font(17)
            ol = _wrap(d, o, fo, (self.W - 160) * S)[:1]
            d.text((_sx(96), _sx(y + 25)), ol[0] if ol else o, font=fo, fill=(255, 255, 255), anchor="lm")
        sc = _scores(g)
        x = 50
        y = TOP + 430
        for p in g["players"]:
            _token(d, x + 16, y + 16, COLORS[p["c"]][2], _plabel(p), r=14)
            _txt(d, x + 38, y + 16, f"{sc[p['id']]}", 18, GOLD, anchor="lm")
            x += 118
        return _finish(img, self.W, self.H)


# ── Uno-style cards ────────────────────────────────────────────────────────

_U_COL = {"R": ("🔴", (226, 60, 60)), "G": ("🟢", (52, 168, 83)), "B": ("🔵", (52, 120, 230)), "Y": ("🟡", (240, 190, 30))}
_U_LABEL = {"skip": "Skip", "rev": "Reverse", "+2": "+2", "wild": "Wild", "+4": "Wild +4"}


def _u_deck():
    deck = []
    for c in "RGBY":
        deck.append((c, "0"))
        for v in list("123456789") + ["skip", "rev", "+2"]:
            deck += [(c, v), (c, v)]
    deck += [("W", "wild")] * 4 + [("W", "+4")] * 4
    random.shuffle(deck)
    return deck


def _u_name(card):
    c, v = card
    if c == "W":
        return "🌈 " + _U_LABEL[v]
    return f"{_U_COL[c][0]} {_U_LABEL.get(v, v)}"


@_register
class Uno(Spec):
    key = "uno"
    title = "🃏 Uno-style Cards"
    blurb = "Match the colour or number, use Skip / Reverse / +2 / Wild, first to empty their hand wins. (In a group everyone's cards are open.)"
    min, max = 2, 6
    W, H = 776, 620
    hint = "play a card or draw"

    def start(self, g):
        deck = _u_deck()
        hands = {p["id"]: [deck.pop() for _ in range(7)] for p in g["players"]}
        top = deck.pop()
        while top[0] == "W":
            deck.insert(0, top)
            top = deck.pop()
        g["st"] = {"deck": deck, "hands": hands, "pile": [top], "color": top[0], "dir": 1, "wild": None, "last": ""}

    def _playable(self, st, card):
        top = st["pile"][-1]
        return card[0] == "W" or card[0] == st["color"] or card[1] == top[1]

    def keyboard(self, g):
        st = g["st"]
        cur = _current(g)
        if st["wild"] is not None:
            return [[(f"{_U_COL[c][0]}", f"c{c}") for c in "RGBY"]]
        hand = st["hands"][cur["id"]]
        btn = [(_u_name(card), f"p{i}") for i, card in enumerate(hand) if self._playable(st, card)]
        rows = [btn[i:i + 3] for i in range(0, len(btn), 3)]
        rows.append([("🂠 Draw", "draw")])
        return rows

    def caption(self, g):
        st = g["st"]
        top = st["pile"][-1]
        out = [f"Top: <b>{_dyn(_u_name(top))}</b>" + (f" · colour {_dyn(_U_COL[st['color']][0])}" if top[0] == 'W' else "")]
        out += [f"{_tag(p)} · {len(st['hands'][p['id']])} card{'s' if len(st['hands'][p['id']]) != 1 else ''}" for p in g["players"]]
        return out

    def turn_text(self, g):
        cur = _current(g)
        if cur["bot"]:
            return f"🤖 {_tag(cur)} is thinking…"
        if g["st"]["wild"] is not None:
            return f"👉 {_tag(cur)} - pick a colour"
        return f"👉 Turn: {_tag(cur)} - play a card or draw"

    def robot(self, g, p):
        st = g["st"]
        hand = st["hands"][p["id"]]
        if st["wild"] is not None:
            counts = {c: sum(1 for x in hand if x[0] == c) for c in "RGBY"}
            return "c" + max(counts, key=counts.get)
        opts = [i for i, c in enumerate(hand) if self._playable(st, c)]
        if not opts:
            return "draw"
        def pri(i):
            c, v = hand[i]
            if c == "W":
                return 0 if len(hand) > 2 else 3
            return 2 if v in ("skip", "rev", "+2") else 1
        return f"p{max(opts, key=lambda i: (pri(i), random.random()))}"

    def _next(self, g, skip=False):
        n = len(g["players"])
        st = g["st"]
        steps = 2 if skip else 1
        for _ in range(steps):
            for _ in range(n):
                g["turn"] = (g["turn"] + st["dir"]) % n
                if not g["players"][g["turn"]]["out"]:
                    break

    def _draw_cards(self, st, pid, k):
        for _ in range(k):
            if not st["deck"]:
                pile = st["pile"]
                st["deck"] = pile[:-1]
                random.shuffle(st["deck"])
                st["pile"] = pile[-1:]
            if st["deck"]:
                st["hands"][pid].append(st["deck"].pop())

    def act(self, g, p, action, secret=None):
        st = g["st"]
        hand = st["hands"][p["id"]]
        if st["wild"] is not None:
            if not (action.startswith("c") and action[1:] in "RGBY" and len(action) == 2):
                return "Pick a colour."
            st["color"] = action[1:]
            card = st["wild"]
            st["wild"] = None
            _log(g, f"{_tag(p)} chooses {_dyn(_U_COL[st['color']][0])}")
            if card[1] == "+4":
                self._next(g)
                v = _current(g)
                self._draw_cards(st, v["id"], 4)
                _log(g, f"{_tag(v)} draws 4 and is skipped")
            self._next(g)
            return None
        if action == "draw":
            self._draw_cards(st, p["id"], 1)
            _log(g, f"{_tag(p)} draws a card")
            self._next(g)
            return None
        if action.startswith("p"):
            try:
                i = int(action[1:])
                card = hand[i]
            except Exception:
                return "Pick a card."
            if not self._playable(st, card):
                return "That card doesn't match."
            hand.pop(i)
            st["pile"].append(card)
            st["last"] = _u_name(card)
            _log(g, f"{_tag(p)} plays <b>{_dyn(_u_name(card))}</b>" + (" · UNO!" if len(hand) == 1 else ""))
            if not hand:
                _win(g, p)
                return None
            c, v = card
            if c == "W":
                st["wild"] = card
                return None
            st["color"] = c
            if v == "skip":
                self._next(g, skip=True)
            elif v == "rev":
                st["dir"] *= -1
                if len(_alive(g)) == 2:
                    return None            # reverse with two players = play again
                self._next(g)
            elif v == "+2":
                self._next(g)
                vict = _current(g)
                self._draw_cards(st, vict["id"], 2)
                _log(g, f"{_tag(vict)} draws 2 and is skipped")
                self._next(g)
            else:
                self._next(g)
            return None
        return "Play a card or draw."

    def _ucard(self, d, x, y, card, w=60, h=86):
        c, v = card
        col = _U_COL[c][1] if c != "W" else (40, 40, 40)
        d.rounded_rectangle([_sx(x) + 3 * S, _sx(y) + 4 * S, _sx(x + w) + 3 * S, _sx(y + h) + 4 * S], radius=8 * S, fill=(0, 0, 0, 110))
        d.rounded_rectangle([_sx(x), _sx(y), _sx(x + w), _sx(y + h)], radius=8 * S, fill=col, outline=(255, 255, 255), width=3 * S)
        d.ellipse([_sx(x + 8), _sx(y + 18), _sx(x + w - 8), _sx(y + h - 18)], fill=(250, 250, 245))
        label = {"skip": "⊘", "rev": "⇄", "wild": "W", "+4": "+4"}.get(v, v)
        _txt(d, x + w / 2, y + h / 2, label, 22 if len(label) > 1 else 28, col if c != "W" else (30, 30, 30))
        if c == "W":
            for k, cc in enumerate("RGBY"):
                _cx, _cy = x + w / 2 + (-10 if k % 2 == 0 else 10), y + h / 2 + (-14 if k < 2 else 14)
                d.ellipse([_sx(_cx - 5), _sx(_cy - 5), _sx(_cx + 5), _sx(_cy + 5)], fill=_U_COL[cc][1])

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "UNO-STYLE", status)
        # pile + colour
        _panel(d, 40, TOP + 16, 300, TOP + 150)
        _txt(d, 56, TOP + 32, "TOP CARD", 13, (170, 180, 200), anchor="la")
        self._ucard(d, 60, TOP + 50, st["pile"][-1])
        d.ellipse([_sx(150), _sx(TOP + 70), _sx(200), _sx(TOP + 120)], fill=_U_COL[st["color"]][1], outline=(255, 255, 255), width=3 * S)
        _txt(d, 175, TOP + 132, "colour", 12, (170, 180, 200))
        _txt(d, 250, TOP + 95, f"{len(st['deck'])}", 26, GOLD)
        _txt(d, 250, TOP + 120, "in deck", 12, (170, 180, 200))
        # counts
        _score_rows(d, g, 320, TOP + 16, self.W - 360, extra=lambda p: f"{len(st['hands'][p['id']])} cards", rowh=30)
        # current hand
        hand = st["hands"][cur["id"]] if g["phase"] == "play" else []
        _panel(d, 40, TOP + 250, self.W - 40, TOP + 480)
        _txt(d, 56, TOP + 266, f"{cur['name'].upper()}'S HAND" if g["phase"] == "play" else "", 13, (170, 180, 200), anchor="la")
        n = len(hand)
        if n:
            step = min(66, (self.W - 140) / max(1, n))
            for k, card in enumerate(hand[:20]):
                self._ucard(d, 56 + k * step, TOP + 290 + (k % 2) * 70, card)
        return _finish(img, self.W, self.H)


# ── 2048 ───────────────────────────────────────────────────────────────────

_T_COL = {0: (205, 193, 180), 2: (238, 228, 218), 4: (237, 224, 200), 8: (242, 177, 121), 16: (245, 149, 99),
          32: (246, 124, 95), 64: (246, 94, 59), 128: (237, 207, 114), 256: (237, 204, 97), 512: (237, 200, 80),
          1024: (237, 197, 63), 2048: (237, 194, 46)}
_T_MOVES = 25


def _t_slide(row):
    tiles = [t for t in row if t]
    out, gain, i = [], 0, 0
    while i < len(tiles):
        if i + 1 < len(tiles) and tiles[i] == tiles[i + 1]:
            out.append(tiles[i] * 2)
            gain += tiles[i] * 2
            i += 2
        else:
            out.append(tiles[i])
            i += 1
    return out + [0] * (4 - len(out)), gain


def _t_move(b, dirn):
    rows = [list(r) for r in b]
    if dirn in ("up", "down"):
        rows = [list(x) for x in zip(*rows)]
    if dirn in ("right", "down"):
        rows = [r[::-1] for r in rows]
    gain = 0
    new = []
    for r in rows:
        s, gn = _t_slide(r)
        new.append(s)
        gain += gn
    if dirn in ("right", "down"):
        new = [r[::-1] for r in new]
    if dirn in ("up", "down"):
        new = [list(x) for x in zip(*new)]
    return new, gain, new != [list(r) for r in b]


@_register
class Game2048(Spec):
    key = "2048"
    title = "🧩 2048"
    blurb = "Everyone gets their own 4×4 board and 25 slides. Merge tiles, chase 2048 - highest score wins."
    min, max = 2, 4
    W, H = 776, 640
    hint = "slide ⬆ ⬇ ⬅ ➡"

    def _spawn(self, b):
        empt = [(r, c) for r in range(4) for c in range(4) if b[r][c] == 0]
        if empt:
            r, c = random.choice(empt)
            b[r][c] = 4 if random.random() < 0.1 else 2

    def start(self, g):
        boards = {}
        for p in g["players"]:
            b = [[0] * 4 for _ in range(4)]
            self._spawn(b)
            self._spawn(b)
            boards[p["id"]] = b
        g["st"] = {"b": boards, "score": {p["id"]: 0 for p in g["players"]}, "moves": {p["id"]: 0 for p in g["players"]}}

    def keyboard(self, g):
        return [[("⬆️", "up")], [("⬅️", "left"), ("⬇️", "down"), ("➡️", "right")]]

    def caption(self, g):
        st = g["st"]
        return [f"{_tag(p)} · {st['score'][p['id']]} pts · {_T_MOVES - st['moves'][p['id']]} slides left" + (" ✔" if p["out"] else "") for p in g["players"]]

    def robot(self, g, p):
        b = g["st"]["b"][p["id"]]
        best, bs = "up", -1
        for dirn in ("up", "left", "right", "down"):
            nb, gain, ch = _t_move(b, dirn)
            if not ch:
                continue
            empt = sum(1 for r in nb for v in r if v == 0)
            s = gain + empt * 2 + random.random()
            if s > bs:
                best, bs = dirn, s
        return best

    def _done(self, g, p):
        p["out"] = True
        if all(q["out"] for q in g["players"]):
            st = g["st"]
            best = max(st["score"].values())
            tops = [q for q in g["players"] if st["score"][q["id"]] == best]
            _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
            return True
        return False

    def act(self, g, p, action, secret=None):
        st = g["st"]
        if action not in ("up", "down", "left", "right"):
            return "Slide up, down, left or right."
        b = st["b"][p["id"]]
        nb, gain, ch = _t_move(b, action)
        if not ch:
            return "Nothing moves that way."
        st["b"][p["id"]] = nb
        self._spawn(nb)
        st["score"][p["id"]] += gain
        st["moves"][p["id"]] += 1
        top = max(v for r in nb for v in r)
        _log(g, f"{_tag(p)} slides {action}" + (f" · +{gain}" if gain else "") + (f" · best tile {top}" if gain else ""))
        stuck = not any(_t_move(nb, d_)[2] for d_ in ("up", "down", "left", "right"))
        if st["moves"][p["id"]] >= _T_MOVES or stuck:
            _log(g, f"{_tag(p)} finishes on <b>{st['score'][p['id']]}</b>")
            if self._done(g, p):
                return None
        _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "2048", status)
        n = len(g["players"])
        if n <= 2:
            cs, gap = 68, 8
            slots = [(60, TOP + 60), (420, TOP + 60)]
        else:
            cs, gap = 46, 6
            slots = [(60, TOP + 40), (420, TOP + 40), (60, TOP + 300), (420, TOP + 300)]
        for i, p in enumerate(g["players"]):
            x0, y0 = slots[i]
            b = st["b"][p["id"]]
            bw = cs * 4 + gap * 5
            d.rounded_rectangle([_sx(x0 - gap), _sx(y0 - gap), _sx(x0 - gap + bw), _sx(y0 - gap + bw)], radius=8 * S,
                                fill=(187, 173, 160), outline=GOLD if (g["phase"] == "play" and cur is p) else (120, 110, 100), width=3 * S)
            for r in range(4):
                for c in range(4):
                    v = b[r][c]
                    x, y = x0 + c * (cs + gap), y0 + r * (cs + gap)
                    d.rounded_rectangle([_sx(x), _sx(y), _sx(x + cs), _sx(y + cs)], radius=5 * S, fill=_T_COL.get(v, (60, 58, 50)))
                    if v:
                        _txt(d, x + cs / 2, y + cs / 2, str(v), int(cs * (0.42 if v < 100 else 0.32 if v < 1000 else 0.26)),
                             (119, 110, 101) if v <= 4 else (249, 246, 242))
            _token(d, x0 + bw / 2 - 60, y0 + bw + 8, COLORS[p["c"]][2], _plabel(p), r=12)
            _txt(d, x0 + bw / 2 - 40, y0 + bw + 8, f"{p['name']} · {st['score'][p['id']]}", 15, (255, 255, 255), anchor="lm")
        return _finish(img, self.W, self.H)


# ── Simon Says ─────────────────────────────────────────────────────────────

_SIMON = [("🔴", (226, 60, 60)), ("🟢", (52, 168, 83)), ("🔵", (52, 120, 230)), ("🟡", (240, 190, 30))]


@_register
class Simon(Spec):
    key = "simon"
    title = "🧠 Simon Says"
    blurb = "Memorise the colour sequence, tap Ready, then repeat it. One slip and you're out. It grows every round."
    min, max = 2, 6
    W, H = 700, 720
    hint = "tap Ready"

    def start(self, g):
        g["st"] = {"seq": [random.randrange(4)], "stage": "show", "pos": 0, "done": set(), "flash": None}

    def keyboard(self, g):
        if g["st"]["stage"] == "show":
            return [[("👀 Ready", "ready")]]
        return [[(_SIMON[k][0], f"k{k}") for k in range(4)]]

    def caption(self, g):
        st = g["st"]
        out = [f"Sequence length: <b>{len(st['seq'])}</b>"]
        out += [f"{_tag(p)}" + (" 💀 out" if p["out"] else " ✅" if p["id"] in st["done"] else "") for p in g["players"]]
        return out

    def turn_text(self, g):
        cur = _current(g)
        st = g["st"]
        if cur["bot"]:
            return f"🤖 {_tag(cur)} is thinking…"
        if st["stage"] == "show":
            return f"👉 {_tag(cur)} - memorise the picture, then tap Ready"
        return f"👉 {_tag(cur)} - repeat it: {st['pos']}/{len(st['seq'])}"

    def robot(self, g, p):
        return "ready" if g["st"]["stage"] == "show" else "auto"

    def _complete(self, g, p):
        st = g["st"]
        st["done"].add(p["id"])
        st["stage"] = "show"
        st["pos"] = 0
        alive = _alive(g)
        if all(q["id"] in st["done"] for q in alive):
            st["seq"].append(random.randrange(4))
            st["done"] = set()
            _log(g, f"Everyone made it - sequence grows to <b>{len(st['seq'])}</b>")
            if len(st["seq"]) > 12:
                _draw_game(g)
                return
        _advance(g)
        while _current(g)["id"] in st["done"] and not _current(g)["out"]:
            _advance(g)

    def _fail(self, g, p):
        st = g["st"]
        p["out"] = True
        st["stage"] = "show"
        st["pos"] = 0
        _log(g, f"💀 {_tag(p)} slipped at step {st['pos'] + 1}")
        alive = _alive(g)
        if len(alive) == 1:
            _win(g, alive[0])
            return
        if not alive:
            _draw_game(g)
            return
        if all(q["id"] in st["done"] for q in alive):
            st["seq"].append(random.randrange(4))
            st["done"] = set()
        _advance(g)
        while _current(g)["id"] in st["done"]:
            _advance(g)

    def act(self, g, p, action, secret=None):
        st = g["st"]
        if st["stage"] == "show":
            if action != "ready":
                return "Tap Ready first."
            st["stage"] = "input"
            st["pos"] = 0
            return None
        if action == "auto":
            ok = all(random.random() < 0.93 for _ in st["seq"])
            if ok:
                _log(g, f"{_tag(p)} repeats all {len(st['seq'])} ✅")
                self._complete(g, p)
            else:
                st["pos"] = random.randrange(len(st["seq"]))
                self._fail(g, p)
            return None
        if not action.startswith("k"):
            return "Tap a colour."
        k = int(action[1:])
        st["flash"] = k
        if k == st["seq"][st["pos"]]:
            st["pos"] += 1
            if st["pos"] >= len(st["seq"]):
                _log(g, f"{_tag(p)} repeats all {len(st['seq'])} ✅")
                self._complete(g, p)
            return None
        self._fail(g, p)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "SIMON SAYS", status)
        cx, cy, r = self.W / 2, TOP + 170, 130
        for k in range(4):
            col = _SIMON[k][1]
            lit = (st["stage"] == "input" and st["flash"] == k)
            d.pieslice([_sx(cx - r), _sx(cy - r), _sx(cx + r), _sx(cy + r)], start=k * 90 - 90, end=k * 90,
                       fill=_shade(col, 0.4) if lit else col, outline=(20, 24, 30), width=6 * S)
        d.ellipse([_sx(cx - 40), _sx(cy - 40), _sx(cx + 40), _sx(cy + 40)], fill=(31, 36, 48), outline=GOLD, width=3 * S)
        _txt(d, cx, cy, str(len(st["seq"])), 26, GOLD)
        _panel(d, 40, TOP + 320, self.W - 40, TOP + 400)
        if st["stage"] == "show" and g["phase"] == "play":
            _txt(d, 56, TOP + 336, "MEMORISE", 13, (170, 180, 200), anchor="la")
            step = min(46, (self.W - 120) / max(1, len(st["seq"])))
            for k, v in enumerate(st["seq"]):
                x = 70 + k * step
                d.ellipse([_sx(x), _sx(TOP + 352), _sx(x + 34), _sx(TOP + 386)], fill=_SIMON[v][1], outline=(255, 255, 255), width=2 * S)
        else:
            _txt(d, 56, TOP + 336, "REPEAT", 13, (170, 180, 200), anchor="la")
            for k in range(len(st["seq"])):
                x = 70 + k * min(46, (self.W - 120) / max(1, len(st["seq"])))
                d.ellipse([_sx(x), _sx(TOP + 352), _sx(x + 34), _sx(TOP + 386)],
                          fill=(80, 90, 110) if k >= st["pos"] else (120, 220, 140), outline=(255, 255, 255), width=2 * S)
        _score_rows(d, g, 40, TOP + 420, self.W - 80, rowh=26)
        return _finish(img, self.W, self.H)


# ── Word Scramble ──────────────────────────────────────────────────────────

_SCR_ROUNDS = 5


@_register
class Scramble(Spec):
    key = "scramble"
    title = "🧩 Word Scramble"
    blurb = "Unscramble the word - first to type it right takes the round. 5 rounds."
    min, max = 2, 6
    W, H = 700, 520
    simultaneous = True
    uses_text = True

    def _new_word(self, st):
        cat = random.choice(list(_HANG_WORDS))
        w = random.choice(_HANG_WORDS[cat])
        letters = list(w)
        for _ in range(20):
            random.shuffle(letters)
            if "".join(letters) != w:
                break
        st["cat"], st["word"], st["scr"], st["tried"] = cat, w, "".join(letters), set()

    def start(self, g):
        g["st"] = {"round": 1, "score": {p["id"]: 0 for p in g["players"]}, "last": ""}
        self._new_word(g["st"])

    def keyboard(self, g):
        return []

    def can_act(self, g, p):
        if g["phase"] != "play" or p["out"]:
            return False
        return not p["bot"] or p["id"] not in g["st"]["tried"]

    def turn_text(self, g):
        st = g["st"]
        return f"👉 Round {st['round']}/{_SCR_ROUNDS} · category <b>{_dyn(st['cat'])}</b> · type the word: <code>{_dyn(st['scr'])}</code>"

    def caption(self, g):
        sc = _scores(g)
        return [f"{_tag(p)} · {sc[p['id']]} pt" for p in g["players"]]

    def robot(self, g, p):
        if not p["bot"]:
            return ""
        st = g["st"]
        return st["word"] if random.random() < 0.35 else st["scr"][::-1]

    def on_text(self, g, p, text):
        return self.act(g, p, text) is None

    def act(self, g, p, action, secret=None):
        st = g["st"]
        guess = action.strip().upper()
        if not guess.isalpha():
            return "Type a word."
        if p["bot"]:
            st["tried"].add(p["id"])
        if guess != st["word"]:
            _log(g, f"{_tag(p)} tries <b>{_dyn(_esc(guess[:16]))}</b> ❌")
            return None
        st["score"][p["id"]] += 1
        _log(g, f"✅ {_tag(p)} got it - <b>{_dyn(st['word'])}</b>")
        if st["round"] >= _SCR_ROUNDS:
            best = max(st["score"].values())
            tops = [q for q in g["players"] if st["score"][q["id"]] == best]
            _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
            return None
        st["round"] += 1
        self._new_word(st)
        return None

    def render(self, g):
        st = g["st"]
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else f"Round {st['round']}/{_SCR_ROUNDS}")
        img, d = _canvas(self.W, self.H, "WORD SCRAMBLE", status)
        _panel(d, 40, TOP + 16, self.W - 40, TOP + 150)
        _txt(d, 56, TOP + 32, st["cat"].upper(), 13, (170, 180, 200), anchor="la")
        word = st["scr"] if g["phase"] == "play" else st["word"]
        n = len(word)
        tw = min(54, int((self.W - 120) / n))
        x0 = self.W / 2 - tw * n / 2
        for i, ch in enumerate(word):
            x = x0 + i * tw
            d.rounded_rectangle([_sx(x + 3) + 3 * S, _sx(TOP + 60) + 4 * S, _sx(x + tw - 3) + 3 * S, _sx(TOP + 130) + 4 * S], radius=6 * S, fill=(0, 0, 0, 110))
            d.rounded_rectangle([_sx(x + 3), _sx(TOP + 60), _sx(x + tw - 3), _sx(TOP + 130)], radius=6 * S, fill=(240, 220, 170), outline=(160, 120, 60), width=2 * S)
            _txt(d, x + tw / 2, TOP + 95, ch, int(tw * 0.7), (90, 60, 20))
        _score_rows(d, g, 40, TOP + 170, self.W - 80, extra=lambda p: f"{st['score'][p['id']]} pt", rowh=36)
        return _finish(img, self.W, self.H)


# ── Archery / Target ───────────────────────────────────────────────────────

_ARROWS = 3


class _Shooter(Spec):
    min, max = 2, 6
    W, H = 700, 640
    skin = "archery"
    hint = "pick aim, then power"

    def start(self, g):
        g["st"] = {"wind": random.choice([-2, -1, 0, 1, 2]), "aim": None, "shots": {p["id"]: [] for p in g["players"]}}

    def keyboard(self, g):
        if g["st"]["aim"] is None:
            return [[("⬅️ Left", "aL"), ("🎯 Centre", "aC"), ("➡️ Right", "aR")]]
        return [[("Low", "pL"), ("Medium", "pM"), ("High", "pH")], [("↩ Back", "back")]]

    def caption(self, g):
        st = g["st"]
        w = st["wind"]
        wind = "calm" if w == 0 else f"{'→' if w > 0 else '←'} {abs(w)}"
        return [f"💨 Wind: <b>{wind}</b>"] + [f"{_tag(p)} · {sum(s[2] for s in st['shots'][p['id']])} pts · {len(st['shots'][p['id']])}/{_ARROWS}" for p in g["players"]]

    def turn_text(self, g):
        cur = _current(g)
        if cur["bot"]:
            return f"🤖 {_tag(cur)} is thinking…"
        if g["st"]["aim"] is not None:
            return f"👉 {_tag(cur)} · aim {g['st']['aim']} - now pick power"
        return f"👉 Turn: {_tag(cur)} - pick aim (mind the wind)"

    def robot(self, g, p):
        st = g["st"]
        if st["aim"] is None:
            w = st["wind"]
            aim = "C" if w == 0 else ("L" if w > 0 else "R")
            if random.random() < 0.2:
                aim = random.choice("LCR")
            return "a" + aim
        return "p" + (random.choice("LMH") if random.random() < 0.2 else "M")

    def act(self, g, p, action, secret=None):
        st = g["st"]
        if action == "back":
            st["aim"] = None
            return None
        if action.startswith("a"):
            if action[1:] not in ("L", "C", "R"):
                return "Left, centre or right."
            st["aim"] = action[1:]
            return None
        if action.startswith("p"):
            if st["aim"] is None:
                return "Pick aim first."
            if action[1:] not in ("L", "M", "H"):
                return "Low, medium or high."
            ax = {"L": -30, "C": 0, "R": 30}[st["aim"]]
            x = ax + st["wind"] * 18 + random.gauss(0, 9)
            y = {"L": 32, "M": 0, "H": -32}[action[1:]] + random.gauss(0, 9)
            dist = (x * x + y * y) ** 0.5
            score = 10 if dist < 12 else 8 if dist < 25 else 6 if dist < 40 else 4 if dist < 55 else 2 if dist < 70 else 0
            st["shots"][p["id"]].append((round(x, 1), round(y, 1), score))
            st["aim"] = None
            _log(g, f"{_tag(p)} shoots · <b>{score}</b>" + (" 🎯 bullseye!" if score == 10 else ""))
            st["wind"] = random.choice([-2, -1, 0, 1, 2])
            if all(len(st["shots"][q["id"]]) >= _ARROWS for q in _alive(g)):
                totals = {q["id"]: sum(s[2] for s in st["shots"][q["id"]]) for q in _alive(g)}
                best = max(totals.values())
                tops = [q for q in _alive(g) if totals[q["id"]] == best]
                _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
                return None
            _advance(g)
            while len(st["shots"][_current(g)["id"]]) >= _ARROWS:
                _advance(g)
            return None
        return "Pick aim, then power."

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, self.title.split(" ", 1)[-1].upper(), status)
        cx, cy = self.W / 2, TOP + 200
        rings = ([(85, (250, 250, 250)), (70, (40, 40, 40)), (55, (60, 120, 220)), (40, (226, 60, 60)), (25, (255, 214, 60)), (12, (255, 240, 120))]
                 if self.skin == "archery" else
                 [(85, (40, 40, 40)), (70, (200, 50, 50)), (55, (40, 40, 40)), (40, (60, 150, 80)), (25, (200, 50, 50)), (12, (60, 150, 80))])
        d.ellipse([_sx(cx - 92) + 6 * S, _sx(cy - 92) + 10 * S, _sx(cx + 92) + 6 * S, _sx(cy + 92) + 10 * S], fill=(0, 0, 0, 110))
        for r, col in rings:
            d.ellipse([_sx(cx - r), _sx(cy - r), _sx(cx + r), _sx(cy + r)], fill=col, outline=(20, 20, 20), width=1 * S)
        if self.skin == "target":
            for k in range(20):
                import math
                a = math.radians(k * 18)
                d.line([(_sx(cx + 12 * math.cos(a)), _sx(cy + 12 * math.sin(a))), (_sx(cx + 85 * math.cos(a)), _sx(cy + 85 * math.sin(a)))], fill=(200, 200, 200), width=1 * S)
        for p in g["players"]:
            for (x, y, sc) in st["shots"][p["id"]]:
                px, py = cx + x, cy + y
                col = COLORS[p["c"]][2]
                d.ellipse([_sx(px - 7), _sx(py - 7), _sx(px + 7), _sx(py + 7)], fill=col, outline=(255, 255, 255), width=2 * S)
        w = st["wind"]
        _panel(d, self.W - 200, TOP + 16, self.W - 40, TOP + 66)
        _txt(d, self.W - 120, TOP + 41, "wind " + ("calm" if w == 0 else ("→" if w > 0 else "←") * abs(w)), 16, GOLD)
        _score_rows(d, g, 40, TOP + 310, self.W - 80, extra=lambda p: f"{sum(s[2] for s in st['shots'][p['id']])}  ({len(st['shots'][p['id']])}/{_ARROWS})", rowh=36)
        return _finish(img, self.W, self.H)


@_register
class Archery(_Shooter):
    key = "archery"
    title = "🏹 Archery"
    blurb = "Three arrows each. Pick where to aim and how hard - and read the wind. Closest to the gold scores most."
    skin = "archery"


@_register
class Target(_Shooter):
    key = "target"
    title = "🎯 Target / Bullseye"
    blurb = "Three darts each at the board. Aim, power, wind - bullseye is 10."
    skin = "target"


# ── Boxing Duel ────────────────────────────────────────────────────────────

_BOX = {"jab": "🥊 Jab", "hook": "💥 Hook", "block": "🛡 Block", "dodge": "💨 Dodge"}


@_register
class Boxing(Spec):
    key = "boxing"
    title = "🥊 Boxing Duel"
    blurb = "Both pick in secret each round: Jab (quick, 10), Hook (big, 22 but can miss), Block (stops hooks), Dodge (slips jabs). KO or best HP after 10 rounds."
    min = max = 2
    W, H = 700, 560
    simultaneous = True

    def start(self, g):
        g["st"] = {"hp": {p["id"]: 100 for p in g["players"]}, "round": 1, "picks": {}, "last": ""}

    def keyboard(self, g):
        return [[(_BOX["jab"], "jab"), (_BOX["hook"], "hook")], [(_BOX["block"], "block"), (_BOX["dodge"], "dodge")]]

    def can_act(self, g, p):
        return g["phase"] == "play" and p["id"] not in g["st"]["picks"]

    def deny(self, g, p):
        return "You already picked this round."

    def caption(self, g):
        st = g["st"]
        return [f"Round {st['round']}/10"] + [f"{_tag(p)} · ❤️ {st['hp'][p['id']]}" + (" ✅" if p["id"] in st["picks"] else "") for p in g["players"]]

    def robot(self, g, p):
        return random.choices(["jab", "hook", "block", "dodge"], [4, 3, 2, 2])[0]

    def _dmg(self, my, their):
        if my == "jab":
            return 0 if their == "dodge" else 5 if their == "block" else 10
        if my == "hook":
            if their == "block":
                return 0
            return 22 if random.random() < 0.7 else 0
        return 0

    def act(self, g, p, action, secret=None):
        st = g["st"]
        if action not in _BOX:
            return "Pick a move."
        st["picks"][p["id"]] = action
        if len(st["picks"]) < 2:
            return None
        a, b = g["players"]
        ma, mb = st["picks"][a["id"]], st["picks"][b["id"]]
        da, db = self._dmg(ma, mb), self._dmg(mb, ma)
        st["hp"][b["id"]] = max(0, st["hp"][b["id"]] - da)
        st["hp"][a["id"]] = max(0, st["hp"][a["id"]] - db)
        st["last"] = f"{_dyn(a['name'])}: {ma.upper()}   {_dyn(b['name'])}: {mb.upper()}"
        _log(g, f"Round {st['round']} · {_tag(a)} {_BOX[ma]} ({da}) · {_tag(b)} {_BOX[mb]} ({db})")
        st["picks"] = {}
        ha, hb = st["hp"][a["id"]], st["hp"][b["id"]]
        if ha <= 0 and hb <= 0:
            _log(g, "Double knockdown!")
            _draw_game(g)
        elif hb <= 0:
            _log(g, f"🏆 KO! {_tag(a)}")
            _win(g, a)
        elif ha <= 0:
            _log(g, f"🏆 KO! {_tag(b)}")
            _win(g, b)
        elif st["round"] >= 10:
            _log(g, "Final bell - judges' decision")
            _win(g, a if ha > hb else b) if ha != hb else _draw_game(g)
        else:
            st["round"] += 1
        return None

    def render(self, g):
        st = g["st"]
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else "Draw" if g["phase"] == "over" else f"Round {st['round']}")
        img, d = _canvas(self.W, self.H, "BOXING DUEL", status)
        # ring
        d.rectangle([_sx(40), _sx(TOP + 40), _sx(self.W - 40), _sx(TOP + 330)], fill=(60, 80, 140))
        for k in range(3):
            d.line([(_sx(40), _sx(TOP + 60 + k * 22)), (_sx(self.W - 40), _sx(TOP + 60 + k * 22))], fill=(230, 60, 60) if k == 1 else (240, 240, 240), width=4 * S)
        a, b = g["players"]
        for p, cx in ((a, 180), (b, self.W - 180)):
            col = COLORS[p["c"]][2]
            cy = TOP + 200
            _token(d, cx, cy, col, _plabel(p), r=56)
            d.ellipse([_sx(cx - 90), _sx(cy - 10), _sx(cx - 54), _sx(cy + 26)], fill=(220, 40, 40), outline=(120, 20, 20), width=3 * S)
            d.ellipse([_sx(cx + 54), _sx(cy - 10), _sx(cx + 90), _sx(cy + 26)], fill=(220, 40, 40), outline=(120, 20, 20), width=3 * S)
            _txt(d, cx, cy + 80, p["name"], 18, (255, 255, 255), shadow=True)
            _bar(d, cx - 100, cy + 96, 200, 18, st["hp"][p["id"]] / 100, (220, 60, 60) if st["hp"][p["id"]] < 35 else (60, 190, 90))
        _txt(d, self.W / 2, TOP + 200, "VS", 40, GOLD, shadow=True)
        if st["last"]:
            _panel(d, 60, TOP + 350, self.W - 60, TOP + 410)
            _txt(d, self.W / 2, TOP + 380, st["last"], 16, (255, 255, 255))
        return _finish(img, self.W, self.H)


# ── Zombie Survival ────────────────────────────────────────────────────────

_Z_WAVES = 8


@_register
class Zombie(Spec):
    key = "zombie"
    title = "🧟 Zombie Survival"
    blurb = "8 waves. Each wave pick Fight (score, take hits), Hide (safe-ish) or Loot (ammo & medkits). Survive with the top score."
    min, max = 2, 4
    W, H = 776, 640
    simultaneous = True

    def start(self, g):
        g["st"] = {"wave": 1, "picks": {}, "hp": {p["id"]: 100 for p in g["players"]}, "ammo": {p["id"]: 1 for p in g["players"]},
                   "score": {p["id"]: 0 for p in g["players"]}, "note": {p["id"]: "" for p in g["players"]}}

    def keyboard(self, g):
        return [[("⚔️ Fight", "fight"), ("🙈 Hide", "hide"), ("🎒 Loot", "loot")]]

    def can_act(self, g, p):
        return g["phase"] == "play" and not p["out"] and p["id"] not in g["st"]["picks"]

    def deny(self, g, p):
        return "You already chose this wave."

    def caption(self, g):
        st = g["st"]
        return [f"🧟 Wave {st['wave']}/{_Z_WAVES}"] + \
               [f"{_tag(p)} · ❤️ {st['hp'][p['id']]} · 🔫 {st['ammo'][p['id']]} · {st['score'][p['id']]} pts" + (" 💀" if p["out"] else " ✅" if p["id"] in st["picks"] else "") for p in g["players"]]

    def robot(self, g, p):
        st = g["st"]
        if st["hp"][p["id"]] < 40:
            return random.choice(["hide", "loot", "loot"])
        return random.choices(["fight", "hide", "loot"], [5, 2, 3])[0]

    def act(self, g, p, action, secret=None):
        st = g["st"]
        if action not in ("fight", "hide", "loot"):
            return "Fight, hide or loot."
        st["picks"][p["id"]] = action
        alive = _alive(g)
        if not all(q["id"] in st["picks"] for q in alive):
            return None
        wave = st["wave"]
        for q in alive:
            a = st["picks"][q["id"]]
            pid = q["id"]
            if a == "fight":
                kills = random.randint(1, 2) + wave // 3 + (2 if st["ammo"][pid] > 0 else 0)
                if st["ammo"][pid] > 0:
                    st["ammo"][pid] -= 1
                dmg = max(0, random.randint(5, 12 + wave * 2) - (6 if kills > 3 else 0))
                st["score"][pid] += kills * 10
                st["hp"][pid] -= dmg
                st["note"][pid] = f"⚔️ {kills} kills, -{dmg} HP"
            elif a == "hide":
                if random.random() < 0.25:
                    dmg = random.randint(8, 16)
                    st["hp"][pid] -= dmg
                    st["note"][pid] = f"🙈 found! -{dmg} HP"
                else:
                    st["note"][pid] = "🙈 safe"
            else:
                if random.random() < 0.6:
                    st["ammo"][pid] += 1
                    st["note"][pid] = "🎒 +1 ammo"
                else:
                    st["hp"][pid] = min(100, st["hp"][pid] + 20)
                    st["note"][pid] = "🎒 medkit +20"
                if random.random() < 0.3:
                    st["hp"][pid] -= 10
                    st["note"][pid] += ", ambushed -10"
            if st["hp"][pid] <= 0:
                st["hp"][pid] = 0
                q["out"] = True
                st["note"][pid] += " 💀"
        _log(g, f"Wave {wave}" + "".join(f" · {_dyn(COLORS[q['c']][0])} {st['note'][q['id']]}" for q in alive))
        st["picks"] = {}
        alive = _alive(g)
        if wave >= _Z_WAVES or not alive:
            pool = alive or g["players"]
            best = max(st["score"][q["id"]] for q in pool)
            tops = [q for q in pool if st["score"][q["id"]] == best]
            _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
            return None
        if len(alive) == 1 and len(g["players"]) > 1:
            _log(g, f"{_tag(alive[0])} is the last one standing")
            _win(g, alive[0])
            return None
        st["wave"] += 1
        return None

    def render(self, g):
        st = g["st"]
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else "Draw" if g["phase"] == "over" else f"Wave {st['wave']}")
        img, d = _canvas(self.W, self.H, "ZOMBIE SURVIVAL", status)
        # horde
        _panel(d, 40, TOP + 16, self.W - 40, TOP + 130, fill=(30, 26, 34), outline=(120, 60, 60))
        n = min(14, 2 + st["wave"] * 2)
        for k in range(n):
            x = 70 + k * 50
            y = TOP + 60 + (k % 2) * 14
            d.ellipse([_sx(x - 12), _sx(y - 30), _sx(x + 12), _sx(y - 6)], fill=(110, 150, 90))
            d.rounded_rectangle([_sx(x - 14), _sx(y - 6), _sx(x + 14), _sx(y + 34)], radius=5 * S, fill=(70, 100, 60))
            for ex in (-5, 5):
                d.ellipse([_sx(x + ex - 2), _sx(y - 22), _sx(x + ex + 2), _sx(y - 18)], fill=(230, 40, 40))
        _txt(d, self.W - 60, TOP + 40, f"WAVE {st['wave']}", 18, (255, 120, 120), anchor="ra")
        for i, p in enumerate(g["players"]):
            y = TOP + 150 + i * 96
            _panel(d, 40, y, self.W - 40, y + 84, outline=(100, 100, 110) if not p["out"] else (120, 50, 50))
            _token(d, 70, y + 42, COLORS[p["c"]][2], _plabel(p), r=16)
            _txt(d, 100, y + 24, p["name"] + ("  💀" if p["out"] else ""), 16, (255, 255, 255), anchor="lm")
            _bar(d, 100, y + 40, 220, 14, st["hp"][p["id"]] / 100, (220, 60, 60) if st["hp"][p["id"]] < 35 else (60, 190, 90))
            _txt(d, 100, y + 68, f"HP {st['hp'][p['id']]}   ammo {st['ammo'][p['id']]}", 12, (170, 180, 200), anchor="lm")
            _txt(d, self.W - 60, y + 30, f"{st['score'][p['id']]} pts", 20, GOLD, anchor="rm")
            note = st["note"][p["id"]]
            for e, w in (("⚔️", "fight:"), ("🙈", "hide:"), ("🎒", "loot:"), ("💀", "DEAD")):
                note = note.replace(e, w)
            _txt(d, self.W - 60, y + 60, note[:40], 12, (200, 200, 210), anchor="rm")
        return _finish(img, self.W, self.H)


# ── Number Puzzle (sliding 3x3) ────────────────────────────────────────────

_PZ_GOAL = (1, 2, 3, 4, 5, 6, 7, 8, 0)
_PZ_MAX = 40


def _pz_neigh(state):
    z = state.index(0)
    r, c = divmod(z, 3)
    for dr, dc in ((-1, 0), (1, 0), (0, -1), (0, 1)):
        rr, cc = r + dr, c + dc
        if 0 <= rr < 3 and 0 <= cc < 3:
            k = rr * 3 + cc
            s = list(state)
            s[z], s[k] = s[k], s[z]
            yield tuple(s), state[k]


def _pz_h(state):
    return sum(abs(i // 3 - (v - 1) // 3) + abs(i % 3 - (v - 1) % 3) for i, v in enumerate(state) if v)


def _pz_solve(start):
    """A* with Manhattan distance - returns the tiles to slide, in order."""
    if start == _PZ_GOAL:
        return []
    frontier = [(_pz_h(start), 0, start)]
    came = {start: (None, None)}
    cost = {start: 0}
    while frontier:
        _, gcost, s = heapq.heappop(frontier)
        if s == _PZ_GOAL:
            path = []
            while came[s][0] is not None:
                path.append(came[s][1])
                s = came[s][0]
            return path[::-1]
        for n, tile in _pz_neigh(s):
            nc = gcost + 1
            if nc < cost.get(n, 10 ** 9):
                cost[n] = nc
                came[n] = (s, tile)
                heapq.heappush(frontier, (nc + _pz_h(n), nc, n))
    return []


@_register
class Puzzle(Spec):
    key = "puzzle"
    title = "🔢 Number Puzzle"
    blurb = "Same scrambled 3×3 for everyone. Tap a tile next to the gap to slide it. First to put 1-8 in order wins."
    min, max = 2, 4
    W, H = 776, 560
    hint = "tap a tile"

    def start(self, g):
        s = list(_PZ_GOAL)
        state = tuple(s)
        for _ in range(60):
            state = random.choice([n for n, _ in _pz_neigh(state)])
        g["st"] = {"b": {p["id"]: list(state) for p in g["players"]}, "moves": {p["id"]: 0 for p in g["players"]},
                   "plan": {}, "last": None}

    def keyboard(self, g):
        cur = _current(g)
        b = g["st"]["b"][cur["id"]]
        tiles = [t for _, t in _pz_neigh(tuple(b))]
        return [[(str(t), f"s{t}") for t in sorted(tiles)]]

    def caption(self, g):
        st = g["st"]
        return [f"{_tag(p)} · {st['moves'][p['id']]} moves · {8 - _pz_wrong(st['b'][p['id']])}/8 in place" + (" ✔" if p["out"] else "") for p in g["players"]]

    def robot(self, g, p):
        st = g["st"]
        b = tuple(st["b"][p["id"]])
        plan = st["plan"].get(p["id"])
        if not plan:
            plan = _pz_solve(b)
            st["plan"][p["id"]] = plan
        if plan and random.random() < 0.85:
            return f"s{plan[0]}"
        st["plan"][p["id"]] = None
        return f"s{random.choice([t for _, t in _pz_neigh(b)])}"

    def act(self, g, p, action, secret=None):
        st = g["st"]
        try:
            t = int(action[1:])
        except Exception:
            return "Tap a tile."
        b = st["b"][p["id"]]
        z = b.index(0)
        k = b.index(t) if t in b else -1
        if k < 0 or not any(tile == t for _, tile in _pz_neigh(tuple(b))):
            return "That tile isn't next to the gap."
        b[z], b[k] = b[k], b[z]
        st["moves"][p["id"]] += 1
        plan = st["plan"].get(p["id"])
        if plan and plan[0] == t:
            plan.pop(0)
        else:
            st["plan"][p["id"]] = None
        st["last"] = p["id"]
        _log(g, f"{_tag(p)} slides <b>{t}</b>")
        if tuple(b) == _PZ_GOAL:
            _log(g, f"🎉 {_tag(p)} solves it in {st['moves'][p['id']]} moves!")
            _win(g, p)
            return None
        if st["moves"][p["id"]] >= _PZ_MAX:
            p["out"] = True
            _log(g, f"{_tag(p)} is out of moves")
            if all(q["out"] for q in g["players"]):
                best = min(_pz_wrong(st["b"][q["id"]]) for q in g["players"])
                tops = [q for q in g["players"] if _pz_wrong(st["b"][q["id"]]) == best]
                _win(g, tops[0]) if len(tops) == 1 else _draw_game(g)
                return None
        _advance(g)
        return None

    def render(self, g):
        st = g["st"]
        cur = _current(g)
        status = (f"{g['winner']['name']} wins!" if g["phase"] == "over" and g["winner"] else
                  "Draw" if g["phase"] == "over" else f"Turn: {cur['name']}")
        img, d = _canvas(self.W, self.H, "NUMBER PUZZLE", status)
        n = len(g["players"])
        cs = 62 if n <= 2 else 46
        gap = 6
        bw = cs * 3 + gap * 4
        total = n * bw + (n - 1) * 30
        x0 = (self.W - total) / 2
        for i, p in enumerate(g["players"]):
            x = x0 + i * (bw + 30)
            y = TOP + 40
            d.rounded_rectangle([_sx(x), _sx(y), _sx(x + bw), _sx(y + bw)], radius=8 * S, fill=(92, 58, 30),
                                outline=GOLD if (g["phase"] == "play" and cur is p) else (60, 40, 20), width=3 * S)
            b = st["b"][p["id"]]
            for k, v in enumerate(b):
                r, c = divmod(k, 3)
                tx, ty = x + gap + c * (cs + gap), y + gap + r * (cs + gap)
                if v:
                    ok = v == k + 1
                    d.rounded_rectangle([_sx(tx), _sx(ty), _sx(tx + cs), _sx(ty + cs)], radius=5 * S, fill=(240, 220, 170) if not ok else (200, 232, 190), outline=(160, 120, 60), width=2 * S)
                    _txt(d, tx + cs / 2, ty + cs / 2, str(v), int(cs * 0.5), (90, 60, 20))
            _token(d, x + 16, y + bw + 22, COLORS[p["c"]][2], _plabel(p), r=12)
            _txt(d, x + 34, y + bw + 22, f"{p['name']} · {st['moves'][p['id']]}", 14, (255, 255, 255), anchor="lm")
        return _finish(img, self.W, self.H)


def _pz_wrong(b):
    return sum(1 for i, v in enumerate(b) if v and v != i + 1)
