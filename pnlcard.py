#!/usr/bin/env python3
"""pnlcard.py - the Clex PnL card posted with every profitable close.

render() draws one 1280x1280 PNG: the CLEX mark, the trade (coin, side,
leverage), the leveraged % in big type, the two prices, a small price path
from open to close, and the footer with the BingX referral code and a QR of
the invite link (assets/bingx_invite_qr.png - made once, so the servers need
no QR library).

Fonts are the DejaVu set matplotlib ships with, so the card looks the same
on Railway (Linux) as anywhere else.
"""

import io
import os
import random

from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont

W = H = 1280
BG = (8, 11, 16)
GREEN = (34, 230, 140)
GOLD = (240, 185, 60)
WHITE = (240, 243, 247)
MUTED = (135, 145, 158)
DIM = (70, 78, 90)

REFERRAL_CODE = "UOGCYC"
_HERE = os.path.dirname(os.path.abspath(__file__))
_QR_FILE = os.path.join(_HERE, "assets", "bingx_invite_qr.png")

_fonts = {}


def _font_dir():
    try:
        import matplotlib
        return os.path.join(matplotlib.get_data_path(), "fonts", "ttf")
    except Exception:
        return ""


def _font(bold: bool, size: int, mono: bool = False):
    key = (bold, size, mono)
    if key not in _fonts:
        name = ("DejaVuSansMono" if mono else "DejaVuSans") + ("-Bold" if bold else "") + ".ttf"
        try:
            _fonts[key] = ImageFont.truetype(os.path.join(_font_dir(), name), size)
        except Exception:
            _fonts[key] = ImageFont.load_default()
    return _fonts[key]


def _px(x) -> str:
    """Price text: thousands separators for big prices, enough digits for
    small coins, never scientific notation."""
    x = float(x)
    a = abs(x)
    if a >= 1000:
        return f"{x:,.1f}"
    dec = 4 if a >= 1 else 6 if a >= 0.01 else 8
    s = f"{x:.{dec}f}".rstrip("0").rstrip(".")
    return s or "0"


def _background():
    img = Image.new("RGB", (W, H), BG)
    glow = Image.new("RGB", (W, H), (0, 0, 0))
    g = ImageDraw.Draw(glow)
    g.ellipse((700, 120, 1400, 820), fill=(10, 70, 45))
    g.ellipse((-250, -250, 450, 350), fill=(60, 45, 10))
    glow = glow.filter(ImageFilter.GaussianBlur(160))
    return ImageChops.add(img, glow)


def _chart(d, entry: float, close: float, side: str, seed: int):
    """A small price path from the open to the close, in market colours: a
    rising candle green, a falling one red - so a SHORT's chart runs red
    (admin 2026-09-24)."""
    rnd = random.Random(seed)
    x0, x1, y_top, y_bot = 740, 1215, 260, 880
    n = 20
    lo, hi = min(entry, close), max(entry, close)
    pad = (hi - lo) * 0.12 or abs(entry) * 0.002
    lo, hi = lo - pad, hi + pad

    def py(p):
        return y_bot - (p - lo) / (hi - lo) * (y_bot - y_top)

    path, noise = [], (hi - lo) * 0.07
    for i in range(1, n + 1):
        f = (i / n) ** 1.15
        path.append(entry + (close - entry) * f + rnd.uniform(-noise, noise) * (1 - i / n))
    path[-1] = close
    cw = (x1 - x0) / n
    prev = entry
    for i, c in enumerate(path):
        o = prev
        col = GREEN if c >= o else (235, 80, 95)
        wick = (hi - lo) * rnd.uniform(0.01, 0.05)
        cx = x0 + i * cw + cw / 2
        d.line((cx, py(max(o, c) + wick), cx, py(min(o, c) - wick)), fill=col, width=3)
        top, bot = py(max(o, c)), py(min(o, c))
        if bot - top < 4:
            bot = top + 4
        d.rounded_rectangle((cx - cw * 0.32, top, cx + cw * 0.32, bot), radius=3, fill=col)
        prev = c
    lab = _font(True, 21, mono=True)
    for p, lbl, col in ((entry, f"OPEN {_px(entry)}", MUTED), (close, f"CLOSE {_px(close)}", GREEN)):
        y = py(p)
        for xx in range(x0, x1, 18):
            d.line((xx, y, xx + 9, y), fill=col, width=2)
        above = (p == close) == (close >= entry)
        if lbl.startswith("CLOSE"):
            d.text((x0, y - 32) if above else (x0, y + 10), lbl, font=lab, fill=col)
        else:
            d.text((x1, y + 10) if above else (x1, y - 32), lbl, font=lab, fill=col, anchor="ra")


def render(symbol: str, side: str, leverage: int, pct: float, close_label: str, close_px: float,
           entry_px: float, tag: str, when: str, seed: int = 0) -> bytes:
    """PNG bytes. pct is the leveraged % (already x leverage). tag is the
    result shown in the gold pill (TP1 HIT, TP2 HIT, TP1 + BE, TIMEOUT)."""
    img = _background()
    d = ImageDraw.Draw(img)
    _chart(d, float(entry_px), float(close_px), side, seed)

    # brand
    d.rounded_rectangle((64, 70, 132, 138), radius=18, fill=GOLD)
    d.text((98, 104), "C", font=_font(True, 50), fill=BG, anchor="mm")
    d.text((150, 104), "CLEX", font=_font(True, 62), fill=WHITE, anchor="lm")
    d.text((66, 160), "Trade smarter with Clex", font=_font(False, 25), fill=MUTED)

    # result pill
    pf = _font(True, 26)
    tw = d.textlength(tag, font=pf)
    d.rounded_rectangle((64, 238, 64 + tw + 40, 284), radius=23, fill=(40, 34, 14), outline=GOLD, width=2)
    d.text((84, 261), tag, font=pf, fill=GOLD, anchor="lm")

    # the trade
    d.text((64, 310), "Realized PnL", font=_font(False, 44), fill=WHITE)
    x = 64
    pair = symbol.replace("-", "").upper()
    for part, col, bold in ((pair, WHITE, True), ("  |  ", DIM, False),
                            ("Long" if side == "BUY" else "Short", GREEN if side == "BUY" else (235, 80, 95), True),
                            ("  |  ", DIM, False), (f"{int(leverage)}X", WHITE, True)):
        f = _font(bold, 50)
        d.text((x, 372), part, font=f, fill=col)
        x += d.textlength(part, font=f)

    big = f"+{pct:.2f}%"
    size = 150
    while size > 90 and d.textlength(big, font=_font(True, size)) > 660:
        size -= 6
    d.text((58, 470), big, font=_font(True, size), fill=GREEN)

    y = 700
    for k, v in ((close_label, _px(close_px)), ("Entry Price", _px(entry_px))):
        d.text((64, y), k, font=_font(False, 36), fill=MUTED)
        d.text((400, y), v, font=_font(True, 36), fill=WHITE)
        y += 68

    # footer
    d.line((64, 1030, W - 64, 1030), fill=(30, 36, 46), width=2)
    # no round badge here - the admin found the footer "C" odd (2026-09-24)
    d.text((64, 1083), "CLEX", font=_font(True, 42), fill=WHITE)
    d.text((64, 1137), when, font=_font(False, 32), fill=MUTED)
    qx = W - 64 - 150
    try:
        qr = Image.open(_QR_FILE).convert("RGB").resize((150, 150), Image.NEAREST)
        img.paste(qr, (qx, 1055))
    except Exception:
        qx = W - 64
    d.text((qx - 24, 1083), "Referral Code", font=_font(False, 30), fill=MUTED, anchor="ra")
    d.text((qx - 24, 1128), REFERRAL_CODE, font=_font(True, 42), fill=WHITE, anchor="ra")

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()
