#!/usr/bin/env python3
"""sharecard.py - the PNG a user shares from the Mini App.

Two shapes, same data:
  * "square"  1080x1080 - for a chat (shareMessage / a plain photo)
  * "story"   1080x1920 - for Telegram Stories (shareToStory)

The card shows one trade the way the trader sees it: pair, side, leverage,
entry / SL / TP, and either the live P/L (running) or the closed result.
Nothing is invented here: every number comes from the caller.
"""

import io
from datetime import datetime, timedelta, timezone

from PIL import Image, ImageDraw, ImageFont

S = 2                                   # render at 2x, downsample
IST = timedelta(hours=5, minutes=30)
BG, PANEL, GOLD, GOLD2 = (10, 13, 18), (18, 22, 29), (255, 176, 32), (255, 205, 120)
TEXT, MUTED, LONG, SHORT, LINE = (230, 234, 240), (107, 116, 128), (0, 209, 143), (255, 77, 94), (30, 36, 46)

_fonts: dict = {}


def _font(size, bold=True):
    key = (size, bold)
    if key in _fonts:
        return _fonts[key]
    cands = []
    try:
        import matplotlib
        import os as _os
        cands.append(_os.path.join(matplotlib.get_data_path(), "fonts", "ttf",
                                   "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"))
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
    _fonts[key] = f or ImageFont.load_default()
    return _fonts[key]


def _money(v):
    v = float(v or 0)
    return ("-" if v < 0 else "") + "$" + f"{abs(v):,.2f}"


def _glow(im, cx, cy, r, color, strength=0.55):
    """A soft radial wash - the app's gold/green ambience."""
    layer = Image.new("RGB", im.size, BG)
    d = ImageDraw.Draw(layer)
    steps = 16
    for i in range(steps, 0, -1):
        k = i / steps
        col = tuple(int(BG[j] + (color[j] - BG[j]) * (1 - k) * strength) for j in range(3))
        d.ellipse((cx - r * k, cy - r * k, cx + r * k, cy + r * k), fill=col)
    return Image.blend(im, layer, 0.5)


def render(t: dict, shape: str = "square", brand: str = "CLEXER · CLEX™ BOT") -> bytes:
    """t: {sym, side, lev, entry, sl, tp1, tp2, pnl_pct, pnl_usd, result, status, by}"""
    W, H = (1080 * S, 1080 * S) if shape == "square" else (1080 * S, 1920 * S)
    im = Image.new("RGB", (W, H), BG)
    im = _glow(im, int(W * 0.15), int(H * 0.02), int(W * 0.75), GOLD, .30)
    im = _glow(im, int(W * 1.0), int(H * 0.18), int(W * 0.6), LONG, .22)
    d = ImageDraw.Draw(im)

    long_side = str(t.get("side", "LONG")).upper() == "LONG"
    accent = LONG if long_side else SHORT
    pct = float(t.get("pnl_pct") or 0)
    win = pct >= 0
    pcol = LONG if win else SHORT
    running = str(t.get("status", "")).upper() == "RUNNING"

    M = int(W * 0.09)                                  # margin
    y = int(H * (0.11 if shape == "square" else 0.17))

    # brand
    d.ellipse((M, y - 6 * S, M + 22 * S, y + 16 * S), fill=GOLD)
    d.text((M + 36 * S, y - 8 * S), brand, font=_font(26), fill=GOLD2)
    y += int(H * (0.055 if shape == "square" else 0.035))

    # pair + badges
    sym = str(t.get("sym", "")).replace("-USDT", "").replace("USDT", "")
    d.text((M, y), f"{sym}/USDT", font=_font(76), fill=TEXT)
    y += 96 * S
    bx = M
    for label, fg, bgc in ((("LONG" if long_side else "SHORT"), accent, tuple(int(c * .22) for c in accent)),
                           (f"{t.get('lev', '')}x", MUTED, PANEL),
                           ("ISOLATED", MUTED, PANEL)):
        if not str(label).strip("x"):
            continue
        f = _font(30)
        w = int(d.textlength(str(label), font=f)) + 34 * S
        d.rounded_rectangle((bx, y, bx + w, y + 52 * S), radius=12 * S, fill=bgc)
        d.text((bx + 17 * S, y + 11 * S), str(label), font=f, fill=fg)
        bx += w + 14 * S
    y += int(H * (0.075 if shape == "square" else 0.05))

    # the number
    head = "UNREALIZED P/L" if running else "RESULT"
    d.text((M, y), head, font=_font(28), fill=MUTED)
    y += 46 * S
    big = f"{'+' if pct >= 0 else ''}{pct:.2f}%"
    d.text((M, y), big, font=_font(150), fill=pcol)
    if t.get("pnl_usd") is not None:
        f = _font(44)
        d.text((M + int(d.textlength(big, font=_font(150))) + 24 * S, y + 90 * S),
               ("+" if float(t["pnl_usd"]) >= 0 else "") + _money(t["pnl_usd"]), font=f, fill=pcol)
    y += int(H * (0.14 if shape == "square" else 0.10))

    # levels panel
    rows = [("ENTRY", t.get("entry")), ("STOP", t.get("sl")), ("TP1", t.get("tp1")), ("TP2", t.get("tp2"))]
    rows = [(k, v) for k, v in rows if v not in (None, "", 0)]
    ph = (len(rows) + 1) * 66 * S
    d.rounded_rectangle((M, y, W - M, y + ph), radius=28 * S, fill=PANEL, outline=LINE, width=2 * S)
    ry = y + 30 * S
    for k, v in rows:
        col = SHORT if k == "STOP" else LONG if k.startswith("TP") else TEXT
        d.text((M + 36 * S, ry), k, font=_font(30, False), fill=MUTED)
        vs = f"{float(v):,.6g}"
        d.text((W - M - 36 * S - int(d.textlength(vs, font=_font(36))), ry - 4 * S), vs, font=_font(36), fill=col)
        ry += 66 * S
    y += ph + int(H * 0.04)

    if not running and t.get("result"):
        d.text((M, y), str(t["result"]).upper(), font=_font(34), fill=GOLD)
        y += 60 * S

    if shape == "story":
        # the empty half of a story is where the invite goes
        cta = str(t.get("cta") or "@CLEXbot")
        f = _font(48)
        tw = int(d.textlength(cta, font=f))
        cy = int(H * 0.72)
        d.rounded_rectangle(((W - tw) // 2 - 46 * S, cy - 26 * S, (W + tw) // 2 + 46 * S, cy + 74 * S),
                            radius=26 * S, fill=GOLD)
        d.text(((W - tw) // 2, cy + 2 * S), cta, font=f, fill=BG)
        sub = "Live signals · copy trading · paper trading"
        f2 = _font(30, False)
        d.text(((W - int(d.textlength(sub, font=f2))) // 2, cy + 108 * S), sub, font=f2, fill=MUTED)

    # footer
    stamp = (datetime.now(timezone.utc) + IST).strftime("%d %b %Y · %H:%M IST")
    fy = H - int(H * (0.085 if shape == "square" else 0.10))
    d.line((M, fy - 28 * S, W - M, fy - 28 * S), fill=LINE, width=2 * S)
    d.text((M, fy), stamp, font=_font(26, False), fill=MUTED)
    tail = "Paper trade · not financial advice" if t.get("virtual") else "Signal by CLEX™ BOT"
    d.text((W - M - int(d.textlength(tail, font=_font(26, False))), fy), tail, font=_font(26, False), fill=MUTED)

    im = im.resize((W // S, H // S), Image.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, "PNG")
    return buf.getvalue()
