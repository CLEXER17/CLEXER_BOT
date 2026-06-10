"""
CLEXER Signal Bot V3 — Full SMC Vision + Multi-TF + Trade Management
- Weekly/4H/1H/5M multi-timeframe analysis
- Auto SMC calculations: OBs, FVGs, BOS/CHoCH, Liquidity
- All levels drawn on chart images before sending to Claude
- London/NY session filter only
- Full trade management: TP1/TP2/SL/Breakeven
"""

import os, time, json, base64, math, requests, anthropic
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch
from io import BytesIO
from datetime import datetime, timezone, timedelta

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY",   "your_key_here")
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN",  "your_token_here")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "@your_channel")

SYMBOL                = "BTCUSDT"
SCAN_INTERVAL_SECONDS = 14400        # 4 hours
BINANCE_BASE          = "https://api1.binance.com/api/v3"
IST                   = timedelta(hours=5, minutes=30)

# ─── TIME HELPERS ──────────────────────────────────────────────────────────────
def now_ist() -> datetime:
    return datetime.now(timezone.utc) + IST

def ist_str() -> str:
    return now_ist().strftime("%d %b %Y  %I:%M %p IST")

def get_session() -> str:
    """
    Detect trading session using IST time (UTC+5:30 / Kolkata).
    London open  = 07:30 IST  (UTC 02:00)
    London close = 16:30 IST  (UTC 11:00)
    NY open      = 18:30 IST  (UTC 13:00)
    NY close     = 01:00 IST  (UTC 19:30, next day)
    """
    t    = now_ist()
    mins = t.hour * 60 + t.minute          # minutes since midnight IST

    LONDON_OPEN  = 7  * 60 + 30            # 07:30 IST
    LONDON_CLOSE = 16 * 60 + 30            # 16:30 IST
    NY_OPEN      = 18 * 60 + 30            # 18:30 IST
    NY_CLOSE     = 24 * 60 + 60            # 01:00 IST next day = 1500 mins

    if LONDON_OPEN <= mins < LONDON_CLOSE: return "LONDON"
    if NY_OPEN <= mins or mins < 60:       return "NEW_YORK"   # 18:30–01:00 IST
    return "ASIA"

def is_trading_hours() -> bool:
    """Only London or NY session — BHABANI's rule"""
    return get_session() in ("LONDON", "NEW_YORK")

def is_ist_sleep() -> bool:
    """Sleep: 01:00 AM – 07:29 AM IST (no signals during this window)"""
    t    = now_ist()
    mins = t.hour * 60 + t.minute
    return 60 <= mins < (7 * 60 + 30)     # 01:00–07:29 IST

# ─── ACTIVE TRADE STATE ───────────────────────────────────────────────────────
active_trade = {
    "signal": None, "entry": None, "sl": None,
    "tp1": None, "tp2": None, "tp1_hit": False,
    "entry_type": "MARKET", "entry_note": "",
}

def reset_trade():
    global active_trade
    active_trade = {"signal": None, "entry": None, "sl": None,
                    "tp1": None, "tp2": None, "tp1_hit": False,
                    "entry_type": "MARKET", "entry_note": ""}

def set_trade(s: dict):
    global active_trade
    active_trade = {
        "signal":     s["signal"],    "entry": s["entry"],
        "sl":         s["sl"],        "tp1": s["tp1"],
        "tp2":        s["tp2"],       "tp1_hit": False,
        "entry_type": s.get("entry_type", "MARKET"),
        "entry_note": s.get("entry_note", ""),
    }

# ─── BINANCE ──────────────────────────────────────────────────────────────────
def get_candles(interval: str, limit: int) -> pd.DataFrame:
    url = f"{BINANCE_BASE}/klines"
    params = {"symbol": SYMBOL, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    rows = []
    for c in r.json():
        rows.append({
            "time":  datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc),
            "open":  float(c[1]), "high": float(c[2]),
            "low":   float(c[3]), "close": float(c[4]), "vol": float(c[5]),
        })
    df = pd.DataFrame(rows).set_index("time")
    return df

def get_ticker() -> dict:
    r = requests.get(f"{BINANCE_BASE}/ticker/24hr",
                     params={"symbol": SYMBOL}, timeout=10)
    r.raise_for_status()
    d = r.json()
    return {
        "price":    float(d["lastPrice"]),
        "change":   float(d["priceChangePercent"]),
        "volume":   float(d["quoteVolume"]),
        "high24":   float(d["highPrice"]),
        "low24":    float(d["lowPrice"]),
    }

# ─── SMC CALCULATIONS ─────────────────────────────────────────────────────────
def find_swing_points(df: pd.DataFrame, lookback: int = 5):
    """Find swing highs and lows"""
    highs, lows = [], []
    for i in range(lookback, len(df) - lookback):
        window_h = df["high"].iloc[i - lookback: i + lookback + 1]
        window_l = df["low"].iloc[i - lookback: i + lookback + 1]
        if df["high"].iloc[i] == window_h.max():
            highs.append({"idx": i, "price": df["high"].iloc[i], "time": df.index[i]})
        if df["low"].iloc[i] == window_l.min():
            lows.append({"idx": i, "price": df["low"].iloc[i], "time": df.index[i]})
    return highs, lows

def detect_trend(df: pd.DataFrame) -> str:
    """Detect HH/HL (bullish) or LH/LL (bearish) trend"""
    highs, lows = find_swing_points(df, lookback=3)
    if len(highs) < 2 or len(lows) < 2:
        return "NEUTRAL"
    last2h = [h["price"] for h in highs[-2:]]
    last2l = [l["price"] for l in lows[-2:]]
    bull = last2h[1] > last2h[0] and last2l[1] > last2l[0]  # HH + HL
    bear = last2h[1] < last2h[0] and last2l[1] < last2l[0]  # LH + LL
    return "BULLISH" if bull else ("BEARISH" if bear else "NEUTRAL")

def detect_bos_choch(df: pd.DataFrame):
    """
    BOS = Break of Structure (trend continuation)
    CHoCH = Change of Character (trend reversal)
    Returns list of events with type, price, index
    """
    events = []
    highs, lows = find_swing_points(df, lookback=3)
    if len(highs) < 2 or len(lows) < 2:
        return events

    # Check last few swings for breaks
    for i in range(1, min(4, len(highs))):
        prev_h = highs[-i - 1]["price"]
        curr_h = highs[-i]["price"]
        idx = highs[-i]["idx"]
        # If last close breaks above swing high → BOS (bullish) or CHoCH (if was bearish)
        if idx < len(df) - 1:
            close_after = df["close"].iloc[idx + 1]
            if close_after > prev_h:
                events.append({"type": "BOS_BULL", "price": prev_h,
                                "idx": idx, "time": df.index[idx]})
                break

    for i in range(1, min(4, len(lows))):
        prev_l = lows[-i - 1]["price"]
        curr_l = lows[-i]["price"]
        idx = lows[-i]["idx"]
        if idx < len(df) - 1:
            close_after = df["close"].iloc[idx + 1]
            if close_after < prev_l:
                events.append({"type": "BOS_BEAR", "price": prev_l,
                                "idx": idx, "time": df.index[idx]})
                break
    return events

def detect_order_blocks(df: pd.DataFrame, n: int = 5):
    """
    Bullish OB: last bearish candle before a bullish BOS
    Bearish OB: last bullish candle before a bearish BOS
    Returns up to n most recent OBs
    """
    obs = []
    closes = df["close"].values
    opens  = df["open"].values
    highs  = df["high"].values
    lows   = df["low"].values

    for i in range(3, len(df) - 3):
        # Bullish OB: bearish candle followed by strong bullish move
        if closes[i] < opens[i]:  # bearish candle
            fwd_move = max(closes[i+1:i+4]) - highs[i]
            candle_size = highs[i] - lows[i]
            if candle_size > 0 and fwd_move > candle_size * 0.5:
                obs.append({
                    "type": "BULL_OB",
                    "top": highs[i], "bottom": lows[i],
                    "mid": (highs[i] + lows[i]) / 2,
                    "idx": i, "time": df.index[i]
                })

        # Bearish OB: bullish candle followed by strong bearish move
        if closes[i] > opens[i]:  # bullish candle
            fwd_move = lows[i] - min(closes[i+1:i+4])
            candle_size = highs[i] - lows[i]
            if candle_size > 0 and fwd_move > candle_size * 0.5:
                obs.append({
                    "type": "BEAR_OB",
                    "top": highs[i], "bottom": lows[i],
                    "mid": (highs[i] + lows[i]) / 2,
                    "idx": i, "time": df.index[i]
                })

    # Return most recent n OBs
    return obs[-n:] if len(obs) > n else obs

def detect_fvgs(df: pd.DataFrame, n: int = 5):
    """
    FVG (Fair Value Gap): 3-candle imbalance
    Bullish FVG: candle[i].low > candle[i-2].high
    Bearish FVG: candle[i].high < candle[i-2].low
    """
    fvgs = []
    for i in range(2, len(df)):
        # Bullish FVG
        if df["low"].iloc[i] > df["high"].iloc[i - 2]:
            fvgs.append({
                "type": "BULL_FVG",
                "top":    df["low"].iloc[i],
                "bottom": df["high"].iloc[i - 2],
                "mid":    (df["low"].iloc[i] + df["high"].iloc[i - 2]) / 2,
                "idx": i, "time": df.index[i]
            })
        # Bearish FVG
        if df["high"].iloc[i] < df["low"].iloc[i - 2]:
            fvgs.append({
                "type": "BEAR_FVG",
                "top":    df["low"].iloc[i - 2],
                "bottom": df["high"].iloc[i],
                "mid":    (df["low"].iloc[i - 2] + df["high"].iloc[i]) / 2,
                "idx": i, "time": df.index[i]
            })
    return fvgs[-n:] if len(fvgs) > n else fvgs

def detect_liquidity(df: pd.DataFrame, lookback: int = 5):
    """
    Equal highs/lows = liquidity resting above/below
    Returns recent swing highs (buy-side liq) and swing lows (sell-side liq)
    """
    highs, lows = find_swing_points(df, lookback=lookback)
    # Group equal highs (within 0.1%)
    buy_liq  = [h for h in highs[-10:]]
    sell_liq = [l for l in lows[-10:]]
    return buy_liq, sell_liq

# ─── CHART DRAWING ────────────────────────────────────────────────────────────
def draw_smc_chart(df: pd.DataFrame, tf: str, obs: list, fvgs: list,
                   bos_events: list, swing_highs: list, swing_lows: list,
                   current_price: float = None) -> str:
    """
    Draw full SMC annotated candlestick chart using matplotlib.
    Returns base64 PNG string.
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 10),
                                   gridspec_kw={"height_ratios": [4, 1]},
                                   facecolor="#0d0d0d")
    ax1.set_facecolor("#0d0d0d")
    ax2.set_facecolor("#0d0d0d")

    n = len(df)
    x = np.arange(n)

    # ── Candlesticks ──────────────────────────────────────────────────────────
    for i, (idx_t, row) in enumerate(df.iterrows()):
        o, h, l, c = row["open"], row["high"], row["low"], row["close"]
        color = "#26a69a" if c >= o else "#ef5350"
        # Wick
        ax1.plot([i, i], [l, h], color=color, linewidth=0.8, zorder=2)
        # Body
        body_h = abs(c - o)
        body_b = min(o, c)
        rect = plt.Rectangle((i - 0.4, body_b), 0.8, body_h,
                              color=color, zorder=3)
        ax1.add_patch(rect)

    # ── Order Blocks ──────────────────────────────────────────────────────────
    for ob in obs:
        idx = ob["idx"]
        if idx >= n:
            continue
        color = "#1a6b3c" if ob["type"] == "BULL_OB" else "#6b1a1a"
        border = "#00e676" if ob["type"] == "BULL_OB" else "#ff5252"
        rect = plt.Rectangle((idx - 0.5, ob["bottom"]),
                              n - idx + 2, ob["top"] - ob["bottom"],
                              color=color, alpha=0.3, zorder=1)
        ax1.add_patch(rect)
        ax1.axhline(ob["mid"], xmin=idx / n, color=border,
                    linewidth=0.6, linestyle="--", alpha=0.6, zorder=1)
        label = "Bull OB" if ob["type"] == "BULL_OB" else "Bear OB"
        ax1.text(idx + 0.5, ob["top"], label, color=border,
                 fontsize=6.5, va="bottom", zorder=5)

    # ── FVGs ─────────────────────────────────────────────────────────────────
    for fvg in fvgs:
        idx = fvg["idx"]
        if idx >= n:
            continue
        color = "#1a3d6b" if fvg["type"] == "BULL_FVG" else "#6b4a1a"
        border = "#40c4ff" if fvg["type"] == "BULL_FVG" else "#ffab40"
        rect = plt.Rectangle((idx - 2, fvg["bottom"]),
                              n - idx + 3, fvg["top"] - fvg["bottom"],
                              color=color, alpha=0.35, zorder=1)
        ax1.add_patch(rect)
        label = "Bull FVG" if fvg["type"] == "BULL_FVG" else "Bear FVG"
        ax1.text(idx + 0.5, fvg["top"], label, color=border,
                 fontsize=6.5, va="bottom", zorder=5)

    # ── Swing Highs / Lows (liquidity) ───────────────────────────────────────
    for sh in swing_highs[-6:]:
        idx = sh["idx"]
        if idx >= n: continue
        ax1.plot(idx, sh["price"], marker="^", color="#ffeb3b",
                 markersize=5, zorder=6)
        ax1.axhline(sh["price"], color="#ffeb3b", linewidth=0.5,
                    linestyle=":", alpha=0.4, zorder=1)

    for sl in swing_lows[-6:]:
        idx = sl["idx"]
        if idx >= n: continue
        ax1.plot(idx, sl["price"], marker="v", color="#ff9800",
                 markersize=5, zorder=6)
        ax1.axhline(sl["price"], color="#ff9800", linewidth=0.5,
                    linestyle=":", alpha=0.4, zorder=1)

    # ── BOS / CHoCH ───────────────────────────────────────────────────────────
    for ev in bos_events:
        idx = ev["idx"]
        if idx >= n: continue
        color = "#b2ff59" if "BULL" in ev["type"] else "#ff4081"
        label = "BOS" if "BOS" in ev["type"] else "CHoCH"
        ax1.axhline(ev["price"], color=color, linewidth=1.0,
                    linestyle="-.", alpha=0.7, zorder=4)
        ax1.text(max(0, idx - 2), ev["price"], label, color=color,
                 fontsize=7, va="bottom", fontweight="bold", zorder=6)

    # ── Current price line ────────────────────────────────────────────────────
    if current_price:
        ax1.axhline(current_price, color="#ffffff", linewidth=1.2,
                    linestyle="--", alpha=0.9, zorder=7)
        ax1.text(n - 1, current_price, f" {current_price:,.0f}",
                 color="#ffffff", fontsize=8, va="center", zorder=8)

    # ── Volume bars ───────────────────────────────────────────────────────────
    for i, (idx_t, row) in enumerate(df.iterrows()):
        color = "#26a69a" if row["close"] >= row["open"] else "#ef5350"
        ax2.bar(i, row["vol"], color=color, alpha=0.7, width=0.8)

    # ── X-axis labels (show every ~10 candles) ────────────────────────────────
    step = max(1, n // 10)
    xticks = x[::step]
    xlabels = [df.index[i].strftime("%m/%d %H:%M") for i in xticks]
    ax1.set_xticks([])
    ax2.set_xticks(xticks)
    ax2.set_xticklabels(xlabels, rotation=30, fontsize=6, color="#aaaaaa")

    # ── Styling ───────────────────────────────────────────────────────────────
    ax1.set_xlim(-1, n + 3)
    ax2.set_xlim(-1, n + 3)
    ax1.tick_params(colors="#aaaaaa", labelsize=7)
    ax2.tick_params(colors="#aaaaaa", labelsize=7)
    for spine in ax1.spines.values(): spine.set_color("#333333")
    for spine in ax2.spines.values(): spine.set_color("#333333")
    ax1.yaxis.tick_right()
    ax1.yaxis.set_label_position("right")
    ax1.set_ylabel("Price (USDT)", color="#aaaaaa", fontsize=8)
    ax2.set_ylabel("Volume", color="#aaaaaa", fontsize=8)

    # Trend text
    trend = detect_trend(df)
    trend_color = "#26a69a" if trend == "BULLISH" else ("#ef5350" if trend == "BEARISH" else "#ffffff")
    ax1.set_title(f"{SYMBOL} {tf}  |  Trend: {trend}",
                  color=trend_color, fontsize=11, pad=6,
                  fontweight="bold", loc="left")

    # Legend
    legend_items = [
        mpatches.Patch(color="#26a69a", label="Bull OB"),
        mpatches.Patch(color="#6b1a1a", label="Bear OB"),
        mpatches.Patch(color="#1a3d6b", label="Bull FVG"),
        mpatches.Patch(color="#6b4a1a", label="Bear FVG"),
    ]
    ax1.legend(handles=legend_items, loc="upper left",
               facecolor="#1a1a1a", edgecolor="#444", labelcolor="#cccccc",
               fontsize=7, framealpha=0.8)

    plt.tight_layout(pad=0.5)
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=120, bbox_inches="tight",
                facecolor="#0d0d0d")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_all_charts(data: dict, ticker: dict) -> dict:
    """Generate all 4 SMC-annotated chart images"""
    charts = {}
    price  = ticker["price"]

    for tf_key, (df, lookback) in data.items():
        obs     = detect_order_blocks(df, n=6)
        fvgs    = detect_fvgs(df, n=6)
        bos     = detect_bos_choch(df)
        s_highs, s_lows = find_swing_points(df, lookback=lookback)
        charts[tf_key] = draw_smc_chart(
            df, tf_key.upper(), obs, fvgs, bos,
            s_highs[-8:], s_lows[-8:], current_price=price
        )
        print(f"    Chart {tf_key}: {len(obs)} OBs, {len(fvgs)} FVGs, {len(bos)} BOS events")

    return charts


# ─── SMC SUMMARY TEXT ─────────────────────────────────────────────────────────
def build_smc_summary(data: dict, ticker: dict) -> str:
    """Build structured text summary of all SMC levels for Claude"""
    lines = []
    price = ticker["price"]

    lines.append(f"=== BTCUSDT LIVE DATA ===")
    lines.append(f"Price: {price:,.2f} | 24h: {ticker['change']:+.2f}%")
    lines.append(f"24h High: {ticker['high24']:,.2f} | Low: {ticker['low24']:,.2f}")
    lines.append(f"Volume: ${ticker['volume']/1e6:.1f}M")
    lines.append(f"Session: {get_session()} | IST: {ist_str()}")
    lines.append("")

    tf_labels = {"weekly": "WEEKLY", "4h": "4H", "1h": "1H", "5m": "5M"}
    for tf_key, (df, lookback) in data.items():
        trend   = detect_trend(df)
        obs     = detect_order_blocks(df, n=4)
        fvgs    = detect_fvgs(df, n=4)
        bos     = detect_bos_choch(df)
        s_highs, s_lows = find_swing_points(df, lookback=lookback)

        lines.append(f"--- {tf_labels.get(tf_key, tf_key)} ---")
        lines.append(f"Trend: {trend}")

        if bos:
            for b in bos[-2:]:
                lines.append(f"  {b['type']}: {b['price']:,.2f}")

        bull_obs = [o for o in obs if o["type"] == "BULL_OB"]
        bear_obs = [o for o in obs if o["type"] == "BEAR_OB"]
        if bull_obs:
            ob = bull_obs[-1]
            lines.append(f"  Bull OB: {ob['bottom']:,.2f} – {ob['top']:,.2f}")
        if bear_obs:
            ob = bear_obs[-1]
            lines.append(f"  Bear OB: {ob['bottom']:,.2f} – {ob['top']:,.2f}")

        bull_fvgs = [f for f in fvgs if f["type"] == "BULL_FVG"]
        bear_fvgs = [f for f in fvgs if f["type"] == "BEAR_FVG"]
        if bull_fvgs:
            fv = bull_fvgs[-1]
            lines.append(f"  Bull FVG: {fv['bottom']:,.2f} – {fv['top']:,.2f}")
        if bear_fvgs:
            fv = bear_fvgs[-1]
            lines.append(f"  Bear FVG: {fv['bottom']:,.2f} – {fv['top']:,.2f}")

        if s_highs:
            lines.append(f"  Last swing high: {s_highs[-1]['price']:,.2f}")
        if s_lows:
            lines.append(f"  Last swing low:  {s_lows[-1]['price']:,.2f}")

        lines.append("")

    return "\n".join(lines)


# ─── CLAUDE: NEW SIGNAL ───────────────────────────────────────────────────────
def analyze_with_claude(ticker: dict, data: dict) -> dict | None:
    client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    price   = ticker["price"]
    session = get_session()

    print("  Generating SMC charts...")
    charts  = generate_all_charts(data, ticker)
    summary = build_smc_summary(data, ticker)

    prompt = f"""You are an expert SMC crypto trader doing a full multi-timeframe analysis.

{summary}

Charts provided (with SMC drawings):
1. WEEKLY — overall trend, macro OBs/FVGs, major structure
2. 4H — key swing highs/lows, BOS/CHoCH, primary OBs, session FVGs
3. 1H — confirmation, entry zone OB/FVG, structure alignment
4. 5M — exact entry timing, momentum, kill zone trigger

SMC RULES TO FOLLOW:
- Weekly bias is king — never trade against it
- 4H must confirm the bias (BOS in direction of trade)
- 1H must have a clear OB or FVG as entry zone
- 5M must show momentum/displacement in trade direction
- SL: last significant structure (swing high for SHORT, swing low for LONG)
- TP1: nearest opposing OB or FVG (min 1:2 R:R)
- TP2: next major swing level (min 1:4 R:R)
- Prefer entries at OB midpoints or FVG fills
- Current session: {session}

ENTRY TYPE:
- "MARKET" if price is AT or INSIDE the entry zone NOW
- "PULLBACK" if price needs to return to entry zone first

You MUST give BUY or SELL. No NO_TRADE responses.

Respond ONLY in this exact JSON (no markdown, no explanation):
{{
  "signal": "BUY" | "SELL",
  "entry": <number>,
  "sl": <number>,
  "tp1": <number>,
  "tp2": <number>,
  "rr": "<e.g. 1:3.5>",
  "entry_type": "MARKET" | "PULLBACK",
  "entry_note": "<pullback instruction or empty>",
  "bias": "BULLISH" | "BEARISH",
  "weekly_trend": "<weekly structure summary>",
  "structure_4h": "<4H BOS/CHoCH and key levels>",
  "entry_zone": "<OB or FVG range being used>",
  "confidence": "HIGH" | "MEDIUM" | "LOW",
  "session": "{session}"
}}"""

    try:
        content = [
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["weekly"]}},
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["4h"]}},
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["1h"]}},
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["5m"]}},
            {"type": "text",  "text": prompt},
        ]
        msg = client.messages.create(
            model="claude-sonnet-4-5", max_tokens=800,
            messages=[{"role": "user", "content": content}]
        )
        raw = msg.content[0].text.strip().replace("```json","").replace("```","").strip()
        return json.loads(raw)
    except Exception as e:
        print(f"  [ERROR] Claude signal: {e}")
        return None


# ─── CLAUDE: TRADE UPDATE ─────────────────────────────────────────────────────
def claude_trade_update(ticker: dict, data: dict) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    price  = ticker["price"]

    # Only send 4H and 1H for update check
    obs_4h  = detect_order_blocks(data["4h"][0], n=4)
    fvgs_4h = detect_fvgs(data["4h"][0], n=4)
    bos_4h  = detect_bos_choch(data["4h"][0])
    s_h_4h, s_l_4h = find_swing_points(data["4h"][0], lookback=5)

    chart_4h = draw_smc_chart(data["4h"][0], "4H", obs_4h, fvgs_4h,
                               bos_4h, s_h_4h[-6:], s_l_4h[-6:], price)
    chart_1h = draw_smc_chart(data["1h"][0], "1H",
                               detect_order_blocks(data["1h"][0], n=4),
                               detect_fvgs(data["1h"][0], n=4),
                               detect_bos_choch(data["1h"][0]),
                               *find_swing_points(data["1h"][0], lookback=5), price)

    t = active_trade
    prompt = f"""Active {t['signal']} trade on BTCUSDT.
Entry: {t['entry']:,.2f} | SL: {t['sl']:,.2f}
TP1: {t['tp1']:,.2f} | TP2: {t['tp2']:,.2f}
TP1 hit: {t['tp1_hit']} | Current price: {price:,.2f}
Session: {get_session()}

Charts show 4H (SMC levels) and 1H (entry confirmation).
Look for: structure breaks, volume, OB/FVG interaction.

Respond with ONLY one word:
HOLD — structure intact, trade valid
WAIT — minor pullback, no action needed
CLOSE — structure broken, exit immediately
NO_VOLUME — low volume, close trade"""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-5", max_tokens=15,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_4h}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_1h}},
                {"type": "text", "text": prompt},
            ]}]
        )
        return msg.content[0].text.strip().upper().split()[0]
    except Exception as e:
        print(f"  [ERROR] Claude update: {e}")
        return "WAIT"


# ─── PRICE STATUS ─────────────────────────────────────────────────────────────
def check_price_status(price: float) -> str:
    t = active_trade
    if t["signal"] is None: return "NONE"
    sig, sl, tp1, tp2 = t["signal"], t["sl"], t["tp1"], t["tp2"]
    if (sig == "SELL" and price >= sl) or (sig == "BUY" and price <= sl):
        return "SL_HIT"
    if (sig == "SELL" and price <= tp2) or (sig == "BUY" and price >= tp2):
        return "TP2_HIT"
    if not t["tp1_hit"]:
        if (sig == "SELL" and price <= tp1) or (sig == "BUY" and price >= tp1):
            return "TP1_HIT"
    return "RUNNING"


# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHANNEL_ID, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True
        }, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"  [ERROR] Telegram: {e}")
        return False


# ─── MESSAGE FORMATS ──────────────────────────────────────────────────────────
def fmt_signal(s: dict) -> str:
    e = "🟢" if s["signal"] == "BUY" else "🔴"
    conf_icon = {"HIGH": "🔥", "MEDIUM": "✨", "LOW": "⚡"}.get(s.get("confidence",""), "")

    entry_line = f"🎯 Entry   <b>{s['entry']:,.0f}</b>"
    if s.get("entry_type") == "PULLBACK" and s.get("entry_note"):
        entry_line += f"\n   ⏳ <i>{s['entry_note']}</i>"

    weekly = s.get("weekly_trend", "")
    entry_zone = s.get("entry_zone", "")

    return (
        f"{e} <b>{s['signal']} — {SYMBOL}</b>  {conf_icon}\n"
        f"🕐 {ist_str()}  |  📍 {s.get('session', get_session())}\n\n"
        f"{entry_line}\n"
        f"🛑 SL       <b>{s['sl']:,.0f}</b>\n"
        f"✅ TP1    <b>{s['tp1']:,.0f}</b>\n"
        f"✅ TP2    <b>{s['tp2']:,.0f}</b>\n"
        f"📊 R:R    <b>{s.get('rr','—')}</b>\n\n"
        f"📈 Bias: {s.get('bias','')}\n"
        + (f"🗓 Weekly: <i>{weekly}</i>\n" if weekly else "")
        + (f"🔷 Zone: <i>{entry_zone}</i>\n" if entry_zone else "")
        + f"\n<i>— Signal by CLEXER V3 —</i>\n"
        f"⚠️ <i>Not financial advice</i>"
    )

def fmt_update(status: str) -> str:
    t = active_trade
    lines = {
        "SL_HIT":    "🛑 <b>SL HIT</b>\nFinding new trade...",
        "TP2_HIT":   "🏆 <b>TP2 HIT — Trade Complete!</b>",
        "TP1_HIT":   f"✅ <b>TP1 HIT</b>\nSL moved to breakeven ({t['entry']:,.0f})\nWaiting for TP2 — <b>{t['tp2']:,.0f}</b>",
        "HOLD":      "📊 <b>HOLD</b> — Structure intact, trade valid",
        "WAIT":      "⏳ <b>WAIT</b> — Trade running, minor pullback",
        "CLOSE":     "⚠️ <b>CLOSE</b> — Structure broken, exit now",
        "NO_VOLUME": "📉 <b>NO VOLUME</b> — Close the trade",
    }
    body = lines.get(status, "⏳ Trade running — WAIT")
    return f"📡 <b>{SYMBOL} UPDATE</b>  {ist_str()}\n\n{body}\n\n<i>— CLEXER V3 —</i>"


# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    print(f"[CLEXER V3] SMC Vision Bot | {SYMBOL} | Scan every {SCAN_INTERVAL_SECONDS}s")
    print(f"[CLEXER V3] Sessions: London (02-09 UTC) + NY (13-20 UTC)")
    send_telegram(
        f"🤖 <b>CLEXER V3 Online</b>\n"
        f"<b>Full SMC Vision Bot</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📊 Weekly + 4H + 1H + 5M\n"
        f"🔷 Auto OBs, FVGs, BOS/CHoCH\n"
        f"⏰ London + NY session only\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<i>— CLEXER V3 —</i>"
    )

    while True:
        try:
            ist_now = now_ist()
            print(f"\n[{ist_now.strftime('%H:%M:%S IST')}] Scanning... Session: {get_session()}")

            # Sleep hours check
            if is_ist_sleep():
                print("  [SLEEP] Outside IST trading hours. Waiting...")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Session filter — only London or NY
            if not is_trading_hours():
                print(f"  [SKIP] Session: {get_session()} — waiting for London/NY")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Fetch all timeframes
            print("  Fetching candles...")
            data = {}
            tf_config = [
                ("weekly", "1w", 52, 5),
                ("4h",     "4h", 200, 5),
                ("1h",     "1h", 100, 5),
                ("5m",     "5m", 50,  3),
            ]
            for key, interval, limit, lookback in tf_config:
                df = get_candles(interval, limit)
                data[key] = (df, lookback)
                time.sleep(0.3)
                print(f"    {key}: {len(df)} candles")

            ticker = get_ticker()
            price  = ticker["price"]
            print(f"  Price: {price:,.2f} | 24h: {ticker['change']:+.2f}%")

            # ── Active trade: check levels first ─────────────────────────────
            if active_trade["signal"] is not None:
                status = check_price_status(price)
                print(f"  Trade status: {status}")

                if status == "TP1_HIT":
                    active_trade["tp1_hit"] = True
                    active_trade["sl"] = active_trade["entry"]   # breakeven
                    send_telegram(fmt_update("TP1_HIT"))

                elif status in ("SL_HIT", "TP2_HIT"):
                    send_telegram(fmt_update(status))
                    reset_trade()
                    print("  Finding new signal after close...")
                    signal = analyze_with_claude(ticker, data)
                    if signal:
                        send_telegram(fmt_signal(signal))
                        set_trade(signal)
                        print(f"  [NEW] {signal['signal']} {signal.get('entry_type','')}")

                else:  # RUNNING — ask Claude
                    print("  Getting Claude trade update...")
                    claude_status = claude_trade_update(ticker, data)
                    print(f"  Claude says: {claude_status}")
                    send_telegram(fmt_update(claude_status))

                    if claude_status in ("CLOSE", "NO_VOLUME"):
                        reset_trade()
                        signal = analyze_with_claude(ticker, data)
                        if signal:
                            send_telegram(fmt_signal(signal))
                            set_trade(signal)

            # ── No active trade — find new signal ────────────────────────────
            else:
                print("  No active trade. Analyzing...")
                signal = analyze_with_claude(ticker, data)
                if signal is None:
                    print("  [ERROR] Claude returned nothing")
                else:
                    send_telegram(fmt_signal(signal))
                    set_trade(signal)
                    print(f"  [SENT] {signal['signal']} | R:R {signal.get('rr','?')} | {signal.get('confidence','?')} confidence")

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("🛑 <b>CLEXER V3 Stopped</b>\n\n<i>— CLEXER —</i>")
            break
        except Exception as e:
            print(f"  [ERROR] {e}")
            import traceback
            traceback.print_exc()

        time.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
