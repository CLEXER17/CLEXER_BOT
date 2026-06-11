"""
CLEXER Signal Bot V4.1 — Adaptive SMC + Smart API Cost Control
─────────────────────────────────────────────────────────────
SCAN LOGIC:
  Every 1 hour  → aggTrades tick-perfect TP/SL check (ZERO API cost)
  Every 4 hours → full signal scan with claude-sonnet-4-5
  SL / TP hit   → Claude API + charts for next signal
  /signal       → force full scan (15 min cooldown)
"""

import os, time, json, base64, requests, anthropic, threading
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from io import BytesIO
from datetime import datetime, timezone, timedelta

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY",   "")
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN",  "")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")
ADMIN_CHAT_ID       = os.getenv("ADMIN_CHAT_ID",       "")

SYMBOL               = "BTCUSDT"
SIGNAL_SCAN_INTERVAL = 14400
PRICE_CHECK_INTERVAL = 3600
BINANCE_BASE         = "https://api1.binance.com/api/v3"
IST                  = timedelta(hours=5, minutes=30)

def now_ist():  return datetime.now(timezone.utc) + IST
def ist_str():  return now_ist().strftime("%d %b %Y  %I:%M %p IST")
def get_session():
    mins = now_ist().hour * 60 + now_ist().minute
    if 450 <= mins < 990:         return "LONDON"
    if mins >= 1110 or mins < 60: return "NEW_YORK"
    return "ASIA"
def is_trading_hours(): return get_session() in ("LONDON", "NEW_YORK")
def is_ist_sleep():
    mins = now_ist().hour * 60 + now_ist().minute
    return 60 <= mins < 450

active_trade = {
    "signal": None, "entry": None, "sl": None,
    "tp1": None, "tp2": None, "tp1_hit": False,
    "entry_type": "MARKET", "entry_note": "",
    "entry_hit": False, "sl_wicked": False, "scan_count": 0,
}
signal_history       = []
force_scan           = threading.Event()
bot_paused           = threading.Event()
last_update_id       = 0
last_force_scan_time = 0
last_signal_scan_time = 0
last_price_check_time = 0

USER_DB_FILE = "users.json"
registered_users: set = set()

def load_users():
    global registered_users
    try:
        if os.path.exists(USER_DB_FILE):
            with open(USER_DB_FILE, "r") as f:
                registered_users = set(json.load(f))
            print(f"[USERS] Loaded {len(registered_users)} users")
    except Exception as e:
        print(f"[USERS] Load error: {e}")
        registered_users = set()

def save_users():
    try:
        with open(USER_DB_FILE, "w") as f:
            json.dump(list(registered_users), f)
    except Exception as e:
        print(f"[USERS] Save error: {e}")

def register_user(chat_id):
    if chat_id not in registered_users:
        registered_users.add(chat_id)
        save_users()

broadcast_pending: dict = {}

trade_stats = {
    "consecutive_sl": 0, "cooldown_scans": 0,
    "total_sl": 0, "total_tp1": 0, "total_tp2": 0,
    "total_signals": 0, "missed_entries": 0, "stop_hunts": 0,
}

def reset_trade():
    global active_trade
    active_trade = {
        "signal": None, "entry": None, "sl": None,
        "tp1": None, "tp2": None, "tp1_hit": False,
        "entry_type": "MARKET", "entry_note": "",
        "entry_hit": False, "sl_wicked": False, "scan_count": 0,
    }

def set_trade(s: dict):
    global active_trade
    active_trade = {
        "signal":     s["signal"],    "entry":      s["entry"],
        "sl":         s["sl"],        "tp1":        s["tp1"],
        "tp2":        s["tp2"],       "tp1_hit":    False,
        "entry_type": s.get("entry_type", "MARKET"),
        "entry_note": s.get("entry_note", ""),
        "entry_hit":  s.get("entry_type", "MARKET") == "MARKET",
        "sl_wicked":  False,          "scan_count": 0,
    }
    trade_stats["total_signals"] += 1
    signal_history.append({
        "time": ist_str(), "signal": s["signal"],
        "entry": s["entry"], "sl": s["sl"],
        "tp1": s["tp1"], "tp2": s["tp2"],
        "rr": s.get("rr", "?"), "confidence": s.get("confidence", "?"),
    })
    if len(signal_history) > 10: signal_history.pop(0)

# ─── BINANCE ──────────────────────────────────────────────────────────────────
def get_candles(interval: str, limit: int) -> pd.DataFrame:
    r = requests.get(f"{BINANCE_BASE}/klines",
                     params={"symbol": SYMBOL, "interval": interval, "limit": limit},
                     timeout=15)
    r.raise_for_status()
    rows = [{"time": datetime.fromtimestamp(c[0]/1000, tz=timezone.utc),
             "open": float(c[1]), "high": float(c[2]),
             "low":  float(c[3]), "close": float(c[4]), "vol": float(c[5])}
            for c in r.json()]
    return pd.DataFrame(rows).set_index("time")

def get_ticker() -> dict:
    r = requests.get(f"{BINANCE_BASE}/ticker/24hr",
                     params={"symbol": SYMBOL}, timeout=10)
    r.raise_for_status(); d = r.json()
    return {"price": float(d["lastPrice"]), "change": float(d["priceChangePercent"]),
            "volume": float(d["quoteVolume"]),
            "high24": float(d["highPrice"]), "low24": float(d["lowPrice"])}

def get_price_range_since(minutes: int) -> dict:
    since_ms    = int((time.time() - minutes * 60) * 1000)
    now_ms      = int(time.time() * 1000)
    all_highs   = []
    all_lows    = []
    chunk_ms    = 5 * 60 * 1000
    chunk_start = since_ms
    while chunk_start < now_ms:
        chunk_end = min(chunk_start + chunk_ms, now_ms)
        try:
            r = requests.get(f"{BINANCE_BASE}/aggTrades",
                params={"symbol": SYMBOL, "startTime": chunk_start,
                        "endTime": chunk_end, "limit": 1000}, timeout=10)
            r.raise_for_status()
            trades = r.json()
            if trades:
                prices = [float(t["p"]) for t in trades]
                all_highs.append(max(prices))
                all_lows.append(min(prices))
        except Exception as e:
            print(f"  [aggTrades] {e}")
        chunk_start = chunk_end + 1
        time.sleep(0.05)
    if not all_highs: return {"high": None, "low": None}
    return {"high": max(all_highs), "low": min(all_lows)}

# ─── SMC CALCULATIONS ─────────────────────────────────────────────────────────
def find_swing_points(df, lookback=5):
    highs, lows = [], []
    for i in range(lookback, len(df)-lookback):
        if df["high"].iloc[i] == df["high"].iloc[i-lookback:i+lookback+1].max():
            highs.append({"idx": i, "price": df["high"].iloc[i], "time": df.index[i]})
        if df["low"].iloc[i] == df["low"].iloc[i-lookback:i+lookback+1].min():
            lows.append({"idx": i, "price": df["low"].iloc[i], "time": df.index[i]})
    return highs, lows

def detect_trend(df):
    h, l = find_swing_points(df, 3)
    if len(h) < 2 or len(l) < 2: return "NEUTRAL"
    hp = [x["price"] for x in h[-2:]]; lp = [x["price"] for x in l[-2:]]
    if hp[1] > hp[0] and lp[1] > lp[0]: return "BULLISH"
    if hp[1] < hp[0] and lp[1] < lp[0]: return "BEARISH"
    return "NEUTRAL"

def detect_bos_choch(df):
    events = []
    h, l = find_swing_points(df, 3)
    if len(h) < 2 or len(l) < 2: return events
    for i in range(1, min(4, len(h))):
        idx = h[-i]["idx"]
        if idx < len(df)-1 and df["close"].iloc[idx+1] > h[-i-1]["price"]:
            events.append({"type": "BOS_BULL", "price": h[-i-1]["price"], "idx": idx}); break
    for i in range(1, min(4, len(l))):
        idx = l[-i]["idx"]
        if idx < len(df)-1 and df["close"].iloc[idx+1] < l[-i-1]["price"]:
            events.append({"type": "BOS_BEAR", "price": l[-i-1]["price"], "idx": idx}); break
    return events

def detect_order_blocks(df, n=5):
    obs = []
    c, o, h, l = df["close"].values, df["open"].values, df["high"].values, df["low"].values
    for i in range(3, len(df)-3):
        sz = h[i] - l[i]
        if sz == 0: continue
        if c[i] < o[i] and max(c[i+1:i+4]) - h[i] > sz * 0.5:
            obs.append({"type": "BULL_OB", "top": h[i], "bottom": l[i], "mid": (h[i]+l[i])/2, "idx": i})
        if c[i] > o[i] and l[i] - min(c[i+1:i+4]) > sz * 0.5:
            obs.append({"type": "BEAR_OB", "top": h[i], "bottom": l[i], "mid": (h[i]+l[i])/2, "idx": i})
    return obs[-n:] if len(obs) > n else obs

def detect_fvgs(df, n=5):
    fvgs = []
    for i in range(2, len(df)):
        if df["low"].iloc[i] > df["high"].iloc[i-2]:
            fvgs.append({"type": "BULL_FVG", "top": df["low"].iloc[i], "bottom": df["high"].iloc[i-2], "idx": i})
        if df["high"].iloc[i] < df["low"].iloc[i-2]:
            fvgs.append({"type": "BEAR_FVG", "top": df["low"].iloc[i-2], "bottom": df["high"].iloc[i], "idx": i})
    return fvgs[-n:] if len(fvgs) > n else fvgs

# ─── CHART DRAWING ────────────────────────────────────────────────────────────
def draw_smc_chart(df, tf, obs, fvgs, bos_events, s_highs, s_lows, price=None) -> str:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 10),
                                   gridspec_kw={"height_ratios": [4, 1]},
                                   facecolor="#0d0d0d")
    ax1.set_facecolor("#0d0d0d"); ax2.set_facecolor("#0d0d0d")
    n = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        o, h, l, c = row["open"], row["high"], row["low"], row["close"]
        col = "#26a69a" if c >= o else "#ef5350"
        ax1.plot([i, i], [l, h], color=col, linewidth=0.8, zorder=2)
        ax1.add_patch(plt.Rectangle((i-0.4, min(o, c)), 0.8, abs(c-o), color=col, zorder=3))
    for ob in obs:
        idx = ob["idx"]
        if idx >= n: continue
        col = "#1a6b3c" if ob["type"] == "BULL_OB" else "#6b1a1a"
        bc  = "#00e676" if ob["type"] == "BULL_OB" else "#ff5252"
        ax1.add_patch(plt.Rectangle((idx-0.5, ob["bottom"]), n-idx+2, ob["top"]-ob["bottom"],
                                    color=col, alpha=0.3, zorder=1))
        ax1.text(idx+0.5, ob["top"], "Bull OB" if ob["type"]=="BULL_OB" else "Bear OB",
                 color=bc, fontsize=6.5, va="bottom", zorder=5)
    for fvg in fvgs:
        idx = fvg["idx"]
        if idx >= n: continue
        col = "#1a3d6b" if fvg["type"] == "BULL_FVG" else "#6b4a1a"
        bc  = "#40c4ff" if fvg["type"] == "BULL_FVG" else "#ffab40"
        ax1.add_patch(plt.Rectangle((idx-2, fvg["bottom"]), n-idx+3, fvg["top"]-fvg["bottom"],
                                    color=col, alpha=0.35, zorder=1))
        ax1.text(idx+0.5, fvg["top"], "Bull FVG" if fvg["type"]=="BULL_FVG" else "Bear FVG",
                 color=bc, fontsize=6.5, va="bottom", zorder=5)
    for sh in s_highs[-6:]:
        if sh["idx"] < n:
            ax1.plot(sh["idx"], sh["price"], "^", color="#ffeb3b", markersize=5, zorder=6)
            ax1.axhline(sh["price"], color="#ffeb3b", linewidth=0.5, linestyle=":", alpha=0.4)
    for sl in s_lows[-6:]:
        if sl["idx"] < n:
            ax1.plot(sl["idx"], sl["price"], "v", color="#ff9800", markersize=5, zorder=6)
            ax1.axhline(sl["price"], color="#ff9800", linewidth=0.5, linestyle=":", alpha=0.4)
    for ev in bos_events:
        if ev["idx"] < n:
            col = "#b2ff59" if "BULL" in ev["type"] else "#ff4081"
            ax1.axhline(ev["price"], color=col, linewidth=1.0, linestyle="-.", alpha=0.7)
            ax1.text(max(0, ev["idx"]-2), ev["price"], "BOS", color=col, fontsize=7,
                     va="bottom", fontweight="bold")
    if price:
        ax1.axhline(price, color="#fff", linewidth=1.2, linestyle="--", alpha=0.9)
        ax1.text(n-1, price, f" {price:,.0f}", color="#fff", fontsize=8, va="center")
    for i, (_, row) in enumerate(df.iterrows()):
        ax2.bar(i, row["vol"],
                color="#26a69a" if row["close"] >= row["open"] else "#ef5350",
                alpha=0.7, width=0.8)
    step = max(1, n // 10)
    ax2.set_xticks(np.arange(n)[::step])
    ax2.set_xticklabels([df.index[i].strftime("%m/%d %H:%M") for i in range(0, n, step)],
                        rotation=30, fontsize=6, color="#aaa")
    ax1.set_xticks([]); ax1.set_xlim(-1, n+3); ax2.set_xlim(-1, n+3)
    for ax in (ax1, ax2):
        ax.tick_params(colors="#aaa", labelsize=7)
        for s in ax.spines.values(): s.set_color("#333")
    ax1.yaxis.tick_right()
    trend = detect_trend(df)
    col   = "#26a69a" if trend=="BULLISH" else ("#ef5350" if trend=="BEARISH" else "#fff")
    ax1.set_title(f"{SYMBOL} {tf}  |  {trend}", color=col, fontsize=11,
                  fontweight="bold", loc="left", pad=6)
    ax1.legend(handles=[
        mpatches.Patch(color="#1a6b3c", label="Bull OB"),
        mpatches.Patch(color="#6b1a1a", label="Bear OB"),
        mpatches.Patch(color="#1a3d6b", label="Bull FVG"),
        mpatches.Patch(color="#6b4a1a", label="Bear FVG"),
    ], loc="upper left", facecolor="#1a1a1a", edgecolor="#444",
       labelcolor="#ccc", fontsize=7, framealpha=0.8)
    plt.tight_layout(pad=0.5)
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=120, bbox_inches="tight", facecolor="#0d0d0d")
    plt.close(fig); buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def generate_all_charts(data, price) -> dict:
    charts = {}
    for tf_key, (df, lb) in data.items():
        obs  = detect_order_blocks(df, 6); fvgs = detect_fvgs(df, 6)
        bos  = detect_bos_choch(df);       sh, sl = find_swing_points(df, lb)
        charts[tf_key] = draw_smc_chart(df, tf_key.upper(), obs, fvgs, bos,
                                        sh[-8:], sl[-8:], price)
        print(f"    Chart {tf_key}: {len(obs)} OBs  {len(fvgs)} FVGs  {len(bos)} BOS")
    return charts

def send_charts_to_channel(charts: dict, label="📊 SMC Analysis"):
    for tf_key, b64 in charts.items():
        try:
            img_bytes = base64.b64decode(b64)
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": TELEGRAM_CHANNEL_ID,
                      "caption": f"{label} — {tf_key.upper()}"},
                files={"photo": (f"chart_{tf_key}.png", img_bytes, "image/png")},
                timeout=20,
            )
            time.sleep(0.4)
        except Exception as e:
            print(f"  [ERROR] Chart send {tf_key}: {e}")

def build_smc_summary(data, ticker) -> str:
    lines = [
        f"=== BTCUSDT LIVE ===",
        f"Price: {ticker['price']:,.2f} | 24h: {ticker['change']:+.2f}%",
        f"Vol: ${ticker['volume']/1e6:.1f}M | Session: {get_session()} | {ist_str()}", "",
    ]
    for tf_key, (df, lb) in data.items():
        trend   = detect_trend(df); obs = detect_order_blocks(df, 4); fvgs = detect_fvgs(df, 4)
        bos     = detect_bos_choch(df); sh, sl = find_swing_points(df, lb)
        lines.append(f"--- {tf_key.upper()} | {trend} ---")
        for b in bos[-2:]: lines.append(f"  {b['type']}: {b['price']:,.2f}")
        bull_ob = [o for o in obs if o["type"] == "BULL_OB"]
        bear_ob = [o for o in obs if o["type"] == "BEAR_OB"]
        if bull_ob: lines.append(f"  Bull OB: {bull_ob[-1]['bottom']:,.2f}–{bull_ob[-1]['top']:,.2f}")
        if bear_ob: lines.append(f"  Bear OB: {bear_ob[-1]['bottom']:,.2f}–{bear_ob[-1]['top']:,.2f}")
        bf  = [f for f in fvgs if f["type"] == "BULL_FVG"]
        brf = [f for f in fvgs if f["type"] == "BEAR_FVG"]
        if bf:  lines.append(f"  Bull FVG: {bf[-1]['bottom']:,.2f}–{bf[-1]['top']:,.2f}")
        if brf: lines.append(f"  Bear FVG: {brf[-1]['bottom']:,.2f}–{brf[-1]['top']:,.2f}")
        if sh: lines.append(f"  Swing High: {sh[-1]['price']:,.2f}")
        if sl: lines.append(f"  Swing Low:  {sl[-1]['price']:,.2f}")
        lines.append("")
    return "\n".join(lines)

# ─── PRICE ADVICE (zero API cost) ─────────────────────────────────────────────
def price_only_advice(price: float) -> str:
    t = active_trade
    sig   = t["signal"];  entry = t["entry"]
    sl    = t["sl"];      tp1   = t["tp1"];  tp2 = t["tp2"]
    sl_dist  = abs(entry - sl)
    tp2_dist = abs(entry - tp2)
    if sig == "BUY":
        dist_to_sl  = price - sl
        dist_to_tp1 = tp1 - price
        dist_to_tp2 = tp2 - price
        pct_to_tp2  = (price - entry) / tp2_dist * 100 if tp2_dist else 0
    else:
        dist_to_sl  = sl - price
        dist_to_tp1 = price - tp1
        dist_to_tp2 = price - tp2
        pct_to_tp2  = (entry - price) / tp2_dist * 100 if tp2_dist else 0

    if pct_to_tp2 >= 75:   advice = "HOLD 🔥"
    elif pct_to_tp2 >= 40: advice = "HOLD ✅"
    elif pct_to_tp2 >= 10: advice = "HOLD"
    else:                  advice = "WAIT"

    tp1_status = "✅ HIT" if t["tp1_hit"] else f"⏳ {abs(dist_to_tp1):.0f} pts"
    return (
        f"🕐 <b>HOURLY CHECK</b>  {ist_str()}\n\n"
        f"{'🟢' if sig=='BUY' else '🔴'} <b>{sig} {SYMBOL}</b> (price-only, no API)\n\n"
        f"💵 Price:    <b>{price:,.2f}</b>\n"
        f"🎯 Entry:    <b>{entry:,.0f}</b>\n"
        f"🛑 SL:       <b>{sl:,.0f}</b>  ({dist_to_sl:.0f} pts)\n"
        f"✅ TP1:      <b>{tp1:,.0f}</b>  {tp1_status}\n"
        f"✅ TP2:      <b>{tp2:,.0f}</b>  ({abs(dist_to_tp2):.0f} pts)\n"
        f"📈 Progress: <b>{max(0, pct_to_tp2):.1f}%</b> to TP2\n"
        f"🤖 Advice:   <b>{advice}</b>\n\n"
        f"<i>— CLEXER V4.1 (aggTrades) —</i>"
    )

# ─── STOP HUNT ────────────────────────────────────────────────────────────────
def detect_stop_hunt(df_5m) -> bool:
    t = active_trade
    if not t["signal"] or not t["entry_hit"]: return False
    sig = t["signal"]; sl = t["sl"]
    for i in range(-3, 0):
        row = df_5m.iloc[i]
        if sig == "BUY"  and row["low"] < sl  and row["close"] > sl and row["close"]-row["low"] > 100: return True
        if sig == "SELL" and row["high"] > sl  and row["close"] < sl and row["high"]-row["close"] > 100: return True
    return False

def detect_entry_missed(price: float) -> bool:
    t = active_trade
    if t["entry_hit"] or t["entry_type"] != "PULLBACK": return False
    sig = t["signal"]; tp2 = t["tp2"]
    if sig == "BUY"  and price >= tp2: return True
    if sig == "SELL" and price <= tp2: return True
    return False

def detect_entry_invalidated(price: float, df_4h) -> bool:
    t = active_trade
    if t["entry_hit"]: return False
    sig = t["signal"]; sl = t["sl"]
    last_close = df_4h["close"].iloc[-1]
    if sig == "BUY"  and last_close < sl: return True
    if sig == "SELL" and last_close > sl: return True
    return False

# ─── CLAUDE ANALYSIS ──────────────────────────────────────────────────────────
def fetch_all_data():
    data = {}
    for key, iv, lim, lb in [("weekly","1w",52,5),("4h","4h",200,5),("1h","1h",100,5),("5m","5m",50,3)]:
        data[key] = (get_candles(iv, lim), lb)
        time.sleep(0.3)
        print(f"    {key}: {len(data[key][0])} candles")
    return data

def analyze_with_claude(ticker, data) -> dict | None:
    client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    price   = ticker["price"]
    session = get_session()
    print(f"  [CLAUDE] Generating charts... model: claude-sonnet-4-5")

    charts  = generate_all_charts(data, price)
    summary = build_smc_summary(data, ticker)
    send_charts_to_channel(charts, "📊 SMC Analysis")

    prompt = f"""{summary}

You are CLEXER — an elite Bitcoin SMC trader analysing 4 charts (Weekly, 4H, 1H, 5M).

════════════════════════════════════════
 ANALYSIS FRAMEWORK
════════════════════════════════════════

STEP 1 — WEEKLY BIAS
Determine overall trend: HH+HL = bullish | LH+LL = bearish | flat = neutral
Note the last weekly swing high and low.

STEP 2 — 4H PRIMARY BIAS
Identify 4H structure: BOS direction, CHoCH if any.
Find the most recent 4H Order Block (last opposing candle before strong move).
Is 4H bullish (HH+HL) or bearish (LH+LL)?

STEP 3 — 1H CONFIRMATION
Does 1H align with 4H? Find the nearest 1H OB or FVG to current price.
This is the entry zone.

STEP 4 — 5M ENTRY TIMING
Is 5M showing momentum in the trade direction?
Higher lows = bullish momentum. Lower highs = bearish momentum.

════════════════════════════════════════
 SIGNAL RULES — YOU MUST ALWAYS GIVE A SIGNAL
════════════════════════════════════════

⚠️ YOU MUST ALWAYS RETURN BUY OR SELL — NEVER SKIP.
Even in ranging markets, give the best directional bias with a PULLBACK entry.
If price is not at a zone right now → set entry_type = PULLBACK.

ENTRY — must be at an OB or FVG zone:
  • BUY:  enter at nearest Bull OB or Bull FVG (1H or 4H)
  • SELL: enter at nearest Bear OB or Bear FVG (1H or 4H)
  • If price IS at the zone now → entry_type = MARKET
  • If price needs to travel to zone → entry_type = PULLBACK

STOP LOSS — just outside the entry zone:
  • BUY at Bull OB/FVG → SL = 20–50 pts below zone bottom
  • SELL at Bear OB/FVG → SL = 20–50 pts above zone top
  • SL should be 100–600 pts from entry naturally

TAKE PROFIT:
  sl_dist = abs(entry - sl)
  • TP1 = entry ± (sl_dist × 2)   minimum 1:2 R:R
  • TP2 = entry ± (sl_dist × 4)   minimum 1:4 R:R

PULLBACK RULE (CRITICAL):
  Current price = {price:,.0f}
  • BUY  pullback → entry MUST be below {price:,.0f}, TP1 and TP2 MUST be above {price:,.0f}
  • SELL pullback → entry MUST be above {price:,.0f}, TP1 and TP2 MUST be below {price:,.0f}

Session: {session}

════════════════════════════════════════

Return ONLY valid JSON, no markdown:
{{
  "signal":       "BUY" or "SELL",
  "entry":        <price at OB/FVG midpoint>,
  "sl":           <price just outside zone>,
  "tp1":          <entry ± sl_dist×2>,
  "tp2":          <entry ± sl_dist×4>,
  "rr":           "1:X.X",
  "entry_type":   "MARKET" or "PULLBACK",
  "entry_note":   "zone e.g. 62100-62400 Bear OB 1H",
  "bias":         "BULLISH" or "BEARISH",
  "weekly_trend": "one line weekly summary",
  "structure_4h": "BOS/CHoCH + HH/HL or LH/LL",
  "entry_zone":   "zone description",
  "confidence":   "HIGH" or "MEDIUM" or "LOW",
  "session":      "{session}",
  "reasoning":    "2-3 sentences explaining the confluence"
}}"""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1000,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["weekly"]}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["4h"]}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["1h"]}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["5m"]}},
                {"type": "text",  "text": prompt},
            ]}]
        )
        raw    = msg.content[0].text.strip().replace("```json","").replace("```","").strip()
        signal = json.loads(raw)

        # Reject WAIT if Claude still tries it
        if signal.get("signal") in ("WAIT", "NONE", "HOLD", None):
            print(f"  [RETRY] Claude returned no signal — forcing directional retry...")
            return analyze_with_claude_retry(ticker, data, charts, summary)

        entry   = float(signal["entry"])
        sl_val  = float(signal["sl"])
        sl_dist = abs(entry - sl_val)

        # SL sanity: too tight (< 50 pts) or too wide (> 3000 pts)
        if sl_dist < 50:
            print(f"  [REJECT] SL {sl_dist:.0f} pts too tight — bad zone")
            return None
        if sl_dist > 3000:
            print(f"  [REJECT] SL {sl_dist:.0f} pts too wide")
            return None

        # Fix PULLBACK TPs if Claude put them wrong side
        etype = signal.get("entry_type", "MARKET")
        sig   = signal["signal"]
        tp1   = float(signal["tp1"])
        tp2   = float(signal["tp2"])
        if etype == "PULLBACK":
            if sig == "BUY" and tp1 <= price:
                signal["tp1"] = round(price + sl_dist * 2, -1)
                signal["tp2"] = round(price + sl_dist * 4, -1)
                print(f"  [FIX] BUY PULLBACK TPs corrected above price")
            elif sig == "SELL" and tp1 >= price:
                signal["tp1"] = round(price - sl_dist * 2, -1)
                signal["tp2"] = round(price - sl_dist * 4, -1)
                print(f"  [FIX] SELL PULLBACK TPs corrected below price")

        tp2_dist     = abs(entry - float(signal["tp2"]))
        rr           = tp2_dist / sl_dist if sl_dist > 0 else 0
        signal["rr"] = f"1:{rr:.1f}"

        print(f"  [OK] {signal['signal']} entry:{entry:,.0f} SL:{sl_val:,.0f} "
              f"({sl_dist:.0f}pts) R:R:{signal['rr']} Conf:{signal.get('confidence','?')}")
        return signal

    except Exception as e:
        print(f"  [ERROR] Claude: {e}")
        return None


def analyze_with_claude_retry(ticker, data, charts, summary) -> dict | None:
    """Single retry with even simpler prompt — forces a directional call"""
    price   = ticker["price"]
    session = get_session()
    prompt  = f"""{summary}

Current price: {price:,.0f} | Session: {session}

Look at the 4 charts. Give me the BEST trade setup available right now.
If price is at a zone → MARKET entry.
If price is not at a zone → PULLBACK entry to the nearest OB or FVG.
You MUST pick BUY or SELL. No exceptions.

SL: just outside the entry zone (20-100 pts beyond zone edge).
TP1: sl_distance × 2 away from entry.
TP2: sl_distance × 4 away from entry.

JSON only:
{{"signal":"BUY","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"1:4","entry_type":"PULLBACK","entry_note":"","bias":"BULLISH","weekly_trend":"","structure_4h":"","entry_zone":"","confidence":"MEDIUM","session":"{session}","reasoning":""}}"""

    try:
        msg = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
            model="claude-sonnet-4-5", max_tokens=600,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["1h"]}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["4h"]}},
                {"type": "text",  "text": prompt},
            ]}]
        )
        raw    = msg.content[0].text.strip().replace("```json","").replace("```","").strip()
        signal = json.loads(raw)
        if signal.get("signal") in ("BUY", "SELL"):
            print(f"  [RETRY OK] {signal['signal']} @ {signal['entry']:,.0f}")
            return signal
    except Exception as e:
        print(f"  [RETRY ERROR] {e}")
    return None

# ─── PRICE STATUS ─────────────────────────────────────────────────────────────
def check_price_status(price: float, high_1h: float, low_1h: float, df_5m=None) -> str:
    t = active_trade
    if not t["signal"]: return "NONE"
    sig, sl, tp1, tp2, entry = t["signal"], t["sl"], t["tp1"], t["tp2"], t["entry"]

    if not t["entry_hit"]:
        # Price passed TP2 without ever reaching entry → signal expired
        if (sig == "BUY"  and high_1h >= tp2): return "ENTRY_MISSED"
        if (sig == "SELL" and low_1h  <= tp2): return "ENTRY_MISSED"
        # SL hit before entry
        if (sig == "BUY"  and low_1h  <= sl): return "SETUP_INVALID"
        if (sig == "SELL" and high_1h >= sl): return "SETUP_INVALID"
        # Entry zone reached
        tol = abs(entry - sl) * 0.3
        if (sig == "BUY"  and price <= entry + tol) or \
           (sig == "SELL" and price >= entry - tol):
            active_trade["entry_hit"] = True
            print(f"  [ENTRY HIT] {sig} {entry:,.0f}")
        else:
            return "WAITING_ENTRY"

    # Stop hunt detection
    if df_5m is not None and not t["sl_wicked"]:
        if detect_stop_hunt(df_5m):
            active_trade["sl_wicked"] = True
            trade_stats["stop_hunts"] += 1
            return "STOP_HUNT"

    if (sig == "SELL" and high_1h >= sl) or (sig == "BUY"  and low_1h <= sl):  return "SL_HIT"
    if (sig == "SELL" and low_1h  <= tp2) or (sig == "BUY"  and high_1h >= tp2): return "TP2_HIT"
    if not t["tp1_hit"]:
        if (sig == "SELL" and low_1h  <= tp1) or (sig == "BUY" and high_1h >= tp1): return "TP1_HIT"
    return "RUNNING"

# ─── 1H PRICE CHECK ───────────────────────────────────────────────────────────
def run_price_check() -> bool:
    global last_price_check_time
    if not active_trade["signal"]: return False
    try:
        ticker   = get_ticker(); price = ticker["price"]
        range_1h = get_price_range_since(60)
        high_1h  = range_1h["high"] or price
        low_1h   = range_1h["low"]  or price
        print(f"  [1H] cur:{price:,.2f} H:{high_1h:,.2f} L:{low_1h:,.2f}")
        df_5m = get_candles("5m", 50)
        df_4h = get_candles("4h", 10)

        if detect_entry_missed(price):
            trade_stats["missed_entries"] += 1
            send_telegram(fmt_update("ENTRY_MISSED"))
            reset_trade(); return True

        if not active_trade["entry_hit"] and detect_entry_invalidated(price, df_4h):
            send_telegram(fmt_update("SETUP_INVALID"))
            reset_trade(); return True

        status = check_price_status(price, high_1h, low_1h, df_5m)
        print(f"  [1H] {active_trade['signal']} | {status}")

        if status == "TP2_HIT":
            trade_stats["total_tp2"] += 1
            trade_stats["consecutive_sl"] = 0
            send_telegram(fmt_update("TP2_HIT"))
            reset_trade(); return True

        elif status == "SL_HIT":
            trade_stats["total_sl"]       += 1
            trade_stats["consecutive_sl"] += 1
            n = trade_stats["consecutive_sl"]
            if n >= 3:
                trade_stats["cooldown_scans"] = 2
                send_telegram(f"🛑 <b>SL HIT</b> ({n} in a row)\n⚠️ Cooling down 2 scans.\n\n<i>— CLEXER V4.1 —</i>")
            elif n == 2:
                trade_stats["cooldown_scans"] = 1
                send_telegram(f"🛑 <b>SL HIT</b> ({n} in a row)\n⏸ Cooling down 1 scan.\n\n<i>— CLEXER V4.1 —</i>")
            else:
                send_telegram(fmt_update("SL_HIT"))
            reset_trade(); return True

        elif status == "TP1_HIT":
            active_trade["tp1_hit"] = True
            active_trade["sl"]      = active_trade["entry"]
            trade_stats["total_tp1"]      += 1
            trade_stats["consecutive_sl"]  = 0
            send_telegram(fmt_update("TP1_HIT"))

        elif status == "STOP_HUNT":
            send_telegram(fmt_update("STOP_HUNT"))

        elif status in ("ENTRY_MISSED", "SETUP_INVALID"):
            send_telegram(fmt_update(status))
            reset_trade(); return True

        elif status == "WAITING_ENTRY":
            active_trade["scan_count"] += 1
            send_telegram(fmt_update("WAITING_ENTRY", price))

        elif status == "RUNNING":
            active_trade["scan_count"] += 1
            send_telegram(price_only_advice(price))

    except Exception as e:
        print(f"  [1H ERROR] {e}")
    return False

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
def send_telegram(text: str) -> bool:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHANNEL_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10)
        r.raise_for_status(); return True
    except Exception as e:
        print(f"  [TG ERROR] {e}"); return False

def send_reply(chat_id, text: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10)
    except Exception as e:
        print(f"  [REPLY ERROR] {e}")

def send_to_user(chat_id, text: str, file_id=None, file_type=None) -> bool:
    try:
        base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
        if file_type == "photo":
            r = requests.post(f"{base}/sendPhoto",
                json={"chat_id": chat_id, "photo": file_id,
                      "caption": text, "parse_mode": "HTML"}, timeout=15)
        elif file_type == "document":
            r = requests.post(f"{base}/sendDocument",
                json={"chat_id": chat_id, "document": file_id,
                      "caption": text, "parse_mode": "HTML"}, timeout=15)
        else:
            r = requests.post(f"{base}/sendMessage",
                json={"chat_id": chat_id, "text": text,
                      "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"  [USER SEND] {chat_id}: {e}"); return False

def do_broadcast(admin_chat_id, text: str, file_id=None, file_type=None):
    targets = list(registered_users) + [TELEGRAM_CHANNEL_ID]
    ok = 0; fail = 0
    for cid in targets:
        if send_to_user(cid, text, file_id, file_type): ok += 1
        else: fail += 1
        time.sleep(0.05)
    send_reply(admin_chat_id,
        f"📢 <b>Broadcast Done</b>\n✅ {ok} delivered | ❌ {fail} failed\n\n<i>— CLEXER V4.1 —</i>")

# ─── MESSAGE FORMATS ──────────────────────────────────────────────────────────
def fmt_signal(s: dict) -> str:
    e   = "🟢" if s["signal"] == "BUY" else "🔴"
    ci  = {"HIGH": "🔥", "MEDIUM": "✨", "LOW": "⚡"}.get(s.get("confidence", ""), "")
    el  = f"🎯 Entry    <b>{s['entry']:,.0f}</b>"
    if s.get("entry_type") == "PULLBACK" and s.get("entry_note"):
        el += f"\n   ⏳ <i>{s['entry_note']}</i>"
    wk  = s.get("weekly_trend", "")
    s4h = s.get("structure_4h", "")
    ez  = s.get("entry_zone", "")
    rs  = s.get("reasoning", "")
    return (
        f"{e} <b>{s['signal']} — {SYMBOL}</b>  {ci}\n"
        f"🕐 {ist_str()}  |  📍 {s.get('session', get_session())}\n\n"
        f"{el}\n"
        f"🛑 SL       <b>{s['sl']:,.0f}</b>\n"
        f"✅ TP1     <b>{s['tp1']:,.0f}</b>\n"
        f"✅ TP2     <b>{s['tp2']:,.0f}</b>\n"
        f"📊 R:R     <b>{s.get('rr', '—')}</b>\n\n"
        + (f"🗓 Weekly: <i>{wk}</i>\n" if wk else "")
        + (f"🔷 4H:     <i>{s4h}</i>\n" if s4h else "")
        + (f"📍 Zone:   <i>{ez}</i>\n" if ez else "")
        + (f"\n💭 <i>{rs}</i>\n" if rs else "")
        + f"\n<i>— Signal by CLEXER V4.1 —</i>\n"
          f"⚠️ <i>Not financial advice</i>"
    )

def fmt_update(status: str, price: float = None) -> str:
    t     = active_trade
    entry = t.get("entry") or 0
    msgs  = {
        "SL_HIT":        "🛑 <b>SL HIT</b> — Finding next trade...",
        "TP1_HIT":       f"✅ <b>TP1 HIT</b>\nSL → Breakeven ({entry:,.0f})\nWaiting TP2 → <b>{t.get('tp2',0):,.0f}</b>",
        "TP2_HIT":       "🏆 <b>TP2 HIT — Trade Complete!</b>",
        "STOP_HUNT":     f"⚡ <b>STOP HUNT</b> — SL wicked, closed above. Holding.",
        "SETUP_INVALID": "❌ <b>Setup Invalid</b> — SL hit before entry. Finding new trade.",
        "ENTRY_MISSED":  f"🚀 <b>Entry Missed</b> — Price flew past TP2 without touching entry {entry:,.0f}. Resetting.",
        "WAITING_ENTRY": (
            f"⏳ <b>Waiting Pullback</b>\n"
            f"Entry zone: <b>{entry:,.0f}</b>\n"
            + (f"Price: <b>{price:,.0f}</b> | Distance: <b>{abs((price or 0)-entry):,.0f} pts</b>" if price else "")
        ),
        "HOLD":      "📊 <b>HOLD</b> — Structure intact",
        "WAIT":      "⏳ <b>WAIT</b> — Trade running",
        "CLOSE":     "⚠️ <b>CLOSE NOW</b> — Structure broken",
        "NO_VOLUME": "📉 <b>CLOSE</b> — No volume",
        "COOLDOWN":  f"⏸ <b>Cooldown</b> — {trade_stats['cooldown_scans']} scan(s) left",
    }
    body = msgs.get(status, "⏳ Trade running")
    return f"📡 <b>{SYMBOL} UPDATE</b>  {ist_str()}\n\n{body}\n\n<i>— CLEXER V4.1 —</i>"

# ─── COMMANDS ─────────────────────────────────────────────────────────────────
COMMANDS_HELP = """🤖 <b>CLEXER V4.1 Commands</b>
━━━━━━━━━━━━━━━━━━━

📊 INFO
/status — Bot status
/price — Live BTC price
/trade — Active trade
/history — Last 5 signals
/stats — Win/loss stats
/session — Session info

🎯 TRADE CONTROL
/close — Close trade
/sltobe — SL to breakeven
/setsl 61500 — Set SL
/settp1 60000 — Set TP1
/settp2 58000 — Set TP2

🤖 BOT CONTROL
/signal — Force scan now
/pause — Pause bot
/resume — Resume bot
/resetsl — Reset SL counter
/setinterval 4 — Set interval (hours)
/users — User count

📢 /broadcast — Send to all users

/help — This menu"""

def handle_command(text: str, chat_id, message: dict = None):
    global SIGNAL_SCAN_INTERVAL, last_force_scan_time, broadcast_pending
    register_user(chat_id)
    parts    = text.strip().split()
    cmd      = parts[0].lower().split("@")[0]
    is_admin = (not ADMIN_CHAT_ID) or (str(chat_id) == str(ADMIN_CHAT_ID))

    admin_cmds = {"/signal","/pause","/resume","/resetsl","/setinterval",
                  "/close","/sltobe","/setsl","/settp1","/settp2","/broadcast","/users"}
    if cmd in admin_cmds and not is_admin:
        send_reply(chat_id, "⛔ Admin only."); return

    if cmd in ("/start", "/help"):
        send_reply(chat_id, COMMANDS_HELP)

    elif cmd == "/status":
        t  = active_trade
        st = "⏸ PAUSED" if bot_paused.is_set() else "▶️ RUNNING"
        cd = f"Cooldown: {trade_stats['cooldown_scans']} scans\n" if trade_stats["cooldown_scans"] else ""
        ti = (f"{t['signal']} @ {t['entry']:,.0f}\n"
              f"SL:{t['sl']:,.0f} TP1:{t['tp1']:,.0f} TP2:{t['tp2']:,.0f}\n"
              f"Entry:{'✅' if t['entry_hit'] else '⏳'} TP1:{'✅' if t['tp1_hit'] else '❌'}"
              ) if t["signal"] else "No active trade"
        send_reply(chat_id,
            f"📊 <b>CLEXER V4.1</b>\n\nBot: {st}\n{cd}"
            f"Session: {get_session()} {'✅' if is_trading_hours() else '⏸'}\n"
            f"IST: {ist_str()}\nInterval: {SIGNAL_SCAN_INTERVAL//3600}h\n"
            f"Users: {len(registered_users)}\n\n<b>Trade:</b>\n{ti}"
        )

    elif cmd == "/price":
        try:
            tk = get_ticker()
            send_reply(chat_id, f"💵 <b>BTCUSDT</b>\n\nPrice: <b>{tk['price']:,.2f}</b>\n"
                f"24h: {tk['change']:+.2f}% | Vol: ${tk['volume']/1e6:.1f}M\n🕐 {ist_str()}")
        except Exception as e: send_reply(chat_id, f"❌ {e}")

    elif cmd == "/trade":
        t = active_trade
        if not t["signal"]: send_reply(chat_id, "📭 No active trade.")
        else:
            try: tk = get_ticker(); pl = f"Current: <b>{tk['price']:,.2f}</b>\n"
            except: pl = ""
            send_reply(chat_id,
                f"📈 <b>Active Trade</b>\n\n{t['signal']} — {SYMBOL}\n{pl}"
                f"Entry: <b>{t['entry']:,.0f}</b> {'✅' if t['entry_hit'] else '⏳'}\n"
                f"SL: <b>{t['sl']:,.0f}</b> | TP1: <b>{t['tp1']:,.0f}</b> {'✅' if t['tp1_hit'] else '⏳'}\n"
                f"TP2: <b>{t['tp2']:,.0f}</b>\nType: {t['entry_type']}\n"
                + (f"<i>{t['entry_note']}</i>" if t.get("entry_note") else ""))

    elif cmd == "/history":
        if not signal_history: send_reply(chat_id, "📭 No history.")
        else:
            lines = ["📜 <b>Last Signals</b>\n"]
            for s in reversed(signal_history[-5:]):
                e = "🟢" if s["signal"]=="BUY" else "🔴"
                lines.append(f"{e} {s['signal']} @ {s['entry']:,.0f} R:R:{s.get('rr','?')} {s.get('confidence','?')}\n"
                             f"   SL:{s['sl']:,.0f} TP1:{s['tp1']:,.0f} TP2:{s['tp2']:,.0f}\n   🕐 {s['time']}\n")
            send_reply(chat_id, "\n".join(lines))

    elif cmd == "/stats":
        ts = trade_stats
        send_reply(chat_id,
            f"📈 <b>Stats</b>\n\nSignals: {ts['total_signals']}\n"
            f"TP1: {ts['total_tp1']} ✅ | TP2: {ts['total_tp2']} 🏆\n"
            f"SL: {ts['total_sl']} 🛑 | Stop hunts: {ts['stop_hunts']} ⚡\n"
            f"Missed entries: {ts['missed_entries']}\nConsec SL: {ts['consecutive_sl']}")

    elif cmd == "/session":
        s = get_session()
        send_reply(chat_id,
            f"📍 <b>Session</b>\n\n{s} {'✅' if is_trading_hours() else '⏸'}\n\n"
            f"🇬🇧 London:   07:30–16:30 IST\n🇺🇸 NY:       18:30–01:00 IST\n"
            f"😴 Sleep:    01:00–07:29 IST\n\n🕐 {ist_str()}")

    elif cmd == "/users":
        send_reply(chat_id, f"👥 <b>Users</b>\n\nTotal: <b>{len(registered_users)}</b>\nChannel: {TELEGRAM_CHANNEL_ID}")

    elif cmd == "/close":
        t = active_trade
        if not t["signal"]: send_reply(chat_id, "📭 No trade.")
        else:
            info = f"{t['signal']} @ {t['entry']:,.0f}"; reset_trade()
            send_telegram(f"⛔ <b>Trade Closed</b>\n{info}\n\n<i>— CLEXER V4.1 —</i>")
            send_reply(chat_id, f"✅ Closed: {info}"); force_scan.set()

    elif cmd == "/sltobe":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No trade.")
        else:
            old = active_trade["sl"]; active_trade["sl"] = active_trade["entry"]
            send_telegram(f"🔄 <b>SL → BE</b> {old:,.0f}→<b>{active_trade['entry']:,.0f}</b>\n\n<i>— CLEXER V4.1 —</i>")
            send_reply(chat_id, f"✅ SL → {active_trade['entry']:,.0f}")

    elif cmd == "/setsl":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No trade.")
        elif len(parts) < 2: send_reply(chat_id, "Usage: /setsl 61500")
        else:
            try:
                v = float(parts[1].replace(",","")); old = active_trade["sl"]; active_trade["sl"] = v
                send_telegram(f"🔄 <b>SL</b> {old:,.0f}→<b>{v:,.0f}</b>\n\n<i>— CLEXER V4.1 —</i>")
                send_reply(chat_id, f"✅ SL={v:,.0f}")
            except: send_reply(chat_id, "❌ /setsl 61500")

    elif cmd == "/settp1":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No trade.")
        elif len(parts) < 2: send_reply(chat_id, "Usage: /settp1 60000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp1"] = v
                send_telegram(f"🔄 <b>TP1→{v:,.0f}</b>\n\n<i>— CLEXER V4.1 —</i>")
                send_reply(chat_id, f"✅ TP1={v:,.0f}")
            except: send_reply(chat_id, "❌ /settp1 60000")

    elif cmd == "/settp2":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No trade.")
        elif len(parts) < 2: send_reply(chat_id, "Usage: /settp2 58000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp2"] = v
                send_telegram(f"🔄 <b>TP2→{v:,.0f}</b>\n\n<i>— CLEXER V4.1 —</i>")
                send_reply(chat_id, f"✅ TP2={v:,.0f}")
            except: send_reply(chat_id, "❌ /settp2 58000")

    elif cmd == "/signal":
        if bot_paused.is_set(): send_reply(chat_id, "⏸ Bot paused. /resume first.")
        else:
            now = time.time(); elapsed = now - last_force_scan_time
            if elapsed < 900 and last_force_scan_time > 0:
                send_reply(chat_id, f"⏳ Cooldown: {int((900-elapsed)/60)} min left")
            else:
                last_force_scan_time = now
                send_reply(chat_id, "🔍 Forcing scan — charts + signal incoming (~30s)")
                force_scan.set()

    elif cmd == "/pause":
        bot_paused.set()
        send_telegram("⏸ <b>Bot Paused</b>\n\n<i>— CLEXER V4.1 —</i>")
        send_reply(chat_id, "✅ Paused.")

    elif cmd == "/resume":
        bot_paused.clear()
        send_telegram("▶️ <b>Bot Resumed</b>\n\n<i>— CLEXER V4.1 —</i>")
        send_reply(chat_id, "✅ Resumed.")

    elif cmd == "/resetsl":
        trade_stats["consecutive_sl"] = 0; trade_stats["cooldown_scans"] = 0
        send_reply(chat_id, "✅ SL counter reset.")

    elif cmd == "/setinterval":
        if len(parts) < 2: send_reply(chat_id, f"Current: {SIGNAL_SCAN_INTERVAL//3600}h\nUsage: /setinterval 4")
        else:
            try:
                h = float(parts[1])
                if h < 1 or h > 24: send_reply(chat_id, "❌ 1–24 only.")
                else:
                    SIGNAL_SCAN_INTERVAL = int(h * 3600)
                    send_reply(chat_id, f"✅ Interval → {h}h")
            except: send_reply(chat_id, "❌ /setinterval 4")

    elif cmd == "/broadcast":
        broadcast_pending[chat_id] = {"step": "waiting_message"}
        send_reply(chat_id, "📢 <b>Broadcast</b>\n\nSend your message now (text + optional image/PDF).\n\n/cancel to abort.")

    elif cmd == "/cancel":
        if chat_id in broadcast_pending: del broadcast_pending[chat_id]; send_reply(chat_id, "❌ Cancelled.")
        else: send_reply(chat_id, "Nothing to cancel.")

    else:
        send_reply(chat_id, f"❓ Unknown: {cmd}\n/help")

def handle_broadcast_message(chat_id, message: dict):
    text    = message.get("text") or message.get("caption") or ""
    photo   = message.get("photo"); doc = message.get("document")
    file_id = None; file_type = None
    if photo:  file_id = photo[-1]["file_id"]; file_type = "photo"
    elif doc:  file_id = doc["file_id"];       file_type = "document"
    if not text and not file_id:
        send_reply(chat_id, "❌ Empty. Send text/image/PDF or /cancel."); return
    del broadcast_pending[chat_id]
    send_reply(chat_id, f"📢 Broadcasting to {len(registered_users)+1} targets...")
    threading.Thread(target=do_broadcast, args=(chat_id, text, file_id, file_type), daemon=True).start()

# ─── COMMAND LISTENER ─────────────────────────────────────────────────────────
def command_listener():
    global last_update_id
    print("[CMD] Listener started")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook", timeout=10)
        print("[CMD] Webhook cleared")
    except: pass
    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"offset": last_update_id+1, "timeout": 20, "allowed_updates": ["message"]},
                timeout=25)
            data = r.json()
            if not data.get("ok"): time.sleep(5); continue
            for upd in data.get("result", []):
                last_update_id = upd["update_id"]
                msg   = upd.get("message", {}); text = msg.get("text","") or ""
                cid   = msg.get("chat",{}).get("id"); uname = msg.get("from",{}).get("username","?")
                if not cid: continue
                print(f"  [CMD] @{uname} ID:{cid}: {text[:40]}")
                register_user(cid)
                if cid in broadcast_pending and not text.startswith("/"):
                    handle_broadcast_message(cid, msg); continue
                if text.startswith("/"): handle_command(text, cid, msg)
        except Exception as e: print(f"  [CMD] {e}")
        time.sleep(2)

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    global last_signal_scan_time, last_price_check_time
    print(f"[CLEXER V4.1] Starting | {SYMBOL} | claude-sonnet-4-5")
    print(f"[CLEXER V4.1] Sessions: London 07:30–16:30 IST | NY 18:30–01:00 IST")
    load_users()
    threading.Thread(target=command_listener, daemon=True).start()

    send_telegram(
        f"🤖 <b>CLEXER V4.1 Online</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📊 Weekly + 4H + 1H + 5M Vision\n"
        f"🎯 aggTrades tick-perfect TP/SL\n"
        f"🔄 ALWAYS gives BUY or SELL signal\n"
        f"🛡 Stop Hunt + Missed Entry detection\n"
        f"🕐 Price check every 1h (zero API cost)\n"
        f"🔍 Full scan every 4h\n"
        f"💬 /help for commands\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<i>— CLEXER V4.1 —</i>"
    )

    TICK = 60

    while True:
        try:
            if bot_paused.is_set(): time.sleep(TICK); continue

            now    = time.time()
            forced = force_scan.is_set()
            if forced: force_scan.clear()

            print(f"\n[{now_ist().strftime('%H:%M IST')}] {get_session()}{' FORCED' if forced else ''}")

            if not forced and is_ist_sleep():
                time.sleep(TICK); continue

            # ── 1H PRICE CHECK ────────────────────────────────────────────────
            price_check_due = (now - last_price_check_time) >= PRICE_CHECK_INTERVAL
            if (price_check_due or forced) and active_trade["signal"]:
                last_price_check_time = now
                need_full_scan = run_price_check()
                if need_full_scan:
                    forced = True; last_signal_scan_time = 0
                elif not forced:
                    time.sleep(TICK); continue

            # ── SIGNAL SCAN DUE? ──────────────────────────────────────────────
            signal_scan_due = (now - last_signal_scan_time) >= SIGNAL_SCAN_INTERVAL
            if not forced and not signal_scan_due:
                time.sleep(TICK); continue

            if not forced and not is_trading_hours():
                print(f"  [WAIT] {get_session()} not London/NY")
                time.sleep(TICK); continue

            # ── COOLDOWN ──────────────────────────────────────────────────────
            if trade_stats["cooldown_scans"] > 0 and not forced:
                trade_stats["cooldown_scans"] -= 1
                remaining = trade_stats["cooldown_scans"]
                print(f"  [COOLDOWN] {remaining} scans left")
                if remaining == 0:
                    send_telegram("🔍 <b>Cooldown over — scanning now</b>\n\n<i>— CLEXER V4.1 —</i>")
                last_signal_scan_time = now
                time.sleep(TICK); continue

            if active_trade["signal"] and not forced:
                print("  [SKIP] Trade active — price check handles it")
                last_signal_scan_time = now
                time.sleep(TICK); continue

            # ── FULL CLAUDE SCAN ──────────────────────────────────────────────
            last_signal_scan_time = now
            print("  Fetching all data for Claude scan...")
            ticker = get_ticker(); price = ticker["price"]
            print(f"  Price: {price:,.2f} | {ticker['change']:+.2f}%")
            data   = fetch_all_data()

            if not active_trade["signal"]:
                signal = analyze_with_claude(ticker, data)
                if signal:
                    send_telegram(fmt_signal(signal))
                    set_trade(signal)
                    print(f"  [SENT] {signal['signal']} R:R:{signal.get('rr','?')} Conf:{signal.get('confidence','?')}")
                else:
                    print("  No signal returned")
            else:
                # Forced with active trade — analysis update only
                signal = analyze_with_claude(ticker, data)
                if signal:
                    send_telegram(
                        f"📊 <b>ANALYSIS UPDATE</b> (trade active)\n\n"
                        f"Running: {active_trade['signal']} @ {active_trade['entry']:,.0f}\n\n"
                        + fmt_signal(signal)
                    )

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("🛑 <b>CLEXER V4.1 Stopped</b>\n\n<i>— CLEXER —</i>")
            break
        except Exception as e:
            print(f"  [ERROR] {e}")
            import traceback; traceback.print_exc()

        time.sleep(TICK)

if __name__ == "__main__":
    main()
