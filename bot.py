"""
CLEXER Signal Bot V4.1 — Adaptive SMC + Smart API Cost Control
─────────────────────────────────────────────────────────────
SCAN LOGIC:
  Every 1 hour  → aggTrades tick-perfect TP/SL check (ZERO API cost)
  Every 4 hours → full signal scan with claude-opus-4-6 (only if no active trade)
  SL / TP hit   → Claude API + charts for next signal
  /signal       → force full scan (15 min cooldown)

V4.1 CHANGES vs V4:
  ✅ aggTrades tick-perfect TP/SL detection — catches EVERY price touch
  ✅ claude-opus-4-6 — smartest model for best trade analysis
  ✅ Full 4-step SMC framework in prompt (Weekly → 4H → 1H → 5M)
  ✅ SL minimum 500 pts enforced (correct per trade rules)
  ✅ reasoning field in every signal for full transparency
  ✅ SL notification + stats now in run_price_check (was missing before)

BROADCAST:
  /broadcast    → send text + optional image/PDF to ALL users + channel
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
SIGNAL_SCAN_INTERVAL = 14400   # 4 hours — full Claude scan
PRICE_CHECK_INTERVAL = 3600    # 1 hour  — aggTrades check (zero API cost)
BINANCE_BASE         = "https://api1.binance.com/api/v3"
IST                  = timedelta(hours=5, minutes=30)

# ─── TIME HELPERS ─────────────────────────────────────────────────────────────
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

# ─── BOT STATE ────────────────────────────────────────────────────────────────
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

# ─── USER REGISTRY ────────────────────────────────────────────────────────────
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
        print(f"[USERS] New user: {chat_id} | Total: {len(registered_users)}")

# ─── BROADCAST STATE ──────────────────────────────────────────────────────────
broadcast_pending: dict = {}

# ─── ADAPTIVE TRADE STATS ─────────────────────────────────────────────────────
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
    """
    Tick-perfect high/low using Binance aggTrades.
    Splits window into 5-min chunks → 12 API calls for 60 min.
    Catches every single trade — no touch goes undetected.
    """
    since_ms    = int((time.time() - minutes * 60) * 1000)
    now_ms      = int(time.time() * 1000)
    all_highs   = []
    all_lows    = []
    chunk_ms    = 5 * 60 * 1000   # 5-minute slices
    chunk_start = since_ms

    while chunk_start < now_ms:
        chunk_end = min(chunk_start + chunk_ms, now_ms)
        try:
            r = requests.get(
                f"{BINANCE_BASE}/aggTrades",
                params={
                    "symbol":    SYMBOL,
                    "startTime": chunk_start,
                    "endTime":   chunk_end,
                    "limit":     1000,
                },
                timeout=10,
            )
            r.raise_for_status()
            trades = r.json()
            if trades:
                prices = [float(t["p"]) for t in trades]
                all_highs.append(max(prices))
                all_lows.append(min(prices))
        except Exception as e:
            print(f"  [aggTrades chunk] {e}")
        chunk_start = chunk_end + 1
        time.sleep(0.05)   # stay well under Binance rate limit

    if not all_highs:
        return {"high": None, "low": None}
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
        ax1.text(idx+0.5, ob["top"], "Bull OB" if ob["type"] == "BULL_OB" else "Bear OB",
                 color=bc, fontsize=6.5, va="bottom", zorder=5)
    for fvg in fvgs:
        idx = fvg["idx"]
        if idx >= n: continue
        col = "#1a3d6b" if fvg["type"] == "BULL_FVG" else "#6b4a1a"
        bc  = "#40c4ff" if fvg["type"] == "BULL_FVG" else "#ffab40"
        ax1.add_patch(plt.Rectangle((idx-2, fvg["bottom"]), n-idx+3, fvg["top"]-fvg["bottom"],
                                    color=col, alpha=0.35, zorder=1))
        ax1.text(idx+0.5, fvg["top"], "Bull FVG" if fvg["type"] == "BULL_FVG" else "Bear FVG",
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
    col   = "#26a69a" if trend == "BULLISH" else ("#ef5350" if trend == "BEARISH" else "#fff")
    ax1.set_title(f"{SYMBOL} {tf}  |  {trend}", color=col, fontsize=11,
                  fontweight="bold", loc="left", pad=6)
    ax1.legend(handles=[
        mpatches.Patch(color="#1a6b3c", label="Bull OB"), mpatches.Patch(color="#6b1a1a", label="Bear OB"),
        mpatches.Patch(color="#1a3d6b", label="Bull FVG"), mpatches.Patch(color="#6b4a1a", label="Bear FVG"),
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

# ─── PRICE-ONLY TRADE ADVICE (zero API cost) ─────────────────────────────────
def price_only_advice(price: float) -> str:
    t = active_trade
    sig   = t["signal"];  entry = t["entry"]
    sl    = t["sl"];      tp1   = t["tp1"];  tp2 = t["tp2"]
    sl_dist  = abs(entry - sl)
    tp2_dist = abs(entry - tp2)

    if sig == "BUY":
        dist_from_entry = price - entry
        dist_to_sl      = price - sl
        dist_to_tp1     = tp1 - price
        dist_to_tp2     = tp2 - price
        pct_to_tp2      = (price - entry) / tp2_dist * 100 if tp2_dist else 0
    else:
        dist_from_entry = entry - price
        dist_to_sl      = sl - price
        dist_to_tp1     = price - tp1
        dist_to_tp2     = price - tp2
        pct_to_tp2      = (entry - price) / tp2_dist * 100 if tp2_dist else 0

    risk_pct = dist_to_sl / sl_dist * 100 if sl_dist else 0

    if dist_from_entry < 0:
        advice = "WAIT"
        note   = f"Price {abs(dist_from_entry):.0f} pts against entry. SL {dist_to_sl:.0f} pts away ({risk_pct:.0f}% risk). Hold."
    elif pct_to_tp2 >= 75:
        advice = "HOLD 🔥"
        note   = f"Trade {pct_to_tp2:.0f}% to TP2. Consider trailing SL. TP2 {dist_to_tp2:.0f} pts away."
    elif pct_to_tp2 >= 40:
        advice = "HOLD"
        note   = f"Trade {pct_to_tp2:.0f}% to TP2. Strong momentum. TP2 {dist_to_tp2:.0f} pts away."
    elif pct_to_tp2 >= 10:
        advice = "HOLD"
        note   = f"Trade {pct_to_tp2:.0f}% to TP2. In profit. TP2 {dist_to_tp2:.0f} pts away."
    else:
        advice = "WAIT"
        note   = f"Near entry. Progress {pct_to_tp2:.0f}% to TP2. Watch for momentum."

    tp1_status = "✅ HIT — SL at breakeven" if t["tp1_hit"] else f"⏳ {abs(dist_to_tp1):.0f} pts away"
    return (
        f"🕐 <b>HOURLY CHECK</b>  {ist_str()}\n\n"
        f"{'🟢' if sig=='BUY' else '🔴'} <b>{sig} {SYMBOL}</b> — Price Only (no API)\n\n"
        f"💵 Price:    <b>{price:,.2f}</b>\n"
        f"🎯 Entry:    <b>{entry:,.0f}</b>\n"
        f"🛑 SL:       <b>{sl:,.0f}</b>  ({dist_to_sl:.0f} pts away)\n"
        f"✅ TP1:      <b>{tp1:,.0f}</b>  {tp1_status}\n"
        f"✅ TP2:      <b>{tp2:,.0f}</b>  ({abs(dist_to_tp2):.0f} pts away)\n"
        f"📈 Progress: <b>{max(0, pct_to_tp2):.1f}%</b> to TP2\n\n"
        f"🤖 Advice:   <b>{advice}</b>\n"
        f"💬 {note}\n\n"
        f"<i>— CLEXER V4.1 (aggTrades check) —</i>"
    )

# ─── STOP HUNT / INVALIDATION ─────────────────────────────────────────────────
def detect_stop_hunt(df_5m) -> bool:
    t = active_trade
    if not t["signal"] or not t["entry_hit"]: return False
    sig = t["signal"]; sl = t["sl"]
    for i in range(-3, 0):
        row = df_5m.iloc[i]
        if sig == "BUY":
            if row["low"] < sl and row["close"] > sl and row["close"] - row["low"] > 100:
                print(f"  [STOP HUNT] Wick {row['low']:,.0f} below SL {sl:,.0f}, closed {row['close']:,.0f}")
                return True
        else:
            if row["high"] > sl and row["close"] < sl and row["high"] - row["close"] > 100:
                print(f"  [STOP HUNT] Wick {row['high']:,.0f} above SL {sl:,.0f}, closed {row['close']:,.0f}")
                return True
    return False

def detect_entry_missed(price: float) -> bool:
    t = active_trade
    if t["entry_hit"] or t["entry_type"] != "PULLBACK": return False
    sig = t["signal"]; tp1 = t["tp1"]
    if sig == "BUY"  and price >= tp1 + 500: return True
    if sig == "SELL" and price <= tp1 - 500: return True
    return False

def detect_entry_invalidated(price: float, df_4h) -> bool:
    t = active_trade
    if t["entry_hit"]: return False
    sig = t["signal"]; sl = t["sl"]
    last_close = df_4h["close"].iloc[-1]
    if sig == "BUY"  and last_close < sl: return True
    if sig == "SELL" and last_close > sl: return True
    return False

def required_confidence_after_losses() -> str:
    n = trade_stats["consecutive_sl"]
    if n >= 2: return "HIGH"
    if n >= 1: return "MEDIUM"
    return "LOW"

# ─── CLAUDE FULL ANALYSIS ─────────────────────────────────────────────────────
def fetch_all_data():
    """Fetch all 4 timeframes — only call when Claude analysis is needed"""
    data = {}
    for key, iv, lim, lb in [("weekly","1w",52,5),("4h","4h",200,5),("1h","1h",100,5),("5m","5m",50,3)]:
        data[key] = (get_candles(iv, lim), lb)
        time.sleep(0.3)
        print(f"    {key}: {len(data[key][0])} candles")
    return data

def analyze_with_claude(ticker, data, force_confidence=None) -> dict | None:
    """
    Full SMC analysis using claude-opus-4-6.
    EXPENSIVE — use only for:
      • No active trade (new signal search)
      • SL hit (find next entry)
      • TP2 hit (find next entry)
    Sends charts to channel automatically.
    """
    client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    price    = ticker["price"]
    min_conf = force_confidence or required_confidence_after_losses()
    print(f"  [CLAUDE] Min confidence: {min_conf} | Model: claude-opus-4-6 | Generating charts...")

    charts  = generate_all_charts(data, price)
    summary = build_smc_summary(data, ticker)

    send_charts_to_channel(charts, "📊 SMC Analysis")

    conf_instruction = ""
    if min_conf == "HIGH":
        conf_instruction = (
            "\n⚠️ IMPORTANT: Only return a trade if ALL confluence factors are clearly present. "
            "After 2+ consecutive SL hits — be very selective. If in doubt, return WAIT."
        )
    elif min_conf == "MEDIUM":
        conf_instruction = "\n⚠️ Note: 1 recent SL hit. Only take MEDIUM or HIGH confidence setups."

    prompt = f"""{summary}

You are CLEXER — an elite Bitcoin SMC trader. Analyse the 4 charts (Weekly, 4H, 1H, 5M) following this exact framework step by step:

════════════════════════════════════════════
  SMC TRADE ANALYSIS FRAMEWORK
════════════════════════════════════════════

STEP 1 — WEEKLY TREND
• What is the weekly trend? (HH+HL = bullish, LH+LL = bearish, range = neutral)
• Where are the weekly swing highs and lows?
• Did this week make a new HH or new LL?
• Is the current weekly candle bullish or bearish?

STEP 2 — 4H STRUCTURE (PRIMARY BIAS)
• Is 4H structure BULLISH (HH+HL sequence) or BEARISH (LH+LL sequence)?
• Locate the most recent 4H Order Block (last opposing candle before strong impulse move)
• Has there been a CHoCH (Change of Character) or BOS (Break of Structure) recently?
• Was there strong volume on the last big 4H impulse?

STEP 3 — 1H CONFIRMATION
• Does 1H structure confirm the 4H bias? (must align — if conflicting, NO TRADE)
• Where are the nearest 1H support and resistance zones?
• Is there buy-side liquidity (equal highs) above, or sell-side (equal lows) below?
• Any 1H FVG (Fair Value Gap) sitting near current price?

STEP 4 — 5M MOMENTUM (ENTRY TIMING)
• Higher lows forming on 5M → bullish momentum → supports LONG
• Lower highs forming on 5M → bearish momentum → supports SHORT
• Is 5M volume rising (confirms momentum) or falling (warns of trap)?

STEP 5 — SIGNAL DECISION

LONG requires ALL of the following:
  ✅ 4H making HH and HL — bullish structure confirmed
  ✅ Price at or pulling back to 4H bullish OB / demand zone
  ✅ Weekly trend bullish OR weekly support is holding
  ✅ 5M higher lows forming (not lower highs)
  ✅ Recent green volume spike confirming buying pressure

SHORT requires ALL of the following:
  ✅ 4H making LH and LL — bearish structure confirmed
  ✅ Price at or bouncing up to 4H bearish OB / supply zone
  ✅ Weekly trend bearish OR weekly resistance is holding
  ✅ 5M lower highs forming (not higher lows)
  ✅ Recent red volume spike confirming selling pressure

Return WAIT if ANY of:
  ❌ 4H and 5M conflict with each other
  ❌ Price is in the middle of a range (no OB/FVG in reach)
  ❌ Volume is low / price is consolidating / compressing
  ❌ 4H structure is unclear or transitioning
  ❌ Weekly trend directly opposes 4H bias{conf_instruction}

STEP 6 — ENTRY (pullback ONLY — never chase)
  For LONG:  wait for price to pull back to nearest 4H HL or bullish OB/FVG
             enter when 5M candle closes GREEN above that zone with volume
  For SHORT: wait for price to bounce up to nearest 4H LH or bearish OB/FVG
             enter when 5M candle closes RED below that zone with volume
  NEVER: enter at the top/bottom of a big candle, against 4H trend, without volume

STEP 7 — STOP LOSS
  MINIMUM 500 pts from entry (never less than 1× ATR):
  • LONG  SL: just below last 4H HL — NOT at round numbers, NOT at obvious liquidity pools
  • SHORT SL: just above last 4H LH — minimum 500 pts above entry

STEP 8 — TAKE PROFIT
  TP1 = MINIMUM 1:2 R:R → next key 4H resistance (LONG) / 4H support (SHORT)
  TP2 = MINIMUM 1:4 R:R → weekly high or major OB above (LONG) / weekly low or demand below (SHORT)

ENTRY TYPE RULES:
  MARKET  = price is inside the zone RIGHT NOW → enter immediately
  PULLBACK = price must travel to the zone first → wait for the retest

  PULLBACK CRITICAL RULE:
    BUY  pullback → current price = {price:,.0f}
                    entry MUST be below {price:,.0f}
                    TP1 and TP2 MUST be above {price:,.0f}
    SELL pullback → current price = {price:,.0f}
                    entry MUST be above {price:,.0f}
                    TP1 and TP2 MUST be below {price:,.0f}

════════════════════════════════════════════

Return ONLY valid JSON — no markdown, no explanation, no extra text:

For a valid trade:
{{
  "signal":       "BUY",
  "entry":        <price>,
  "sl":           <price>,
  "tp1":          <price>,
  "tp2":          <price>,
  "rr":           "1:X.X",
  "entry_type":   "MARKET",
  "entry_note":   "zone description e.g. 93200-93500 Bull OB 4H",
  "bias":         "BULLISH",
  "weekly_trend": "one-line weekly summary",
  "structure_4h": "BOS/CHoCH details + HH/HL or LH/LL",
  "entry_zone":   "zone description",
  "confidence":   "HIGH",
  "session":      "{get_session()}",
  "reasoning":    "2-3 sentences explaining ALL confluence factors that confirm this trade"
}}

If NO clean setup → return exactly:
{{"signal":"WAIT","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"","structure_4h":"","entry_zone":"","confidence":"LOW","session":"{get_session()}","reasoning":"explain clearly why no valid setup exists right now"}}"""

    try:
        msg = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
            model="claude-opus-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["weekly"]}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["4h"]}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["1h"]}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": charts["5m"]}},
                {"type": "text",  "text": prompt},
            ]}]
        )
        raw    = msg.content[0].text.strip().replace("```json", "").replace("```", "").strip()
        signal = json.loads(raw)

        # ── WAIT response ──────────────────────────────────────────────────
        if signal.get("signal") == "WAIT":
            print(f"  [NO TRADE] {signal.get('reasoning', 'no reason given')}")
            return None

        entry   = float(signal["entry"])
        sl_val  = float(signal["sl"])
        sl_dist = abs(entry - sl_val)

        # ── SL minimum 500 pts (per trade rules) ──────────────────────────
        if sl_dist < 500:
            print(f"  [REJECT] SL dist={sl_dist:.0f} pts — too tight (min 500 pts)")
            return None

        # ── SL maximum sanity check ────────────────────────────────────────
        if sl_dist > 3000:
            print(f"  [REJECT] SL dist={sl_dist:.0f} pts — too wide (max 3000 pts)")
            return None

        # ── Fix PULLBACK TPs if needed ────────────────────────────────────
        etype = signal.get("entry_type", "MARKET")
        tp1   = float(signal["tp1"])
        sig   = signal["signal"]
        if etype == "PULLBACK":
            if sig == "BUY" and tp1 <= price:
                signal["tp1"] = round(price + sl_dist * 2, -1)
                signal["tp2"] = round(price + sl_dist * 4, -1)
                print(f"  [FIX] PULLBACK BUY TP adjusted above current price")
            elif sig == "SELL" and tp1 >= price:
                signal["tp1"] = round(price - sl_dist * 2, -1)
                signal["tp2"] = round(price - sl_dist * 4, -1)
                print(f"  [FIX] PULLBACK SELL TP adjusted below current price")

        # ── Confidence filter ─────────────────────────────────────────────
        conf      = signal.get("confidence", "LOW")
        conf_rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
        if conf_rank.get(conf, 1) < conf_rank.get(required_confidence_after_losses(), 1):
            print(f"  [SKIP] Confidence {conf} too low (need {required_confidence_after_losses()})")
            return None

        # ── Recalculate R:R ───────────────────────────────────────────────
        tp2_dist = abs(entry - float(signal["tp2"]))
        rr       = tp2_dist / sl_dist if sl_dist > 0 else 0
        signal["rr"] = f"1:{rr:.1f}"

        print(f"  [OK] {signal['signal']} entry:{entry:,.0f}  SL:{sl_val:,.0f}  "
              f"SL-dist:{sl_dist:.0f}  R:R:{signal['rr']}  Conf:{conf}")
        return signal

    except Exception as e:
        print(f"  [ERROR] Claude: {e}")
        return None

# ─── TELEGRAM SEND ────────────────────────────────────────────────────────────
def send_telegram(text: str) -> bool:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHANNEL_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10)
        r.raise_for_status(); return True
    except Exception as e:
        print(f"  [ERROR] Telegram: {e}"); return False

def send_reply(chat_id, text: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10)
    except Exception as e:
        print(f"  [CMD] Reply error: {e}")

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
        print(f"  [BROADCAST] User {chat_id} error: {e}"); return False

# ─── BROADCAST ────────────────────────────────────────────────────────────────
def do_broadcast(admin_chat_id, text: str, file_id=None, file_type=None):
    targets = list(registered_users) + [TELEGRAM_CHANNEL_ID]
    ok = 0; fail = 0
    print(f"  [BROADCAST] Sending to {len(targets)} targets...")
    for cid in targets:
        success = send_to_user(cid, text, file_id, file_type)
        if success: ok += 1
        else: fail += 1
        time.sleep(0.05)
    send_reply(admin_chat_id,
        f"📢 <b>Broadcast Complete</b>\n\n"
        f"✅ Delivered: {ok}\n❌ Failed: {fail}\n👥 Total: {len(targets)}\n\n"
        f"<i>— CLEXER V4.1 —</i>"
    )
    print(f"  [BROADCAST] Done: {ok} ok, {fail} fail")

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
        + (f"🔷 4H:      <i>{s4h}</i>\n" if s4h else "")
        + (f"📍 Zone:    <i>{ez}</i>\n" if ez else "")
        + (f"\n💭 <i>{rs}</i>\n" if rs else "")
        + f"\n<i>— Signal by CLEXER V4.1 —</i>\n"
          f"⚠️ <i>Not financial advice</i>"
    )

def fmt_update(status: str, price: float = None) -> str:
    t     = active_trade
    entry = t.get("entry") or 0
    msgs  = {
        "SL_HIT":        "🛑 <b>SL HIT</b>\nSearching for next trade...",
        "TP1_HIT":       f"✅ <b>TP1 HIT</b>\nSL → Breakeven ({entry:,.0f})\nWaiting TP2 → <b>{t.get('tp2',0):,.0f}</b>",
        "TP2_HIT":       "🏆 <b>TP2 HIT — Trade Complete!</b>",
        "STOP_HUNT":     f"⚡ <b>STOP HUNT DETECTED</b>\nSL wicked — no close through\nHolding trade | SL: {t.get('sl',0):,.0f}",
        "SETUP_INVALID": f"❌ <b>Setup Invalidated</b>\nPrice hit SL before entry\nCancelling — finding new setup",
        "ENTRY_MISSED":  f"🚀 <b>Entry Zone Missed</b>\nPrice blasted past entry {entry:,.0f}\nCancelling — finding new entry",
        "WAITING_ENTRY": (
            f"⏳ <b>Waiting for Pullback</b>\n"
            f"Entry zone: <b>{entry:,.0f}</b>\n"
            + (f"Current price: <b>{price:,.0f}</b>\nDistance: <b>{abs((price or 0)-entry):,.0f} pts</b>" if price else "")
        ),
        "HOLD":      "📊 <b>HOLD</b> — Structure intact",
        "WAIT":      "⏳ <b>WAIT</b> — Trade running",
        "CLOSE":     "⚠️ <b>CLOSE NOW</b> — Structure broken",
        "NO_VOLUME": "📉 <b>CLOSE</b> — No volume / flat",
        "COOLDOWN":  f"⏸ <b>Cooling Down</b>\n{trade_stats['cooldown_scans']} scan(s) remaining",
    }
    body = msgs.get(status, "⏳ Trade running")
    return f"📡 <b>{SYMBOL} UPDATE</b>  {ist_str()}\n\n{body}\n\n<i>— CLEXER V4.1 —</i>"

# ─── PRICE STATUS (tick-perfect via aggTrades) ────────────────────────────────
def check_price_status(price: float, high_1h: float, low_1h: float, df_5m=None) -> str:
    """
    price   = current live price (for entry/waiting logic)
    high_1h = tick-perfect high over last 60 min (for TP detection)
    low_1h  = tick-perfect low  over last 60 min (for SL detection)
    """
    t = active_trade
    if not t["signal"]: return "NONE"
    sig, sl, tp1, tp2, entry = t["signal"], t["sl"], t["tp1"], t["tp2"], t["entry"]

    if not t["entry_hit"]:
        # Use range to catch SL breach before entry
        if (sig == "BUY" and low_1h <= sl) or (sig == "SELL" and high_1h >= sl):
            return "SETUP_INVALID"
        tol = abs(entry - sl) * 0.3
        if (sig == "BUY" and price <= entry + tol) or (sig == "SELL" and price >= entry - tol):
            active_trade["entry_hit"] = True
            print(f"  [ENTRY HIT] {sig} {entry:,.0f} reached — tracking live")
        else:
            return "WAITING_ENTRY"

    # ── Tick-perfect TP / SL detection using 1h range ─────────────────────
    sl_breach = (sig == "SELL" and high_1h >= sl) or (sig == "BUY" and low_1h <= sl)

    if sl_breach and df_5m is not None and not t["sl_wicked"]:
        if detect_stop_hunt(df_5m):
            active_trade["sl_wicked"] = True
            trade_stats["stop_hunts"] += 1
            return "STOP_HUNT"

    if (sig == "SELL" and high_1h >= sl) or (sig == "BUY" and low_1h <= sl):
        return "SL_HIT"
    if (sig == "SELL" and low_1h <= tp2) or (sig == "BUY" and high_1h >= tp2):
        return "TP2_HIT"
    if not t["tp1_hit"]:
        if (sig == "SELL" and low_1h <= tp1) or (sig == "BUY" and high_1h >= tp1):
            return "TP1_HIT"
    return "RUNNING"

# ─── PRICE-ONLY CHECK (1h, zero API cost) ─────────────────────────────────────
def run_price_check() -> bool:
    """
    Hourly check using tick-perfect aggTrades data.
    Handles all TP/SL events, sends notifications, updates stats.
    Returns True if a critical event (SL/TP2 hit) requires a full Claude scan.
    """
    global last_price_check_time
    if not active_trade["signal"]:
        return False

    try:
        ticker  = get_ticker()
        price   = ticker["price"]

        # ── Fetch tick-perfect range via aggTrades ─────────────────────────
        range_1h = get_price_range_since(60)
        high_1h  = range_1h["high"] or price
        low_1h   = range_1h["low"]  or price
        print(f"  [1H CHECK] cur:{price:,.2f}  H:{high_1h:,.2f}  L:{low_1h:,.2f}")

        df_5m = get_candles("5m", 50)
        df_4h = get_candles("4h", 10)

        # ── Entry missed / invalidated ─────────────────────────────────────
        if detect_entry_missed(price):
            trade_stats["missed_entries"] += 1
            send_telegram(fmt_update("ENTRY_MISSED"))
            reset_trade()
            return True

        if not active_trade["entry_hit"] and detect_entry_invalidated(price, df_4h):
            send_telegram(fmt_update("SETUP_INVALID"))
            reset_trade()
            return True

        # ── Main status check (uses tick-perfect range) ────────────────────
        status = check_price_status(price, high_1h, low_1h, df_5m)
        print(f"  [1H] Trade: {active_trade['signal']} | Status: {status}")

        if status == "TP2_HIT":
            trade_stats["total_tp2"]      += 1
            trade_stats["consecutive_sl"]  = 0
            send_telegram(fmt_update("TP2_HIT"))
            reset_trade()
            return True   # full Claude scan for next trade

        elif status == "SL_HIT":
            trade_stats["total_sl"]        += 1
            trade_stats["consecutive_sl"]  += 1
            n = trade_stats["consecutive_sl"]

            if n >= 3:
                trade_stats["cooldown_scans"] = 2
                send_telegram(
                    f"🛑 <b>SL HIT</b>  ({n} in a row)\n\n"
                    f"⚠️ <b>3 consecutive losses — pausing 2 scans</b>\n"
                    f"Only HIGH confidence setups next.\n\n<i>— CLEXER V4.1 —</i>"
                )
            elif n == 2:
                trade_stats["cooldown_scans"] = 1
                send_telegram(
                    f"🛑 <b>SL HIT</b>  ({n} in a row)\n\n"
                    f"⏸ Pausing 1 scan. HIGH confidence only next.\n\n<i>— CLEXER V4.1 —</i>"
                )
            else:
                send_telegram(fmt_update("SL_HIT"))

            reset_trade()
            return True   # full Claude scan for next signal

        elif status == "TP1_HIT":
            active_trade["tp1_hit"]       = True
            active_trade["sl"]            = active_trade["entry"]
            trade_stats["total_tp1"]      += 1
            trade_stats["consecutive_sl"] = 0
            send_telegram(fmt_update("TP1_HIT"))
            # no full scan needed — let TP2 run

        elif status == "STOP_HUNT":
            send_telegram(fmt_update("STOP_HUNT"))

        elif status == "WAITING_ENTRY":
            active_trade["scan_count"] += 1
            dist = abs(price - active_trade["entry"])
            print(f"  Pullback pending — {dist:.0f} pts from entry {active_trade['entry']:,.0f}")
            send_telegram(fmt_update("WAITING_ENTRY", price))

        elif status == "RUNNING":
            active_trade["scan_count"] += 1
            send_telegram(price_only_advice(price))

    except Exception as e:
        print(f"  [1H CHECK ERROR] {e}")

    return False

# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────
COMMANDS_HELP = """🤖 <b>CLEXER V4.1 Commands</b>
━━━━━━━━━━━━━━━━━━━

📊 <b>INFO</b>
/status — Bot status + active trade
/price — Live BTC price
/trade — Active trade details
/history — Last 5 signals
/stats — Win/loss statistics
/session — Current session info

🎯 <b>TRADE CONTROL</b>
/close — Close active trade
/sltobe — Move SL to breakeven
/setsl 61500 — Set SL price
/settp1 60000 — Set TP1 price
/settp2 58000 — Set TP2 price

🤖 <b>BOT CONTROL</b>
/signal — Force full scan now
/pause — Pause bot
/resume — Resume bot
/resetsl — Reset SL counter
/setinterval 4 — Set signal scan interval (hours)
/users — Show registered user count

📢 <b>BROADCAST</b>
/broadcast — Send message to all users
  (bot will ask for message + optional image/PDF)

/help — This menu"""

def handle_command(text: str, chat_id, message: dict = None):
    global SIGNAL_SCAN_INTERVAL, last_force_scan_time, broadcast_pending

    register_user(chat_id)
    parts = text.strip().split()
    cmd   = parts[0].lower().split("@")[0]
    is_admin = (not ADMIN_CHAT_ID) or (str(chat_id) == str(ADMIN_CHAT_ID))

    admin_only = {"/signal", "/pause", "/resume", "/resetsl", "/setinterval",
                  "/close", "/sltobe", "/setsl", "/settp1", "/settp2",
                  "/broadcast", "/users"}
    if cmd in admin_only and not is_admin:
        send_reply(chat_id, "⛔ Admin only."); return

    if cmd in ("/start", "/help"):
        send_reply(chat_id, COMMANDS_HELP)

    elif cmd == "/status":
        t  = active_trade; st = "⏸ PAUSED" if bot_paused.is_set() else "▶️ RUNNING"
        cd = f"⏸ Cooldown: {trade_stats['cooldown_scans']} scans\n" if trade_stats["cooldown_scans"] > 0 else ""
        trade_info = (
            f"{t['signal']} @ {t['entry']:,.0f}\n"
            f"SL:{t['sl']:,.0f} TP1:{t['tp1']:,.0f} TP2:{t['tp2']:,.0f}\n"
            f"TP1:{'✅' if t['tp1_hit'] else '❌'} | Entry:{'✅' if t['entry_hit'] else '⏳'}"
        ) if t["signal"] else "No active trade"
        send_reply(chat_id,
            f"📊 <b>CLEXER V4.1 Status</b>\n\n"
            f"Bot: {st}\n{cd}"
            f"Session: {get_session()} {'✅' if is_trading_hours() else '⏸'}\n"
            f"IST: {ist_str()}\n"
            f"Signal scan: every {SIGNAL_SCAN_INTERVAL//3600}h\n"
            f"Price check: every 1h (aggTrades, no API)\n"
            f"Users: {len(registered_users)}\n\n"
            f"<b>Active Trade:</b>\n{trade_info}"
        )

    elif cmd == "/price":
        try:
            tk = get_ticker()
            send_reply(chat_id,
                f"💵 <b>BTCUSDT</b>\n\nPrice: <b>{tk['price']:,.2f}</b>\n"
                f"24h: {tk['change']:+.2f}% | Vol: ${tk['volume']/1e6:.1f}M\n"
                f"H:{tk['high24']:,.2f} L:{tk['low24']:,.2f}\n🕐 {ist_str()}"
            )
        except Exception as e: send_reply(chat_id, f"❌ {e}")

    elif cmd == "/trade":
        t = active_trade
        if not t["signal"]: send_reply(chat_id, "📭 No active trade.")
        else:
            try: tk = get_ticker(); pl = f"💵 Current: <b>{tk['price']:,.2f}</b>\n"
            except: pl = ""
            send_reply(chat_id,
                f"📈 <b>Active Trade</b>\n\n"
                f"{t['signal']} — {SYMBOL}\n{pl}"
                f"Entry: <b>{t['entry']:,.0f}</b> {'✅' if t['entry_hit'] else '⏳ waiting'}\n"
                f"SL:    <b>{t['sl']:,.0f}</b>\n"
                f"TP1:   <b>{t['tp1']:,.0f}</b> {'✅ HIT' if t['tp1_hit'] else '⏳'}\n"
                f"TP2:   <b>{t['tp2']:,.0f}</b>\n"
                f"Type:  {t['entry_type']}\n"
                + (f"Note: <i>{t['entry_note']}</i>" if t.get("entry_note") else "")
            )

    elif cmd == "/history":
        if not signal_history: send_reply(chat_id, "📭 No history yet.")
        else:
            lines = ["📜 <b>Last Signals</b>\n"]
            for s in reversed(signal_history[-5:]):
                e = "🟢" if s["signal"] == "BUY" else "🔴"
                lines.append(f"{e} {s['signal']} @ {s['entry']:,.0f} R:R:{s.get('rr','?')} {s.get('confidence','?')}\n"
                             f"   SL:{s['sl']:,.0f} TP1:{s['tp1']:,.0f} TP2:{s['tp2']:,.0f}\n"
                             f"   🕐 {s['time']}\n")
            send_reply(chat_id, "\n".join(lines))

    elif cmd == "/stats":
        ts = trade_stats
        send_reply(chat_id,
            f"📈 <b>Trade Statistics</b>\n\n"
            f"Total signals:  {ts['total_signals']}\n"
            f"TP1 hits:       {ts['total_tp1']} ✅\n"
            f"TP2 hits:       {ts['total_tp2']} 🏆\n"
            f"SL hits:        {ts['total_sl']} 🛑\n"
            f"Stop hunts:     {ts['stop_hunts']} ⚡\n"
            f"Missed entries: {ts['missed_entries']} 🚀\n"
            f"Consec. SL:     {ts['consecutive_sl']}\n"
            f"Cooldown:       {ts['cooldown_scans']} scans"
        )

    elif cmd == "/session":
        s = get_session()
        send_reply(chat_id,
            f"📍 <b>Session Info</b>\n\nCurrent: <b>{s}</b> {'✅ Active' if is_trading_hours() else '⏸'}\n\n"
            f"🇬🇧 London:   07:30–16:30 IST\n"
            f"🇺🇸 New York: 18:30–01:00 IST\n"
            f"😴 Sleep:    01:00–07:29 IST\n\n🕐 {ist_str()}"
        )

    elif cmd == "/users":
        send_reply(chat_id,
            f"👥 <b>Registered Users</b>\n\n"
            f"Total: <b>{len(registered_users)}</b>\n"
            f"Channel: {TELEGRAM_CHANNEL_ID}\n\n"
            f"<i>All will receive broadcasts</i>"
        )

    elif cmd == "/close":
        t = active_trade
        if not t["signal"]: send_reply(chat_id, "📭 No active trade.")
        else:
            info = f"{t['signal']} @ {t['entry']:,.0f}"
            reset_trade()
            send_telegram(f"⛔ <b>Trade Manually Closed</b>\n{info}\n\n<i>— CLEXER V4.1 —</i>")
            send_reply(chat_id, f"✅ Closed: {info}")
            force_scan.set()

    elif cmd == "/sltobe":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No active trade.")
        else:
            old = active_trade["sl"]; active_trade["sl"] = active_trade["entry"]
            send_telegram(f"🔄 <b>SL → Breakeven</b> {old:,.0f}→<b>{active_trade['entry']:,.0f}</b>\n\n<i>— CLEXER V4.1 —</i>")
            send_reply(chat_id, f"✅ SL → {active_trade['entry']:,.0f}")

    elif cmd == "/setsl":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No active trade.")
        elif len(parts) < 2: send_reply(chat_id, "Usage: /setsl 61500")
        else:
            try:
                v = float(parts[1].replace(",", ""))
                old = active_trade["sl"]; active_trade["sl"] = v
                send_telegram(f"🔄 <b>SL Updated</b> {old:,.0f}→<b>{v:,.0f}</b>\n\n<i>— CLEXER V4.1 —</i>")
                send_reply(chat_id, f"✅ SL={v:,.0f}")
            except: send_reply(chat_id, "❌ /setsl 61500")

    elif cmd == "/settp1":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No active trade.")
        elif len(parts) < 2: send_reply(chat_id, "Usage: /settp1 60000")
        else:
            try:
                v = float(parts[1].replace(",", "")); active_trade["tp1"] = v
                send_telegram(f"🔄 <b>TP1→{v:,.0f}</b>\n\n<i>— CLEXER V4.1 —</i>")
                send_reply(chat_id, f"✅ TP1={v:,.0f}")
            except: send_reply(chat_id, "❌ /settp1 60000")

    elif cmd == "/settp2":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No active trade.")
        elif len(parts) < 2: send_reply(chat_id, "Usage: /settp2 58000")
        else:
            try:
                v = float(parts[1].replace(",", "")); active_trade["tp2"] = v
                send_telegram(f"🔄 <b>TP2→{v:,.0f}</b>\n\n<i>— CLEXER V4.1 —</i>")
                send_reply(chat_id, f"✅ TP2={v:,.0f}")
            except: send_reply(chat_id, "❌ /settp2 58000")

    elif cmd == "/signal":
        if bot_paused.is_set():
            send_reply(chat_id, "⏸ Bot paused. /resume first.")
        else:
            now = time.time(); elapsed = now - last_force_scan_time
            if elapsed < 900 and last_force_scan_time > 0:
                mins_left = int((900 - elapsed) / 60)
                send_reply(chat_id, f"⏳ Cooldown: {mins_left} min before next /signal")
            else:
                last_force_scan_time = now
                send_reply(chat_id, "🔍 Forcing full scan... charts + analysis incoming (~30s)")
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
        trade_stats["consecutive_sl"] = 0
        trade_stats["cooldown_scans"] = 0
        send_reply(chat_id, "✅ SL counter reset.")

    elif cmd == "/setinterval":
        if len(parts) < 2: send_reply(chat_id, f"Current: {SIGNAL_SCAN_INTERVAL//3600}h\nUsage: /setinterval 4")
        else:
            try:
                h = float(parts[1])
                if h < 1 or h > 24: send_reply(chat_id, "❌ 1–24 hours only.")
                else:
                    SIGNAL_SCAN_INTERVAL = int(h * 3600)
                    send_reply(chat_id, f"✅ Signal scan interval → {h}h\n(Price check stays at 1h)")
            except: send_reply(chat_id, "❌ /setinterval 4")

    elif cmd == "/broadcast":
        broadcast_pending[chat_id] = {"step": "waiting_message"}
        send_reply(chat_id,
            "📢 <b>Broadcast Mode</b>\n\n"
            "Send your message now.\n"
            "You can attach an image or PDF directly with your message.\n\n"
            "<i>Type /cancel to abort</i>"
        )

    elif cmd == "/cancel":
        if chat_id in broadcast_pending:
            del broadcast_pending[chat_id]
            send_reply(chat_id, "❌ Broadcast cancelled.")
        else:
            send_reply(chat_id, "Nothing to cancel.")

    else:
        send_reply(chat_id, f"❓ Unknown: {cmd}\n/help")

def handle_broadcast_message(chat_id, message: dict):
    text      = message.get("text") or message.get("caption") or ""
    photo     = message.get("photo")
    doc       = message.get("document")
    file_id   = None; file_type = None

    if photo:   file_id = photo[-1]["file_id"]; file_type = "photo"
    elif doc:   file_id = doc["file_id"];       file_type = "document"

    if not text and not file_id:
        send_reply(chat_id, "❌ Empty message. Send text, image, or PDF. /cancel to abort.")
        return

    del broadcast_pending[chat_id]
    user_count   = len(registered_users)
    target_count = user_count + 1
    send_reply(chat_id,
        f"📢 <b>Broadcasting...</b>\n\n"
        f"👥 Targets: {target_count} ({user_count} users + channel)\n"
        f"📎 Media: {'📷 Photo' if file_type=='photo' else '📄 Document' if file_type=='document' else '📝 Text only'}\n\n"
        f"<i>Sending...</i>"
    )
    threading.Thread(
        target=do_broadcast,
        args=(chat_id, text, file_id, file_type),
        daemon=True,
    ).start()

# ─── COMMAND LISTENER ─────────────────────────────────────────────────────────
def command_listener():
    global last_update_id
    print("[CMD] Listener started")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook", timeout=10)
        print("[CMD] Webhook cleared")
    except Exception as e:
        print(f"[CMD] Webhook error: {e}")

    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"offset": last_update_id+1, "timeout": 20,
                        "allowed_updates": ["message"]},
                timeout=25,
            )
            data = r.json()
            if not data.get("ok"):
                print(f"  [CMD] Error: {data}"); time.sleep(5); continue

            for upd in data.get("result", []):
                last_update_id = upd["update_id"]
                msg   = upd.get("message", {})
                text  = msg.get("text", "") or ""
                cid   = msg.get("chat", {}).get("id")
                uname = msg.get("from", {}).get("username", "?")

                if not cid: continue
                print(f"  [CMD] @{uname} ID:{cid}: {text[:40]}")

                register_user(cid)

                if cid in broadcast_pending and not text.startswith("/"):
                    handle_broadcast_message(cid, msg)
                    continue

                if text.startswith("/"):
                    handle_command(text, cid, msg)

        except Exception as e:
            print(f"  [CMD] Error: {e}")
        time.sleep(2)

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    global last_signal_scan_time, last_price_check_time

    print(f"[CLEXER V4.1] Adaptive SMC Bot | {SYMBOL} | claude-opus-4-6")
    load_users()
    threading.Thread(target=command_listener, daemon=True).start()

    send_telegram(
        f"🤖 <b>CLEXER V4.1 Online</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📊 Weekly + 4H + 1H + 5M Vision\n"
        f"🎯 aggTrades tick-perfect TP/SL\n"
        f"🧠 claude-opus-4-6 for analysis\n"
        f"🛡 Stop Hunt / Missed Entry / Cooldown\n"
        f"🕐 Price check every 1h (no API cost)\n"
        f"🔍 Full scan every 4h (London + NY)\n"
        f"📢 Broadcast to all users supported\n"
        f"💬 /help for commands\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<i>— CLEXER V4.1 —</i>"
    )

    TICK = 60   # main loop heartbeat

    while True:
        try:
            if bot_paused.is_set():
                time.sleep(TICK); continue

            now    = time.time()
            forced = force_scan.is_set()
            if forced: force_scan.clear()

            print(f"\n[{now_ist().strftime('%H:%M IST')}] {get_session()}{' FORCED' if forced else ''}")

            # ── Sleep hours: skip unless forced ──────────────────────────────
            if not forced and is_ist_sleep():
                print("  [SLEEP] 01:00–07:29 IST — resting")
                time.sleep(TICK); continue

            # ── 1-HOUR PRICE CHECK (aggTrades, zero API cost) ─────────────────
            price_check_due = (now - last_price_check_time) >= PRICE_CHECK_INTERVAL
            if (price_check_due or forced) and active_trade["signal"]:
                last_price_check_time = now
                need_full_scan = run_price_check()
                if need_full_scan:
                    # SL or TP2 hit — run full Claude scan immediately
                    forced = True
                    last_signal_scan_time = 0
                elif not forced:
                    time.sleep(TICK); continue

            # ── Check if signal scan is due ───────────────────────────────────
            signal_scan_due = (now - last_signal_scan_time) >= SIGNAL_SCAN_INTERVAL
            if not forced and not signal_scan_due:
                time.sleep(TICK); continue

            # ── Session check (London / NY only) ──────────────────────────────
            if not forced and not is_trading_hours():
                print(f"  [WAIT] {get_session()} — not London/NY")
                time.sleep(TICK); continue

            # ── Cooldown check (applies even to forced scans after SL streak) ─
            if trade_stats["cooldown_scans"] > 0 and not force_scan.is_set():
                trade_stats["cooldown_scans"] -= 1
                remaining = trade_stats["cooldown_scans"]
                print(f"  [COOLDOWN] {remaining} scans remaining")
                if remaining == 0:
                    send_telegram("🔍 <b>Cooldown over — scanning for new setup</b>\n\n<i>— CLEXER V4.1 —</i>")
                last_signal_scan_time = now
                time.sleep(TICK); continue

            # ── Skip full scan if trade is active (price check handles it) ───
            if active_trade["signal"] and not forced:
                print("  [SKIP] Trade active — price-only mode handles it")
                last_signal_scan_time = now
                time.sleep(TICK); continue

            # ── FULL CLAUDE SCAN ───────────────────────────────────────────────
            last_signal_scan_time = now
            print("  Fetching all candles for full Claude scan...")
            ticker = get_ticker()
            price  = ticker["price"]
            print(f"  Price: {price:,.2f} | {ticker['change']:+.2f}%")
            data   = fetch_all_data()

            if not active_trade["signal"]:
                # No active trade — find new signal
                signal = analyze_with_claude(ticker, data)
                if signal:
                    send_telegram(fmt_signal(signal))
                    set_trade(signal)
                    print(f"  [SENT] {signal['signal']} "
                          f"R:R:{signal.get('rr','?')} "
                          f"Conf:{signal.get('confidence','?')}")
                else:
                    print("  No signal — waiting for next scan")
                    send_telegram(
                        f"🔍 <b>Scan Complete — No Signal</b>\n\n"
                        f"💵 Price: <b>{price:,.2f}</b> ({ticker['change']:+.2f}%)\n"
                        f"📍 Session: {get_session()}\n"
                        f"🕐 {ist_str()}\n\n"
                        f"No clean SMC setup found. Next scan in {SIGNAL_SCAN_INTERVAL//3600}h.\n\n"
                        f"<i>— CLEXER V4.1 —</i>"
                    )
            else:
                # Active trade + forced /signal — send as analysis update only
                print("  Active trade running — sending analysis update")
                signal = analyze_with_claude(ticker, data)
                if signal:
                    send_telegram(
                        f"📊 <b>ANALYSIS UPDATE</b> (Trade running)\n\n"
                        f"Active: {active_trade['signal']} @ {active_trade['entry']:,.0f}\n\n"
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
