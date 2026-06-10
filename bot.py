
"""
CLEXER Signal Bot V3 — Full SMC Vision + Commands
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

SYMBOL                = "BTCUSDT"
SCAN_INTERVAL_SECONDS = 14400
BINANCE_BASE          = "https://api1.binance.com/api/v3"
IST                   = timedelta(hours=5, minutes=30)

# ─── TIME HELPERS ─────────────────────────────────────────────────────────────
def now_ist():
    return datetime.now(timezone.utc) + IST

def ist_str():
    return now_ist().strftime("%d %b %Y  %I:%M %p IST")

def get_session():
    mins = now_ist().hour * 60 + now_ist().minute
    if 450 <= mins < 990:          return "LONDON"    # 07:30–16:30 IST
    if mins >= 1110 or mins < 60:  return "NEW_YORK"  # 18:30–01:00 IST
    return "ASIA"

def is_trading_hours():
    return get_session() in ("LONDON", "NEW_YORK")

def is_ist_sleep():
    mins = now_ist().hour * 60 + now_ist().minute
    return 60 <= mins < 450   # 01:00–07:29 IST

# ─── BOT STATE ────────────────────────────────────────────────────────────────
active_trade = {
    "signal": None, "entry": None, "sl": None,
    "tp1": None, "tp2": None, "tp1_hit": False,
    "entry_type": "MARKET", "entry_note": "",
}
signal_history = []
force_scan     = threading.Event()
bot_paused     = threading.Event()
last_update_id = 0

def reset_trade():
    global active_trade
    active_trade = {"signal": None, "entry": None, "sl": None,
                    "tp1": None, "tp2": None, "tp1_hit": False,
                    "entry_type": "MARKET", "entry_note": ""}

def set_trade(s: dict):
    global active_trade
    active_trade = {
        "signal": s["signal"], "entry": s["entry"],
        "sl": s["sl"],         "tp1": s["tp1"],
        "tp2": s["tp2"],       "tp1_hit": False,
        "entry_type": s.get("entry_type", "MARKET"),
        "entry_note": s.get("entry_note", ""),
    }
    signal_history.append({
        "time": ist_str(), "signal": s["signal"],
        "entry": s["entry"], "sl": s["sl"],
        "tp1": s["tp1"], "tp2": s["tp2"],
    })
    if len(signal_history) > 10:
        signal_history.pop(0)

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
    r.raise_for_status()
    d = r.json()
    return {"price":  float(d["lastPrice"]),  "change": float(d["priceChangePercent"]),
            "volume": float(d["quoteVolume"]),
            "high24": float(d["highPrice"]),   "low24":  float(d["lowPrice"])}

# ─── SMC CALCULATIONS ─────────────────────────────────────────────────────────
def find_swing_points(df, lookback=5):
    highs, lows = [], []
    for i in range(lookback, len(df) - lookback):
        if df["high"].iloc[i] == df["high"].iloc[i-lookback:i+lookback+1].max():
            highs.append({"idx": i, "price": df["high"].iloc[i], "time": df.index[i]})
        if df["low"].iloc[i] == df["low"].iloc[i-lookback:i+lookback+1].min():
            lows.append({"idx": i, "price": df["low"].iloc[i], "time": df.index[i]})
    return highs, lows

def detect_trend(df):
    highs, lows = find_swing_points(df, 3)
    if len(highs) < 2 or len(lows) < 2: return "NEUTRAL"
    h = [x["price"] for x in highs[-2:]]
    l = [x["price"] for x in lows[-2:]]
    if h[1] > h[0] and l[1] > l[0]: return "BULLISH"
    if h[1] < h[0] and l[1] < l[0]: return "BEARISH"
    return "NEUTRAL"

def detect_bos_choch(df):
    events = []
    highs, lows = find_swing_points(df, 3)
    if len(highs) < 2 or len(lows) < 2: return events
    for i in range(1, min(4, len(highs))):
        idx = highs[-i]["idx"]
        if idx < len(df)-1 and df["close"].iloc[idx+1] > highs[-i-1]["price"]:
            events.append({"type": "BOS_BULL", "price": highs[-i-1]["price"], "idx": idx})
            break
    for i in range(1, min(4, len(lows))):
        idx = lows[-i]["idx"]
        if idx < len(df)-1 and df["close"].iloc[idx+1] < lows[-i-1]["price"]:
            events.append({"type": "BOS_BEAR", "price": lows[-i-1]["price"], "idx": idx})
            break
    return events

def detect_order_blocks(df, n=5):
    obs = []
    c, o, h, l = df["close"].values, df["open"].values, df["high"].values, df["low"].values
    for i in range(3, len(df)-3):
        sz = h[i] - l[i]
        if sz == 0: continue
        if c[i] < o[i] and max(c[i+1:i+4]) - h[i] > sz * 0.5:
            obs.append({"type": "BULL_OB", "top": h[i], "bottom": l[i],
                        "mid": (h[i]+l[i])/2, "idx": i})
        if c[i] > o[i] and l[i] - min(c[i+1:i+4]) > sz * 0.5:
            obs.append({"type": "BEAR_OB", "top": h[i], "bottom": l[i],
                        "mid": (h[i]+l[i])/2, "idx": i})
    return obs[-n:] if len(obs) > n else obs

def detect_fvgs(df, n=5):
    fvgs = []
    for i in range(2, len(df)):
        if df["low"].iloc[i] > df["high"].iloc[i-2]:
            fvgs.append({"type": "BULL_FVG", "top": df["low"].iloc[i],
                         "bottom": df["high"].iloc[i-2], "idx": i})
        if df["high"].iloc[i] < df["low"].iloc[i-2]:
            fvgs.append({"type": "BEAR_FVG", "top": df["low"].iloc[i-2],
                         "bottom": df["high"].iloc[i], "idx": i})
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
        ax1.add_patch(plt.Rectangle((i-0.4, min(o,c)), 0.8, abs(c-o), color=col, zorder=3))

    for ob in obs:
        idx = ob["idx"]
        if idx >= n: continue
        col = "#1a6b3c" if ob["type"] == "BULL_OB" else "#6b1a1a"
        bc  = "#00e676" if ob["type"] == "BULL_OB" else "#ff5252"
        ax1.add_patch(plt.Rectangle((idx-0.5, ob["bottom"]), n-idx+2,
                                    ob["top"]-ob["bottom"], color=col, alpha=0.3, zorder=1))
        ax1.text(idx+0.5, ob["top"], "Bull OB" if ob["type"]=="BULL_OB" else "Bear OB",
                 color=bc, fontsize=6.5, va="bottom", zorder=5)

    for fvg in fvgs:
        idx = fvg["idx"]
        if idx >= n: continue
        col = "#1a3d6b" if fvg["type"] == "BULL_FVG" else "#6b4a1a"
        bc  = "#40c4ff" if fvg["type"] == "BULL_FVG" else "#ffab40"
        ax1.add_patch(plt.Rectangle((idx-2, fvg["bottom"]), n-idx+3,
                                    fvg["top"]-fvg["bottom"], color=col, alpha=0.35, zorder=1))
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
            ax1.text(max(0, ev["idx"]-2), ev["price"], "BOS",
                     color=col, fontsize=7, va="bottom", fontweight="bold")

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
    ax1.set_xticks([])
    ax1.set_xlim(-1, n+3); ax2.set_xlim(-1, n+3)
    for ax in (ax1, ax2):
        ax.tick_params(colors="#aaa", labelsize=7)
        for s in ax.spines.values(): s.set_color("#333")
    ax1.yaxis.tick_right()
    trend = detect_trend(df)
    col = "#26a69a" if trend=="BULLISH" else ("#ef5350" if trend=="BEARISH" else "#fff")
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
        obs = detect_order_blocks(df, 6); fvgs = detect_fvgs(df, 6)
        bos = detect_bos_choch(df);       sh, sl = find_swing_points(df, lb)
        charts[tf_key] = draw_smc_chart(df, tf_key.upper(), obs, fvgs, bos,
                                        sh[-8:], sl[-8:], price)
        print(f"    Chart {tf_key}: {len(obs)} OBs {len(fvgs)} FVGs {len(bos)} BOS")
    return charts

def build_smc_summary(data, ticker) -> str:
    lines = [
        f"=== BTCUSDT LIVE ===",
        f"Price: {ticker['price']:,.2f} | 24h: {ticker['change']:+.2f}%",
        f"High: {ticker['high24']:,.2f} | Low: {ticker['low24']:,.2f}",
        f"Vol: ${ticker['volume']/1e6:.1f}M | Session: {get_session()} | {ist_str()}", ""
    ]
    for tf_key, (df, lb) in data.items():
        trend = detect_trend(df)
        obs   = detect_order_blocks(df, 4); fvgs = detect_fvgs(df, 4)
        bos   = detect_bos_choch(df);       sh, sl = find_swing_points(df, lb)
        lines.append(f"--- {tf_key.upper()} | Trend: {trend} ---")
        for b in bos[-2:]:
            lines.append(f"  {b['type']}: {b['price']:,.2f}")
        bull_ob = [o for o in obs if o["type"] == "BULL_OB"]
        bear_ob = [o for o in obs if o["type"] == "BEAR_OB"]
        if bull_ob: lines.append(f"  Bull OB: {bull_ob[-1]['bottom']:,.2f}–{bull_ob[-1]['top']:,.2f}")
        if bear_ob: lines.append(f"  Bear OB: {bear_ob[-1]['bottom']:,.2f}–{bear_ob[-1]['top']:,.2f}")
        bf  = [f for f in fvgs if f["type"] == "BULL_FVG"]
        brf = [f for f in fvgs if f["type"] == "BEAR_FVG"]
        if bf:  lines.append(f"  Bull FVG: {bf[-1]['bottom']:,.2f}–{bf[-1]['top']:,.2f}")
        if brf: lines.append(f"  Bear FVG: {brf[-1]['bottom']:,.2f}–{brf[-1]['top']:,.2f}")
        if sh:  lines.append(f"  Swing High: {sh[-1]['price']:,.2f}")
        if sl:  lines.append(f"  Swing Low:  {sl[-1]['price']:,.2f}")
        lines.append("")
    return "\n".join(lines)

# ─── CLAUDE: NEW SIGNAL ───────────────────────────────────────────────────────
def analyze_with_claude(ticker, data) -> dict | None:
    client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    price   = ticker["price"]
    session = get_session()
    print("  Generating SMC charts...")
    charts  = generate_all_charts(data, price)
    summary = build_smc_summary(data, ticker)

    prompt = f"""{summary}

You are an expert SMC trader. Charts: Weekly, 4H, 1H, 5M — all have SMC drawings.

═══ STEP 1 — FIND ENTRY ZONE ═══
Entry MUST be at one of these — no exceptions:
  • 1H Order Block (OB) — last opposing candle before strong impulse move
  • 1H Fair Value Gap (FVG) — 3-candle price imbalance
  • 4H OB or FVG if no 1H zone is nearby current price
If price is NOT near any OB/FVG → set entry_type = PULLBACK, entry = zone midpoint

═══ STEP 2 — SET SL FROM THE ZONE ═══
SL goes just OUTSIDE the entry zone:
  • BUY at Bull OB/FVG → SL = 10–30 pts BELOW zone bottom
  • SELL at Bear OB/FVG → SL = 10–30 pts ABOVE zone top
This gives a natural SL of 50–300 pts — NEVER hundreds or thousands.
If SL would be 500+ pts away, you picked the WRONG zone — go back to Step 1.

═══ STEP 3 — SET TP FROM SL DISTANCE ═══
sl_dist = abs(entry - sl)
  • TP1 = entry ± (sl_dist × 2)   — must hit next OB/FVG or swing
  • TP2 = entry ± (sl_dist × 4)   — must hit major swing level

═══ STEP 4 — CONFIRM MULTI-TF ═══
  • Weekly direction must agree
  • 4H must show BOS in trade direction
  • 5M must show momentum/displacement
  • Session: {session}

ENTRY TYPE:
  • MARKET — price is inside the OB/FVG zone right now
  • PULLBACK — price must return to zone (entry_note = exact zone + what to look for)

You MUST give BUY or SELL. If no clean zone, give the best PULLBACK setup.

JSON only, no markdown:
{{
  "signal": "BUY"|"SELL",
  "entry": <OB or FVG midpoint>,
  "sl": <just outside zone>,
  "tp1": <entry ± sl_dist×2>,
  "tp2": <entry ± sl_dist×4>,
  "rr": "<1:X.X>",
  "entry_type": "MARKET"|"PULLBACK",
  "entry_note": "<zone + trigger or empty>",
  "bias": "BULLISH"|"BEARISH",
  "weekly_trend": "<weekly structure one line>",
  "structure_4h": "<4H BOS/CHoCH key levels>",
  "entry_zone": "<e.g. 61400–61600 Bear OB 1H>",
  "sl_distance": <points as number>,
  "confidence": "HIGH"|"MEDIUM"|"LOW",
  "session": "{session}"
}}"""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-5", max_tokens=800,
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

        # ── Sanity check only — log bad values, don't force-clamp ─────────────
        entry    = float(signal["entry"])
        sl       = float(signal["sl"])
        sl_dist  = abs(entry - sl)
        tp1_dist = abs(entry - float(signal["tp1"]))
        tp2_dist = abs(entry - float(signal["tp2"]))

        if sl_dist > 500:
            print(f"  [WARN] SL dist={sl_dist:.0f} pts — Claude picked bad entry zone, retrying...")
            return None   # return None so main loop retries next scan

        if tp1_dist < sl_dist * 1.5:
            print(f"  [WARN] TP1 too close (R:R {tp1_dist/sl_dist:.1f}) — bad signal")
            return None

        rr = tp2_dist / sl_dist if sl_dist > 0 else 0
        signal["rr"] = f"1:{rr:.1f}"
        print(f"  [OK] Entry:{entry:,.0f} SL:{sl:,.0f} ({sl_dist:.0f}pts) R:R {signal['rr']}")
        return signal

    except Exception as e:
        print(f"  [ERROR] Claude signal: {e}")
        return None

# ─── CLAUDE: TRADE UPDATE ─────────────────────────────────────────────────────
def claude_trade_update(ticker, data) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    price  = ticker["price"]
    df4    = data["4h"][0]; df1 = data["1h"][0]
    sh4, sl4 = find_swing_points(df4, 5)
    sh1, sl1 = find_swing_points(df1, 5)
    c4 = draw_smc_chart(df4, "4H", detect_order_blocks(df4,4), detect_fvgs(df4,4),
                        detect_bos_choch(df4), sh4[-6:], sl4[-6:], price)
    c1 = draw_smc_chart(df1, "1H", detect_order_blocks(df1,4), detect_fvgs(df1,4),
                        detect_bos_choch(df1), sh1[-6:], sl1[-6:], price)
    t = active_trade
    prompt = f"""Active {t['signal']} BTCUSDT trade.
Entry:{t['entry']:,.0f} | SL:{t['sl']:,.0f} | TP1:{t['tp1']:,.0f} | TP2:{t['tp2']:,.0f}
TP1 hit:{t['tp1_hit']} | Price:{price:,.2f} | Session:{get_session()}

Reply ONE word only:
HOLD — structure intact, trade valid
WAIT — minor pullback, no action
CLOSE — structure broken, exit now
NO_VOLUME — low volume, close trade"""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-5", max_tokens=15,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": c4}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": c1}},
                {"type": "text",  "text": prompt},
            ]}]
        )
        return msg.content[0].text.strip().upper().split()[0]
    except Exception as e:
        print(f"  [ERROR] Claude update: {e}")
        return "WAIT"

# ─── PRICE STATUS ─────────────────────────────────────────────────────────────
def check_price_status(price: float) -> str:
    t = active_trade
    if not t["signal"]: return "NONE"
    sig, sl, tp1, tp2 = t["signal"], t["sl"], t["tp1"], t["tp2"]
    if (sig=="SELL" and price>=sl)  or (sig=="BUY" and price<=sl):  return "SL_HIT"
    if (sig=="SELL" and price<=tp2) or (sig=="BUY" and price>=tp2): return "TP2_HIT"
    if not t["tp1_hit"]:
        if (sig=="SELL" and price<=tp1) or (sig=="BUY" and price>=tp1): return "TP1_HIT"
    return "RUNNING"

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

# ─── MESSAGE FORMATS ──────────────────────────────────────────────────────────
def fmt_signal(s: dict) -> str:
    e  = "🟢" if s["signal"] == "BUY" else "🔴"
    ci = {"HIGH": "🔥", "MEDIUM": "✨", "LOW": "⚡"}.get(s.get("confidence",""), "")
    el = f"🎯 Entry   <b>{s['entry']:,.0f}</b>"
    if s.get("entry_type") == "PULLBACK" and s.get("entry_note"):
        el += f"\n   ⏳ <i>{s['entry_note']}</i>"
    wk = s.get("weekly_trend",""); ez = s.get("entry_zone","")
    return (
        f"{e} <b>{s['signal']} — {SYMBOL}</b>  {ci}\n"
        f"🕐 {ist_str()}  |  📍 {s.get('session', get_session())}\n\n"
        f"{el}\n"
        f"🛑 SL       <b>{s['sl']:,.0f}</b>\n"
        f"✅ TP1    <b>{s['tp1']:,.0f}</b>\n"
        f"✅ TP2    <b>{s['tp2']:,.0f}</b>\n"
        f"📊 R:R    <b>{s.get('rr','—')}</b>\n\n"
        + (f"🗓 Weekly: <i>{wk}</i>\n" if wk else "")
        + (f"🔷 Zone:   <i>{ez}</i>\n" if ez else "")
        + f"\n<i>— Signal by CLEXER V3 —</i>\n"
          f"⚠️ <i>Not financial advice</i>"
    )

def fmt_update(status: str) -> str:
    t = active_trade
    lines = {
        "SL_HIT":    "🛑 <b>SL HIT</b>\nFinding next trade...",
        "TP2_HIT":   "🏆 <b>TP2 HIT — Trade Complete!</b>",
        "TP1_HIT":   f"✅ <b>TP1 HIT</b>\nSL → Breakeven ({t['entry']:,.0f})\nWaiting TP2 → <b>{t['tp2']:,.0f}</b>",
        "HOLD":      "📊 <b>HOLD</b> — Structure intact",
        "WAIT":      "⏳ <b>WAIT</b> — Trade running",
        "CLOSE":     "⚠️ <b>CLOSE NOW</b> — Structure broken",
        "NO_VOLUME": "📉 <b>CLOSE</b> — No volume",
    }
    body = lines.get(status, "⏳ Trade running — WAIT")
    return f"📡 <b>{SYMBOL} UPDATE</b>  {ist_str()}\n\n{body}\n\n<i>— CLEXER V3 —</i>"

# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────
COMMANDS_HELP = """🤖 <b>CLEXER V3 Commands</b>
━━━━━━━━━━━━━━━━━━━

📊 <b>INFO</b>
/status — Bot status + active trade
/price — Live BTC price
/trade — Active trade details
/history — Last 5 signals
/session — Current session info

🎯 <b>TRADE CONTROL</b>
/close — Close active trade manually
/sltobe — Move SL to breakeven
/setsl 61500 — Set new SL price
/settp1 60000 — Set new TP1 price
/settp2 58000 — Set new TP2 price

🤖 <b>BOT CONTROL</b>
/signal — Force scan NOW
/pause — Pause new signals
/resume — Resume signals
/setinterval 2 — Scan every 2 hours

/help — Show this menu"""

def handle_command(text: str, chat_id):
    global SCAN_INTERVAL_SECONDS
    parts = text.strip().split()
    cmd   = parts[0].lower().split("@")[0]

    if ADMIN_CHAT_ID and str(chat_id) != str(ADMIN_CHAT_ID):
        send_reply(chat_id, "⛔ Unauthorized.")
        return

    if cmd in ("/start", "/help"):
        send_reply(chat_id, COMMANDS_HELP)

    elif cmd == "/status":
        t  = active_trade
        st = "⏸ PAUSED" if bot_paused.is_set() else "▶️ RUNNING"
        trade_info = (
            f"{t['signal']} @ {t['entry']:,.0f}\n"
            f"SL:{t['sl']:,.0f} | TP1:{t['tp1']:,.0f} | TP2:{t['tp2']:,.0f}\n"
            f"TP1 Hit: {'✅' if t['tp1_hit'] else '❌'}"
        ) if t["signal"] else "No active trade"
        send_reply(chat_id,
            f"📊 <b>CLEXER V3 Status</b>\n\n"
            f"Bot: {st}\n"
            f"Session: {get_session()} {'✅' if is_trading_hours() else '⏸'}\n"
            f"IST: {ist_str()}\n"
            f"Interval: {SCAN_INTERVAL_SECONDS//3600}h\n\n"
            f"<b>Active Trade:</b>\n{trade_info}"
        )

    elif cmd == "/price":
        try:
            tk = get_ticker()
            send_reply(chat_id,
                f"💵 <b>BTCUSDT Live</b>\n\n"
                f"Price: <b>{tk['price']:,.2f}</b>\n"
                f"24h:   {tk['change']:+.2f}%\n"
                f"High:  {tk['high24']:,.2f}\n"
                f"Low:   {tk['low24']:,.2f}\n"
                f"Vol:   ${tk['volume']/1e6:.1f}M\n\n"
                f"🕐 {ist_str()}"
            )
        except Exception as e:
            send_reply(chat_id, f"❌ Error: {e}")

    elif cmd == "/trade":
        t = active_trade
        if not t["signal"]:
            send_reply(chat_id, "📭 No active trade right now.")
        else:
            send_reply(chat_id,
                f"📈 <b>Active Trade</b>\n\n"
                f"Signal: <b>{t['signal']} — {SYMBOL}</b>\n"
                f"Entry:  <b>{t['entry']:,.0f}</b>\n"
                f"SL:     <b>{t['sl']:,.0f}</b>\n"
                f"TP1:    <b>{t['tp1']:,.0f}</b> {'✅ HIT' if t['tp1_hit'] else '⏳'}\n"
                f"TP2:    <b>{t['tp2']:,.0f}</b>\n"
                + (f"Note: <i>{t['entry_note']}</i>" if t.get("entry_note") else "")
            )

    elif cmd == "/history":
        if not signal_history:
            send_reply(chat_id, "📭 No signal history yet.")
        else:
            lines = ["📜 <b>Last Signals</b>\n"]
            for s in reversed(signal_history[-5:]):
                e = "🟢" if s["signal"]=="BUY" else "🔴"
                lines.append(
                    f"{e} {s['signal']} @ {s['entry']:,.0f}\n"
                    f"   SL:{s['sl']:,.0f} TP1:{s['tp1']:,.0f} TP2:{s['tp2']:,.0f}\n"
                    f"   🕐 {s['time']}\n"
                )
            send_reply(chat_id, "\n".join(lines))

    elif cmd == "/session":
        s = get_session()
        send_reply(chat_id,
            f"📍 <b>Session Info</b>\n\n"
            f"Current: <b>{s}</b> {'✅ Trading' if is_trading_hours() else '⏸ Waiting'}\n\n"
            f"🇬🇧 London:   07:30–16:30 IST\n"
            f"🇺🇸 New York: 18:30–01:00 IST\n"
            f"😴 Sleep:    01:00–07:29 IST\n\n"
            f"🕐 {ist_str()}"
        )

    elif cmd == "/close":
        t = active_trade
        if not t["signal"]:
            send_reply(chat_id, "📭 No active trade to close.")
        else:
            info = f"{t['signal']} @ {t['entry']:,.0f}"
            reset_trade()
            send_telegram(f"⛔ <b>Trade Manually Closed</b>\n{info}\n\n<i>— CLEXER V3 —</i>")
            send_reply(chat_id, f"✅ Closed: {info}")
            force_scan.set()

    elif cmd == "/sltobe":
        t = active_trade
        if not t["signal"]:
            send_reply(chat_id, "📭 No active trade.")
        else:
            old_sl = t["sl"]
            active_trade["sl"] = active_trade["entry"]
            send_telegram(
                f"🔄 <b>SL → Breakeven</b>\n\n"
                f"Old SL: {old_sl:,.0f}\n"
                f"New SL: <b>{active_trade['entry']:,.0f}</b> (Entry)\n\n"
                f"<i>— CLEXER V3 —</i>"
            )
            send_reply(chat_id, f"✅ SL → breakeven: {active_trade['entry']:,.0f}")

    elif cmd == "/setsl":
        if not active_trade["signal"]:
            send_reply(chat_id, "📭 No active trade.")
        elif len(parts) < 2:
            send_reply(chat_id, "Usage: /setsl 61500")
        else:
            try:
                new_sl = float(parts[1].replace(",",""))
                old_sl = active_trade["sl"]
                active_trade["sl"] = new_sl
                send_telegram(f"🔄 <b>SL Updated</b>\nOld:{old_sl:,.0f} → New:<b>{new_sl:,.0f}</b>\n\n<i>— CLEXER V3 —</i>")
                send_reply(chat_id, f"✅ SL set to {new_sl:,.0f}")
            except ValueError:
                send_reply(chat_id, "❌ Use: /setsl 61500")

    elif cmd == "/settp1":
        if not active_trade["signal"]:
            send_reply(chat_id, "📭 No active trade.")
        elif len(parts) < 2:
            send_reply(chat_id, "Usage: /settp1 60000")
        else:
            try:
                new_tp = float(parts[1].replace(",",""))
                active_trade["tp1"] = new_tp
                send_telegram(f"🔄 <b>TP1 → {new_tp:,.0f}</b>\n\n<i>— CLEXER V3 —</i>")
                send_reply(chat_id, f"✅ TP1 set to {new_tp:,.0f}")
            except ValueError:
                send_reply(chat_id, "❌ Use: /settp1 60000")

    elif cmd == "/settp2":
        if not active_trade["signal"]:
            send_reply(chat_id, "📭 No active trade.")
        elif len(parts) < 2:
            send_reply(chat_id, "Usage: /settp2 58000")
        else:
            try:
                new_tp = float(parts[1].replace(",",""))
                active_trade["tp2"] = new_tp
                send_telegram(f"🔄 <b>TP2 → {new_tp:,.0f}</b>\n\n<i>— CLEXER V3 —</i>")
                send_reply(chat_id, f"✅ TP2 set to {new_tp:,.0f}")
            except ValueError:
                send_reply(chat_id, "❌ Use: /settp2 58000")

    elif cmd == "/signal":
        if bot_paused.is_set():
            send_reply(chat_id, "⏸ Bot is paused. Use /resume first.")
        else:
            send_reply(chat_id, "🔍 Forcing scan now... Check channel in ~30 seconds.")
            force_scan.set()

    elif cmd == "/pause":
        bot_paused.set()
        send_telegram("⏸ <b>Bot Paused</b> — No new signals\n\n<i>— CLEXER V3 —</i>")
        send_reply(chat_id, "✅ Bot paused. Use /resume to restart.")

    elif cmd == "/resume":
        bot_paused.clear()
        send_telegram("▶️ <b>Bot Resumed</b>\n\n<i>— CLEXER V3 —</i>")
        send_reply(chat_id, "✅ Bot resumed.")

    elif cmd == "/setinterval":
        if len(parts) < 2:
            send_reply(chat_id, f"Usage: /setinterval 2\nCurrent: {SCAN_INTERVAL_SECONDS//3600}h")
        else:
            try:
                hours = float(parts[1])
                if hours < 1 or hours > 24:
                    send_reply(chat_id, "❌ Must be 1–24 hours.")
                else:
                    SCAN_INTERVAL_SECONDS = int(hours * 3600)
                    send_reply(chat_id, f"✅ Interval set to {hours}h")
            except ValueError:
                send_reply(chat_id, "❌ Use: /setinterval 2")

    else:
        send_reply(chat_id, f"❓ Unknown: {cmd}\nUse /help")

# ─── COMMAND LISTENER THREAD ──────────────────────────────────────────────────
def command_listener():
    global last_update_id
    print("[CMD] Command listener started")

    # Clear webhook so getUpdates works properly
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook",
                     timeout=10)
        print("[CMD] Webhook cleared OK")
    except Exception as e:
        print(f"[CMD] Webhook clear error: {e}")

    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"offset": last_update_id + 1, "timeout": 20,
                        "allowed_updates": ["message"]},
                timeout=25
            )
            data = r.json()
            if not data.get("ok"):
                print(f"  [CMD] getUpdates error: {data}")
                time.sleep(5)
                continue
            for upd in data.get("result", []):
                last_update_id = upd["update_id"]
                msg   = upd.get("message", {})
                text  = msg.get("text", "")
                cid   = msg.get("chat", {}).get("id")
                uname = msg.get("from", {}).get("username", "?")
                if cid:
                    print(f"  [CMD] @{uname} (ID:{cid}): {text[:40]}")
                if text and text.startswith("/") and cid:
                    handle_command(text, cid)
        except Exception as e:
            print(f"  [CMD] Listener error: {e}")
        time.sleep(2)

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    print(f"[CLEXER V3] Starting | {SYMBOL} | {SCAN_INTERVAL_SECONDS}s interval")
    print(f"[CLEXER V3] Sessions: London 07:30–16:30 IST | NY 18:30–01:00 IST")

    threading.Thread(target=command_listener, daemon=True).start()

    send_telegram(
        f"🤖 <b>CLEXER V3 Online</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📊 Weekly + 4H + 1H + 5M Vision\n"
        f"🔷 Auto OBs, FVGs, BOS/CHoCH\n"
        f"⏰ London + NY sessions only (IST)\n"
        f"💬 Send /help for commands\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<i>— CLEXER V3 —</i>"
    )

    while True:
        try:
            if bot_paused.is_set():
                time.sleep(60)
                continue

            print(f"\n[{now_ist().strftime('%H:%M IST')}] Session: {get_session()}")

            if is_ist_sleep():
                print("  [SLEEP] 01:00–07:29 IST — waiting")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            if not is_trading_hours():
                print(f"  [WAIT] {get_session()} — not London/NY")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Fetch all timeframes
            print("  Fetching candles...")
            data = {}
            for key, interval, limit, lb in [
                ("weekly", "1w",  52, 5),
                ("4h",     "4h", 200, 5),
                ("1h",     "1h", 100, 5),
                ("5m",     "5m",  50, 3),
            ]:
                data[key] = (get_candles(interval, limit), lb)
                time.sleep(0.3)
                print(f"    {key}: {len(data[key][0])} candles")

            ticker = get_ticker()
            price  = ticker["price"]
            print(f"  Price: {price:,.2f} | {ticker['change']:+.2f}%")

            # Active trade check
            if active_trade["signal"]:
                status = check_price_status(price)
                print(f"  Trade: {active_trade['signal']} | Status: {status}")

                if status == "TP1_HIT":
                    active_trade["tp1_hit"] = True
                    active_trade["sl"]      = active_trade["entry"]   # breakeven
                    send_telegram(fmt_update("TP1_HIT"))

                elif status in ("SL_HIT", "TP2_HIT"):
                    send_telegram(fmt_update(status))
                    reset_trade()
                    signal = analyze_with_claude(ticker, data)
                    if signal:
                        send_telegram(fmt_signal(signal))
                        set_trade(signal)

                else:   # RUNNING
                    print("  Getting Claude trade update...")
                    cs = claude_trade_update(ticker, data)
                    print(f"  Claude: {cs}")
                    send_telegram(fmt_update(cs))
                    if cs in ("CLOSE", "NO_VOLUME"):
                        reset_trade()
                        signal = analyze_with_claude(ticker, data)
                        if signal:
                            send_telegram(fmt_signal(signal))
                            set_trade(signal)

            else:   # No active trade
                print("  No active trade. Analyzing...")
                signal = analyze_with_claude(ticker, data)
                if signal:
                    send_telegram(fmt_signal(signal))
                    set_trade(signal)
                    print(f"  [SENT] {signal['signal']} R:R:{signal.get('rr','?')} {signal.get('confidence','?')}")
                else:
                    print("  No valid signal found — waiting for next scan")

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("🛑 <b>CLEXER V3 Stopped</b>\n\n<i>— CLEXER —</i>")
            break
        except Exception as e:
            print(f"  [ERROR] {e}")
            import traceback; traceback.print_exc()

        # Smart sleep — wakes instantly on /signal command
        waited = 0
        while waited < SCAN_INTERVAL_SECONDS:
            if force_scan.is_set():
                force_scan.clear()
                print("  [CMD] Force scan triggered")
                break
            time.sleep(30)
            waited += 30

if __name__ == "__main__":
    main()
