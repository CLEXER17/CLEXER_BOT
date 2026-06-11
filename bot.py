"""
CLEXER Signal Bot V4 — Adaptive SMC + All Market Scenarios
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
    "entry_hit": False,
    "sl_wicked": False,     # SL briefly touched but not closed through (stop hunt)
    "scan_count": 0,        # how many scans this trade has been active
}
signal_history = []
force_scan     = threading.Event()
bot_paused     = threading.Event()
last_update_id = 0

# ─── ADAPTIVE TRADE STATS ─────────────────────────────────────────────────────
# Tracks losses + cooldown to avoid overtrading after bad conditions
trade_stats = {
    "consecutive_sl":  0,     # SL hits in a row (resets on TP hit)
    "cooldown_scans":  0,     # scans to skip before next signal
    "total_sl":        0,
    "total_tp1":       0,
    "total_tp2":       0,
    "total_signals":   0,
    "missed_entries":  0,     # PULLBACK entries price never reached
    "stop_hunts":      0,     # times SL wicked but trade continued
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
        "signal":     s["signal"],
        "entry":      s["entry"],
        "sl":         s["sl"],
        "tp1":        s["tp1"],
        "tp2":        s["tp2"],
        "tp1_hit":    False,
        "entry_type": s.get("entry_type", "MARKET"),
        "entry_note": s.get("entry_note", ""),
        "entry_hit":  s.get("entry_type", "MARKET") == "MARKET",
        "sl_wicked":  False,
        "scan_count": 0,
    }
    trade_stats["total_signals"] += 1
    signal_history.append({
        "time": ist_str(), "signal": s["signal"],
        "entry": s["entry"], "sl": s["sl"],
        "tp1": s["tp1"], "tp2": s["tp2"],
        "rr": s.get("rr","?"), "confidence": s.get("confidence","?"),
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
    if len(h)<2 or len(l)<2: return "NEUTRAL"
    hp = [x["price"] for x in h[-2:]]; lp = [x["price"] for x in l[-2:]]
    if hp[1]>hp[0] and lp[1]>lp[0]: return "BULLISH"
    if hp[1]<hp[0] and lp[1]<lp[0]: return "BEARISH"
    return "NEUTRAL"

def detect_bos_choch(df):
    events = []
    h, l = find_swing_points(df, 3)
    if len(h)<2 or len(l)<2: return events
    for i in range(1, min(4, len(h))):
        idx = h[-i]["idx"]
        if idx < len(df)-1 and df["close"].iloc[idx+1] > h[-i-1]["price"]:
            events.append({"type":"BOS_BULL","price":h[-i-1]["price"],"idx":idx}); break
    for i in range(1, min(4, len(l))):
        idx = l[-i]["idx"]
        if idx < len(df)-1 and df["close"].iloc[idx+1] < l[-i-1]["price"]:
            events.append({"type":"BOS_BEAR","price":l[-i-1]["price"],"idx":idx}); break
    return events

def detect_order_blocks(df, n=5):
    obs = []
    c,o,h,l = df["close"].values, df["open"].values, df["high"].values, df["low"].values
    for i in range(3, len(df)-3):
        sz = h[i]-l[i]
        if sz==0: continue
        if c[i]<o[i] and max(c[i+1:i+4])-h[i] > sz*0.5:
            obs.append({"type":"BULL_OB","top":h[i],"bottom":l[i],"mid":(h[i]+l[i])/2,"idx":i})
        if c[i]>o[i] and l[i]-min(c[i+1:i+4]) > sz*0.5:
            obs.append({"type":"BEAR_OB","top":h[i],"bottom":l[i],"mid":(h[i]+l[i])/2,"idx":i})
    return obs[-n:] if len(obs)>n else obs

def detect_fvgs(df, n=5):
    fvgs = []
    for i in range(2, len(df)):
        if df["low"].iloc[i] > df["high"].iloc[i-2]:
            fvgs.append({"type":"BULL_FVG","top":df["low"].iloc[i],"bottom":df["high"].iloc[i-2],"idx":i})
        if df["high"].iloc[i] < df["low"].iloc[i-2]:
            fvgs.append({"type":"BEAR_FVG","top":df["low"].iloc[i-2],"bottom":df["high"].iloc[i],"idx":i})
    return fvgs[-n:] if len(fvgs)>n else fvgs

# ─── CHART DRAWING ────────────────────────────────────────────────────────────
def draw_smc_chart(df, tf, obs, fvgs, bos_events, s_highs, s_lows, price=None) -> str:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18,10),
                                   gridspec_kw={"height_ratios":[4,1]},
                                   facecolor="#0d0d0d")
    ax1.set_facecolor("#0d0d0d"); ax2.set_facecolor("#0d0d0d")
    n = len(df)
    for i,(_, row) in enumerate(df.iterrows()):
        o,h,l,c = row["open"],row["high"],row["low"],row["close"]
        col = "#26a69a" if c>=o else "#ef5350"
        ax1.plot([i,i],[l,h],color=col,linewidth=0.8,zorder=2)
        ax1.add_patch(plt.Rectangle((i-0.4,min(o,c)),0.8,abs(c-o),color=col,zorder=3))
    for ob in obs:
        idx=ob["idx"]
        if idx>=n: continue
        col="#1a6b3c" if ob["type"]=="BULL_OB" else "#6b1a1a"
        bc="#00e676" if ob["type"]=="BULL_OB" else "#ff5252"
        ax1.add_patch(plt.Rectangle((idx-0.5,ob["bottom"]),n-idx+2,ob["top"]-ob["bottom"],color=col,alpha=0.3,zorder=1))
        ax1.text(idx+0.5,ob["top"],"Bull OB" if ob["type"]=="BULL_OB" else "Bear OB",color=bc,fontsize=6.5,va="bottom",zorder=5)
    for fvg in fvgs:
        idx=fvg["idx"]
        if idx>=n: continue
        col="#1a3d6b" if fvg["type"]=="BULL_FVG" else "#6b4a1a"
        bc="#40c4ff" if fvg["type"]=="BULL_FVG" else "#ffab40"
        ax1.add_patch(plt.Rectangle((idx-2,fvg["bottom"]),n-idx+3,fvg["top"]-fvg["bottom"],color=col,alpha=0.35,zorder=1))
        ax1.text(idx+0.5,fvg["top"],"Bull FVG" if fvg["type"]=="BULL_FVG" else "Bear FVG",color=bc,fontsize=6.5,va="bottom",zorder=5)
    for sh in s_highs[-6:]:
        if sh["idx"]<n:
            ax1.plot(sh["idx"],sh["price"],"^",color="#ffeb3b",markersize=5,zorder=6)
            ax1.axhline(sh["price"],color="#ffeb3b",linewidth=0.5,linestyle=":",alpha=0.4)
    for sl in s_lows[-6:]:
        if sl["idx"]<n:
            ax1.plot(sl["idx"],sl["price"],"v",color="#ff9800",markersize=5,zorder=6)
            ax1.axhline(sl["price"],color="#ff9800",linewidth=0.5,linestyle=":",alpha=0.4)
    for ev in bos_events:
        if ev["idx"]<n:
            col="#b2ff59" if "BULL" in ev["type"] else "#ff4081"
            ax1.axhline(ev["price"],color=col,linewidth=1.0,linestyle="-.",alpha=0.7)
            ax1.text(max(0,ev["idx"]-2),ev["price"],"BOS",color=col,fontsize=7,va="bottom",fontweight="bold")
    if price:
        ax1.axhline(price,color="#fff",linewidth=1.2,linestyle="--",alpha=0.9)
        ax1.text(n-1,price,f" {price:,.0f}",color="#fff",fontsize=8,va="center")
    for i,(_, row) in enumerate(df.iterrows()):
        ax2.bar(i,row["vol"],color="#26a69a" if row["close"]>=row["open"] else "#ef5350",alpha=0.7,width=0.8)
    step=max(1,n//10)
    ax2.set_xticks(np.arange(n)[::step])
    ax2.set_xticklabels([df.index[i].strftime("%m/%d %H:%M") for i in range(0,n,step)],rotation=30,fontsize=6,color="#aaa")
    ax1.set_xticks([]); ax1.set_xlim(-1,n+3); ax2.set_xlim(-1,n+3)
    for ax in (ax1,ax2):
        ax.tick_params(colors="#aaa",labelsize=7)
        for s in ax.spines.values(): s.set_color("#333")
    ax1.yaxis.tick_right()
    trend=detect_trend(df)
    col="#26a69a" if trend=="BULLISH" else ("#ef5350" if trend=="BEARISH" else "#fff")
    ax1.set_title(f"{SYMBOL} {tf}  |  {trend}",color=col,fontsize=11,fontweight="bold",loc="left",pad=6)
    ax1.legend(handles=[
        mpatches.Patch(color="#1a6b3c",label="Bull OB"), mpatches.Patch(color="#6b1a1a",label="Bear OB"),
        mpatches.Patch(color="#1a3d6b",label="Bull FVG"), mpatches.Patch(color="#6b4a1a",label="Bear FVG"),
    ],loc="upper left",facecolor="#1a1a1a",edgecolor="#444",labelcolor="#ccc",fontsize=7,framealpha=0.8)
    plt.tight_layout(pad=0.5)
    buf=BytesIO()
    plt.savefig(buf,format="png",dpi=120,bbox_inches="tight",facecolor="#0d0d0d")
    plt.close(fig); buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def generate_all_charts(data, price) -> dict:
    charts = {}
    for tf_key,(df,lb) in data.items():
        obs=detect_order_blocks(df,6); fvgs=detect_fvgs(df,6)
        bos=detect_bos_choch(df); sh,sl=find_swing_points(df,lb)
        charts[tf_key]=draw_smc_chart(df,tf_key.upper(),obs,fvgs,bos,sh[-8:],sl[-8:],price)
        print(f"    Chart {tf_key}: {len(obs)} OBs {len(fvgs)} FVGs {len(bos)} BOS")
    return charts

def build_smc_summary(data, ticker) -> str:
    lines=[f"=== BTCUSDT LIVE ===",
           f"Price: {ticker['price']:,.2f} | 24h: {ticker['change']:+.2f}%",
           f"Vol: ${ticker['volume']/1e6:.1f}M | Session: {get_session()} | {ist_str()}",""]
    for tf_key,(df,lb) in data.items():
        trend=detect_trend(df); obs=detect_order_blocks(df,4); fvgs=detect_fvgs(df,4)
        bos=detect_bos_choch(df); sh,sl=find_swing_points(df,lb)
        lines.append(f"--- {tf_key.upper()} | {trend} ---")
        for b in bos[-2:]: lines.append(f"  {b['type']}: {b['price']:,.2f}")
        bull_ob=[o for o in obs if o["type"]=="BULL_OB"]
        bear_ob=[o for o in obs if o["type"]=="BEAR_OB"]
        if bull_ob: lines.append(f"  Bull OB: {bull_ob[-1]['bottom']:,.2f}–{bull_ob[-1]['top']:,.2f}")
        if bear_ob: lines.append(f"  Bear OB: {bear_ob[-1]['bottom']:,.2f}–{bear_ob[-1]['top']:,.2f}")
        bf=[f for f in fvgs if f["type"]=="BULL_FVG"]; brf=[f for f in fvgs if f["type"]=="BEAR_FVG"]
        if bf:  lines.append(f"  Bull FVG: {bf[-1]['bottom']:,.2f}–{bf[-1]['top']:,.2f}")
        if brf: lines.append(f"  Bear FVG: {brf[-1]['bottom']:,.2f}–{brf[-1]['top']:,.2f}")
        if sh: lines.append(f"  Swing High: {sh[-1]['price']:,.2f}")
        if sl: lines.append(f"  Swing Low:  {sl[-1]['price']:,.2f}")
        lines.append("")
    return "\n".join(lines)

# ─── ADAPTIVE SCENARIO DETECTION ─────────────────────────────────────────────

def detect_stop_hunt(df_5m) -> bool:
    """
    STOP HUNT: SL was wicked by a wick but 5M candle CLOSED back inside trade range.
    Returns True if latest candle looks like a stop hunt.
    """
    t = active_trade
    if not t["signal"] or not t["entry_hit"]: return False
    sig = t["signal"]; sl = t["sl"]; entry = t["entry"]

    # Check last 3 candles for wick past SL but close back inside
    for i in range(-3, 0):
        row = df_5m.iloc[i]
        if sig == "BUY":
            # Wick below SL but closed ABOVE SL
            if row["low"] < sl and row["close"] > sl:
                reversal = row["close"] - row["low"]
                if reversal > 100:  # closed back up 100+ pts
                    print(f"  [STOP HUNT] Wick to {row['low']:,.0f} below SL {sl:,.0f} but closed at {row['close']:,.0f}")
                    return True
        else:  # SELL
            # Wick above SL but closed BELOW SL
            if row["high"] > sl and row["close"] < sl:
                reversal = row["high"] - row["close"]
                if reversal > 100:
                    print(f"  [STOP HUNT] Wick to {row['high']:,.0f} above SL {sl:,.0f} but closed at {row['close']:,.0f}")
                    return True
    return False

def detect_entry_missed(price: float) -> bool:
    """
    MISSED ENTRY: PULLBACK signal where price never came to entry
    and has now moved 1000+ pts PAST TP1 in the trade direction.
    Trade is over without us — cancel and find new setup.
    """
    t = active_trade
    if t["entry_hit"] or t["entry_type"] != "PULLBACK": return False
    sig = t["signal"]; tp1 = t["tp1"]
    if sig == "BUY"  and price >= tp1 + 500: return True
    if sig == "SELL" and price <= tp1 - 500: return True
    return False

def detect_entry_invalidated(price: float, df_4h) -> bool:
    """
    ENTRY ZONE INVALIDATED: For PULLBACK signal, price moved so far
    from entry in the WRONG direction that the setup is no longer valid.
    """
    t = active_trade
    if t["entry_hit"]: return False
    sig = t["signal"]; sl = t["sl"]
    # If price closed THROUGH the SL before even reaching entry — invalidated
    last_close = df_4h["close"].iloc[-1]
    if sig == "BUY"  and last_close < sl: return True
    if sig == "SELL" and last_close > sl: return True
    return False

def required_confidence_after_losses() -> str:
    """Returns minimum confidence level required based on consecutive SL hits"""
    n = trade_stats["consecutive_sl"]
    if n >= 3: return "HIGH"    # 3+ losses: only HIGH confidence
    if n >= 2: return "HIGH"    # 2 losses: HIGH only
    if n >= 1: return "MEDIUM"  # 1 loss: MEDIUM or HIGH
    return "LOW"                # No recent loss: take any signal

def should_wait_for_pattern() -> bool:
    """True if bot should skip this scan and wait for cleaner pattern"""
    return trade_stats["cooldown_scans"] > 0

# ─── CLAUDE: NEW SIGNAL ───────────────────────────────────────────────────────
def analyze_with_claude(ticker, data, force_confidence=None) -> dict | None:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    price  = ticker["price"]
    min_conf = force_confidence or required_confidence_after_losses()
    print(f"  Min confidence required: {min_conf}")
    print("  Generating SMC charts...")
    charts  = generate_all_charts(data, price)
    summary = build_smc_summary(data, ticker)

    conf_instruction = ""
    if min_conf == "HIGH":
        conf_instruction = "\n⚠️ IMPORTANT: Only return HIGH confidence signals. If setup is not perfect, still give BUY/SELL but set confidence=LOW and entry_type=PULLBACK."

    prompt = f"""{summary}

You are an expert SMC trader. Charts: Weekly, 4H, 1H, 5M — all have SMC drawings.
Session: {get_session()}{conf_instruction}

ENTRY RULES — entry MUST be at a real OB or FVG zone:
  - 1H OB (Order Block): last opposing candle before strong impulse
  - 1H FVG (Fair Value Gap): 3-candle imbalance
  - 4H OB/FVG if no 1H zone near price

SL: just OUTSIDE the entry zone (50-300 pts max).
TP1: entry +/- (sl_dist x 2) -- must align with opposing OB/FVG/swing.
TP2: entry +/- (sl_dist x 4) -- must align with MAJOR swing level.

ENTRY TYPE:
  - MARKET: price is INSIDE zone right now -- enter immediately
  - PULLBACK: price must travel to zone first

CRITICAL PULLBACK RULES (read carefully):
  If PULLBACK: TP1 and TP2 MUST be BEYOND current price in the final move direction.
  BUY pullback example: current=62000, entry=61150, TP1 must be ABOVE 62000 not below it.
  SELL pullback example: current=60000, entry=60850, TP1 must be BELOW 60000 not above it.
  NEVER set TP1 between entry and current price -- it will trigger instantly before trade starts.
  entry_note must describe the ENTRY ZONE only, not a different price level.

You MUST give BUY or SELL. No excuses.

JSON only, no markdown:
{{
  "signal": "BUY"|"SELL",
  "entry": <number>,
  "sl": <number>,
  "tp1": <number>,
  "tp2": <number>,
  "rr": "<1:X.X>",
  "entry_type": "MARKET"|"PULLBACK",
  "entry_note": "<zone + rejection trigger or empty>",
  "bias": "BULLISH"|"BEARISH",
  "weekly_trend": "<weekly one line>",
  "structure_4h": "<4H BOS/CHoCH>",
  "entry_zone": "<e.g. 61400–61600 Bear OB 1H>",
  "confidence": "HIGH"|"MEDIUM"|"LOW",
  "session": "{get_session()}"
}}"""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-5", max_tokens=800,
            messages=[{"role":"user","content":[
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":charts["weekly"]}},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":charts["4h"]}},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":charts["1h"]}},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":charts["5m"]}},
                {"type":"text","text":prompt},
            ]}]
        )
        raw    = msg.content[0].text.strip().replace("```json","").replace("```","").strip()
        signal = json.loads(raw)

        entry   = float(signal["entry"])
        sl      = float(signal["sl"])
        sl_dist = abs(entry - sl)

        # Reject if SL too wide (wrong zone picked)
        if sl_dist > 500:
            print(f"  [REJECT] SL dist={sl_dist:.0f} pts — bad zone, skip")
            return None

        # Reject PULLBACK if TP1 is between entry and current price (will trigger instantly)
        etype = signal.get("entry_type", "MARKET")
        tp1   = float(signal["tp1"])
        if etype == "PULLBACK":
            sig = signal["signal"]
            if sig == "BUY" and tp1 <= price:
                print(f"  [REJECT] PULLBACK BUY TP1={tp1:,.0f} is below current price {price:,.0f} — would trigger instantly")
                # Auto-fix: shift TP1 and TP2 above current price
                signal["tp1"] = round(price + sl_dist * 2, -1)
                signal["tp2"] = round(price + sl_dist * 4, -1)
                print(f"  [FIX] TP1 -> {signal['tp1']:,.0f}  TP2 -> {signal['tp2']:,.0f}")
            elif sig == "SELL" and tp1 >= price:
                print(f"  [REJECT] PULLBACK SELL TP1={tp1:,.0f} is above current price {price:,.0f} — would trigger instantly")
                signal["tp1"] = round(price - sl_dist * 2, -1)
                signal["tp2"] = round(price - sl_dist * 4, -1)
                print(f"  [FIX] TP1 -> {signal['tp1']:,.0f}  TP2 -> {signal['tp2']:,.0f}")

        # Reject if min confidence not met
        conf = signal.get("confidence","LOW")
        conf_rank = {"HIGH":3,"MEDIUM":2,"LOW":1}
        if conf_rank.get(conf,1) < conf_rank.get(min_conf,1):
            print(f"  [SKIP] Confidence {conf} < required {min_conf} after {trade_stats['consecutive_sl']} losses")
            return None

        tp2_dist = abs(entry - float(signal["tp2"]))
        rr = tp2_dist / sl_dist if sl_dist > 0 else 0
        signal["rr"] = f"1:{rr:.1f}"
        print(f"  [OK] {signal['signal']} Entry:{entry:,.0f} SL:{sl:,.0f} ({sl_dist:.0f}pts) R:R:{signal['rr']} Conf:{conf}")
        return signal

    except Exception as e:
        print(f"  [ERROR] Claude signal: {e}")
        return None

# ─── CLAUDE: TRADE UPDATE ─────────────────────────────────────────────────────
def claude_trade_update(ticker, data) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    price  = ticker["price"]
    df4=data["4h"][0]; df1=data["1h"][0]
    sh4,sl4=find_swing_points(df4,5); sh1,sl1=find_swing_points(df1,5)
    c4=draw_smc_chart(df4,"4H",detect_order_blocks(df4,4),detect_fvgs(df4,4),detect_bos_choch(df4),sh4[-6:],sl4[-6:],price)
    c1=draw_smc_chart(df1,"1H",detect_order_blocks(df1,4),detect_fvgs(df1,4),detect_bos_choch(df1),sh1[-6:],sl1[-6:],price)
    t = active_trade
    prompt = f"""Active {t['signal']} BTCUSDT trade.
Entry:{t['entry']:,.0f} | SL:{t['sl']:,.0f} | TP1:{t['tp1']:,.0f} | TP2:{t['tp2']:,.0f}
TP1 hit:{t['tp1_hit']} | Price:{price:,.2f} | Session:{get_session()}
Scans active: {t['scan_count']}

Reply ONE word:
HOLD — structure intact, trade valid
WAIT — minor pullback, no action
CLOSE — structure broken against trade, exit now
NO_VOLUME — low volume/flat market, exit"""
    try:
        msg = client.messages.create(
            model="claude-sonnet-4-5", max_tokens=15,
            messages=[{"role":"user","content":[
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":c4}},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":c1}},
                {"type":"text","text":prompt},
            ]}]
        )
        return msg.content[0].text.strip().upper().split()[0]
    except Exception as e:
        print(f"  [ERROR] Claude update: {e}"); return "WAIT"

# ─── PRICE STATUS CHECK ───────────────────────────────────────────────────────
def check_price_status(price: float, df_5m=None) -> str:
    t = active_trade
    if not t["signal"]: return "NONE"
    sig,sl,tp1,tp2,entry = t["signal"],t["sl"],t["tp1"],t["tp2"],t["entry"]

    # ── PULLBACK: not yet entered ─────────────────────────────────────────────
    if not t["entry_hit"]:
        # SL hit before entry = setup invalidated
        if (sig=="BUY" and price<=sl) or (sig=="SELL" and price>=sl): return "SETUP_INVALID"
        # Price reached entry zone
        tol = abs(entry - sl) * 0.3  # 30% of SL distance as tolerance
        if (sig=="BUY" and price<=entry+tol) or (sig=="SELL" and price>=entry-tol):
            active_trade["entry_hit"] = True
            print(f"  [ENTRY HIT] {sig} entry {entry:,.0f} reached (price={price:,.2f})")
        else:
            return "WAITING_ENTRY"

    # ── Entry hit — check stop hunt first ─────────────────────────────────────
    sl_breach = (sig=="SELL" and price>=sl) or (sig=="BUY" and price<=sl)
    if sl_breach and df_5m is not None and not t["sl_wicked"]:
        if detect_stop_hunt(df_5m):
            active_trade["sl_wicked"] = True
            trade_stats["stop_hunts"] += 1
            return "STOP_HUNT"   # SL wicked, likely stop hunt — hold for now

    # ── Normal SL / TP checks ─────────────────────────────────────────────────
    if (sig=="SELL" and price>=sl) or (sig=="BUY" and price<=sl): return "SL_HIT"
    if (sig=="SELL" and price<=tp2) or (sig=="BUY" and price>=tp2): return "TP2_HIT"
    if not t["tp1_hit"]:
        if (sig=="SELL" and price<=tp1) or (sig=="BUY" and price>=tp1): return "TP1_HIT"
    return "RUNNING"

# ─── AFTER-SL LOGIC ──────────────────────────────────────────────────────────
def handle_sl_hit(ticker, data):
    """Adaptive logic after SL hit — decides whether to re-enter or wait"""
    trade_stats["total_sl"] += 1
    trade_stats["consecutive_sl"] += 1
    n = trade_stats["consecutive_sl"]

    print(f"  [SL HIT] Consecutive SLs: {n}")

    if n >= 3:
        # 3+ consecutive losses — pause 2 scans, notify, wait for STRONG pattern
        trade_stats["cooldown_scans"] = 2
        msg = (
            f"🛑 <b>SL HIT</b>\n\n"
            f"⚠️ <b>3 consecutive losses — pausing</b>\n"
            f"Waiting 2 scans for clean pattern to form\n"
            f"Will only enter HIGH confidence setups\n\n"
            f"<i>— CLEXER V4 —</i>"
        )
        send_telegram(msg)
        print("  [COOLDOWN] 2 scans — too many losses")
        return

    if n == 2:
        # 2 consecutive losses — pause 1 scan, require HIGH confidence
        trade_stats["cooldown_scans"] = 1
        msg = (
            f"🛑 <b>SL HIT</b>  ({n} in a row)\n\n"
            f"⏸ Waiting 1 scan for structure to reset\n"
            f"Next signal must be HIGH confidence only\n\n"
            f"<i>— CLEXER V4 —</i>"
        )
        send_telegram(msg)
        print("  [COOLDOWN] 1 scan — 2 losses in a row")
        return

    # 1st loss — look for new signal but with raised bar
    send_telegram(fmt_update("SL_HIT"))
    signal = analyze_with_claude(ticker, data)
    if signal:
        send_telegram(fmt_signal(signal))
        set_trade(signal)
    else:
        print("  No high-quality signal found after SL — waiting next scan")

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
def send_telegram(text: str) -> bool:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id":TELEGRAM_CHANNEL_ID,"text":text,"parse_mode":"HTML","disable_web_page_preview":True},
            timeout=10)
        r.raise_for_status(); return True
    except Exception as e:
        print(f"  [ERROR] Telegram: {e}"); return False

def send_reply(chat_id, text: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id":chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":True},
            timeout=10)
    except Exception as e:
        print(f"  [CMD] Reply error: {e}")

# ─── MESSAGE FORMATS ──────────────────────────────────────────────────────────
def fmt_signal(s: dict) -> str:
    e  = "🟢" if s["signal"]=="BUY" else "🔴"
    ci = {"HIGH":"🔥","MEDIUM":"✨","LOW":"⚡"}.get(s.get("confidence",""),"")
    el = f"🎯 Entry   <b>{s['entry']:,.0f}</b>"
    if s.get("entry_type")=="PULLBACK" and s.get("entry_note"):
        el += f"\n   ⏳ <i>{s['entry_note']}</i>"
    wk=s.get("weekly_trend",""); ez=s.get("entry_zone","")
    return (
        f"{e} <b>{s['signal']} — {SYMBOL}</b>  {ci}\n"
        f"🕐 {ist_str()}  |  📍 {s.get('session',get_session())}\n\n"
        f"{el}\n"
        f"🛑 SL       <b>{s['sl']:,.0f}</b>\n"
        f"✅ TP1    <b>{s['tp1']:,.0f}</b>\n"
        f"✅ TP2    <b>{s['tp2']:,.0f}</b>\n"
        f"📊 R:R    <b>{s.get('rr','—')}</b>\n\n"
        + (f"🗓 Weekly: <i>{wk}</i>\n" if wk else "")
        + (f"🔷 Zone:   <i>{ez}</i>\n" if ez else "")
        + f"\n<i>— Signal by CLEXER V4 —</i>\n"
          f"⚠️ <i>Not financial advice</i>"
    )

def fmt_update(status: str) -> str:
    t = active_trade
    msgs = {
        "SL_HIT":          "🛑 <b>SL HIT</b>\nSearching for next trade...",
        "TP1_HIT":         f"✅ <b>TP1 HIT</b>\nSL → Breakeven ({t['entry']:,.0f})\nWaiting TP2 → <b>{t['tp2']:,.0f}</b>",
        "TP2_HIT":         "🏆 <b>TP2 HIT — Trade Complete!</b>",
        "STOP_HUNT":       f"⚡ <b>STOP HUNT DETECTED</b>\nSL wicked but no close through\nHolding trade — watching next candle\nSL: {t['sl']:,.0f}",
        "SETUP_INVALID":   f"❌ <b>Setup Invalidated</b>\nPrice hit SL before reaching entry\nCancelling signal — finding new setup",
        "ENTRY_MISSED":    f"🚀 <b>Entry Zone Missed</b>\nPrice blasted past entry {t['entry']:,.0f} without pullback\nCancelling — will find new entry from current level",
        "WAITING_ENTRY":   f"⏳ <b>Waiting for Pullback</b>\nEntry zone: <b>{t['entry']:,.0f}</b>\nPrice hasn't reached zone yet",
        "HOLD":            "📊 <b>HOLD</b> — Structure intact",
        "WAIT":            "⏳ <b>WAIT</b> — Trade running",
        "CLOSE":           "⚠️ <b>CLOSE NOW</b> — Structure broken",
        "NO_VOLUME":       "📉 <b>CLOSE</b> — No volume / flat market",
        "COOLDOWN":        f"⏸ <b>Cooling Down</b>\nWaiting for clean pattern\nNext: {trade_stats['cooldown_scans']} scan(s) remaining",
    }
    body = msgs.get(status, "⏳ Trade running — WAIT")
    return f"📡 <b>{SYMBOL} UPDATE</b>  {ist_str()}\n\n{body}\n\n<i>— CLEXER V4 —</i>"

# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────
COMMANDS_HELP = """🤖 <b>CLEXER V4 Commands</b>
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
/signal — Force scan NOW
/pause — Pause signals
/resume — Resume signals
/resetsl — Reset SL hit counter
/setinterval 2 — Scan every 2h

/help — This menu"""

def handle_command(text: str, chat_id):
    global SCAN_INTERVAL_SECONDS
    parts = text.strip().split()
    cmd   = parts[0].lower().split("@")[0]

    if ADMIN_CHAT_ID and str(chat_id) != str(ADMIN_CHAT_ID):
        send_reply(chat_id, "⛔ Unauthorized."); return

    if cmd in ("/start","/help"):
        send_reply(chat_id, COMMANDS_HELP)

    elif cmd == "/status":
        t=active_trade; st="⏸ PAUSED" if bot_paused.is_set() else "▶️ RUNNING"
        cd=f"⏸ Cooldown: {trade_stats['cooldown_scans']} scans\n" if trade_stats["cooldown_scans"]>0 else ""
        trade_info=(
            f"{t['signal']} @ {t['entry']:,.0f}\n"
            f"SL:{t['sl']:,.0f} TP1:{t['tp1']:,.0f} TP2:{t['tp2']:,.0f}\n"
            f"TP1:{'✅' if t['tp1_hit'] else '❌'} | Entry hit:{'✅' if t['entry_hit'] else '⏳'}"
        ) if t["signal"] else "No active trade"
        send_reply(chat_id,
            f"📊 <b>CLEXER V4 Status</b>\n\n"
            f"Bot: {st}\n{cd}"
            f"Session: {get_session()} {'✅' if is_trading_hours() else '⏸'}\n"
            f"IST: {ist_str()}\n"
            f"Interval: {SCAN_INTERVAL_SECONDS//3600}h\n\n"
            f"<b>Active Trade:</b>\n{trade_info}"
        )

    elif cmd == "/price":
        try:
            tk=get_ticker()
            send_reply(chat_id,
                f"💵 <b>BTCUSDT</b>\n\nPrice: <b>{tk['price']:,.2f}</b>\n"
                f"24h: {tk['change']:+.2f}% | Vol: ${tk['volume']/1e6:.1f}M\n"
                f"H:{tk['high24']:,.2f} L:{tk['low24']:,.2f}\n🕐 {ist_str()}"
            )
        except Exception as e: send_reply(chat_id, f"❌ {e}")

    elif cmd == "/trade":
        t=active_trade
        if not t["signal"]: send_reply(chat_id, "📭 No active trade.")
        else:
            send_reply(chat_id,
                f"📈 <b>Active Trade</b>\n\n"
                f"{t['signal']} — {SYMBOL}\n"
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
            lines=["📜 <b>Last Signals</b>\n"]
            for s in reversed(signal_history[-5:]):
                e="🟢" if s["signal"]=="BUY" else "🔴"
                lines.append(f"{e} {s['signal']} @ {s['entry']:,.0f} R:R:{s.get('rr','?')} {s.get('confidence','?')}\n"
                             f"   SL:{s['sl']:,.0f} TP1:{s['tp1']:,.0f} TP2:{s['tp2']:,.0f}\n"
                             f"   🕐 {s['time']}\n")
            send_reply(chat_id, "\n".join(lines))

    elif cmd == "/stats":
        ts=trade_stats
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
        s=get_session()
        send_reply(chat_id,
            f"📍 <b>Session Info</b>\n\nCurrent: <b>{s}</b> {'✅ Active' if is_trading_hours() else '⏸'}\n\n"
            f"🇬🇧 London:   07:30–16:30 IST\n"
            f"🇺🇸 New York: 18:30–01:00 IST\n"
            f"😴 Sleep:    01:00–07:29 IST\n\n🕐 {ist_str()}"
        )

    elif cmd == "/close":
        t=active_trade
        if not t["signal"]: send_reply(chat_id, "📭 No active trade.")
        else:
            info=f"{t['signal']} @ {t['entry']:,.0f}"
            reset_trade()
            send_telegram(f"⛔ <b>Trade Manually Closed</b>\n{info}\n\n<i>— CLEXER V4 —</i>")
            send_reply(chat_id, f"✅ Closed: {info}")
            force_scan.set()

    elif cmd == "/sltobe":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No active trade.")
        else:
            old=active_trade["sl"]; active_trade["sl"]=active_trade["entry"]
            send_telegram(f"🔄 <b>SL → Breakeven</b>\nOld:{old:,.0f} → New:<b>{active_trade['entry']:,.0f}</b>\n\n<i>— CLEXER V4 —</i>")
            send_reply(chat_id, f"✅ SL → {active_trade['entry']:,.0f}")

    elif cmd == "/setsl":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /setsl 61500")
        else:
            try:
                v=float(parts[1].replace(",",""))
                old=active_trade["sl"]; active_trade["sl"]=v
                send_telegram(f"🔄 <b>SL Updated</b> {old:,.0f}→<b>{v:,.0f}</b>\n\n<i>— CLEXER V4 —</i>")
                send_reply(chat_id, f"✅ SL={v:,.0f}")
            except: send_reply(chat_id, "❌ /setsl 61500")

    elif cmd == "/settp1":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp1 60000")
        else:
            try:
                v=float(parts[1].replace(",","")); active_trade["tp1"]=v
                send_telegram(f"🔄 <b>TP1→{v:,.0f}</b>\n\n<i>— CLEXER V4 —</i>"); send_reply(chat_id, f"✅ TP1={v:,.0f}")
            except: send_reply(chat_id, "❌ /settp1 60000")

    elif cmd == "/settp2":
        if not active_trade["signal"]: send_reply(chat_id, "📭 No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp2 58000")
        else:
            try:
                v=float(parts[1].replace(",","")); active_trade["tp2"]=v
                send_telegram(f"🔄 <b>TP2→{v:,.0f}</b>\n\n<i>— CLEXER V4 —</i>"); send_reply(chat_id, f"✅ TP2={v:,.0f}")
            except: send_reply(chat_id, "❌ /settp2 58000")

    elif cmd == "/signal":
        if bot_paused.is_set(): send_reply(chat_id, "⏸ Paused. Use /resume first.")
        else: send_reply(chat_id, "🔍 Forcing scan... ~30 seconds."); force_scan.set()

    elif cmd == "/pause":
        bot_paused.set()
        send_telegram("⏸ <b>Bot Paused</b>\n\n<i>— CLEXER V4 —</i>")
        send_reply(chat_id, "✅ Paused.")

    elif cmd == "/resume":
        bot_paused.clear()
        send_telegram("▶️ <b>Bot Resumed</b>\n\n<i>— CLEXER V4 —</i>")
        send_reply(chat_id, "✅ Resumed.")

    elif cmd == "/resetsl":
        trade_stats["consecutive_sl"] = 0
        trade_stats["cooldown_scans"] = 0
        send_reply(chat_id, "✅ SL counter reset. Bot will accept any confidence level.")

    elif cmd == "/setinterval":
        if len(parts)<2: send_reply(chat_id, f"Current: {SCAN_INTERVAL_SECONDS//3600}h\nUsage: /setinterval 2")
        else:
            try:
                h=float(parts[1])
                if h<1 or h>24: send_reply(chat_id, "❌ 1–24 hours only.")
                else:
                    SCAN_INTERVAL_SECONDS=int(h*3600)
                    send_reply(chat_id, f"✅ Interval → {h}h")
            except: send_reply(chat_id, "❌ /setinterval 2")

    else: send_reply(chat_id, f"❓ Unknown: {cmd}\n/help")

# ─── COMMAND LISTENER ─────────────────────────────────────────────────────────
def command_listener():
    global last_update_id
    print("[CMD] Listener started")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook", timeout=10)
        print("[CMD] Webhook cleared")
    except Exception as e: print(f"[CMD] Webhook error: {e}")
    while True:
        try:
            r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                           params={"offset":last_update_id+1,"timeout":20,"allowed_updates":["message"]},
                           timeout=25)
            data=r.json()
            if not data.get("ok"):
                print(f"  [CMD] Error: {data}"); time.sleep(5); continue
            for upd in data.get("result",[]):
                last_update_id=upd["update_id"]
                msg=upd.get("message",{}); text=msg.get("text","")
                cid=msg.get("chat",{}).get("id"); uname=msg.get("from",{}).get("username","?")
                if cid: print(f"  [CMD] @{uname} ID:{cid}: {text[:40]}")
                if text and text.startswith("/") and cid: handle_command(text, cid)
        except Exception as e: print(f"  [CMD] Error: {e}")
        time.sleep(2)

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    print(f"[CLEXER V4] Adaptive SMC Bot | {SYMBOL}")
    threading.Thread(target=command_listener, daemon=True).start()
    send_telegram(
        f"🤖 <b>CLEXER V4 Online</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📊 Weekly + 4H + 1H + 5M Vision\n"
        f"🛡 Adaptive: Stop Hunt / Missed Entry / Cooldown\n"
        f"⏰ London + NY sessions (IST)\n"
        f"💬 /help for commands\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<i>— CLEXER V4 —</i>"
    )

    while True:
        try:
            if bot_paused.is_set(): time.sleep(60); continue

            forced=force_scan.is_set()
            if forced: force_scan.clear()

            print(f"\n[{now_ist().strftime('%H:%M IST')}] {get_session()}{' FORCED' if forced else ''}")

            if not forced and is_ist_sleep():
                print("  [SLEEP] 01:00–07:29 IST"); time.sleep(SCAN_INTERVAL_SECONDS); continue

            if not forced and not is_trading_hours():
                print(f"  [WAIT] {get_session()} — not London/NY"); time.sleep(SCAN_INTERVAL_SECONDS); continue

            # Cooldown check
            if trade_stats["cooldown_scans"] > 0 and not forced:
                trade_stats["cooldown_scans"] -= 1
                remaining = trade_stats["cooldown_scans"]
                print(f"  [COOLDOWN] {remaining} scans remaining — waiting for clean pattern")
                if remaining == 0:
                    send_telegram("🔍 <b>Cooldown over — scanning for new setup</b>\n\n<i>— CLEXER V4 —</i>")
                time.sleep(SCAN_INTERVAL_SECONDS); continue

            # Fetch data
            print("  Fetching candles...")
            data = {}
            for key,iv,lim,lb in [("weekly","1w",52,5),("4h","4h",200,5),("1h","1h",100,5),("5m","5m",50,3)]:
                data[key]=(get_candles(iv,lim),lb); time.sleep(0.3)
                print(f"    {key}: {len(data[key][0])} candles")

            ticker=get_ticker(); price=ticker["price"]
            print(f"  Price: {price:,.2f} | {ticker['change']:+.2f}%")
            df_5m=data["5m"][0]

            # ── Active trade ─────────────────────────────────────────────────
            if active_trade["signal"]:
                active_trade["scan_count"] += 1

                # Check if PULLBACK entry was missed (price blasted past TP1)
                if detect_entry_missed(price):
                    trade_stats["missed_entries"] += 1
                    print(f"  [ENTRY MISSED] Signal cancelled — finding new entry")
                    send_telegram(fmt_update("ENTRY_MISSED"))
                    reset_trade()
                    signal=analyze_with_claude(ticker,data)
                    if signal: send_telegram(fmt_signal(signal)); set_trade(signal)
                    time.sleep(SCAN_INTERVAL_SECONDS); continue

                # Check if setup was invalidated before entry
                if not active_trade["entry_hit"] and detect_entry_invalidated(price, data["4h"][0]):
                    print(f"  [SETUP INVALID] Price moved against signal before entry")
                    send_telegram(fmt_update("SETUP_INVALID"))
                    reset_trade()
                    signal=analyze_with_claude(ticker,data)
                    if signal: send_telegram(fmt_signal(signal)); set_trade(signal)
                    time.sleep(SCAN_INTERVAL_SECONDS); continue

                status=check_price_status(price, df_5m)
                print(f"  Trade: {active_trade['signal']} | Status: {status}")

                if status == "STOP_HUNT":
                    send_telegram(fmt_update("STOP_HUNT"))
                    # Don't exit — it's a stop hunt. SL_WICKED flag is now set.
                    # Next scan if price truly closes through SL = real SL hit

                elif status == "TP1_HIT":
                    active_trade["tp1_hit"]=True
                    active_trade["sl"]=active_trade["entry"]   # breakeven
                    trade_stats["total_tp1"] += 1
                    trade_stats["consecutive_sl"] = 0   # reset on any TP hit
                    send_telegram(fmt_update("TP1_HIT"))

                elif status == "TP2_HIT":
                    trade_stats["total_tp2"] += 1
                    trade_stats["consecutive_sl"] = 0
                    send_telegram(fmt_update("TP2_HIT"))
                    reset_trade()
                    signal=analyze_with_claude(ticker,data)
                    if signal: send_telegram(fmt_signal(signal)); set_trade(signal)

                elif status == "SL_HIT":
                    reset_trade()
                    handle_sl_hit(ticker,data)

                elif status == "WAITING_ENTRY":
                    t=active_trade
                    dist=abs(price-t["entry"])
                    print(f"  Pullback pending — {dist:.0f} pts from entry {t['entry']:,.0f}")
                    # Only notify channel every 2 scans to avoid spam
                    if t["scan_count"] % 2 == 0:
                        send_telegram(fmt_update("WAITING_ENTRY"))

                else:   # RUNNING
                    print("  Getting Claude update...")
                    cs=claude_trade_update(ticker,data)
                    print(f"  Claude: {cs}")
                    send_telegram(fmt_update(cs))
                    if cs in ("CLOSE","NO_VOLUME"):
                        reset_trade()
                        signal=analyze_with_claude(ticker,data)
                        if signal: send_telegram(fmt_signal(signal)); set_trade(signal)

            # ── No active trade ───────────────────────────────────────────────
            else:
                print("  No active trade. Analyzing...")
                signal=analyze_with_claude(ticker,data)
                if signal:
                    send_telegram(fmt_signal(signal)); set_trade(signal)
                    print(f"  [SENT] {signal['signal']} R:R:{signal.get('rr','?')} {signal.get('confidence','?')}")
                else:
                    print("  No signal found — waiting next scan")

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("🛑 <b>CLEXER V4 Stopped</b>\n\n<i>— CLEXER —</i>"); break
        except Exception as e:
            print(f"  [ERROR] {e}")
            import traceback; traceback.print_exc()

        waited=0
        while waited < SCAN_INTERVAL_SECONDS:
            if force_scan.is_set(): print("  [CMD] Force scan"); break
            time.sleep(30); waited += 30

if __name__ == "__main__":
    main()
