"""
CLEXER Signal Bot — Vision + Trade Management
"""

import os
import time
import json
import base64
import requests
import anthropic
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf
from io import BytesIO
from datetime import datetime, timezone, timedelta

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY",   "your_key_here")
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN",  "your_token_here")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "@your_channel")

SYMBOL                = "BTCUSDT"
SCAN_INTERVAL_SECONDS = 14400       # 4 hours
BINANCE_BASE          = "https://api1.binance.com/api/v3"
IST                   = timedelta(hours=5, minutes=30)

# ─── IST TIME HELPERS ─────────────────────────────────────────────────────────
def now_ist() -> datetime:
    return datetime.now(timezone.utc) + IST

def ist_str() -> str:
    t = now_ist()
    return t.strftime("%d %b %Y  %I:%M %p IST")

def is_trading_hours() -> bool:
    """Only trade 5:30 AM to 11:59 PM IST — no signals during sleep hours"""
    t = now_ist()
    mins = t.hour * 60 + t.minute
    return (5 * 60 + 30) <= mins <= (23 * 60 + 59)  # 5:30 AM to 11:59 PM

# ─── ACTIVE TRADE STATE ───────────────────────────────────────────────────────
active_trade = {
    "signal":     None,
    "entry":      None,
    "sl":         None,
    "tp1":        None,
    "tp2":        None,
    "tp1_hit":    False,
    "entry_type": "MARKET",   # "MARKET" or "PULLBACK"
    "entry_note": "",
}

def reset_trade():
    global active_trade
    active_trade = {"signal": None, "entry": None, "sl": None,
                    "tp1": None, "tp2": None, "tp1_hit": False,
                    "entry_type": "MARKET", "entry_note": ""}

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
    }

# ─── BINANCE FETCH ────────────────────────────────────────────────────────────
def get_candles(symbol: str, interval: str, limit: int = 100) -> list[dict]:
    url = f"{BINANCE_BASE}/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    candles = []
    for c in r.json():
        candles.append({
            "time":  datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "open":  float(c[1]), "high": float(c[2]),
            "low":   float(c[3]), "close": float(c[4]), "vol": float(c[5]),
        })
    return candles

def get_current_price(symbol: str) -> float:
    r = requests.get(f"{BINANCE_BASE}/ticker/price", params={"symbol": symbol}, timeout=10)
    r.raise_for_status()
    return float(r.json()["price"])

# ─── CHART GENERATOR ─────────────────────────────────────────────────────────
def generate_chart(candles: list[dict], tf: str) -> str:
    df = pd.DataFrame(candles)
    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time")
    df = df.rename(columns={"open": "Open", "high": "High",
                             "low": "Low", "close": "Close", "vol": "Volume"}).astype(float)
    style = mpf.make_mpf_style(
        base_mpf_style="charles", gridstyle="--",
        gridcolor="#2a2a2a", facecolor="#0d0d0d", edgecolor="#333333",
        figcolor="#0d0d0d", y_on_right=True,
        rc={"axes.labelcolor": "#cccccc", "xtick.color": "#aaaaaa", "ytick.color": "#aaaaaa"}
    )
    buf = BytesIO()
    mpf.plot(df, type="candle", style=style, volume=True,
             savefig=dict(fname=buf, dpi=120, bbox_inches="tight"),
             figsize=(16, 8), title=f"\n{SYMBOL} {tf}")
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

# ─── CLAUDE: NEW SIGNAL (always BUY or SELL) ─────────────────────────────────
def analyze_with_claude(symbol: str, price: float, candle_data: dict) -> dict | None:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print("  Generating chart images...")
    chart_4h  = generate_chart(candle_data["4h"],  "4H")
    chart_1h  = generate_chart(candle_data["1h"],  "1H")
    chart_15m = generate_chart(candle_data["15m"], "15M")

    prompt = f"""You are an expert SMC crypto trader analyzing BTCUSDT.
Charts: 4H (macro bias), 1H (structure), 15M (entry).
Current price: {price}

You MUST give either BUY or SELL. Never say NO TRADE.
Even in choppy markets, give the best directional bias with a pullback entry.

SMC Analysis:
1. 4H trend: BOS/CHoCH, last major swing high and low
2. 1H structure: confirm bias, OB and FVG locations
3. 15M: best entry POI — OB, FVG, or CHoCH

STOP LOSS: Place at last significant 4H swing HIGH (SELL) or LOW (BUY).
TP1: Nearest FVG or 1H OB (400-600 pts).
TP2: Next MAJOR 4H swing low (SELL) or high (BUY) — minimum 1500 pts.

ENTRY TYPE rules:
- If price is at or near the entry zone RIGHT NOW → entry_type = "MARKET"
- If price needs to retrace to a better level first → entry_type = "PULLBACK"
  and entry_note = "Wait for price to pull back to [entry], look for rejection"

Respond ONLY in this exact JSON (no markdown):
{{
  "signal": "BUY" | "SELL",
  "entry": <number>,
  "sl": <number>,
  "tp1": <number>,
  "tp2": <number>,
  "rr": "<e.g. 1:3.5>",
  "entry_type": "MARKET" | "PULLBACK",
  "entry_note": "<pullback instruction or empty string>",
  "bias": "BULLISH" | "BEARISH",
  "structure": "<4H and 1H structure>",
  "key_level": "<OB or FVG range>",
  "confidence": "HIGH" | "MEDIUM" | "LOW"
}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5", max_tokens=600,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_4h}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_1h}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_15m}},
                {"type": "text",  "text": prompt}
            ]}]
        )
        raw = message.content[0].text.strip().replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except Exception as e:
        print(f"  [ERROR] Claude new signal: {e}")
        return None

# ─── CLAUDE: TRADE UPDATE ─────────────────────────────────────────────────────
def claude_trade_update(price: float, candle_data: dict) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    chart_4h = generate_chart(candle_data["4h"], "4H")
    chart_1h = generate_chart(candle_data["1h"], "1H")
    t = active_trade
    prompt = f"""Active {t['signal']} trade on BTCUSDT.
Entry: {t['entry']} | SL: {t['sl']} | TP1: {t['tp1']} | TP2: {t['tp2']}
TP1 hit: {t['tp1_hit']} | Current price: {price}

Look at the 4H and 1H charts. What should the trader do?

Reply with ONLY one of these:
HOLD
WAIT
CLOSE
NO_VOLUME"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5", max_tokens=10,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_4h}},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_1h}},
                {"type": "text",  "text": prompt}
            ]}]
        )
        return message.content[0].text.strip().upper()
    except Exception as e:
        print(f"  [ERROR] Claude update: {e}")
        return "WAIT"

# ─── PRICE STATUS CHECK ───────────────────────────────────────────────────────
def check_price_status(price: float) -> str:
    t = active_trade
    if t["signal"] is None:
        return "NONE"
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

    # Entry line — show pullback note if needed
    if s.get("entry_type") == "PULLBACK" and s.get("entry_note"):
        entry_line = (
            f"🎯 Entry   <b>{s['entry']:,.0f}</b>\n"
            f"   ⏳ <i>{s['entry_note']}</i>"
        )
    else:
        entry_line = f"🎯 Entry   <b>{s['entry']:,.0f}</b>"

    return (
        f"{e} <b>{s['signal']} — {SYMBOL}</b>\n"
        f"🕐 {ist_str()}\n\n"
        f"{entry_line}\n"
        f"🛑 SL       <b>{s['sl']:,.0f}</b>\n"
        f"✅ TP1    <b>{s['tp1']:,.0f}</b>\n"
        f"✅ TP2    <b>{s['tp2']:,.0f}</b>\n\n"
        f"<i>— Signal by CLEXER —</i>\n"
        f"⚠️ <i>Not financial advice</i>"
    )

def fmt_update(status: str) -> str:
    t = active_trade
    lines = {
        "SL_HIT":    "🛑 <b>SL HIT</b>\nWait for next trade",
        "TP2_HIT":   "🏆 <b>TP2 HIT — Trade Complete</b>",
        "TP1_HIT":   f"✅ <b>TP1 HIT</b>\nSL moved to Entry\nWaiting for TP2 — <b>{t['tp2']:,.0f}</b>",
        "HOLD":      "📊 <b>Market choppy — HOLD</b>",
        "WAIT":      "⏳ <b>Past trade running\nWait for SL or TP2</b>",
        "CLOSE":     "⚠️ <b>Structure broken — CLOSE the trade</b>",
        "NO_VOLUME": "📉 <b>No volume — CLOSE the trade</b>",
    }
    body = lines.get(status, "⏳ <b>Trade still running — WAIT</b>")
    return f"📡 <b>{SYMBOL} UPDATE</b>  {ist_str()}\n\n{body}\n\n<i>— CLEXER —</i>"

def fmt_finding_new() -> str:
    return f"🔍 <b>Finding new trade...</b>\n\n<i>— CLEXER —</i>"

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    print(f"[BOT] CLEXER Vision Bot | Scan every {SCAN_INTERVAL_SECONDS}s | IST trading hours only")
    send_telegram(
        f"🤖 <b>CLEXER Bot Online</b>\n"
        f"{SYMBOL} | 4H + 1H + 15M Vision\n"
        f"⏰ Active: 5:30 AM – 11:59 PM IST\n\n"
        f"<i>— CLEXER —</i>"
    )

    while True:
        try:
            ist_now = now_ist()
            print(f"\n[{ist_now.strftime('%H:%M:%S IST')}] Scanning {SYMBOL}...")

            # ── Sleep hours check ─────────────────────────────────────────────
            if not is_trading_hours():
                print("  [SLEEP] Outside trading hours (5:30 AM – 11:59 PM IST). Waiting...")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # ── Fetch data ────────────────────────────────────────────────────
            candle_data = {}
            for tf in ["15m", "1h", "4h"]:
                candle_data[tf] = get_candles(SYMBOL, tf, limit=100)
                time.sleep(0.3)

            price = get_current_price(SYMBOL)
            print(f"  Price: {price:,.2f}")

            # ── Active trade: check price levels first ─────────────────────────
            if active_trade["signal"] is not None:
                status = check_price_status(price)
                print(f"  Trade status: {status}")

                if status == "TP1_HIT":
                    active_trade["tp1_hit"] = True
                    active_trade["sl"] = active_trade["entry"]  # breakeven
                    send_telegram(fmt_update("TP1_HIT"))

                elif status in ("SL_HIT", "TP2_HIT"):
                    send_telegram(fmt_update(status))
                    reset_trade()
                    signal = analyze_with_claude(SYMBOL, price, candle_data)
                    if signal:
                        send_telegram(fmt_finding_new())
                        send_telegram(fmt_signal(signal))
                        set_trade(signal)
                        print(f"  [NEW] {signal['signal']} signal sent after {status}")

                else:  # RUNNING — ask Claude
                    print("  Getting Claude trade update...")
                    claude_status = claude_trade_update(price, candle_data)
                    print(f"  Claude says: {claude_status}")
                    send_telegram(fmt_update(claude_status))

                    if claude_status in ("CLOSE", "NO_VOLUME"):
                        reset_trade()
                        signal = analyze_with_claude(SYMBOL, price, candle_data)
                        if signal:
                            send_telegram(fmt_finding_new())
                            send_telegram(fmt_signal(signal))
                            set_trade(signal)

            # ── No active trade — find new signal ─────────────────────────────
            else:
                signal = analyze_with_claude(SYMBOL, price, candle_data)
                if signal is None:
                    print("  [ERROR] Claude returned nothing")
                else:
                    send_telegram(fmt_signal(signal))
                    set_trade(signal)
                    print(f"  [SENT] {signal['signal']} | {signal.get('entry_type','MARKET')}")

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("🛑 <b>CLEXER Bot Stopped</b>\n\n<i>— CLEXER —</i>")
            break
        except Exception as e:
            print(f"  [ERROR] {e}")

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
