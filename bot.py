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
from datetime import datetime, timezone

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY",   "your_key_here")
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN",  "your_token_here")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "@your_channel")

SYMBOL                = "BTCUSDT"
SCAN_INTERVAL_SECONDS = 14400
BINANCE_BASE          = "https://api1.binance.com/api/v3"

# ─── ACTIVE TRADE STATE ───────────────────────────────────────────────────────
active_trade = {
    "signal":   None,   # "BUY" or "SELL"
    "entry":    None,
    "sl":       None,
    "tp1":      None,
    "tp2":      None,
    "tp1_hit":  False,
}

def reset_trade():
    global active_trade
    active_trade = {"signal": None, "entry": None, "sl": None,
                    "tp1": None, "tp2": None, "tp1_hit": False}

def set_trade(signal: dict):
    global active_trade
    active_trade = {
        "signal":  signal["signal"],
        "entry":   signal["entry"],
        "sl":      signal["sl"],
        "tp1":     signal["tp1"],
        "tp2":     signal["tp2"],
        "tp1_hit": False,
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

# ─── CLAUDE: NEW SIGNAL ───────────────────────────────────────────────────────
def analyze_with_claude(symbol: str, price: float, candle_data: dict) -> dict | None:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print("  Generating chart images...")
    chart_4h  = generate_chart(candle_data["4h"],  "4H")
    chart_1h  = generate_chart(candle_data["1h"],  "1H")
    chart_15m = generate_chart(candle_data["15m"], "15M")

    prompt = f"""You are an expert SMC crypto trader analyzing BTCUSDT.
Charts provided: 4H (macro), 1H (structure), 15M (entry).
Current price: {price}

SMC Analysis:
1. 4H: Trend direction, BOS/CHoCH, last major swing high/low
2. 1H: Confirm bias, 1H BOS/CHoCH, nearest OB and FVG
3. 15M: POI present? CHoCH for entry confirmation?

STOP LOSS: Place at last significant 4H swing HIGH (for SELL) or swing LOW (for BUY).
TP1: Nearest FVG or 1H OB fill (400-600 pts from entry).
TP2: Next MAJOR 4H swing low (SELL) or swing high (BUY) — minimum 1500 pts from entry.

RULES:
- Signal only if R:R >= 1:3
- NO TRADE if market is choppy or ranging
- TP2 must be a real structural liquidity target on 4H

Respond ONLY in this exact JSON (no markdown):
{{
  "signal": "BUY" | "SELL" | "NO TRADE",
  "entry": <number or null>,
  "sl": <number or null>,
  "tp1": <number or null>,
  "tp2": <number or null>,
  "rr": "<e.g. 1:3.5 or null>",
  "bias": "BULLISH" | "BEARISH" | "NEUTRAL",
  "structure": "<4H and 1H structure summary>",
  "key_level": "<OB or FVG price range>",
  "confidence": "HIGH" | "MEDIUM" | "LOW",
  "reason": "<2-3 sentences with price levels>"
}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5", max_tokens=800,
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
TP1 already hit: {t['tp1_hit']}
Current price: {price}

Look at the 4H and 1H charts. Assess the trade.

Reply with ONLY one of these exact options:
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
        print(f"  [ERROR] Claude trade update: {e}")
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
    return (
        f"{e} <b>{s['signal']} — {SYMBOL}</b>\n\n"
        f"🎯 Entry   <b>{s['entry']:,.0f}</b>\n"
        f"🛑 SL        <b>{s['sl']:,.0f}</b>\n"
        f"✅ TP1     <b>{s['tp1']:,.0f}</b>\n"
        f"✅ TP2     <b>{s['tp2']:,.0f}</b>\n\n"
        f"<i>— Signal by CLEXER —</i>\n"
        f"⚠️ <i>Not financial advice</i>"
    )

def fmt_update(status: str) -> str:
    t = active_trade
    lines = {
        "SL_HIT":      "🛑 <b>SL HIT</b>\nWait for next trade",
        "TP2_HIT":     "🏆 <b>TP2 HIT — Trade Complete</b>\nWell done",
        "TP1_HIT":     f"✅ <b>TP1 HIT</b>\nSL moved to Entry\nWaiting for TP2 — <b>{t['tp2']:,.0f}</b>",
        "HOLD":        "📊 <b>Market choppy — HOLD</b>",
        "WAIT":        f"⏳ <b>Past trade still running</b>\nWait for SL or TP2",
        "CLOSE":       "⚠️ <b>Structure broken — CLOSE the trade</b>",
        "NO_VOLUME":   "📉 <b>No volume — CLOSE the trade</b>",
    }
    body = lines.get(status, f"⏳ <b>Trade still running — WAIT</b>")
    return f"📡 <b>{SYMBOL} UPDATE</b>\n\n{body}\n\n<i>— CLEXER —</i>"

def fmt_new_after_close() -> str:
    return f"🔍 <b>Finding new trade...</b>\n\n<i>— CLEXER —</i>"

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    print(f"[BOT] CLEXER Vision Bot started | Scan every {SCAN_INTERVAL_SECONDS}s")
    send_telegram(f"🤖 <b>CLEXER Bot Online</b>\n{SYMBOL} | 4H + 1H + 15M Vision\n\n<i>— CLEXER —</i>")

    while True:
        try:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning {SYMBOL}...")

            candle_data = {}
            for tf in ["15m", "1h", "4h"]:
                candle_data[tf] = get_candles(SYMBOL, tf, limit=100)
                time.sleep(0.3)

            price = get_current_price(SYMBOL)
            print(f"  Price: {price:,.2f}")

            # ── Active trade exists ───────────────────────────────────────────
            if active_trade["signal"] is not None:
                status = check_price_status(price)
                print(f"  Trade status: {status}")

                if status == "TP1_HIT":
                    active_trade["tp1_hit"] = True
                    active_trade["sl"] = active_trade["entry"]  # move SL to breakeven
                    send_telegram(fmt_update("TP1_HIT"))

                elif status in ("SL_HIT", "TP2_HIT"):
                    send_telegram(fmt_update(status))
                    reset_trade()
                    # Look for new trade immediately
                    signal = analyze_with_claude(SYMBOL, price, candle_data)
                    if signal and signal["signal"] != "NO TRADE":
                        send_telegram(fmt_new_after_close())
                        send_telegram(fmt_signal(signal))
                        set_trade(signal)
                        print(f"  [NEW] New signal sent after {status}")

                else:  # RUNNING — ask Claude for update
                    print("  Generating update charts...")
                    claude_status = claude_trade_update(price, candle_data)
                    print(f"  Claude update: {claude_status}")

                    if claude_status == "CLOSE":
                        send_telegram(fmt_update("CLOSE"))
                        reset_trade()
                        signal = analyze_with_claude(SYMBOL, price, candle_data)
                        if signal and signal["signal"] != "NO TRADE":
                            send_telegram(fmt_new_after_close())
                            send_telegram(fmt_signal(signal))
                            set_trade(signal)
                    else:
                        send_telegram(fmt_update(claude_status))

            # ── No active trade — look for new signal ─────────────────────────
            else:
                signal = analyze_with_claude(SYMBOL, price, candle_data)
                if signal is None:
                    print("  [SKIP] Claude returned no signal")
                elif signal["signal"] == "NO TRADE":
                    print(f"  [SKIP] NO TRADE | Bias: {signal['bias']}")
                else:
                    send_telegram(fmt_signal(signal))
                    set_trade(signal)
                    print(f"  [SENT] {signal['signal']} signal sent")

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("🛑 <b>CLEXER Bot Stopped</b>\n\n<i>— CLEXER —</i>")
            break
        except Exception as e:
            print(f"  [ERROR] {e}")

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
