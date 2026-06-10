"""
Crypto Signal Bot — Vision Enhanced
- Generates candlestick chart images from Binance data
- Claude SEES the charts (same as TradingView+Claude)
- Sends BUY/SELL signals to Telegram Channel
"""

import os
import time
import json
import base64
import requests
import anthropic
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Headless mode for Railway server
import mplfinance as mpf
from io import BytesIO
from datetime import datetime, timezone

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY",  "your_claude_api_key_here")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "your_telegram_bot_token_here")
TELEGRAM_CHANNEL_ID= os.getenv("TELEGRAM_CHANNEL_ID","@your_channel_username")

SYMBOL = "BTCUSDT"
SCAN_INTERVAL_SECONDS = 14400        # 4 hours
BINANCE_BASE = "https://api1.binance.com/api/v3"

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
            "open":  float(c[1]),
            "high":  float(c[2]),
            "low":   float(c[3]),
            "close": float(c[4]),
            "vol":   float(c[5]),
        })
    return candles

def get_current_price(symbol: str) -> float:
    r = requests.get(f"{BINANCE_BASE}/ticker/price", params={"symbol": symbol}, timeout=10)
    r.raise_for_status()
    return float(r.json()["price"])

# ─── GENERATE CHART IMAGE ─────────────────────────────────────────────────────
def generate_chart(candles: list[dict], tf: str) -> str:
    """Render candlestick + volume chart → return base64 PNG string"""
    df = pd.DataFrame(candles)
    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time")
    df = df.rename(columns={
        "open": "Open", "high": "High",
        "low": "Low", "close": "Close", "vol": "Volume"
    }).astype(float)

    style = mpf.make_mpf_style(
        base_mpf_style="charles",
        gridstyle="--",
        gridcolor="#2a2a2a",
        facecolor="#0d0d0d",
        edgecolor="#333333",
        figcolor="#0d0d0d",
        y_on_right=True,
        rc={"axes.labelcolor": "#cccccc", "xtick.color": "#aaaaaa", "ytick.color": "#aaaaaa"}
    )

    buf = BytesIO()
    mpf.plot(
        df,
        type="candle",
        style=style,
        volume=True,
        savefig=dict(fname=buf, dpi=120, bbox_inches="tight"),
        figsize=(16, 8),
        title=f"\n{SYMBOL}  {tf}  |  {len(candles)} candles",
    )
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

# ─── CLAUDE VISION ANALYSIS ───────────────────────────────────────────────────
def analyze_with_claude(symbol: str, price: float, candle_data: dict) -> dict | None:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    print("  Generating chart images...")
    chart_4h  = generate_chart(candle_data["4h"],  "4H")
    chart_1h  = generate_chart(candle_data["1h"],  "1H")
    chart_15m = generate_chart(candle_data["15m"], "15M")

    prompt = f"""You are an expert SMC (Smart Money Concepts) crypto trader.
You are looking at three BTCUSDT charts: 4H (macro), 1H (structure), 15M (entry).
Current live price: {price}

Analyze visually using Smart Money Concepts:

1. HTF STRUCTURE (4H chart)
   - Uptrend or downtrend? Recent BOS or CHoCH?
   - Last major swing high and swing low (exact price)
   - Nearest 4H Order Block (bullish or bearish)

2. MID STRUCTURE (1H chart)
   - Confirm or deny 4H bias
   - 1H BOS or CHoCH location
   - Nearest 1H OB and FVG

3. ENTRY TIMEFRAME (15M chart)
   - Is price at a POI?
   - CHoCH on 15M confirming entry direction?
   - Clean OB or FVG to target

4. TARGETS
   - TP1 = nearest FVG fill or minor OB (300-600 pts)
   - TP2 = MAJOR swing low (SELL) or swing high (BUY) — 1000+ pts minimum
   - SL = just above/below entry OB (tight invalidation)

RULES:
- Only signal BUY or SELL if R:R >= 1:3
- NO TRADE if choppy, ranging, or no clean SMC setup
- Use exact price levels visible on the charts

Respond ONLY in this exact JSON (no markdown, no extra text):
{{
  "signal": "BUY" | "SELL" | "NO TRADE",
  "entry": <number or null>,
  "sl": <number or null>,
  "tp1": <number or null>,
  "tp2": <number or null>,
  "rr": "<e.g. 1:3.5 or null>",
  "bias": "BULLISH" | "BEARISH" | "NEUTRAL",
  "structure": "<4H and 1H structure in one line>",
  "key_level": "<exact OB or FVG price range>",
  "confidence": "HIGH" | "MEDIUM" | "LOW",
  "reason": "<2-3 sentences with specific price levels from the charts>"
}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=800,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_4h}},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_1h}},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_15m}},
                    {"type": "text", "text": prompt}
                ]
            }]
        )
        raw = message.content[0].text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"  [ERROR] Non-JSON from Claude: {raw[:200]}")
        return None
    except Exception as e:
        print(f"  [ERROR] Claude API: {e}")
        return None

# ─── TELEGRAM SEND ────────────────────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"  [ERROR] Telegram: {e}")
        return False

# ─── FORMAT SIGNAL ────────────────────────────────────────────────────────────
def format_signal(signal: dict, price: float) -> str:
    emoji      = "🟢" if signal["signal"] == "BUY"     else "🔴"
    bias_emoji = "📈" if signal["bias"]   == "BULLISH" else "📉" if signal["bias"] == "BEARISH" else "➡️"
    conf_emoji = "🔥" if signal["confidence"] == "HIGH" else "⚡" if signal["confidence"] == "MEDIUM" else "💧"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return (
        f"{emoji} <b>{signal['signal']} SIGNAL — {SYMBOL}</b>\n"
        f"🕐 {now}\n\n"
        f"💵 Current Price: <b>{price:,.2f}</b>\n"
        f"🎯 Entry: <b>{signal['entry']:,.2f}</b>\n"
        f"🛑 Stop Loss: <b>{signal['sl']:,.2f}</b>\n"
        f"✅ TP1: <b>{signal['tp1']:,.2f}</b>\n"
        f"✅ TP2: <b>{signal['tp2']:,.2f}</b>\n"
        f"⚖️ R:R — <b>{signal['rr']}</b>\n\n"
        f"{bias_emoji} HTF Bias: <b>{signal['bias']}</b>\n"
        f"{conf_emoji} Confidence: <b>{signal['confidence']}</b>\n"
        f"📊 Structure: {signal['structure']}\n"
        f"🏛️ Key Level: {signal['key_level']}\n\n"
        f"💬 <i>{signal['reason']}</i>\n\n"
        f"⚠️ <i>Not financial advice. Trade at your own risk.</i>"
    )

# ─── DUPLICATE FILTER ─────────────────────────────────────────────────────────
last_signal = {"signal": None, "entry": None}

def is_new_signal(new: dict) -> bool:
    global last_signal
    if new["signal"] == "NO TRADE":
        return False
    if new["signal"] != last_signal["signal"]:
        return True
    if new.get("entry") and new["entry"] != last_signal.get("entry"):
        return True
    return False

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    global last_signal
    print(f"[BOT] Starting BTCUSDT Vision Signal Bot | Scan every {SCAN_INTERVAL_SECONDS}s")
    send_telegram(
        f"🤖 <b>Vision Signal Bot Started</b>\n"
        f"Monitoring: <b>{SYMBOL}</b>\n"
        f"Mode: 📊 Chart Vision (4H + 1H + 15M)\n"
        f"🔍 First scan in progress..."
    )

    while True:
        try:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning {SYMBOL}...")

            candle_data = {}
            for tf, limit in [("15m", 100), ("1h", 100), ("4h", 100)]:
                candle_data[tf] = get_candles(SYMBOL, tf, limit=limit)
                time.sleep(0.3)

            price = get_current_price(SYMBOL)
            print(f"  Price: {price:,.2f}")

            signal = analyze_with_claude(SYMBOL, price, candle_data)
            if signal is None:
                print("  [SKIP] No valid signal")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            print(f"  Signal: {signal['signal']} | Bias: {signal['bias']} | Conf: {signal['confidence']}")

            if is_new_signal(signal):
                if send_telegram(format_signal(signal, price)):
                    print("  [SENT] Signal sent to Telegram")
                    last_signal = {"signal": signal["signal"], "entry": signal.get("entry")}
                else:
                    print("  [FAIL] Telegram send failed")
            else:
                print("  [SKIP] Same signal, not resending")

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("🛑 <b>Signal Bot Stopped.</b>")
            break
        except Exception as e:
            print(f"  [ERROR] {e}")

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
