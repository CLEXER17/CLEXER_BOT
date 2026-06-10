"""
Crypto Signal Bot
- Fetches BTCUSDT candles from Binance
- Analyzes with Claude API (SMC-based)
- Sends signals to Telegram Channel
"""

import os
import time
import json
import requests
import anthropic
from datetime import datetime, timezone

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "your_claude_api_key_here")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "your_telegram_bot_token_here")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "@your_channel_username")  # e.g. @mycryptosignals

SYMBOL = "BTCUSDT"
TIMEFRAMES = ["15m", "1h", "4h"]   # Multi-TF analysis
SCAN_INTERVAL_SECONDS = 14400       # Run every 4 hours
BINANCE_BASE = "https://api1.binance.com/api/v3"

# ─── BINANCE CANDLE FETCH ─────────────────────────────────────────────────────
def get_candles(symbol: str, interval: str, limit: int = 50) -> list[dict]:
    url = f"{BINANCE_BASE}/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    raw = r.json()
    candles = []
    for c in raw:
        candles.append({
            "time": datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
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

# ─── SUMMARIZE CANDLES FOR PROMPT ────────────────────────────────────────────
def candles_to_text(candles: list[dict], tf: str) -> str:
    lines = [f"Timeframe: {tf}"]
    lines.append("Time | O | H | L | C | Vol")
    for c in candles[-20:]:  # last 20 candles only to save tokens
        lines.append(f"{c['time']} | {c['open']} | {c['high']} | {c['low']} | {c['close']} | {c['vol']:.2f}")
    return "\n".join(lines)

# ─── CLAUDE ANALYSIS ──────────────────────────────────────────────────────────
def analyze_with_claude(symbol: str, price: float, candle_data: dict) -> dict | None:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    candle_sections = "\n\n".join([
        candles_to_text(candle_data[tf], tf) for tf in TIMEFRAMES
    ])

    prompt = f"""You are an expert SMC (Smart Money Concepts) crypto trader.
Analyze the following BTCUSDT candle data across multiple timeframes and generate a precise trading signal.

Current Price: {price}

{candle_sections}

Analyze using:
1. Market Structure (BOS/CHoCH on 4H and 1H)
2. Order Blocks (OB) — identify nearest bullish/bearish OBs
3. Fair Value Gaps (FVG)
4. Liquidity levels (swing highs/lows likely to be swept)
5. HTF bias (4H) vs LTF entry (15m)

Respond ONLY in this exact JSON format (no markdown, no extra text):
{{
  "signal": "BUY" | "SELL" | "NO TRADE",
  "entry": <price or null>,
  "sl": <stop loss price or null>,
  "tp1": <take profit 1 or null>,
  "tp2": <take profit 2 or null>,
  "rr": "<risk:reward ratio or null>",
  "bias": "BULLISH" | "BEARISH" | "NEUTRAL",
  "structure": "<one line: e.g. BOS bullish on 4H, CHoCH bearish on 1H>",
  "key_level": "<nearest OB or FVG level>",
  "confidence": "HIGH" | "MEDIUM" | "LOW",
  "reason": "<2-3 sentence analysis summary>"
}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
       raw = message.content[0].text.strip()
raw = raw.replace("```json", "").replace("```", "").strip()
return json.loads(raw)
    except json.JSONDecodeError:
        print(f"[ERROR] Claude returned non-JSON: {raw}")
        return None
    except Exception as e:
        print(f"[ERROR] Claude API: {e}")
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
        print(f"[ERROR] Telegram send: {e}")
        return False

# ─── FORMAT SIGNAL MESSAGE ────────────────────────────────────────────────────
def format_signal(signal: dict, price: float) -> str:
    emoji = "🟢" if signal["signal"] == "BUY" else "🔴" if signal["signal"] == "SELL" else "⚪"
    bias_emoji = "📈" if signal["bias"] == "BULLISH" else "📉" if signal["bias"] == "BEARISH" else "➡️"
    conf_emoji = "🔥" if signal["confidence"] == "HIGH" else "⚡" if signal["confidence"] == "MEDIUM" else "💧"

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if signal["signal"] == "NO TRADE":
        return (
            f"⚪ <b>NO TRADE — {SYMBOL}</b>\n"
            f"🕐 {now}\n"
            f"💵 Price: <b>{price:,.2f}</b>\n"
            f"{bias_emoji} Bias: {signal['bias']}\n"
            f"📊 Structure: {signal['structure']}\n"
            f"💬 {signal['reason']}"
        )

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

# ─── SIGNAL STATE (avoid duplicate sends) ─────────────────────────────────────
last_signal = {"signal": None, "entry": None}

def is_new_signal(new: dict) -> bool:
    global last_signal
    if new["signal"] == "NO TRADE":
        return False  # Never send NO TRADE to channel
    if new["signal"] != last_signal["signal"]:
        return True
    if new.get("entry") and new["entry"] != last_signal.get("entry"):
        return True
    return False

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    global last_signal
    print(f"[BOT] Starting BTCUSDT Signal Bot | Scan every {SCAN_INTERVAL_SECONDS}s")
    send_telegram(f"🤖 <b>Signal Bot Started</b>\nMonitoring: <b>{SYMBOL}</b>\nTimeframes: 15m | 1H | 4H\n🔍 First scan in progress...")

    while True:
        try:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning {SYMBOL}...")

            # Fetch candles for all TFs
            candle_data = {}
            for tf in TIMEFRAMES:
                candle_data[tf] = get_candles(SYMBOL, tf, limit=50)
                time.sleep(0.3)  # Binance rate limit buffer

            price = get_current_price(SYMBOL)
            print(f"  Price: {price:,.2f}")

            # Claude analysis
            signal = analyze_with_claude(SYMBOL, price, candle_data)
            if signal is None:
                print("  [SKIP] No valid signal from Claude")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            print(f"  Signal: {signal['signal']} | Bias: {signal['bias']} | Confidence: {signal['confidence']}")

            if is_new_signal(signal):
                msg = format_signal(signal, price)
                if send_telegram(msg):
                    print(f"  [SENT] Signal sent to Telegram channel")
                    last_signal = {"signal": signal["signal"], "entry": signal.get("entry")}
                else:
                    print(f"  [FAIL] Telegram send failed")
            else:
                print(f"  [SKIP] Same signal as before, not resending")

        except KeyboardInterrupt:
            print("\n[BOT] Stopped by user.")
            send_telegram("🛑 <b>Signal Bot Stopped.</b>")
            break
        except Exception as e:
            print(f"  [ERROR] {e}")

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
