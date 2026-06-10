# CRYPTO SIGNAL BOT — SETUP GUIDE

## Step 1: Install dependencies
```
pip install anthropic requests
```

## Step 2: Create Telegram Bot
1. Open Telegram → search @BotFather
2. Send /newbot → follow steps → copy the BOT TOKEN
3. Create a channel → make your bot an Admin in it
4. Channel ID = @yourchannelusername (public) or numeric ID (private)

## Step 3: Set your keys
Edit bot.py lines 18-20:
```
ANTHROPIC_API_KEY = "sk-ant-..."
TELEGRAM_BOT_TOKEN = "123456:ABC..."
TELEGRAM_CHANNEL_ID = "@yourchannel"
```

OR set as environment variables (recommended):
```
set ANTHROPIC_API_KEY=sk-ant-...
set TELEGRAM_BOT_TOKEN=123456:ABC...
set TELEGRAM_CHANNEL_ID=@yourchannel
```

## Step 4: Run
```
python bot.py
```

## Step 5: Keep running (optional)
Use Windows Task Scheduler or run in background:
```
pythonw bot.py
```

## Scan interval
Default: every 5 minutes. Change SCAN_INTERVAL_SECONDS in bot.py line 26.

## Signal logic
- Only sends NEW signals to channel (no duplicate spam)
- NO TRADE signals are silently skipped
- Analyzes 15m + 1H + 4H candles via SMC method
- Claude picks: BUY / SELL / NO TRADE with entry, SL, TP1, TP2, R:R
