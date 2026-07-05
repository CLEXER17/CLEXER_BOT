[README.md](https://github.com/user-attachments/files/29673419/README.md)
# CLEXER V17.8.5

A Telegram trading bot that scans BTC and alt-coins for signals using Claude (Anthropic), posts them to a channel/DM, and optionally auto-copies trades onto users' own BingX perpetual futures accounts.

## Features

- **BTC signal analysis** — scheduled or on-demand (`/signal`), using TradingView/BingX chart data + Claude to produce BUY/SELL/no-signal calls with entry, SL, TP1, TP2.
- **Scan1 / Scan2 alt-coin scanning** — independent scheduled pipelines that scan BingX perpetuals for setups, each with its own schedule and copy-trade toggle.
- **Copy trade** — users connect their own BingX API key/secret; the bot places matching orders on their account with configurable margin, manual leverage, or auto-risk (leverage computed from a max-$-loss target).
- **Per-type copy trade toggle** — BTC / Scan1 / Scan2 auto-copy can each be turned on/off independently.
- **SL/TP management** — move SL to breakeven, set custom SL/TP1/TP2 on any open trade (BTC or any open Scan1/Scan2 position) via a tap-to-enter price keypad, with direction/entry/live-price validation so an order can't be placed in a way that would trigger instantly or make no sense.
- **Wick-check safety net** — re-verifies long-running trades against real candle highs/lows to catch missed TP/SL updates, plus a 12-hour hard timeout that force-closes stale trades.
- **Orphan/ghost position recovery** — a background monitor (every 30s) reconciles each copy user's real BingX positions against the bot's own state: adopts positions the bot didn't know about (with an emergency SL), and cleans up state for positions that closed outside the bot's control. Falls back to asking Claude only when an automatic fix itself fails.
- **Admin controls** — pause/stop, AI model switch (Opus 4.8 / Fable 5), API gateway switch (direct Anthropic / Aerolink), broadcast (users/channels/both), user directory with per-user stats, Contact Admin / Signal Channel button toggles, confirmation prompts on every destructive action.
- **Mini app** — a small Telegram Mini App (`clexer-miniapp.html`) with a maintenance-mode screen.
- **Cost tracking** — every Claude API call is logged with token counts and cost (`/report`).

## Files

| File | Purpose |
|---|---|
| `bot.py` | Main bot process — Telegram command/callback handling, scan scheduling, Claude calls, trade state, help menus |
| `copytrade.py` | All BingX copy-trade logic — placing/cancelling orders, per-user settings, position sync/reconciliation |
| `clexer-miniapp.html` | Telegram Mini App (maintenance screen) |
| `api.py` | Small API surface (see file for current scope) |
| `requirements.txt` | Python dependencies |
| `start_clexer.bat` | Local Windows launch script |

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
2. Set environment variables (see below).
3. Run:
   ```
   python bot.py
   ```

Data (trade state, user DB, logs) is persisted under `DATA_DIR` (defaults to the working directory) so it survives restarts on platforms with a persistent volume (e.g. Railway).

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | ✅ | Bot token from BotFather |
| `ADMIN_CHAT_ID` | ✅ | Telegram chat ID with admin access |
| `ANTHROPIC_API_KEY` | ✅ | Claude API key (direct gateway) |
| `TELEGRAM_CHANNEL_ID` | | Primary broadcast channel |
| `TELEGRAM_CHANNEL_ID_2` | | Secondary broadcast channel |
| `CT_ENCRYPT_KEY` | ✅ (for copy trade) | Key used to encrypt users' BingX API credentials at rest |
| `AEROLINK_API_KEY` / `AEROLINK_BASE_URL` | | Alternate Claude API gateway (toggle via `/gateway`, always resets to direct on deploy) |
| `DATA_DIR` | | Where state/logs/user DB are stored (default: `.`) |
| `TV_BRIDGE_URL` | | TradingView chart bridge, if used |
| `MINI_APP_URL` | | URL for the Telegram Mini App |
| `TRADE_LOG_WEBHOOK` | | Optional webhook that mirrors every CSV trade-log row |
| `PUSH_STATE_SECRET` | | Auth secret for `CLEXER_API_URL` state push, if used |
| `CLEXER_API_URL` | | External API endpoint for state sync, if used |

## Key admin commands

Run `/help` in Telegram for the full categorized menu. A few entry points:

- `/go`, `/pause`, `/stop` — bot run state
- `/scan1`, `/scan2`, `/scantoggle` — alt-coin scan control
- `/scancopy` (or `/ctpause`) — per-type (BTC/Scan1/Scan2) copy-trade on/off
- `/synccheck` — manual orphan/ghost position audit
- `/report` — Claude API cost report
- `/broadcast` — message all users and/or channels
- `/userstats` — total/active/blocked user breakdown

## Safety notes

- Copy trade places real orders on users' connected BingX accounts — SL/TP edits are validated against entry price and live market price before submission to avoid orders that would trigger instantly.
- BingX enforces a minimum order size (0.001 BTC); very small margin/risk combinations can be pushed above the exchange minimum, which the bot now surfaces as a warning rather than applying silently.
- Destructive actions (resets, disconnects, closes, kicks) require an explicit Yes/Cancel confirmation.
