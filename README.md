<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:0F0C29,50:302B63,100:6B46C1&height=220&section=header&text=CLEXER%20V17.8.5&fontSize=60&fontColor=ffffff&animation=fadeIn&fontAlignY=35&desc=AI-Powered%20Trading%20Signal%20%26%20Copy-Trade%20Engine&descAlignY=55&descSize=20" width="100%"/>

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=600&size=24&duration=2600&pause=900&color=9D7BFF&center=true&vCenter=true&width=700&lines=%F0%9F%95%AF%EF%B8%8F+Scanning+BTC+%2B+Alt-Coins+24%2F7;%F0%9F%A4%96+Claude-Powered+Signal+Analysis;%F0%9F%93%88+Auto-Executing+on+BingX;%F0%9F%9B%A1%EF%B8%8F+Risk-Managed+%7C+VIP%2FFree+Tiered" alt="typing-svg" />

<br/>

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Telegram](https://img.shields.io/badge/Telegram-Bot%20API-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![Claude](https://img.shields.io/badge/Claude-Opus%205%20%7C%20Fable%205-6B46C1?style=for-the-badge&logo=anthropic&logoColor=white)](https://www.anthropic.com/)
[![BingX](https://img.shields.io/badge/BingX-Perpetual%20Futures-4C1D95?style=for-the-badge&logo=binance&logoColor=white)](https://bingx.com/)
[![FastAPI](https://img.shields.io/badge/API-FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Railway](https://img.shields.io/badge/Deploy-Railway-8B5CF6?style=for-the-badge&logo=railway&logoColor=white)](https://railway.app/)

<br/>

```
📊 4H ▲▲▼▲  |  1H ▲▲▲▼  |  5M ▲▼▲▲     🤖 CLEXER SCANNING...     🟢 BUY  🛑 SL  🎯 TP1  🏆 TP2
```

</div>

---

## 📖 About

CLEXER is a Telegram trading bot that scans **BTC and alt-coins** across multiple timeframes using **Claude (Anthropic)**, posts structured signals to Telegram, and — if a user connects their **BingX** account — auto-copies those signals as real orders with configurable risk management.

It runs five independent signal pipelines — **BTC**, **Scan1**, **Scan2**, and two auto-scheduled alt-coin test tracks (**TS1**/**TS2**) — each with its own schedule, AI model/gateway choice, entry style, and copy-trade toggle, plus a companion **Telegram Mini App** (real-time portfolio, copy-trade settings, and per-tier virtual/paper trading) and a full **VIP/Free tiering system** for monetizing signal distribution across any number of Telegram channels and bot users.

---

## ✨ Feature Overview

<details>
<summary><b>🎯 Signal Engine</b></summary>

| Capability | Detail |
|---|---|
| BTC Analysis | Scheduled or on-demand (`/signal`), multi-timeframe (Weekly/4H/1H/5M) via TradingView + BingX candle data |
| Scan1 / Scan2 | Independent alt-coin scanning pipelines, each with its own schedule and candidate-picking logic |
| TS1 / TS2 | Auto-scheduled alt-coin test tracks with their own independent slot schedules and verified/unverified promotion tracking |
| `/coin` Lookup | On-demand analysis of any coin, choosing **Market** or **Pullback** entry style before Claude analyzes it |
| Entry Styles | **Market** (instant fill) or **Zone/Pullback** (limit order at a computed price range) — per scan type |
| AI Model & Gateway | Opus 5 / Fable 5, Direct or Aerolink (up to 20 rotating keys) — set **independently** per scan type |
| Verified-Slot Priority | VIP-verified time slots always run — never silently skipped for a busy regular cycle — with bounded true-parallel execution |
| Auto-Blacklist | A slot with 3+ tracked trades below its win-rate target is blocked automatically; new/unproven slots are left alone until they've actually recorded 3 |
| Wick-Check Safety Net | Re-verifies long-running trades against real candle highs/lows every 4h (after 6h runtime) to catch missed TP/SL |
| 12-Hour Timeout | Force-closes any trade still running after 12 hours |

</details>

<details>
<summary><b>🔄 Copy Trading</b></summary>

| Capability | Detail |
|---|---|
| Account Linking | Users connect their own BingX API key/secret (encrypted at rest) |
| Sizing | Fixed margin, manual leverage, or **Auto-Risk** (leverage computed from a max-$-loss target) |
| Per-Type Toggle | BTC / Scan1 / Scan2 / Demo1 / Demo2 copy-trade can each be turned on/off independently |
| TP1 Close % | Configurable split between TP1 and TP2 (default 50/50), tap-keypad or manual entry |
| **Trailing SL** | At the halfway point to TP1, auto-moves SL to the halfway point toward entry — locks in capital before TP1 hits, on/off per BTC/Scan1/Scan2/Demo1/Demo2 |
| Orphan/Ghost Recovery | Background monitor (60s loop) reconciles real BingX positions vs. bot state, restores missing SL/TP on bot-known trades, and cleans up stale state |
| **Orphan Adjust Toggle** | Off by default — a position on a user's BingX account that didn't come from a real bot signal is left completely alone unless explicitly turned on |
| **Virtual (Paper) Trading** | Opt-in, off by default per user — mirrors real signal outcomes into a simulated balance, sized off the user's own Mini App calculator settings, gated by tier (VIP gets every VIP signal, Free gets only free-shared ones) |

</details>

<details>
<summary><b>⭐ VIP / Free Tiering</b></summary>

| Capability | Detail |
|---|---|
| Multi-Channel | Any number of VIP or Free Telegram channels, each independently managed via `/channelmgmt` |
| VIP Mirror (Channel 2) | An optional extra channel that automatically receives every VIP signal, lifecycle update, and the pinned daily recap — kept separate from the editable VIP/Free list on purpose |
| Free Daily Quota | Admin-set signal cap per day, active only 06:00–19:00 IST — free-tier users copy exactly what the free channel got |
| Per-Channel Pause | Pause/resume the Signal Channel, VIP Mirror, or any individual VIP/Free channel from a single picker screen (`/pausechannel`, `/resumechannel`) |
| VIP Promotion | Date-range VIP grants (tap or type), works even for users who've never connected BingX |
| Auto Join-Request Approval | Bot auto-approves/declines private VIP channel join requests based on live tier status |
| Expiry Handling | 24h renew-or-removed grace reminder, then auto-downgrade + auto-kick from VIP channel(s) + a trade-history CSV for their VIP window |
| VIP-Accurate Stats | `/stats` win rate reflects only trades actually shown in VIP — not diluted by regular-grid/unverified runs nobody in VIP ever saw |

</details>

<details>
<summary><b>📱 Telegram Mini App</b></summary>

| Capability | Detail |
|---|---|
| Portfolio | Real balance, equity curve (7D/30D/All), Daily P/L (paginated, tap a bar for its date), Best/Worst trade, best session, risk exposure |
| Trades | Live active positions across BTC/Scan1/Scan2, closed-trade history |
| Copy | Connect/manage BingX credentials, sizing/leverage, per-user settings |
| Virtual | Practice-mode calculator + automatic per-tier paper-trade mirroring, with its own on/off toggle |
| Backend | Separate FastAPI service (`api.py`) — Postgres-backed KV store shared with the bot process, admin-controlled maintenance mode |

</details>

<details>
<summary><b>🖼️ Chart Relay & Group Hygiene</b></summary>

| Capability | Detail |
|---|---|
| Chart Images | Every real signal (BTC/Scan1/Scan2/TS1/TS2) gets a chart image attached as a reply-thread to its entry post, fetched from @CoinTrendzBot via a dedicated shared Telegram group |
| Userbot Relay | A real user-account session (Telethon), not the bot token, captures the chart reply — Bot API can never receive messages from another bot |
| `/cp <coin>` | On-demand chart preview in the admin's own DM |
| Auto-Cleanup | The shared relay group is wiped automatically once no scan/chart request is in flight — never mid-request |
| Bot-to-Bot Silence | The bot never processes or replies to messages from any other bot account, anywhere, for safety |

</details>

<details>
<summary><b>🛡️ Admin & Governance</b></summary>

| Capability | Detail |
|---|---|
| Co-Admin | Delegate Scan Control + Trade Control access to one trusted user — no billing, user management, resets, or broadcast |
| Settings Profiles | "Mine" vs "Co-Admin" — swap the entire AI/schedule/toggle configuration with one tap, nothing lost either way |
| Confirmation Gates | Every destructive action (resets, disconnects, closes, kicks) requires explicit Yes/Cancel |
| Broadcast | Message users, channels, or both — target Free/VIP/Specific User, mention every seen user in a group broadcast, quote an existing message to reuse its formatting/image exactly (via `copyMessage`), premium emoji rendering |
| Scheduled Broadcasts | Set a future date+time; list, edit the time, edit the message content, or cancel any pending one |
| Cost Tracking | Every Claude API call logged with token counts + cost (`/report`), across up to 20 rotating Aerolink keys |

</details>

---

## 🗂️ Project Structure

| File | Purpose |
|---|---|
| `bot.py` | Main process — Telegram command/callback handling, scan scheduling, Claude calls, trade state, broadcast, help menus |
| `copytrade.py` | BingX copy-trade logic — order placement/cancellation, per-user settings, position sync, virtual/paper trading engine |
| `api.py` | FastAPI backend for the Mini App — Postgres-backed KV store, trade history/stats, virtual-trading endpoints, payment webhook |
| `clexer-miniapp.html` | Telegram Mini App — Portfolio, Trades, Copy, and Virtual tabs (single-file HTML/CSS/JS) |
| `userbot_login.py` | One-time local script to generate the Telethon session string for the chart-relay userbot |
| `requirements.txt` | Python dependencies |
| `start_clexer.bat` | Local Windows launch script |

---

## 🚀 Setup

```bash
pip install -r requirements.txt
```

Set the environment variables below, then:

```bash
python bot.py
```

> Data (trade state, user DB, settings, logs) persists under `DATA_DIR` — point this at a persistent volume (e.g. Railway) so state survives redeploys. If running `api.py` as a separate service, both processes share state through it (`CLEXER_API_URL`).

<details>
<summary><b>🔐 Environment Variables</b></summary>

| Variable | Required | Purpose |
|---|:---:|---|
| `TELEGRAM_BOT_TOKEN` | ✅ | Bot token from BotFather |
| `ADMIN_CHAT_ID` | ✅ | Telegram chat ID with full admin access |
| `ANTHROPIC_API_KEY` | ✅ | Claude API key (Direct gateway) |
| `CT_ENCRYPT_KEY` | ✅ | Encrypts users' BingX API credentials at rest |
| `TELEGRAM_CHANNEL_ID` | | Legacy Signal channel — pausable, always gets every signal |
| `TELEGRAM_CHANNEL_ID_2` | | Optional VIP mirror channel (see VIP/Free Tiering above) — VIP/Free channels beyond this are managed in-bot via `/channelmgmt` |
| `AEROLINK_API_KEY` (+ up to 20 keys) / `AEROLINK_BASE_URL` | | Alternate Claude gateway, toggled per scan type via `/aiconfig` |
| `TG_USER_API_ID` / `TG_USER_API_HASH` / `TG_USER_SESSION_STRING` | | Telethon userbot credentials for the chart-relay group (see `userbot_login.py`) |
| `COINTRENDZ_GROUP_ID` | | Shared private group both the bot and @CoinTrendzBot are members of, for chart image capture |
| `DATA_DIR` | | Persistent storage path (default `.`) |
| `TV_BRIDGE_URL` | | TradingView chart bridge |
| `MINI_APP_URL` | | Telegram Mini App URL |
| `TRADE_LOG_WEBHOOK` | | Mirrors every CSV trade-log row |
| `PUSH_STATE_SECRET` / `CLEXER_API_URL` | | Shared state-sync endpoint between `bot.py` and `api.py` |

</details>

---

## ⌨️ Key Admin Commands

> Run `/help` in Telegram for the full categorized, room-based menu.

| Command | Does |
|---|---|
| `/go` `/pause` `/stop` | Bot run state |
| `/scan1` `/scan2` `/scantoggle` | Alt-coin scan control |
| `/coin <symbol>` | On-demand coin analysis — pick Market or Pullback entry |
| `/aiconfig` | AI model + gateway, per scan type |
| `/entrystyle` | Market vs Zone entries |
| `/tp1size` | TP1 close % |
| `/trailsl` | Trailing SL on/off, per scan type |
| `/scancopy` / `/ctpause` | Copy-trade on/off per type, plus the Orphan Adjust toggle |
| `/setvip` `/setfree` | Promote/demote a user's tier |
| `/channelmgmt` | Manage VIP/Free channels + daily free quota |
| `/pausechannel` `/resumechannel` | Pick a channel (Signal, VIP Mirror, or any VIP/Free) to pause or resume |
| `/coadmin` | Delegate limited admin access |
| `/synccheck` | Manual orphan/ghost position audit |
| `/stats` | Win-rate/trade statistics (VIP-accurate) |
| `/report` | Claude API cost report |
| `/broadcast` | Message users and/or channels — quote a message to reuse its exact formatting |
| `/schedulebroadcast` `/scheduledbroadcasts` | Schedule a broadcast for later; list/edit time/edit message/cancel |
| `/userstats` | Total/active/blocked user breakdown |

---

## ⚠️ Safety Notes

- Copy trade places **real orders** on users' connected BingX accounts — every SL/TP edit is validated against entry price and live market price so an order can never be placed in a way that triggers instantly.
- The bot never adopts or adjusts a user's own manually-opened BingX positions unless the **Orphan Adjust** toggle is explicitly turned on.
- BingX enforces a **0.001 BTC minimum order size**; combinations that would fall under it are surfaced as an explicit warning rather than silently oversized.
- All destructive actions require **explicit Yes/Cancel confirmation** — no accidental resets, disconnects, or kicks.
- The bot never processes or replies to messages sent by another bot account, in any chat.

<div align="center">

```
🕯️ ▂▅▇█▇▅▂  ▁▃▆█▆▃▁  ▂▄▇█▇▄▂     🤖 SIGNAL LOCKED     ⚡ EXECUTING ON BINGX ⚡
```

*Built for speed, safety, and signal quality.*

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:6B46C1,50:302B63,100:0F0C29&height=120&section=footer" width="100%"/>

</div>
