[README.md](https://github.com/user-attachments/files/29726401/README.md)
<div align="center">

# ⚡ CLEXER V17.8.5

### AI-Powered BTC & Alt-Coin Signal Engine — with Live BingX Copy Trading

*Claude-driven multi-timeframe analysis → Telegram signals → automatic BingX execution*

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Telegram](https://img.shields.io/badge/Telegram-Bot%20API-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![Claude](https://img.shields.io/badge/Claude-Opus%204.8%20%7C%20Fable%205-6B46C1?style=for-the-badge&logo=anthropic&logoColor=white)](https://www.anthropic.com/)
[![BingX](https://img.shields.io/badge/BingX-Perpetual%20Futures-4C1D95?style=for-the-badge)](https://bingx.com/)
[![Railway](https://img.shields.io/badge/Deploy-Railway-8B5CF6?style=for-the-badge&logo=railway&logoColor=white)](https://railway.app/)

</div>

---

## 📖 About

CLEXER is a Telegram trading bot that scans **BTC and alt-coins** across multiple timeframes using **Claude (Anthropic)**, posts structured signals to Telegram, and — if a user connects their **BingX** account — auto-copies those signals as real orders with configurable risk management.

It runs three independent pipelines (**BTC**, **Scan1**, **Scan2**), each with its own schedule, AI model/gateway choice, entry style, and copy-trade toggle — plus a full **VIP/Free tiering system** for monetizing signal distribution across Telegram channels and bot users.

---

## ✨ Feature Overview

<details>
<summary><b>🎯 Signal Engine</b></summary>

| Capability | Detail |
|---|---|
| BTC Analysis | Scheduled or on-demand (`/signal`), multi-timeframe (Weekly/4H/1H/5M) via TradingView + BingX candle data |
| Scan1 / Scan2 | Independent alt-coin scanning pipelines, each with its own schedule and candidate-picking logic |
| Entry Styles | **Market** (instant fill) or **Zone** (limit order at the lower bound of a computed price range) — per scan type |
| AI Model & Gateway | Opus 4.8 / Fable 5, Direct or Aerolink — set **independently** for BTC, Scan1, Scan2, and Test/Demo |
| Wick-Check Safety Net | Re-verifies long-running trades against real candle highs/lows every 4h (after 6h runtime) to catch missed TP/SL |
| 12-Hour Timeout | Force-closes any trade still running after 12 hours |

</details>

<details>
<summary><b>🔄 Copy Trading</b></summary>

| Capability | Detail |
|---|---|
| Account Linking | Users connect their own BingX API key/secret (encrypted at rest) |
| Sizing | Fixed margin, manual leverage, or **Auto-Risk** (leverage computed from a max-$-loss target) |
| Per-Type Toggle | BTC / Scan1 / Scan2 copy-trade can each be turned on/off independently |
| TP1 Close % | Configurable split between TP1 and TP2 (default 50/50), tap-keypad or manual entry |
| **Trailing SL** | At the halfway point to TP1, auto-moves SL to the halfway point toward entry — locks in capital before TP1 hits, on/off per BTC/Scan1/Scan2 |
| Orphan/Ghost Recovery | Background monitor (30s loop) reconciles real BingX positions vs. bot state — adopts unknown positions with an emergency SL, cleans up stale state |

</details>

<details>
<summary><b>⭐ VIP / Free Tiering</b></summary>

| Capability | Detail |
|---|---|
| Multi-Channel | Any number of VIP or Free Telegram channels, each independently managed |
| Free Daily Quota | Admin-set signal cap per day, active only 06:00–19:00 IST — free-tier users copy exactly what the free channel got |
| VIP Promotion | Date-range VIP grants (tap or type), works even for users who've never connected BingX |
| Auto Join-Request Approval | Bot auto-approves/declines private VIP channel join requests based on live tier status |
| Expiry Handling | 24h renew-or-removed grace reminder, then auto-downgrade + auto-kick from VIP channel(s) + a trade-history CSV for their VIP window |

</details>

<details>
<summary><b>🛡️ Admin & Governance</b></summary>

| Capability | Detail |
|---|---|
| Co-Admin | Delegate Scan Control + Trade Control access to one trusted user — no billing, user management, resets, or broadcast |
| Settings Profiles | "Mine" vs "Co-Admin" — swap the entire AI/schedule/toggle configuration with one tap, nothing lost either way |
| Confirmation Gates | Every destructive action (resets, disconnects, closes, kicks) requires explicit Yes/Cancel |
| Broadcast | Message all users, all channels, or both — with blocked-user auto-exclusion |
| Cost Tracking | Every Claude API call logged with token counts + cost (`/report`) |

</details>

---

## 🗂️ Project Structure

| File | Purpose |
|---|---|
| `bot.py` | Main process — Telegram command/callback handling, scan scheduling, Claude calls, trade state, help menus |
| `copytrade.py` | BingX copy-trade logic — order placement/cancellation, per-user settings, position sync |
| `clexer-miniapp.html` | Telegram Mini App (maintenance-mode screen) |
| `api.py` | Supplementary API surface |
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

> Data (trade state, user DB, settings, logs) persists under `DATA_DIR` — point this at a persistent volume (e.g. Railway) so state survives redeploys.

<details>
<summary><b>🔐 Environment Variables</b></summary>

| Variable | Required | Purpose |
|---|:---:|---|
| `TELEGRAM_BOT_TOKEN` | ✅ | Bot token from BotFather |
| `ADMIN_CHAT_ID` | ✅ | Telegram chat ID with full admin access |
| `ANTHROPIC_API_KEY` | ✅ | Claude API key (Direct gateway) |
| `CT_ENCRYPT_KEY` | ✅ | Encrypts users' BingX API credentials at rest |
| `TELEGRAM_CHANNEL_ID` / `_2` | | Legacy broadcast channels (VIP/Free channels are managed separately in-bot) |
| `AEROLINK_API_KEY` / `AEROLINK_BASE_URL` | | Alternate Claude gateway, toggled per scan type via `/aiconfig` |
| `DATA_DIR` | | Persistent storage path (default `.`) |
| `TV_BRIDGE_URL` | | TradingView chart bridge |
| `MINI_APP_URL` | | Telegram Mini App URL |
| `TRADE_LOG_WEBHOOK` | | Mirrors every CSV trade-log row |
| `PUSH_STATE_SECRET` / `CLEXER_API_URL` | | External state-sync endpoint |

</details>

---

## ⌨️ Key Admin Commands

> Run `/help` in Telegram for the full categorized, room-based menu.

| Command | Does |
|---|---|
| `/go` `/pause` `/stop` | Bot run state |
| `/scan1` `/scan2` `/scantoggle` | Alt-coin scan control |
| `/aiconfig` | AI model + gateway, per scan type |
| `/entrystyle` | Market vs Zone entries |
| `/tp1size` | TP1 close % |
| `/trailsl` | Trailing SL on/off, per scan type |
| `/scancopy` | Copy-trade on/off, per scan type |
| `/setvip` `/setfree` | Promote/demote a user's tier |
| `/channelmgmt` | Manage VIP/Free channels + daily free quota |
| `/coadmin` | Delegate limited admin access |
| `/synccheck` | Manual orphan/ghost position audit |
| `/report` | Claude API cost report |
| `/broadcast` | Message users and/or channels |
| `/userstats` | Total/active/blocked user breakdown |

---

## ⚠️ Safety Notes

- Copy trade places **real orders** on users' connected BingX accounts — every SL/TP edit is validated against entry price and live market price so an order can never be placed in a way that triggers instantly.
- BingX enforces a **0.001 BTC minimum order size**; combinations that would fall under it are surfaced as an explicit warning rather than silently oversized.
- All destructive actions require **explicit Yes/Cancel confirmation** — no accidental resets, disconnects, or kicks.

<div align="center">

---

*Built for speed, safety, and signal quality.*

</div>
