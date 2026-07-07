"""
CLEXER Signal Bot V17.8.5
"""

import os, time, json, base64, requests, anthropic, threading, re, subprocess

# Install Playwright Chromium at startup (fast if already installed)
print("[STARTUP] Ensuring Playwright Chromium is installed...")
print("[STARTUP] Playwright Chromium ready")

# Global TradingView chart lock — held by scan while switching symbol + fetching all TFs.
# All other TV access (price/candle checks) must NOT switch the chart; they use non-blocking
# acquire so they fall through to BingX when scan holds the chart.
_tv_chart_lock = threading.Lock()
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from io import BytesIO
from datetime import datetime, timezone, timedelta
from collections import defaultdict

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# --- CONFIG -------------------------------------------------------------------
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY",   "")
AEROLINK_API_KEY    = os.getenv("AEROLINK_API_KEY",    "")   # separate key issued by aerolink.lat — never mix with ANTHROPIC_API_KEY
AEROLINK_BASE_URL   = os.getenv("AEROLINK_BASE_URL",   "https://capi.aerolink.lat/")
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN",  "")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")
ADMIN_CHAT_ID       = os.getenv("ADMIN_CHAT_ID",       "")
TV_BRIDGE_URL       = os.getenv("TV_BRIDGE_URL", "").rstrip("/")
MINI_APP_URL        = os.getenv("MINI_APP_URL", "").rstrip("/")   # Railway mini app URL for chart screenshots

SYMBOL               = "BTCUSDT"
TICK_INTERVAL        = 5     # price check every 5s when trade active
PRICE_CHECK_INTERVAL = 3600
SIGNAL_SCAN_INTERVAL = 14400
NEWS_CHECK_INTERVAL  = 1800
BINANCE_BASE         = "https://api1.binance.com/api/v3"
IST                  = timedelta(hours=5, minutes=30)

SEND_CHARTS       = False   # OFF by default - /images on to enable
CHART_SNAP_ENABLED = True   # /chartson /chartsoff toggle
CHART_TFS         = ["weekly", "4h", "1h", "5m"]
SEND_NEWS         = False
MAX_NEWS_AGE      = 4
MAX_NEWS_PER_RUN  = 3

NEWS_SOURCES = [
    {"url": "https://feeds.feedburner.com/CoinDesk",          "name": "CoinDesk"},
    {"url": "https://cointelegraph.com/rss",                   "name": "CoinTelegraph"},
    {"url": "https://www.newsbtc.com/feed/",                   "name": "NewsBTC"},
    {"url": "https://cryptopotato.com/feed/",                  "name": "CryptoPotato"},
    {"url": "https://bitcoinmagazine.com/.rss/full/",          "name": "BTC Magazine"},
    {"url": "https://decrypt.co/feed",                         "name": "Decrypt"},
    {"url": "https://ambcrypto.com/feed/",                     "name": "AMBCrypto"},
    {"url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "name": "CoinDesk2"},
]

tv_bridge_state = {
    "online": False, "cdp_ok": False, "last_seen": 0,
    "last_check": 0, "fail_count": 0, "source": "BINANCE",
    "tv_version": "", "tv_symbol": "", "cached_intervals": [],
    "check_interval": 60,
}

def now_ist():  return datetime.now(timezone.utc) + IST
def ist_str():  return now_ist().strftime("%d %b %Y  %I:%M %p IST")

def _next_schedule_times():
    """Returns (next_btc_scan, next_scan1, next_scan2) as display strings.
    next_btc_scan has no 'IST' suffix (caller appends it); the other two already include it."""
    _ist_now = now_ist()
    _now_hm  = (_ist_now.hour, _ist_now.minute)
    _scan_hrs = {7, 11, 15, 19, 23}
    _next_btc_scan = next((f"{h}:21" for h in sorted(_scan_hrs)
                            if h > _ist_now.hour or (h == _ist_now.hour and _ist_now.minute < 21)),
                           "07:21 tomorrow")
    def _next_slot(schedule):
        _fut = [(h, m) for h, m in schedule if (h, m) > _now_hm]
        if _fut:
            _h, _m = _fut[0]
            return f"{_h}:{_m:02d} IST"
        _h, _m = schedule[0]
        return f"{_h}:{_m:02d} IST (tomorrow)"
    return _next_btc_scan, _next_slot(SCAN1_SCHEDULE), _next_slot(SCAN2_SCHEDULE)
def get_session():
    mins = now_ist().hour * 60 + now_ist().minute
    if 450 <= mins < 990:         return "LONDON"
    if mins >= 1110 or mins < 60: return "NEW_YORK"
    return "ASIA"
def is_trading_hours(): return get_session() in ("LONDON", "NEW_YORK")
def is_ist_sleep():
    mins = now_ist().hour * 60 + now_ist().minute
    return 60 <= mins < 450

def is_weekend_sleep() -> bool:
    """True from Friday 22:00 IST to Sunday 23:00 IST — full bot pause."""
    t = now_ist()
    wd = t.weekday()   # 0=Mon … 4=Fri, 5=Sat, 6=Sun
    mins = t.hour * 60 + t.minute
    if wd == 4 and mins >= 22 * 60:   return True   # Fri 22:00+
    if wd == 5:                        return True   # All Saturday
    if wd == 6 and mins < 23 * 60:    return True   # Sun before 23:00
    return False

_weekend_sleep_notified = False

active_trade = {
    "signal": None, "entry": None, "sl": None,
    "tp1": None, "tp2": None, "tp1_hit": False,
    "entry_type": "MARKET", "entry_note": "",
    "entry_hit": False, "sl_wicked": False, "scan_count": 0,
}
scan1_trades = []   # list of active scan1 trade dicts (unlimited slots)
scan2_trades = []   # list of active scan2 trade dicts (unlimited slots)
demo_scan1_trades = []   # DEMO trades — test strategy only, no copytrade
demo_scan2_trades = []
SCAN1_AUTO_ENABLED = True
TEST_SCAN_ENABLED = False
TEST_SCAN_MINUTE  = 5
_test_scan1_last_hour = -1
_test_scan2_last_hour = -1
last_scan_tick_time = 0
signal_history        = []
scan_history          = []   # closed scan trades — appended on TP/SL/missed
trade_outcomes        = []
force_scan            = threading.Event()
bot_paused            = threading.Event()  # PAUSE: freezes everything
bot_stopped           = threading.Event()  # STOP: blocks new scans only, monitoring continues
btc_analysis_enabled  = False  # OFF by default — /btcanalysis on to enable
SCAN_MODEL             = "claude-opus-4-8"  # BTC's model — switch via /model button or /aiconfig
USE_AEROLINK           = False  # BTC's gateway — switch via /gateway button or /aiconfig
SCAN1_MODEL    = "claude-opus-4-8"  # Scan1's model  — set via /aiconfig
SCAN1_AEROLINK = False              # Scan1's gateway — set via /aiconfig
SCAN2_MODEL    = "claude-opus-4-8"  # Scan2's model  — set via /aiconfig
SCAN2_AEROLINK = False              # Scan2's gateway — set via /aiconfig
TEST_MODEL     = "claude-opus-4-8"  # /test /demo model  — set via /aiconfig
TEST_AEROLINK  = False              # /test /demo gateway — set via /aiconfig
ZONE_ENTRY_ENABLED = False  # Scan1/Scan2 entry style — MARKET (instant) vs ZONE (limit order at a price range's midpoint). Set via /entrystyle
_ZONE_BAND_PCT = 0.008      # zone width — ±0.8% around the computed entry price
CO_ADMIN_CHAT_ID  = ""    # a single trusted friend who gets ONE extra permission: /tradelog. No user mgmt, no billing, no resets, no broadcast.
CO_ADMIN_ENABLED  = False # ON = the co-admin permission is active AND their contact button shows next to Contact Admin
TRAIL_SL_BTC   = False  # Trailing SL — halfway to TP1, move SL to halfway toward entry. Set via /trailsl
TRAIL_SL_SCAN1 = False
TRAIL_SL_SCAN2 = False

def _apply_trail_sl(ver: int, t: dict, price: float):
    """Fixed 50/50 rule: once price reaches the halfway point to TP1, move SL to
    the halfway point between the original SL and entry — locks in more capital
    without waiting for TP1 itself. Runs once per trade (trail_sl_moved guards it)."""
    if t.get("trail_sl_moved") or t.get("tp1_hit"):
        return
    enabled = TRAIL_SL_SCAN1 if ver == 1 else TRAIL_SL_SCAN2
    if not enabled:
        return
    entry = t["entry"]; tp1 = t["tp1"]; sig = t["signal"]
    orig_sl = t.get("trail_sl_orig", t["sl"])
    t["trail_sl_orig"] = orig_sl
    midpoint_price = (entry + tp1) / 2
    hit = (sig == "BUY" and price >= midpoint_price) or (sig == "SELL" and price <= midpoint_price)
    if not hit:
        return
    new_sl = (orig_sl + entry) / 2
    t["sl"] = new_sl
    t["trail_sl_moved"] = True
    ct.update_scan_sl(t["symbol"], new_sl)
    save_state()
    send_telegram(
        f"🛡️ <b>Trailing SL — #{t['symbol']}</b>  Scan{ver}\n\n"
        f"Price reached halfway to TP1 — SL moved <b>{orig_sl:,.4g} → {new_sl:,.4g}</b> to lock in more capital.\n\n"
        f"<i>🛡️ Capital protected</i>")

def _apply_trail_sl_btc(price: float):
    if not TRAIL_SL_BTC or active_trade.get("trail_sl_moved") or active_trade.get("tp1_hit"):
        return
    entry = active_trade["entry"]; tp1 = active_trade["tp1"]; sig = active_trade["signal"]
    orig_sl = active_trade.get("trail_sl_orig", active_trade["sl"])
    active_trade["trail_sl_orig"] = orig_sl
    midpoint_price = (entry + tp1) / 2
    hit = (sig == "BUY" and price >= midpoint_price) or (sig == "SELL" and price <= midpoint_price)
    if not hit:
        return
    new_sl = (orig_sl + entry) / 2
    active_trade["sl"] = new_sl
    active_trade["trail_sl_moved"] = True
    ct.on_update_sl(new_sl)
    save_active_trade()
    send_telegram(
        f"🛡️ <b>Trailing SL — BTC</b>\n\n"
        f"Price reached halfway to TP1 — SL moved <b>{orig_sl:,.0f} → {new_sl:,.0f}</b> to lock in more capital.\n\n"
        f"<i>🛡️ Capital protected</i>")
# ─── VIP / Free channels + user tiers ──────────────────────────────────────
CHANNELS: list = []  # [{"id": str, "tier": "vip"/"free", "label": str}, ...] — any number of each
FREE_SIGNAL_DAILY_LIMIT = 0   # max signals shared to free channels/users per day (0 = none shared)
_free_signal_tracker = {"date": "", "count": 0}  # resets automatically when the IST date rolls over

def _channels_by_tier(tier: str) -> list:
    return [c["id"] for c in CHANNELS if c.get("tier") == tier and c.get("id")]

def _in_free_window() -> bool:
    now = datetime.now(timezone.utc) + IST
    return 6 <= now.hour < 19  # 06:00–19:00 IST

def _free_quota_available() -> bool:
    global _free_signal_tracker
    now = datetime.now(timezone.utc) + IST
    today = now.strftime("%Y-%m-%d")
    if _free_signal_tracker.get("date") != today:
        _free_signal_tracker = {"date": today, "count": 0}
    return _in_free_window() and _free_signal_tracker["count"] < FREE_SIGNAL_DAILY_LIMIT

def _consume_free_quota():
    _free_signal_tracker["count"] = _free_signal_tracker.get("count", 0) + 1

def send_to_tier_channels(text: str, share_free: bool):
    """Sends to every registered VIP channel always, and to FREE channels only
    if share_free is True (the daily quota decision made once per signal)."""
    text = _apply_premium_emojis(text)
    for cid in _channels_by_tier("vip"):
        try:
            r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
            if not r.json().get("ok"):
                print(f"  [TIER CHANNEL] vip {cid} rejected: {r.json().get('description')}")
        except Exception as e: print(f"  [TIER CHANNEL] vip {cid}: {e}")
    if share_free:
        for cid in _channels_by_tier("free"):
            try:
                r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": cid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
                if not r.json().get("ok"):
                    print(f"  [TIER CHANNEL] free {cid} rejected: {r.json().get('description')}")
            except Exception as e: print(f"  [TIER CHANNEL] free {cid}: {e}")

ACTIVE_PROFILE = "mine"   # "mine" or "coadmin" — which scan-settings snapshot is currently live
_SETTINGS_PROFILES = {"mine": {}, "coadmin": {}}  # each holds a snapshot of every setting co-admin can touch

def _snapshot_scan_settings() -> dict:
    return {
        "scan_model": SCAN_MODEL, "use_aerolink": USE_AEROLINK,
        "scan1_model": SCAN1_MODEL, "scan1_aerolink": SCAN1_AEROLINK,
        "scan2_model": SCAN2_MODEL, "scan2_aerolink": SCAN2_AEROLINK,
        "test_model": TEST_MODEL, "test_aerolink": TEST_AEROLINK,
        "zone_entry_enabled": ZONE_ENTRY_ENABLED,
        "tp1_close_pct": ct.TP1_CLOSE_PCT,
        "scan1_auto": SCAN1_AUTO_ENABLED, "scan2_auto": SCAN2_AUTO_ENABLED,
        "test_scan": TEST_SCAN_ENABLED, "btc_analysis": btc_analysis_enabled,
        "scan1_schedule": list(SCAN1_SCHEDULE), "scan2_schedule": list(SCAN2_SCHEDULE),
        "scan1_test_schedule": list(SCAN1_TEST_SCHEDULE),
        "btc_ct_enabled": ct.BTC_CT_ENABLED, "scan1_ct_enabled": ct.SCAN1_CT_ENABLED,
        "scan2_ct_enabled": ct.SCAN2_CT_ENABLED,
    }

def _apply_scan_settings(d: dict):
    global SCAN_MODEL, USE_AEROLINK, SCAN1_MODEL, SCAN1_AEROLINK, SCAN2_MODEL, SCAN2_AEROLINK
    global TEST_MODEL, TEST_AEROLINK, ZONE_ENTRY_ENABLED, SCAN1_AUTO_ENABLED, SCAN2_AUTO_ENABLED
    global TEST_SCAN_ENABLED, btc_analysis_enabled, SCAN1_SCHEDULE, SCAN2_SCHEDULE, SCAN1_TEST_SCHEDULE
    if not d:
        return  # nothing snapshotted yet for this profile — leave current values as-is
    SCAN_MODEL = d.get("scan_model", SCAN_MODEL); USE_AEROLINK = d.get("use_aerolink", USE_AEROLINK)
    SCAN1_MODEL = d.get("scan1_model", SCAN1_MODEL); SCAN1_AEROLINK = d.get("scan1_aerolink", SCAN1_AEROLINK)
    SCAN2_MODEL = d.get("scan2_model", SCAN2_MODEL); SCAN2_AEROLINK = d.get("scan2_aerolink", SCAN2_AEROLINK)
    TEST_MODEL = d.get("test_model", TEST_MODEL); TEST_AEROLINK = d.get("test_aerolink", TEST_AEROLINK)
    ZONE_ENTRY_ENABLED = d.get("zone_entry_enabled", ZONE_ENTRY_ENABLED)
    ct.TP1_CLOSE_PCT = d.get("tp1_close_pct", ct.TP1_CLOSE_PCT)
    SCAN1_AUTO_ENABLED = d.get("scan1_auto", SCAN1_AUTO_ENABLED); SCAN2_AUTO_ENABLED = d.get("scan2_auto", SCAN2_AUTO_ENABLED)
    TEST_SCAN_ENABLED = d.get("test_scan", TEST_SCAN_ENABLED); btc_analysis_enabled = d.get("btc_analysis", btc_analysis_enabled)
    SCAN1_SCHEDULE = d.get("scan1_schedule", SCAN1_SCHEDULE); SCAN2_SCHEDULE = d.get("scan2_schedule", SCAN2_SCHEDULE)
    SCAN1_TEST_SCHEDULE = d.get("scan1_test_schedule", SCAN1_TEST_SCHEDULE)
    ct.BTC_CT_ENABLED = d.get("btc_ct_enabled", ct.BTC_CT_ENABLED); ct.SCAN1_CT_ENABLED = d.get("scan1_ct_enabled", ct.SCAN1_CT_ENABLED)
    ct.SCAN2_CT_ENABLED = d.get("scan2_ct_enabled", ct.SCAN2_CT_ENABLED)
CONTACT_ADMIN_ENABLED  = True   # shows/hides the "Contact Admin" button for users — toggled via /adminlinks
SIGNAL_CHANNEL_ENABLED = True   # shows/hides the "Signal Channel" button for users — toggled via /adminlinks
SIGNAL_CHANNEL_LINK    = ""     # admin-provided channel link — set/removed via /adminlinks
last_update_id        = 0
last_force_scan_time  = 0
last_signal_scan_time = 0
last_price_check_time = 0
last_tick_time        = 0
last_news_check_time  = 0
posted_news_guids: set = set()
latest_news_context: list = []
trade_lock = threading.Lock()

DATA_DIR           = os.getenv("DATA_DIR", ".")
CLEXER_API_URL     = os.getenv("CLEXER_API_URL", "").rstrip("/")
PUSH_STATE_SECRET  = os.getenv("PUSH_STATE_SECRET", "")
os.makedirs(DATA_DIR, exist_ok=True)
USER_DB_FILE       = os.path.join(DATA_DIR, "users.json")
ACTIVE_TRADE_FILE  = os.path.join(DATA_DIR, "active_trade.json")
registered_users: set = set()
user_usernames: dict = {}   # chat_id (str) → @username, best-effort, for admin display
blocked_users: set = set()  # chat_ids where sendMessage failed with "bot was blocked by the user"

RATE_LIMIT_USES   = 2
RATE_LIMIT_WINDOW = 3600
friend_usage = defaultdict(list)

def check_rate_limit(chat_id, cmd):
    if str(chat_id) == str(ADMIN_CHAT_ID): return (True, None)
    now = time.time()
    key = (str(chat_id), cmd)
    friend_usage[key] = [t for t in friend_usage[key] if now - t < RATE_LIMIT_WINDOW]
    if len(friend_usage[key]) >= RATE_LIMIT_USES:
        oldest   = friend_usage[key][0]
        reset_dt = datetime.fromtimestamp(oldest + RATE_LIMIT_WINDOW, timezone.utc) + IST
        return (False, reset_dt.strftime("%I:%M %p IST"))
    friend_usage[key].append(now)
    return (True, None)

def load_users():
    global registered_users, user_usernames, blocked_users
    try:
        if os.path.exists(USER_DB_FILE):
            with open(USER_DB_FILE, "r") as f:
                d = json.load(f)
            if isinstance(d, list):
                registered_users = set(int(x) for x in d)  # legacy format — just a list of ids
            else:
                registered_users = set(int(x) for x in d.get("users", []))
                user_usernames   = {str(k): v for k, v in d.get("usernames", {}).items()}
                blocked_users    = set(int(x) for x in d.get("blocked", []))
    except Exception as e:
        print(f"[USERS] Load error: {e}"); registered_users = set()

def save_users():
    try:
        with open(USER_DB_FILE, "w") as f:
            json.dump({
                "users": list(registered_users),
                "usernames": user_usernames,
                "blocked": list(blocked_users),
            }, f)
    except Exception as e: print(f"[USERS] Save error: {e}")

def is_co_admin(chat_id) -> bool:
    return bool(CO_ADMIN_ENABLED and CO_ADMIN_CHAT_ID and str(chat_id) == str(CO_ADMIN_CHAT_ID))

def _co_admin_allowed_commands() -> set:
    """Co-admin's permission set = every command in Scan Control + Trade Control
    (force scans, BTC scan control, SL/TP/close on any trade), plus /tradelog
    specifically. Nothing from Copy Admin (user mgmt), Settings, Broadcast, or
    billing/report screens — those stay admin-only."""
    cmds = set()
    for subcats in (_SCAN_SUBCATS, _TRADECONTROL_SUBCATS):
        for _label, entries in subcats.values():
            for entry in entries:
                cmds.add(entry[0])
    cmds.add("/tradelog")
    return cmds

def register_user(chat_id, username=None):
    chat_id = int(chat_id)
    changed = False
    if username and user_usernames.get(str(chat_id)) != username:
        user_usernames[str(chat_id)] = username; changed = True
    if chat_id in blocked_users:
        blocked_users.discard(chat_id); changed = True  # they messaged us — clearly not blocked
    if chat_id not in registered_users:
        registered_users.add(chat_id); changed = True
    if changed:
        save_users()

def _build_users_summary():
    # Negative chat_ids are groups/channels, not individual users — exclude them.
    _real_users   = [u for u in registered_users if int(u) > 0]
    _real_blocked = [u for u in blocked_users if int(u) > 0]
    _total_users  = len(_real_users)
    _active_users = len([u for u in ct.active_ids() if int(u) > 0])
    _blocked_unames = [f"@{user_usernames[str(u)]}" if user_usernames.get(str(u)) else str(u) for u in _real_blocked]
    _blocked_str = ", ".join(_blocked_unames) if _blocked_unames else "none"
    return (
        f"👥 Total users: {_total_users}\n"
        f"🟢 Using copy trade: {_active_users}\n"
        f"🚫 Blocked bot ({len(_real_blocked)}): {_blocked_str}\n"
    )

def _user_dm_link(chat_id):
    uname = user_usernames.get(str(chat_id))
    if uname:
        return f'<a href="https://t.me/{uname}">@{uname}</a>'
    # tg://user?id= opens the user's profile card even without a username. Telegram
    # sometimes auto-detects raw digits as a phone number and overrides the link on
    # just that number — inconsistently, depending on the digits. Keeping the link
    # text non-numeric ("Open Profile") sidesteps that entirely; the plain ID is
    # shown separately afterward for reference/copying.
    return f'<a href="tg://user?id={chat_id}">👤 Open Profile</a> — ID <code>{chat_id}</code> (no username set)'

def _render_user_list_text(title, ids):
    if not ids:
        return f"<b>{title}</b>\n\nNone."
    lines = [f"{i+1}. {_user_dm_link(uid)}" for i, uid in enumerate(ids)]
    return f"<b>{title} ({len(ids)})</b>\n\n" + "\n".join(lines)

broadcast_pending: dict = {}
pending_input: dict = {}   # cid → {"cmd": "/settp1"} — waiting for user to type the value
_last_help_msg: dict = {}  # cid → message_id of last /help message (for dedup/cleanup)
_tp_state: dict = {}       # cid → {"target": "scan1"/"scan2"/"demo", "digits": [], "times": [(h,m),...], "msg_id": int}
_pending_confirm: dict = {}  # cid → {"action": str, "label": str, "back_cb": str} — awaiting Yes/Cancel on a destructive action
_np_state: dict = {}       # cid → {"target": "setsize"/"setleverage"/"setrisk", "digits": str, "back_cb": str}
_NP_CONFIG = {
    "setsize":     {"label": "Margin Per Trade",       "unit": "USDT", "cmd": "/setsize",     "decimals": True},
    "setleverage": {"label": "Leverage",                "unit": "x",    "cmd": "/setleverage", "decimals": False},
    "setrisk":     {"label": "Auto-Risk (Max Loss)",    "unit": "USDT", "cmd": "/setrisk",     "decimals": True},
    "tp1size":     {"label": "TP1 Close %",             "unit": "%",    "cmd": "/tp1size",     "decimals": False},
    "freelimit":   {"label": "Free Channel Daily Limit", "unit": "signals/day", "cmd": "/freelimit", "decimals": False},
}

_pp_state: dict = {}       # cid → {"action","kind","symbol","idx","digits": str, "back_cb": str} — tap price-picker
_vip_state: dict = {}      # cid → {"uid","stage":"start"/"end","digits":str,"start":str} — VIP date-range tap-picker

def _vip_render(chat_id, cid, msg_id):
    st = _vip_state.get(str(cid))
    if not st:
        return
    label = "Start Date" if st["stage"] == "start" else "End Date"
    d = st["digits"]
    disp = d
    if len(d) > 2: disp = d[:2] + "." + d[2:]
    if len(d) > 4: disp = disp[:5] + "." + d[4:]
    text = (
        f"📅 <b>VIP {label}</b>\n\n"
        f"Entering: <code>{disp or '—'}</code>\n\n"
        f"<i>Tap 8 digits: DD MM YYYY (e.g. 17082026 for 17.08.2026)</i>")
    rows = [
        [{"text": str(n), "callback_data": f"vip_d:{n}"} for n in (1, 2, 3)],
        [{"text": str(n), "callback_data": f"vip_d:{n}"} for n in (4, 5, 6)],
        [{"text": str(n), "callback_data": f"vip_d:{n}"} for n in (7, 8, 9)],
        [{"text": "0", "callback_data": "vip_d:0"}],
        [{"text": "⌨️ Type Instead", "callback_data": "vip_manual"}],
        [{"text": "◀️ Erase", "callback_data": "vip_prev"}, {"text": "🚫 Back", "callback_data": "vip_back"}],
    ]
    if len(d) == 8:
        rows.insert(-1, [{"text": "💾 Next" if st["stage"] == "start" else "💾 Save", "callback_data": "vip_save"}])
    _help_edit_or_send(chat_id, text, {"inline_keyboard": rows}, message_id=msg_id)

def _digits_to_date(d: str) -> str:
    return f"{d[0:2]}.{d[2:4]}.{d[4:8]}"
_TRDPICK_BACKCB: dict = {} # cid → back_cb of the menu the trade-picker was opened from
_TRDPICK_LABELS = {
    "sltobe":     "🛡 Move SL to Breakeven",
    "setsl":      "🔧 Set Custom SL",
    "settp1":     "🎯 Set Custom TP1",
    "settp2":     "🏆 Set Custom TP2",
    "closetrade": "❌ Close a Coin",
}

def _all_open_trades():
    """Every currently open trade across BTC + Scan1 + Scan2, for the trade-picker screens."""
    out = []
    if active_trade.get("signal"):
        out.append({"kind": "btc", "symbol": "BTC-USDT", "idx": 0,
                     "label": f"₿ BTC-USDT — {active_trade['signal']} (Active Trade)"})
    for i, t in enumerate(scan1_trades):
        out.append({"kind": "scan1", "symbol": t.get("symbol", "?"), "idx": i,
                     "label": f"🔍 {t.get('symbol','?')} — Scan1"})
    for i, t in enumerate(scan2_trades):
        out.append({"kind": "scan2", "symbol": t.get("symbol", "?"), "idx": i,
                     "label": f"🔍 {t.get('symbol','?')} — Scan2"})
    return out

def _send_trade_pick_screen(chat_id, cid, action, msg_id, back_cb):
    trades = _all_open_trades()
    _TRDPICK_BACKCB[str(cid)] = back_cb
    label = _TRDPICK_LABELS[action]
    if not trades:
        _help_edit_or_send(chat_id, f"{label}\n\nNo open trades right now.",
            {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": back_cb}]]}, message_id=msg_id)
        return
    rows = [[{"text": t["label"], "callback_data": f"trdpick:{action}:{t['kind']}:{t['idx']}"}] for t in trades]
    rows.append([{"text": "◀️  Back", "callback_data": back_cb}])
    _help_edit_or_send(chat_id, f"{label}\n\nChoose a trade:",
        {"inline_keyboard": rows}, message_id=msg_id)

def _pp_render(chat_id, cid, msg_id):
    st = _pp_state.get(str(cid))
    if not st:
        return
    label = {"setsl": "Stop Loss", "settp1": "TP1", "settp2": "TP2"}[st["action"]]
    val = st["digits"] or "0"
    text = (
        f"🔢 <b>Set {label} — {st['symbol']}</b>\n\n"
        f"Entering: <code>{val}</code>\n\n"
        f"<i>Tap digits to build the price. Use . for decimals.</i>")
    rows = [
        [{"text": str(n), "callback_data": f"pp_d:{n}"} for n in (1, 2, 3)],
        [{"text": str(n), "callback_data": f"pp_d:{n}"} for n in (4, 5, 6)],
        [{"text": str(n), "callback_data": f"pp_d:{n}"} for n in (7, 8, 9)],
        [{"text": "0", "callback_data": "pp_d:0"}, {"text": ".", "callback_data": "pp_d:."}],
        [{"text": "⌨️ Type Instead", "callback_data": "pp_manual"}],
        [{"text": "◀️ Erase", "callback_data": "pp_prev"}, {"text": "🚫 Back", "callback_data": "pp_back"}],
    ]
    if st["digits"]:
        rows.insert(-2, [{"text": "💾 Save", "callback_data": "pp_save"}])
    _help_edit_or_send(chat_id, text, {"inline_keyboard": rows}, message_id=msg_id)

def _get_live_price(kind, symbol):
    """Best-effort current market price, used to block SL/TP edits that would
    trigger the order instantly. Returns None if it can't be fetched — callers
    should fall back to entry-only validation rather than fail the edit."""
    try:
        if kind == "btc":
            return float(get_ticker()["price"])
        r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/price",
                          params={"symbol": symbol}, timeout=5).json()
        p = float((r.get("data") or {}).get("price", 0))
        return p or None
    except Exception:
        return None

def _validate_price_for_side(action, side, entry, price, current_price=None):
    """A BUY's TPs must sit above both entry AND the current market price, and its
    SL below both (reverse for a SELL) — otherwise the order would either make no
    sense relative to your cost basis, or trigger instantly since the exchange
    already considers the condition met. Returns (ok, reason)."""
    is_sl = action == "setsl"
    if side == "BUY":
        bound = min(entry, current_price) if current_price else entry
        bad = price >= bound if is_sl else price <= bound
        if bad:
            ref = "current price" if current_price and bound == current_price else "entry"
            word = "BELOW" if is_sl else "ABOVE"
            return False, f"{'SL' if is_sl else 'TP'} must be {word} {ref} ({bound:,.6f}) for a BUY."
    else:  # SELL
        bound = max(entry, current_price) if current_price else entry
        bad = price <= bound if is_sl else price >= bound
        if bad:
            ref = "current price" if current_price and bound == current_price else "entry"
            word = "ABOVE" if is_sl else "BELOW"
            return False, f"{'SL' if is_sl else 'TP'} must be {word} {ref} ({bound:,.6f}) for a SELL."
    return True, ""

def _apply_trade_price_edit(action, kind, symbol, idx, price):
    """Applies a setsl/settp1/settp2 edit to the picked trade. Returns (ok, reason)."""
    if kind == "btc":
        if not active_trade.get("signal"):
            return False, "Trade no longer open."
        current_price = _get_live_price("btc", symbol)
        ok, reason = _validate_price_for_side(action, active_trade["signal"], active_trade["entry"], price, current_price)
        if not ok:
            return False, reason
        if action == "setsl":
            active_trade["sl"] = price; ct.on_update_sl(price)
        elif action == "settp1":
            active_trade["tp1"] = price; ct.update_tp("tp1", price)
        elif action == "settp2":
            active_trade["tp2"] = price
            ct.update_tp("tp2", price, full_remaining=active_trade.get("tp1_hit", False))
        save_state()
        return True, ""
    lst = scan1_trades if kind == "scan1" else scan2_trades
    if not (0 <= idx < len(lst)) or lst[idx].get("symbol") != symbol:
        return False, "Trade no longer open."
    current_price = _get_live_price(kind, symbol)
    ok, reason = _validate_price_for_side(action, lst[idx]["signal"], lst[idx]["entry"], price, current_price)
    if not ok:
        return False, reason
    if action == "setsl":
        lst[idx]["sl"] = price; ct.update_scan_sl(symbol, price)
    elif action == "settp1":
        lst[idx]["tp1"] = price; ct.update_scan_tp(symbol, "tp1", price)
    elif action == "settp2":
        lst[idx]["tp2"] = price; ct.update_scan_tp(symbol, "tp2", price)
    save_state()
    return True, ""

trade_stats = {
    "consecutive_sl": 0, "cooldown_scans": 0,
    "total_sl": 0, "total_tp1": 0, "total_tp2": 0,
    "total_signals": 0, "missed_entries": 0, "stop_hunts": 0,
    "scan_sl": 0, "scan_tp1": 0, "scan_tp2": 0, "scan_signals": 0,
    "scan1_sl": 0, "scan1_tp1": 0, "scan1_tp2": 0, "scan1_signals": 0,
    "scan2_sl": 0, "scan2_tp1": 0, "scan2_tp2": 0, "scan2_signals": 0,
}

STATE_FILE       = os.path.join(DATA_DIR, "clexer_state.json")
TRADE_LOG_CSV    = os.path.join(DATA_DIR, "trade_history.csv")
API_COST_LOG     = os.path.join(DATA_DIR, "api_cost_log.csv")

# Opus 4-8 pricing per token
_OPUS_IN_COST  = 15.0 / 1_000_000   # $15 per 1M input tokens
_OPUS_OUT_COST = 75.0 / 1_000_000   # $75 per 1M output tokens
# Fable 5 pricing per token
_FABLE_IN_COST  = 10.0 / 1_000_000  # $10 per 1M input tokens
_FABLE_OUT_COST = 50.0 / 1_000_000  # $50 per 1M output tokens
# Haiku 4-5 pricing per token
_HAIKU_IN_COST  = 0.80 / 1_000_000
_HAIKU_OUT_COST = 4.0  / 1_000_000

def _log_api_usage(call_type: str, model: str, input_tokens: int, output_tokens: int):
    """Log every Claude API call with token count and cost to CSV."""
    import csv
    from datetime import datetime, timezone, timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(IST)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    if "haiku" in model:
        cost = input_tokens * _HAIKU_IN_COST + output_tokens * _HAIKU_OUT_COST
    elif "fable" in model:
        cost = input_tokens * _FABLE_IN_COST + output_tokens * _FABLE_OUT_COST
    else:
        cost = input_tokens * _OPUS_IN_COST  + output_tokens * _OPUS_OUT_COST
    headers = ["date","time","call_type","model","input_tokens","output_tokens","cost_usd"]
    row = [date_str, time_str, call_type, model, input_tokens, output_tokens, f"{cost:.6f}"]
    write_header = not os.path.exists(API_COST_LOG)
    try:
        with open(API_COST_LOG, "a", newline="") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(headers)
            w.writerow(row)
    except Exception as e:
        print(f"  [API LOG] {e}")
TRADE_LOG_WEBHOOK = os.getenv("TRADE_LOG_WEBHOOK", "")   # optional — set in Railway env vars

def _claude_text(msg):
    """Extract the text from a Claude response, skipping ThinkingBlocks
    (extended-thinking models return those as content[0] before the real text block)."""
    if not msg.content:
        return ""
    for block in msg.content:
        if getattr(block, "type", None) == "text":
            return block.text.strip()
    return ""

def _ai_model(kind: str = "btc") -> str:
    """Which Claude model to use for this scan type — each of btc/scan1/scan2/test
    has its own independent model + gateway choice, set via /aiconfig."""
    return {"btc": SCAN_MODEL, "scan1": SCAN1_MODEL, "scan2": SCAN2_MODEL, "test": TEST_MODEL}.get(kind, SCAN_MODEL)

def _ai_aerolink(kind: str = "btc") -> bool:
    return {"btc": USE_AEROLINK, "scan1": SCAN1_AEROLINK, "scan2": SCAN2_AEROLINK, "test": TEST_AEROLINK}.get(kind, USE_AEROLINK)

def _claude_client(kind: str = "btc"):
    """Returns an Anthropic client for the given scan type (btc/scan1/scan2/test).
    When that type's gateway is Aerolink, uses ONLY the separate AEROLINK_API_KEY +
    AEROLINK_BASE_URL — the real ANTHROPIC_API_KEY is never touched or sent to the gateway."""
    if _ai_aerolink(kind) and AEROLINK_API_KEY:
        return anthropic.Anthropic(api_key=AEROLINK_API_KEY, base_url=AEROLINK_BASE_URL)
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

_CSV_HEADERS = ["type","coin","direction","signal_time","entry_price","sl_price","tp1_price","tp2_price",
                 "entry_trigger_time","tp1_hit_time","tp2_hit_time","sl_hit_time","timeout_time","result","notes"]

def _ensure_csv():
    if not os.path.exists(TRADE_LOG_CSV):
        import csv
        with open(TRADE_LOG_CSV, "w", newline="") as f:
            csv.writer(f).writerow(_CSV_HEADERS)

def log_trade_event(row: dict):
    """Log a trade event. 'open' rows are appended; all other events update the existing open row."""
    import csv
    _ensure_csv()
    result = row.get("result", "open")

    if result == "open":
        # New trade — append a fresh row
        ordered = {h: row.get(h, "") for h in _CSV_HEADERS}
        try:
            with open(TRADE_LOG_CSV, "a", newline="") as f:
                csv.DictWriter(f, fieldnames=_CSV_HEADERS).writerow(ordered)
        except Exception as e:
            print(f"  [LOG] CSV write error: {e}")
        if TRADE_LOG_WEBHOOK:
            try: requests.post(TRADE_LOG_WEBHOOK, json=ordered, timeout=8)
            except Exception as e: print(f"  [LOG] Webhook error: {e}")
        return

    # For TP1/TP2/SL/BE/TIMEOUT etc — find and update the matching open row
    coin      = row.get("coin", "")
    direction = row.get("direction", "")
    trade_type = row.get("type", "")

    try:
        with open(TRADE_LOG_CSV, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception as e:
        print(f"  [LOG] CSV read error: {e}"); return

    # Find the LAST open row for this coin+direction+type (most recent active trade)
    match_idx = None
    for i, r in enumerate(rows):
        if (r.get("coin") == coin and r.get("direction") == direction
                and r.get("type") == trade_type and r.get("result") == "open"):
            match_idx = i

    if match_idx is not None:
        # Update existing row with new columns from the event — "result" is handled
        # separately below so a TP1_partial update can't clobber the "open" marker
        for key, val in row.items():
            if key in _CSV_HEADERS and key != "result" and val:
                rows[match_idx][key] = val
        # Only mark result as final if this event closes the trade
        # TP1_partial keeps it "open" (still running), everything else closes it
        if result != "TP1_partial":
            rows[match_idx]["result"] = result
    else:
        # No matching open row found — append as new row (fallback)
        rows.append({h: row.get(h, "") for h in _CSV_HEADERS})

    try:
        with open(TRADE_LOG_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=_CSV_HEADERS)
            w.writeheader()
            w.writerows(rows)
    except Exception as e:
        print(f"  [LOG] CSV write error: {e}")

    if TRADE_LOG_WEBHOOK:
        ordered = {h: rows[match_idx][h] if match_idx is not None else row.get(h, "") for h in _CSV_HEADERS}
        try: requests.post(TRADE_LOG_WEBHOOK, json=ordered, timeout=8)
        except Exception as e: print(f"  [LOG] Webhook error: {e}")

def _ist_str_now() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")

def save_state():
    state = {
        "trade":        active_trade,
        "scan1_trades": scan1_trades,
        "scan2_trades": scan2_trades,
        "stats":        trade_stats,
        "history":      signal_history,
        "outcomes":     trade_outcomes,
        "scan_history": scan_history,
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[STATE] Save error: {e}")
    if CLEXER_API_URL:
        try:
            hdrs = {"X-Push-Secret": PUSH_STATE_SECRET} if PUSH_STATE_SECRET else {}
            requests.post(f"{CLEXER_API_URL}/push_state", json=state, headers=hdrs, timeout=5)
        except Exception as e:
            print(f"[STATE] Push error: {e}")

def save_active_trade():
    save_state()

def load_active_trade():
    global active_trade, scan1_trades, scan2_trades, trade_stats, signal_history, trade_outcomes, scan_history
    path = STATE_FILE if os.path.exists(STATE_FILE) else ACTIVE_TRADE_FILE
    try:
        if os.path.exists(path):
            d = json.load(open(path))
            trade_stats.update(d.get("stats", {}))
            signal_history[:] = d.get("history", [])
            trade_outcomes[:]  = d.get("outcomes", [])
            scan_history[:]    = d.get("scan_history", [])
            t = d.get("trade", {})
            if t.get("signal"):
                active_trade = t
                print(f"[STATE] Restored BTC trade: {t['signal']} @ {t['entry']:,.0f} "
                      f"entry_hit:{t.get('entry_hit')} tp1_hit:{t.get('tp1_hit')}")
            scan1_trades[:] = [x for x in d.get("scan1_trades", []) if x.get("signal")]
            scan2_trades[:] = [x for x in d.get("scan2_trades", []) if x.get("signal")]
            if scan1_trades: print(f"[STATE] Restored scan1: {[t['symbol'] for t in scan1_trades]}")
            if scan2_trades: print(f"[STATE] Restored scan2: {[t['symbol'] for t in scan2_trades]}")
            print(f"[STATE] Stats restored — SL:{trade_stats['total_sl']} "
                  f"TP1:{trade_stats['total_tp1']} TP2:{trade_stats['total_tp2']} "
                  f"Signals:{trade_stats['total_signals']}")
        else:
            print("[STATE] No state file found — fresh start")
        save_state()  # push to API on startup
    except Exception as e:
        print(f"[STATE] Load error: {e}")

def reset_trade():
    global active_trade
    with trade_lock:
        active_trade = {
            "signal": None, "entry": None, "sl": None,
            "tp1": None, "tp2": None, "tp1_hit": False,
            "entry_type": "MARKET", "entry_note": "",
            "entry_hit": False, "sl_wicked": False, "scan_count": 0,
        }
    save_active_trade()
    ct.clear_last_signal()  # every path that ends the BTC trade funnels through here —
                            # keeps /ctstatus from showing a stale "Active Signal"

def set_trade(s: dict):
    global active_trade
    with trade_lock:
        active_trade = {
            "signal": s["signal"], "entry": s["entry"],
            "sl": s["sl"], "tp1": s["tp1"], "tp2": s["tp2"], "tp1_hit": False,
            "entry_type": s.get("entry_type", "MARKET"),
            "entry_note": s.get("entry_note", ""),
            "entry_hit": s.get("entry_type", "MARKET") == "MARKET",
            "entry_time": time.time(),   # used to clip price range checks to post-entry only
            "sl_wicked": False, "scan_count": 0,
        }
    trade_stats["total_signals"] += 1
    signal_history.append({
        "time": ist_str(), "signal": s["signal"],
        "entry": s["entry"], "sl": s["sl"],
        "tp1": s["tp1"], "tp2": s["tp2"],
        "rr": s.get("rr", "?"), "confidence": s.get("confidence", "?"),
    })
    if len(signal_history) > 20: signal_history.pop(0)
    save_state()

def log_trade_outcome(reason: str, detail: str = ""):
    trade_outcomes.append({
        "time": ist_str(), "signal": active_trade.get("signal"),
        "entry": active_trade.get("entry"), "reason": reason, "detail": detail,
    })
    if len(trade_outcomes) > 20: trade_outcomes.pop(0)
    save_state()

# ═══════════════════════════════════════════════════════════════════════════════
#  TV BRIDGE
# ═══════════════════════════════════════════════════════════════════════════════

def tv_ping():
    if not TV_BRIDGE_URL: return None
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/health", timeout=5)
        if r.status_code == 200: return r.json()
    except: pass
    return None

def tv_update_state():
    result = tv_ping(); now = time.time()
    tv_bridge_state["last_check"] = now
    if result:
        tv_bridge_state.update({
            "online": True, "last_seen": now, "fail_count": 0,
            "source": "TRADINGVIEW",
            "tv_version": result.get("tv_version", ""),
            "tv_symbol": result.get("symbol", ""),
            "cdp_ok": result.get("cdp_connected", False),
            "cached_intervals": result.get("cached_intervals", []),
        })
        return True
    else:
        tv_bridge_state["fail_count"] += 1
        if tv_bridge_state["fail_count"] >= 2:
            tv_bridge_state["online"] = False
            tv_bridge_state["source"] = "BINANCE"
        return False

def tv_get_candles(interval, limit):
    if not TV_BRIDGE_URL or not tv_bridge_state["online"]: return None
    tv_map = {"weekly": "W", "4h": "4H", "1h": "1H", "5m": "5", "1m": "1"}
    iv = tv_map.get(interval, interval)
    if iv not in ("W", "4H", "1H", "5"): return None  # bridge only caches these 4 TFs
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/candles",
            params={"symbol": SYMBOL, "interval": iv, "limit": limit},
            timeout=15)
        if r.status_code != 200: return None
        data = r.json()
        if not data.get("candles"): return None
        rows = [{
            "time": datetime.fromtimestamp(c["t"]/1000 if c["t"]>1e10 else c["t"], tz=timezone.utc),
            "open": float(c["o"]), "high": float(c["h"]),
            "low": float(c["l"]), "close": float(c["c"]), "vol": float(c.get("v", 0)),
        } for c in data["candles"]]
        df = pd.DataFrame(rows).set_index("time")
        print(f"      [TV] {interval}: {len(df)} candles OK")
        return df
    except Exception as e:
        print(f"      [TV] {interval} error: {e}"); return None

def take_miniapp_screenshots():
    return []  # Chromium/Playwright removed

def tv_get_ticker():
    if not TV_BRIDGE_URL or not tv_bridge_state["online"]: return None
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/ticker", params={"symbol": SYMBOL}, timeout=8)
        if r.status_code == 200:
            d = r.json(); price = float(d.get("price", 0))
            if price > 0:
                return {"price": price, "change": float(d.get("change_pct", 0)),
                    "volume": float(d.get("volume", 0)), "high24": float(d.get("high24", price)),
                    "low24": float(d.get("low24", price)), "source": "TRADINGVIEW"}
    except Exception as e: print(f"      [TV ticker] {e}")
    return None

def tv_set_symbol(symbol: str) -> bool:
    """Switch TradingView chart to symbol AND wait for all TFs to load.
    Returns True only if at least 4H and 1H candles were actually loaded for the new symbol."""
    if not TV_BRIDGE_URL or not tv_bridge_state["online"]: return False
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/load_symbol",
                         params={"symbol": symbol}, timeout=60)
        if r.status_code == 200:
            d = r.json()
            loaded = d.get("loaded", {})
            print(f"  [TV load_symbol] {symbol} — loaded TFs: {loaded}")
            # Verify that 4H and 1H actually loaded (not 0 = timed out)
            if loaded.get("4H", 0) > 0 and loaded.get("1H", 0) > 0:
                return True
            print(f"  [TV load_symbol] ⚠️ TFs incomplete: {loaded} — treating as failed")
            return False
    except Exception as e:
        print(f"  [TV load_symbol] {e}")
    return False

def tv_get_candles_for(symbol: str, interval: str, limit: int, live_price: float = 0.0):
    """Fetch candles from TV bridge for any symbol (used during /scan for alts).
    Retries up to 3x if bridge returns symbol_mismatch (chart not yet switched).
    live_price: if provided, sanity-checks last close against real price (>30% diff = reject)."""
    if not TV_BRIDGE_URL or not tv_bridge_state["online"]: return None
    tv_map = {"weekly":"W","4h":"4H","1h":"1H","5m":"5","1m":"1"}
    iv = tv_map.get(interval, interval)
    for attempt in range(5):
        try:
            r = requests.get(f"{TV_BRIDGE_URL}/candles",
                params={"symbol": symbol, "interval": iv, "limit": limit},
                timeout=15)
            if r.status_code == 409:
                err = r.json().get("error","")
                print(f"      [TV] {symbol} {interval} symbol_mismatch (attempt {attempt+1}): {err}")
                time.sleep(3)
                continue
            if r.status_code == 503:
                # Candles not yet in cache — TV chart still loading, wait and retry
                print(f"      [TV] {symbol} {interval} cache empty (attempt {attempt+1}/5) — waiting...")
                time.sleep(3)
                continue
            if r.status_code != 200:
                print(f"      [TV] {symbol} {interval} HTTP {r.status_code}")
                return None
            data = r.json()
            if not data.get("candles"):
                print(f"      [TV] {symbol} {interval} empty candles (attempt {attempt+1}/5) — waiting...")
                time.sleep(3)
                continue
            rows = [{
                "time": datetime.fromtimestamp(c["t"]/1000 if c["t"]>1e10 else c["t"], tz=timezone.utc),
                "open": float(c["o"]), "high": float(c["h"]),
                "low": float(c["l"]), "close": float(c["c"]), "volume": float(c.get("v", 0)),
            } for c in data["candles"]]
            df = pd.DataFrame(rows).set_index("time")
            # Sanity check: last close must be within 30% of real price
            if live_price > 0 and len(df) > 0:
                last_close = float(df["close"].iloc[-1])
                ratio = abs(last_close - live_price) / live_price
                if ratio > 0.30:
                    print(f"      [TV] ⚠️ PRICE MISMATCH — {symbol} {interval}: last_close={last_close:.6g} live={live_price:.6g} diff={ratio*100:.1f}%")
                    return None   # reject — wrong symbol's data
            print(f"      [TV] {symbol} {interval}: {len(df)} candles OK")
            return df
        except Exception as e:
            print(f"      [TV] {symbol} {interval} error: {e}"); return None
    print(f"      [TV] {symbol} {interval}: gave up after 5 retries (still not loaded)")
    return None

def fetch_tv_screenshots():
    """Fetch screenshots for all timeframes."""
    if not TV_BRIDGE_URL or not tv_bridge_state["online"]: return {}
    screenshots = {}
    for tf_name, tf_code in [("W","W"),("4H","4H"),("1H","1H"),("5","5")]:
        try:
            r = requests.get(f"{TV_BRIDGE_URL}/screenshot",
                params={"interval": tf_code}, timeout=20)
            if r.status_code == 200:
                img_b64 = r.json().get("image_base64")
                if img_b64 and len(img_b64) > 1000:
                    screenshots[tf_name] = img_b64
                    print(f"      [SCREENSHOT] {tf_name}: {len(img_b64)//1024}KB OK")
            time.sleep(0.5)
        except Exception as e:
            print(f"      [SCREENSHOT] {tf_name}: {e}")
    return screenshots

def fetch_tv_all_data():
    """
    Fetch everything from tv_bridge in ONE call using /all_data endpoint (v9+).
    Returns: { ticker, candles, pine_labels, pine_lines, pine_boxes, studies }
    Falls back to separate calls if /all_data not available.
    """
    if not TV_BRIDGE_URL or not tv_bridge_state["online"]: return {}
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/all_data", timeout=20)
        if r.status_code == 200:
            data = r.json()
            if "error" not in data:
                print(f"      [ALL_DATA] labels:{len(data.get('pine_labels',[]))} "
                      f"lines:{len(data.get('pine_lines',[]))} "
                      f"boxes:{len(data.get('pine_boxes',[]))} "
                      f"studies:{len(data.get('studies',[]))}")
                return data
    except Exception as e:
        print(f"      [ALL_DATA] {e} — falling back to /indicators")
    # Fallback: old /indicators endpoint
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/indicators", timeout=15)
        if r.status_code == 200:
            data = r.json()
            if "error" not in data:
                print(f"      [INDICATORS] {len(data.get('raw_studies',[]))} studies OK")
                return {"studies": data.get("raw_studies",[]),
                        "pine_labels": [], "pine_lines": [], "pine_boxes": [],
                        "clexer_sniper": data.get("clexer_sniper"),
                        "spaceman_levels": data.get("spaceman_levels",[]),
                        "poi_vol_surge": data.get("poi_vol_surge")}
    except Exception as e: print(f"      [INDICATORS] {e}")
    return {}

def fetch_tv_indicators():
    """Legacy wrapper — now calls fetch_tv_all_data."""
    return fetch_tv_all_data()

def fetch_spaceman_levels() -> dict:
    """
    Calculate SpacemanBTC Key Levels directly from BingX OHLCV data.
    Replicates Pine Script logic from Key Levels SpacemanBTC IDWM v13.1 exactly.
    No TV bridge needed — works always, fully labeled.
    """
    import math as _math
    from datetime import datetime, timezone

    # BingX swap uses BTC-USDT format, not BTCUSDT
    bx_sym = SYMBOL.replace("USDT", "-USDT") if "-" not in SYMBOL else SYMBOL

    def _klines(interval, limit):
        # Try both symbol formats
        for sym in [bx_sym, SYMBOL]:
            try:
                r = requests.get(
                    "https://open-api.bingx.com/openApi/swap/v2/quote/klines",
                    params={"symbol": sym, "interval": interval, "limit": limit},
                    timeout=10).json()
                rows = r.get("data", [])
                if not rows:
                    print(f"  [SPACEMAN] {interval} empty: code={r.get('code')} msg={r.get('msg','')}")
                    continue
                result = []
                for row in rows:
                    if isinstance(row, dict):
                        result.append({
                            "t": int(float(row.get("time") or row.get("openTime") or 0)),
                            "o": float(row.get("open", 0)),
                            "h": float(row.get("high", 0)),
                            "l": float(row.get("low", 0)),
                            "c": float(row.get("close", 0)),
                        })
                if result:
                    return result
            except Exception as e:
                print(f"  [SPACEMAN KLINES] {interval} {sym}: {e}")
        return []

    levels = []

    try:
        # ── Weekly levels ──────────────────────────────────────────────
        wk = _klines("1w", 3)
        if len(wk) >= 2:
            weekly_open  = wk[-1]["o"]
            pw_high      = wk[-2]["h"]
            pw_low       = wk[-2]["l"]
            pw_mid       = (pw_high + pw_low) / 2
            levels += [
                {"label": "Weekly Open",    "price": weekly_open},
                {"label": "Prev Week High", "price": pw_high},
                {"label": "Prev Week Low",  "price": pw_low},
                {"label": "Prev Week Mid",  "price": pw_mid},
            ]

        # ── Monday levels — first day candle of current week ──────────
        d1 = _klines("1d", 10)
        if d1:
            # Find Monday (TV Pine: first daily bar of the week)
            for bar in reversed(d1):
                ts  = bar["t"] / 1000
                dow = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()  # 0=Mon
                if dow == 0:
                    levels += [
                        {"label": "Monday High", "price": bar["h"]},
                        {"label": "Monday Low",  "price": bar["l"]},
                        {"label": "Monday Mid",  "price": (bar["h"] + bar["l"]) / 2},
                    ]
                    break

        # ── Monthly + Quarterly + Yearly — one call, 14 bars covers all ──
        mo = _klines("1M", 14)
        if len(mo) >= 2:
            monthly_open = mo[-1]["o"]
            pm_high      = mo[-2]["h"]
            pm_low       = mo[-2]["l"]
            pm_mid       = (pm_high + pm_low) / 2
            levels += [
                {"label": "Monthly Open",    "price": monthly_open},
                {"label": "Prev Month High", "price": pm_high},
                {"label": "Prev Month Low",  "price": pm_low},
                {"label": "Prev Month Mid",  "price": pm_mid},
            ]

        # Quarterly + Yearly — use calendar boundaries from timestamps
        import datetime as _dt
        now_utc   = _dt.datetime.now(_dt.timezone.utc)
        cur_year  = now_utc.year
        cur_month = now_utc.month
        cur_qnum  = (cur_month - 1) // 3  # 0=Q1,1=Q2,2=Q3,3=Q4

        def _bar_dt(b):
            ts = b["t"]
            if ts > 1e12: ts = ts / 1000
            return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc)

        def _qnum(b):
            return (_bar_dt(b).month - 1) // 3

        sorted_mo = sorted(mo, key=lambda b: b["t"])

        # Calendar-year bars — all months of current year (including partial current month)
        all_yr = [b for b in sorted_mo if _bar_dt(b).year == cur_year]
        if all_yr:
            levels += [
                {"label": "Yearly Open",      "price": all_yr[0]["o"]},
                {"label": "Current Year Mid", "price": (
                    max(b["h"] for b in all_yr) +
                    min(b["l"] for b in all_yr)
                ) / 2},
            ]

        # Calendar-quarter bars
        cur_q_bars = [b for b in sorted_mo if _bar_dt(b).year == cur_year and _qnum(b) == cur_qnum]
        prev_qnum  = (cur_qnum - 1) % 4
        prev_qyr   = cur_year if cur_qnum > 0 else cur_year - 1
        prev_q_bars = [b for b in sorted_mo if _bar_dt(b).year == prev_qyr and _qnum(b) == prev_qnum]

        if cur_q_bars:
            levels.append({"label": "Quarterly Open", "price": cur_q_bars[0]["o"]})
        if prev_q_bars:
            pq_h = max(b["h"] for b in prev_q_bars)
            pq_l = min(b["l"] for b in prev_q_bars)
            levels.append({"label": "Prev Quarter Mid", "price": (pq_h + pq_l) / 2})

    except Exception as e:
        print(f"  [SPACEMAN CALC] {e}")

    if not levels:
        return {}

    try: price = get_ticker()["price"]
    except: price = 0

    below = [l for l in levels if l["price"] < price]
    above = [l for l in levels if l["price"] > price]
    return {
        "all_levels":         sorted(levels, key=lambda x: x["price"]),
        "nearest_support":    max(below, key=lambda x: x["price"]) if below else None,
        "nearest_resistance": min(above, key=lambda x: x["price"]) if above else None,
        "count":              len(levels),
        "source":             "calculated",
    }

def build_indicator_context(data: dict) -> str:
    if not data: return ""
    lines = ["\n\nTRADINGVIEW CHART DATA:"]

    # Pine Boxes — OB/FVG exact zones
    boxes = data.get("pine_boxes", [])
    if boxes:
        lines.append("\nSMC ZONES (Pine Boxes — exact OB/FVG levels):")
        for b in boxes[:10]:
            h = b.get("high", 0); l = b.get("low", 0); study = b.get("study","")
            if h and l:
                lines.append(f"  Zone: {l:,.0f} - {h:,.0f}  ({study})")

    # Pine Labels — BOS, CHoCH, swing text
    labels = data.get("pine_labels", [])
    if labels:
        lines.append("\nSMC LABELS (BOS/CHoCH/Swing levels):")
        for lb in labels[:15]:
            price = lb.get("price", 0); text = lb.get("text",""); study = lb.get("study","")
            if price:
                lines.append(f"  {text}: {price:,.0f}  ({study})")

    # Pine Lines — horizontal S/R levels
    pine_lines = data.get("pine_lines", [])
    if pine_lines:
        lines.append("\nKEY PRICE LEVELS (Pine Lines):")
        prices = sorted(set([round(l.get("price",0)) for l in pine_lines if l.get("price",0) > 1000]))
        lines.append(f"  {prices[:12]}")

    # Studies — RSI, EMA, MACD etc.
    studies = data.get("studies", [])
    if studies:
        lines.append("\nACTIVE INDICATORS:")
        for s in studies[:10]:
            name = s.get("name",""); vals = s.get("values", s.get("value",""))
            if name: lines.append(f"  {name}: {vals}")

    # Legacy fields (old /indicators format)
    clexer = data.get("clexer_sniper")
    if clexer: lines.append(f"\nCLEXER SNIPER: {clexer.get('text','active')[:150]}")
    spaceman = data.get("spaceman_levels", [])
    if spaceman:
        levels = sorted(set([round(l, 0) for l in spaceman]))
        lines.append(f"SPACEMAN KEY LEVELS: {levels[:10]}")
    poi = data.get("poi_vol_surge")
    if poi: lines.append(f"POI VOL SURGE: {poi.get('text','')[:150]}")

    # SpacemanBTC Key Levels — fetched directly, works even when hidden
    spaceman = fetch_spaceman_levels()
    if spaceman.get("all_levels"):
        lines.append("\nSPACEMAN KEY LEVELS (Weekly/Monthly/Quarterly/Yearly S&R):")
        for lv in spaceman["all_levels"]:
            lines.append(f"  {lv['label']}: {lv['price']:,.2f}")
        if spaceman.get("nearest_support"):
            s = spaceman["nearest_support"]
            lines.append(f"  >> Nearest Support:    {s['label']} @ {s['price']:,.2f}")
        if spaceman.get("nearest_resistance"):
            r = spaceman["nearest_resistance"]
            lines.append(f"  >> Nearest Resistance: {r['label']} @ {r['price']:,.2f}")

    if len(lines) == 1: return ""
    lines.append("\nUse above levels as ADDITIONAL CONFIRMATION with price structure.")
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════════════
#  BINGX BTC FALLBACK  (primary fallback — Binance kept as last resort)
# ═══════════════════════════════════════════════════════════════════════════════

def bingx_get_btc_candles(interval, limit):
    iv_map = {"weekly": "1w", "4h": "4h", "1h": "1h", "5m": "5m", "1m": "1m"}
    df = bingx_klines("BTC-USDT", iv_map.get(interval, interval), limit)
    if df is not None:
        df = df.rename(columns={"volume": "vol"})
        print(f"      [BINGX BTC] {interval}: {len(df)} candles")
        return df
    return None

def bingx_get_btc_ticker():
    try:
        r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
                         params={"symbol": "BTC-USDT"}, timeout=8).json()
        d = r.get("data", {})
        if isinstance(d, list): d = d[0] if d else {}
        price = float(d.get("lastPrice", 0))
        if price > 0:
            return {
                "price":  price,
                "change": float(d.get("priceChangePercent", 0)),
                "volume": float(d.get("quoteVolume", 0)),
                "high24": float(d.get("highPrice", price)),
                "low24":  float(d.get("lowPrice",  price)),
                "source": "BINGX",
            }
    except Exception as e:
        print(f"      [BINGX BTC TICKER] {e}")
    return None

def binance_get_candles(interval, limit):
    iv_map = {"weekly": "1w", "4h": "4h", "1h": "1h", "5m": "5m"}
    r = requests.get(f"{BINANCE_BASE}/klines",
        params={"symbol": SYMBOL, "interval": iv_map.get(interval, interval), "limit": limit}, timeout=15)
    r.raise_for_status()
    rows = [{"time": datetime.fromtimestamp(c[0]/1000, tz=timezone.utc),
        "open": float(c[1]), "high": float(c[2]), "low": float(c[3]),
        "close": float(c[4]), "vol": float(c[5])} for c in r.json()]
    df = pd.DataFrame(rows).set_index("time")
    print(f"      [BINANCE] {interval}: {len(df)} candles"); return df

def binance_get_ticker():
    r = requests.get(f"{BINANCE_BASE}/ticker/24hr", params={"symbol": SYMBOL}, timeout=10)
    r.raise_for_status(); d = r.json()
    return {"price": float(d["lastPrice"]), "change": float(d["priceChangePercent"]),
        "volume": float(d["quoteVolume"]), "high24": float(d["highPrice"]),
        "low24": float(d["lowPrice"]), "source": "BINANCE"}

def get_candles(interval, limit):
    if TV_BRIDGE_URL:
        tv_update_state()
        if tv_bridge_state["online"]:
            # Non-blocking: if scan holds the TV chart, skip TV and use BingX
            if _tv_chart_lock.acquire(blocking=False):
                try:
                    df = tv_get_candles(interval, limit)
                    if df is not None and len(df) >= 2: return df
                finally:
                    _tv_chart_lock.release()
            else:
                print("  [TV] chart locked by scan — using BingX for candles")
    # BingX primary fallback
    df = bingx_get_btc_candles(interval, limit)
    if df is not None and len(df) >= 2: return df
    # Binance last resort
    return binance_get_candles(interval, limit)

def get_ticker():
    if TV_BRIDGE_URL:
        tv_update_state()
        if tv_bridge_state["online"]:
            if _tv_chart_lock.acquire(blocking=False):
                try:
                    tk = tv_get_ticker()
                    if tk: return tk
                finally:
                    _tv_chart_lock.release()
            else:
                print("  [TV] chart locked by scan — using BingX for ticker")
    # BingX primary fallback
    tk = bingx_get_btc_ticker()
    if tk: return tk
    # Binance last resort
    return binance_get_ticker()

def get_price_range_since(minutes, since_ts: float = None):
    # since_ts: Unix timestamp — if provided, use it instead of (now - minutes)
    # This prevents pre-entry wicks from falsely triggering SL/TP
    if since_ts:
        since_ms = int(since_ts * 1000)
    else:
        since_ms = int((time.time() - minutes*60)*1000)
    now_ms   = int(time.time()*1000)
    all_highs, all_lows = [], []
    chunk_start = since_ms
    while chunk_start < now_ms:
        chunk_end = min(chunk_start + 5*60*1000, now_ms)
        try:
            r = requests.get(f"{BINANCE_BASE}/aggTrades",
                params={"symbol": SYMBOL, "startTime": chunk_start, "endTime": chunk_end, "limit": 1000}, timeout=10)
            r.raise_for_status(); trades = r.json()
            if trades:
                prices = [float(t["p"]) for t in trades]
                all_highs.append(max(prices)); all_lows.append(min(prices))
        except Exception as e: print(f"  [aggTrades] {e}")
        chunk_start = chunk_end + 1; time.sleep(0.05)
    return {"high": max(all_highs) if all_highs else None, "low": min(all_lows) if all_lows else None}

def get_recent_range(minutes: int = 3) -> tuple[float, float]:
    """
    Return (high, low) over the last N minutes using 1m candles.
    Catches spikes that happen between tick checks (millisecond wicks).
    Falls back to Binance if TV offline.
    """
    try:
        df = get_candles("1m", minutes + 1)
        return float(df["high"].max()), float(df["low"].min())
    except Exception as e:
        print(f"  [RANGE] {e}")
        return 0.0, 0.0

def get_current_source():
    if TV_BRIDGE_URL and tv_bridge_state["online"]: return "TradingView"
    return "BingX"

def is_tv_online():
    return bool(TV_BRIDGE_URL and tv_bridge_state["online"] and tv_bridge_state["cdp_ok"])

# --- SMC CALCULATIONS ---------------------------------------------------------
def find_swing_points(df, lookback=5):
    highs, lows = [], []
    for i in range(lookback, len(df) - lookback):
        if df["high"].iloc[i] == df["high"].iloc[i-lookback:i+lookback+1].max():
            highs.append({"idx": i, "price": df["high"].iloc[i], "time": df.index[i]})
        if df["low"].iloc[i] == df["low"].iloc[i-lookback:i+lookback+1].min():
            lows.append({"idx": i, "price": df["low"].iloc[i], "time": df.index[i]})
    return highs, lows

def detect_trend(df):
    # lookback=2 — faster swing confirmation, catches recent moves
    h, l = find_swing_points(df, 2)
    if len(h) >= 2 and len(l) >= 2:
        hp = [x["price"] for x in h[-3:]]; lp = [x["price"] for x in l[-3:]]
        bull_votes = 0; bear_votes = 0
        for i in range(len(hp)-1):
            if hp[i+1] > hp[i]: bull_votes += 1
            elif hp[i+1] < hp[i]: bear_votes += 1
        for i in range(len(lp)-1):
            if lp[i+1] > lp[i]: bull_votes += 1
            elif lp[i+1] < lp[i]: bear_votes += 1
        if bull_votes > bear_votes and bull_votes >= 2: return "BULLISH"
        if bear_votes > bull_votes and bear_votes >= 2: return "BEARISH"
    # Fallback: price slope over last 10 candles (catches fast moves before swings confirm)
    if len(df) >= 10:
        c = df["close"].values
        slope = (c[-1] - c[-10]) / c[-10] * 100
        if slope >  2.0: return "BULLISH"
        if slope < -2.0: return "BEARISH"
    return "NEUTRAL"

def detect_bos_choch(df):
    events = []; h, l = find_swing_points(df, 3)
    if len(h) < 2 or len(l) < 2: return events
    for i in range(1, min(4, len(h))):
        idx = h[-i]["idx"]
        if idx < len(df)-1 and df["close"].iloc[idx+1] > h[-i-1]["price"]:
            events.append({"type": "BOS_BULL", "price": h[-i-1]["price"], "idx": idx}); break
    for i in range(1, min(4, len(l))):
        idx = l[-i]["idx"]
        if idx < len(df)-1 and df["close"].iloc[idx+1] < l[-i-1]["price"]:
            events.append({"type": "BOS_BEAR", "price": l[-i-1]["price"], "idx": idx}); break
    return events

def detect_order_blocks(df, n=5):
    obs = []; c, o, h, l = df["close"].values, df["open"].values, df["high"].values, df["low"].values
    for i in range(3, len(df)-3):
        sz = h[i]-l[i]
        if sz == 0: continue
        if c[i] < o[i] and max(c[i+1:i+4])-h[i] > sz*0.5:
            obs.append({"type": "BULL_OB", "top": h[i], "bottom": l[i], "mid": (h[i]+l[i])/2, "idx": i})
        if c[i] > o[i] and l[i]-min(c[i+1:i+4]) > sz*0.5:
            obs.append({"type": "BEAR_OB", "top": h[i], "bottom": l[i], "mid": (h[i]+l[i])/2, "idx": i})
    return obs[-n:] if len(obs) > n else obs

def detect_fvgs(df, n=5):
    fvgs = []
    for i in range(2, len(df)):
        if df["low"].iloc[i] > df["high"].iloc[i-2]:
            fvgs.append({"type": "BULL_FVG", "top": df["low"].iloc[i], "bottom": df["high"].iloc[i-2], "idx": i})
        if df["high"].iloc[i] < df["low"].iloc[i-2]:
            fvgs.append({"type": "BEAR_FVG", "top": df["low"].iloc[i-2], "bottom": df["high"].iloc[i], "idx": i})
    return fvgs[-n:] if len(fvgs) > n else fvgs

def calc_atr(df, period=14):
    if len(df) < period+1: return 0
    h, l, c = df["high"].values, df["low"].values, df["close"].values
    trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1, len(df))]
    return float(np.mean(trs[-period:])) if trs else 0

def get_volume_profile(df, n=5):
    if len(df) < 10: return {"avg_vol": 0, "last_big_candle": None}
    avg_vol = float(df["vol"].tail(20).mean()); last_big = None
    for i in range(len(df)-1, max(0, len(df)-30), -1):
        if df["vol"].iloc[i] > avg_vol*1.5:
            last_big = {"idx": i, "vol": float(df["vol"].iloc[i]),
                "vol_ratio": float(df["vol"].iloc[i]/avg_vol),
                "is_bull": bool(df["close"].iloc[i] > df["open"].iloc[i]),
                "close": float(df["close"].iloc[i]), "candles_ago": len(df)-1-i}
            break
    return {"avg_vol": avg_vol, "last_big_candle": last_big}

def draw_smc_chart(df, tf, obs, fvgs, bos_events, s_highs, s_lows_list, price=None):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 10),
        gridspec_kw={"height_ratios": [4,1]}, facecolor="#0d0d0d")
    ax1.set_facecolor("#0d0d0d"); ax2.set_facecolor("#0d0d0d"); n = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        o, h, l, c = row["open"], row["high"], row["low"], row["close"]
        col = "#26a69a" if c >= o else "#ef5350"
        ax1.plot([i,i],[l,h], color=col, linewidth=0.8, zorder=2)
        ax1.add_patch(plt.Rectangle((i-0.4, min(o,c)), 0.8, abs(c-o), color=col, zorder=3))
    for ob in obs:
        idx = ob["idx"]
        if idx >= n: continue
        col = "#1a6b3c" if ob["type"]=="BULL_OB" else "#6b1a1a"
        bc  = "#00e676" if ob["type"]=="BULL_OB" else "#ff5252"
        ax1.add_patch(plt.Rectangle((idx-0.5, ob["bottom"]), n-idx+2, ob["top"]-ob["bottom"], color=col, alpha=0.3, zorder=1))
        ax1.text(idx+0.5, ob["top"], "Bull OB" if ob["type"]=="BULL_OB" else "Bear OB", color=bc, fontsize=6.5, va="bottom", zorder=5)
    for fvg in fvgs:
        idx = fvg["idx"]
        if idx >= n: continue
        col = "#1a3d6b" if fvg["type"]=="BULL_FVG" else "#6b4a1a"
        bc  = "#40c4ff" if fvg["type"]=="BULL_FVG" else "#ffab40"
        ax1.add_patch(plt.Rectangle((idx-2, fvg["bottom"]), n-idx+3, fvg["top"]-fvg["bottom"], color=col, alpha=0.35, zorder=1))
        ax1.text(idx+0.5, fvg["top"], "Bull FVG" if fvg["type"]=="BULL_FVG" else "Bear FVG", color=bc, fontsize=6.5, va="bottom", zorder=5)
    for sh in s_highs[-6:]:
        if sh["idx"] < n:
            ax1.plot(sh["idx"], sh["price"], "^", color="#ffeb3b", markersize=5, zorder=6)
            ax1.axhline(sh["price"], color="#ffeb3b", linewidth=0.5, linestyle=":", alpha=0.4)
    for sl_pt in s_lows_list[-6:]:
        if sl_pt["idx"] < n:
            ax1.plot(sl_pt["idx"], sl_pt["price"], "v", color="#ff9800", markersize=5, zorder=6)
            ax1.axhline(sl_pt["price"], color="#ff9800", linewidth=0.5, linestyle=":", alpha=0.4)
    for ev in bos_events:
        if ev["idx"] < n:
            col = "#b2ff59" if "BULL" in ev["type"] else "#ff4081"
            ax1.axhline(ev["price"], color=col, linewidth=1.0, linestyle="-.", alpha=0.7)
            ax1.text(max(0,ev["idx"]-2), ev["price"], "BOS", color=col, fontsize=7, va="bottom", fontweight="bold")
    if price:
        ax1.axhline(price, color="#fff", linewidth=1.2, linestyle="--", alpha=0.9)
        ax1.text(n-1, price, f" {price:,.0f}", color="#fff", fontsize=8, va="center")
    for i, (_, row) in enumerate(df.iterrows()):
        ax2.bar(i, row["vol"], color="#26a69a" if row["close"]>=row["open"] else "#ef5350", alpha=0.7, width=0.8)
    step = max(1, n//10)
    ax2.set_xticks(np.arange(n)[::step])
    ax2.set_xticklabels([df.index[i].strftime("%m/%d %H:%M") for i in range(0,n,step)], rotation=30, fontsize=6, color="#aaa")
    ax1.set_xticks([]); ax1.set_xlim(-1, n+3); ax2.set_xlim(-1, n+3)
    for ax in (ax1, ax2):
        ax.tick_params(colors="#aaa", labelsize=7)
        for s in ax.spines.values(): s.set_color("#333")
    ax1.yaxis.tick_right()
    trend = detect_trend(df)
    col = "#26a69a" if trend=="BULLISH" else ("#ef5350" if trend=="BEARISH" else "#fff")
    ax1.set_title(f"{SYMBOL} {tf}  |  {trend}  |  {get_current_source()}",
        color=col, fontsize=11, fontweight="bold", loc="left", pad=6)
    ax1.legend(handles=[
        mpatches.Patch(color="#1a6b3c", label="Bull OB"), mpatches.Patch(color="#6b1a1a", label="Bear OB"),
        mpatches.Patch(color="#1a3d6b", label="Bull FVG"), mpatches.Patch(color="#6b4a1a", label="Bear FVG"),
    ], loc="upper left", facecolor="#1a1a1a", edgecolor="#444", labelcolor="#ccc", fontsize=7, framealpha=0.8)
    plt.tight_layout(pad=0.5)
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=120, bbox_inches="tight", facecolor="#0d0d0d")
    plt.close(fig); buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def _build_chart(df, lb, tf_key, price):
    obs = detect_order_blocks(df, 6); fvgs = detect_fvgs(df, 6)
    bos = detect_bos_choch(df); sh, sl_pts = find_swing_points(df, lb)
    return draw_smc_chart(df, tf_key.upper(), obs, fvgs, bos, sh[-8:], sl_pts[-8:], price)

def generate_all_charts(data, price):
    charts = {}
    for tf_key, (df, lb) in data.items(): charts[tf_key] = _build_chart(df, lb, tf_key, price)
    return charts

def send_charts_to_channel(charts, label="SMC Analysis"):
    if not SEND_CHARTS: return
    for tf_key in CHART_TFS:
        b64 = charts.get(tf_key)
        if not b64: continue
        try:
            img_bytes = base64.b64decode(b64)
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": TELEGRAM_CHANNEL_ID, "caption": f"{label} - {tf_key.upper()} [{get_current_source()}]"},
                files={"photo": (f"chart_{tf_key}.png", img_bytes, "image/png")}, timeout=20)
            time.sleep(0.4)
        except Exception as e: print(f"  [CHART SEND] {tf_key}: {e}")

def build_smc_summary(data, ticker):
    src = ticker.get("source", "UNKNOWN")
    lines = [f"=== BTCUSDT LIVE ===",
        f"Price: {ticker['price']:,.2f} | 24h: {ticker['change']:+.2f}%",
        f"Vol: ${ticker['volume']/1e6:.1f}M | Session: {get_session()} | {ist_str()}",
        f"Data Source: {src}", ""]
    for tf_key, (df, lb) in data.items():
        trend = detect_trend(df); obs = detect_order_blocks(df, 4); fvgs = detect_fvgs(df, 4)
        bos = detect_bos_choch(df); sh, sl_pts = find_swing_points(df, lb)
        atr = calc_atr(df, 14); volp = get_volume_profile(df)
        lines.append(f"--- {tf_key.upper()} | {trend} | ATR:{atr:,.0f} | AvgVol:{volp['avg_vol']:,.0f} ---")
        if volp["last_big_candle"]:
            b = volp["last_big_candle"]
            lines.append(f"  Last big vol candle: {'GREEN' if b['is_bull'] else 'RED'} {b['candles_ago']} bars ago, vol={b['vol_ratio']:.1f}x avg, close={b['close']:,.0f}")
        for b in bos[-2:]: lines.append(f"  {b['type']}: {b['price']:,.2f}")
        bull_ob = [o for o in obs if o["type"]=="BULL_OB"]; bear_ob = [o for o in obs if o["type"]=="BEAR_OB"]
        if bull_ob: lines.append(f"  Bull OB: {bull_ob[-1]['bottom']:,.2f}-{bull_ob[-1]['top']:,.2f}")
        if bear_ob: lines.append(f"  Bear OB: {bear_ob[-1]['bottom']:,.2f}-{bear_ob[-1]['top']:,.2f}")
        bf = [f for f in fvgs if f["type"]=="BULL_FVG"]; brf = [f for f in fvgs if f["type"]=="BEAR_FVG"]
        if bf:  lines.append(f"  Bull FVG: {bf[-1]['bottom']:,.2f}-{bf[-1]['top']:,.2f}")
        if brf: lines.append(f"  Bear FVG: {brf[-1]['bottom']:,.2f}-{brf[-1]['top']:,.2f}")
        if sh:      lines.append(f"  Swing High: {sh[-1]['price']:,.2f}")
        if sl_pts:  lines.append(f"  Swing Low:  {sl_pts[-1]['price']:,.2f}")
        lines.append("")
    return "\n".join(lines)

def extract_json_from_response(text: str):
    if not text or not text.strip(): return None
    cleaned = re.sub(r'```json\s*', '', text); cleaned = re.sub(r'```\s*', '', cleaned).strip()
    try: return json.loads(cleaned)
    except: pass
    start = cleaned.find('{')
    if start == -1: return None
    depth = 0; end = -1
    for i in range(start, len(cleaned)):
        if cleaned[i] == '{': depth += 1
        elif cleaned[i] == '}':
            depth -= 1
            if depth == 0: end = i+1; break
    if end == -1: return None
    try: return json.loads(cleaned[start:end])
    except: return None

# ═══════════════════════════════════════════════════════════════════════════════
#  PROMPTS
# ═══════════════════════════════════════════════════════════════════════════════

# BTC_PROMPT_MODE controls which BTC analysis prompt is used:
#   "V9"  = CLEXER_V9_CURRENT  — always-new-prompt, CRITICAL JSON header (default)
#   "V7"  = CLEXER_V7_CLASSIC  — TV/Binance split, no CRITICAL header, session notes
BTC_PROMPT_MODE = "V9"

def build_new_prompt_v9(summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note):
    return f"""{summary}{validate_ctx}{outcome_ctx}{news_ctx}

CRITICAL: Respond with RAW JSON ONLY. No markdown. No bold. No steps. No explanation. Just the JSON object.

You are CLEXER - elite BTC futures trader.
Current Price: {price:,.0f}
Use ONLY the data provided in the summary above. Do not invent levels.

═══════════════════════════════════════════════════
 STEP 1 — CHECK CURRENT PRICE
═══════════════════════════════════════════════════
Current price is {price:,.0f}. Know this before analyzing any timeframe.

═══════════════════════════════════════════════════
 STEP 2 — WEEKLY (background bias only)
═══════════════════════════════════════════════════
Last 3-5 weekly closes:
  all going up   = BULLISH background
  all going down = BEARISH background
Context only — not a hard rule.

═══════════════════════════════════════════════════
 STEP 3 — 4H TREND (most important)
═══════════════════════════════════════════════════
Look at last 15-20 4H candles. Find swing highs and swing lows:
  HH + HL = BULLISH structure → look for LONG
  LH + LL = BEARISH structure → look for SHORT
  Mixed    = NEUTRAL → WAIT (hard stop)

Find these two exact prices:
  LAST 4H SWING HIGH = most recent peak
  LAST 4H SWING LOW  = most recent trough
These are your key reference levels.

═══════════════════════════════════════════════════
 STEP 4 — 4H VOLUME CHECK
═══════════════════════════════════════════════════
Last big volume candle:
  GREEN = buyers in control → confirms LONG (HIGH confidence)
  RED   = sellers in control → confirms SHORT (HIGH confidence)
Volume opposes 4H structure → MEDIUM confidence (still trade, not WAIT).
Volume absent/unclear → LOW confidence (still trade).
Only 4H structure NEUTRAL is a hard WAIT.

═══════════════════════════════════════════════════
 STEP 5 — 1H CONFIRMATION
═══════════════════════════════════════════════════
1H same as 4H → proceed with confidence
1H neutral    → acceptable, proceed
1H opposite   → lower confidence, proceed cautiously
Nothing else needed on 1H.

═══════════════════════════════════════════════════
 STEP 6 — 5M ENTRY TIMING
═══════════════════════════════════════════════════
Look at last 10-15 5M candles.

For LONG (4H bullish):
  Find most recent small pause — 3-4 consecutive 5M candles moving
  SIDEWAYS or slightly down (small bodies) after a move up.
  LOW of that pause cluster = entry price.
  If price already PAST the pause and moving up → MARKET entry at {price:,.0f}.

For SHORT (4H bearish):
  Find most recent small pause — 3-4 consecutive 5M candles moving
  SIDEWAYS or slightly up (small bodies) after a move down.
  HIGH of that pause cluster = entry price.
  If price already PAST the pause and moving down → MARKET entry at {price:,.0f}.

═══════════════════════════════════════════════════
 STEP 7 — DISTANCE CHECK
═══════════════════════════════════════════════════
distance = abs({price:,.0f} - entry) / {price:,.0f} × 100

IF distance > 1%  → entry stale → use MARKET at {price:,.0f}
IF distance ≤ 0.3% → MARKET at {price:,.0f}
IF 0.3% < distance ≤ 1% → PULLBACK, wait for price to reach entry

═══════════════════════════════════════════════════
 STEP 8 — STOP LOSS (based on pause cluster, NOT ATR)
═══════════════════════════════════════════════════
For LONG: SL = lowest low of pause cluster − 0.3% buffer
For SHORT: SL = highest high of pause cluster + 0.3% buffer

Minimum SL distance: 0.5% of entry
Maximum SL distance: 2.5% of entry
If pause SL tighter than 0.5% → expand to 0.5% minimum.

═══════════════════════════════════════════════════
 STEP 9 — TAKE PROFIT
═══════════════════════════════════════════════════
sl_dist = abs(entry - sl)
TP1 = entry ± (sl_dist × 2)
TP2 = entry ± (sl_dist × 4)

LONG:  TP1 and TP2 must be ABOVE {price:,.0f}
SHORT: TP1 and TP2 must be BELOW {price:,.0f}
If wrong side → recalculate from {price:,.0f} instead.

═══════════════════════════════════════════════════
 STEP 10 — FINAL CHECKS
═══════════════════════════════════════════════════
✅ 4H structure clear (not neutral)?
✅ Entry distance within 1%?
✅ SL at least 0.5% from entry?
✅ TP1 and TP2 on correct side of {price:,.0f}?
✅ Pause has at least 3 candles?
✅ 4H volume checked (confidence level set)?
If ALL pass → trade. If 4H NEUTRAL → WAIT. All other fails → adjust, don't WAIT.

{conf_note}

═══════════════════════════════════════════════════
 OUTPUT — RAW JSON ONLY. NO MARKDOWN. NO TEXT BEFORE/AFTER.
═══════════════════════════════════════════════════

WAIT:
{{"signal":"WAIT","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"NEUTRAL","structure_4h":"NEUTRAL","entry_zone":"","confidence":"LOW","session":"{session}","reasoning":"4H structure is NEUTRAL — no clear HH+HL or LH+LL.","trade_valid":null}}

HOLD:
{{"signal":"HOLD","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"BULLISH or BEARISH or NEUTRAL","structure_4h":"HH+HL or LH+LL or NEUTRAL","entry_zone":"","confidence":"HIGH or MEDIUM or LOW","session":"{session}","reasoning":"Why active trade still valid.","trade_valid":true}}

Trade:
{{"signal":"BUY or SELL","entry":<5M pause low/high or market>,"sl":<pause extreme ± buffer>,"tp1":<price>,"tp2":<price>,"rr":"1:2.0","entry_type":"MARKET or PULLBACK","entry_note":"instruction with price","bias":"BULLISH or BEARISH","weekly_trend":"BULLISH or BEARISH or NEUTRAL","structure_4h":"HH+HL or LH+LL","entry_zone":"5M pause cluster description","confidence":"HIGH or MEDIUM or LOW","session":"{session}","reasoning":"1)Weekly bias 2)4H structure HH/HL or LH/LL 3)Volume HIGH/MEDIUM/LOW confidence 4)1H confirmation 5)5M pause cluster entry 6)SL=pause extreme±0.3% 7)TP=sl×2 and sl×4","trade_valid":null}}"""


def build_old_prompt_v9(summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note, session_note):
    return f"""{summary}{validate_ctx}{outcome_ctx}{news_ctx}

You are CLEXER - elite BTC trader (Binance fallback mode).
Current Price: {price:,.0f}
Use ONLY the data in the summary. Do not invent levels.

STEP 1 - WEEKLY: closes UP=BULLISH, DOWN=BEARISH.
STEP 2 - 4H TREND: HH+HL=BULLISH, LH+LL=BEARISH, flat=NEUTRAL.
  Find LAST 4H SWING HIGH and LAST 4H SWING LOW - these are entry levels.
STEP 3 - VOLUME: last big GREEN=buy pressure, RED=sell pressure. Confirms only.
STEP 4 - 1H: same=strong, neutral=ok, opposite=lower confidence (not a block).
STEP 5 - 5M: higher lows=bullish now, lower highs=bearish now. Conflict=lower confidence only.

SIGNAL:
LONG:  4H bullish + last big vol GREEN + 1H not bearish + price above last 4H swing low
SHORT: 4H bearish + last big vol RED  + 1H not bullish + price below last 4H swing high

WAIT only: 4H NEUTRAL | 4H+1H both opposite | no swing within 2000 pts
DO NOT WAIT for: low vol, 5M conflict, 1H lag, weekly neutral, missing vol candle

ENTRY: LONG=last 4H swing LOW | SHORT=last 4H swing HIGH
  Within 100 pts = MARKET | else = PULLBACK

SL: sl_dist = ATR_1H × 1.5 | Min 500 | Max 2500
  LONG SL = entry - sl_dist | SHORT SL = entry + sl_dist | offset round numbers by 50 pts

TP: TP1 = entry ± sl_dist×2 | TP2 = entry ± sl_dist×4
  LONG: both must be > {price:,.0f} | SHORT: both must be < {price:,.0f}

ACTIVE TRADE: same direction=HOLD | flipped=new signal
{conf_note}{session_note}

OUTPUT RAW JSON ONLY:

WAIT:  {{"signal":"WAIT","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"NEUTRAL","structure_4h":"NEUTRAL","entry_zone":"","confidence":"LOW","session":"{session}","reasoning":"exact condition triggered","trade_valid":null}}
HOLD:  {{"signal":"HOLD","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"BULLISH or BEARISH or NEUTRAL","structure_4h":"HH+HL or LH+LL or NEUTRAL","entry_zone":"","confidence":"HIGH or MEDIUM or LOW","session":"{session}","reasoning":"why valid with swing prices","trade_valid":true}}
Trade: {{"signal":"BUY or SELL","entry":<4H swing price>,"sl":<price>,"tp1":<price>,"tp2":<price>,"rr":"1:4.0","entry_type":"MARKET or PULLBACK","entry_note":"instruction with price","bias":"BULLISH or BEARISH","weekly_trend":"","structure_4h":"HH+HL or LH+LL","entry_zone":"","confidence":"HIGH or MEDIUM or LOW","session":"{session}","reasoning":"1)Weekly 2)4H swings 3)Volume 4)1H 5)5M 6)Entry 7)SL 8)TP","trade_valid":null}}"""

# ── CLEXER_V7_CLASSIC prompts ─────────────────────────────────────────────────
# Original V7.0 BTC analysis — TV/Binance split, no CRITICAL header, session notes.
# Restored via /btcmode on. Switch back with /btcmode off.

def build_new_prompt_v7(summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note):
    return f"""{summary}{validate_ctx}{outcome_ctx}{news_ctx}

You are CLEXER - elite BTC trader using TradingView data.
Current Price: {price:,.0f}

Use ONLY the data provided in the summary above. Do not invent levels.

═══════════════════════════════════════════════════
 STEP 1 - WEEKLY DIRECTION
═══════════════════════════════════════════════════
From weekly data: closes going UP = BULLISH, DOWN = BEARISH.
Swing high higher than last = bullish. Lower = bearish.
This gives background bias only.

═══════════════════════════════════════════════════
 STEP 2 - 4H TREND (MOST IMPORTANT)
═══════════════════════════════════════════════════
HH + HL = 4H BULLISH. LH + LL = 4H BEARISH. Mixed = NEUTRAL.
Find from swing data:
  LAST 4H SWING HIGH = most recent resistance (price went up then back down)
  LAST 4H SWING LOW  = most recent support (price went down then back up)
These are your entry reference points.

═══════════════════════════════════════════════════
 STEP 3 - 4H VOLUME CONFIRMATION
═══════════════════════════════════════════════════
Last big GREEN candle = buying pressure (confirms LONG).
Last big RED candle   = selling pressure (confirms SHORT).
0-5 bars ago = strong | 6-15 = moderate | 15+ = weak.
Volume CONFIRMS bias - does not create it alone.

═══════════════════════════════════════════════════
 STEP 4 - 1H CONFIRMATION
═══════════════════════════════════════════════════
Same as 4H = strong confirmation. Neutral = acceptable.
Opposite = lower confidence - NOT a block alone.

═══════════════════════════════════════════════════
 STEP 5 - 5M TIMING
═══════════════════════════════════════════════════
Higher lows = bullish NOW. Lower highs = bearish NOW.
5M conflict = lower confidence only, NOT a block.

═══════════════════════════════════════════════════
 STEP 6 - DECIDE DIRECTION
═══════════════════════════════════════════════════
LONG requires ALL:
  ✅ 4H trend = BULLISH (HH+HL)
  ✅ Last big 4H or 1H vol candle = GREEN
  ✅ 1H not actively bearish
  ✅ Price {price:,.0f} is above last 4H swing LOW
  ✅ Weekly not making new lows this week

SHORT requires ALL:
  ✅ 4H trend = BEARISH (LH+LL)
  ✅ Last big 4H or 1H vol candle = RED
  ✅ 1H not actively bullish
  ✅ Price {price:,.0f} is below last 4H swing HIGH
  ✅ Weekly not making new highs this week

WAIT only when:
  ❌ 4H trend = NEUTRAL (no clear swing pattern)
  ❌ 4H AND 1H both trending OPPOSITE directions
  ❌ No 4H swing point within 2000 pts of current price
  ❌ Last 5 4H candles all flat (total consolidation)

DO NOT WAIT for: low volume, 5M conflict, 1H lag,
weekly neutral, missing big volume candle.

═══════════════════════════════════════════════════
 STEP 7 - ENTRY
═══════════════════════════════════════════════════
LONG:  entry = last 4H SWING LOW price
SHORT: entry = last 4H SWING HIGH price

If price {price:,.0f} is within 100 pts of that level:
  entry_type = MARKET
Else:
  entry_type = PULLBACK, entry_note = "wait for pullback/bounce to [price]"

NEVER enter at top/bottom of big momentum candle.

═══════════════════════════════════════════════════
 CRITICAL ENTRY DISTANCE RULE
═══════════════════════════════════════════════════
After finding the entry level, check:
  distance = abs(current_price - entry)

IF distance > 1500 pts:
  DO NOT use that swing level as entry.

  For LONG:
    Find the most recent 4H swing low WITHIN 1000 pts of {price:,.0f}.
    If no swing low within 1000 pts → use MARKET entry at {price:,.0f}.

  For SHORT:
    Find the most recent 4H swing high WITHIN 1000 pts of {price:,.0f}.
    If no swing high within 1000 pts → use MARKET entry at {price:,.0f}.

VERIFY before outputting - if ANY check fails use MARKET entry at {price:,.0f}:
  BUY:  entry <= {price:,.0f} + 200 | tp1 > {price:,.0f} | tp2 > tp1 | abs(entry - {price:,.0f}) < 1500
  SELL: entry >= {price:,.0f} - 200 | tp1 < {price:,.0f} | tp2 < tp1 | abs(entry - {price:,.0f}) < 1500

═══════════════════════════════════════════════════
 STEP 8 - STOP LOSS
═══════════════════════════════════════════════════
sl_dist = ATR_1H × 1.5
Min 500 pts | Max 2500 pts
LONG  SL = entry - sl_dist (below 4H swing low)
SHORT SL = entry + sl_dist (above 4H swing high)
Never at round number - offset by 50 pts.
Never exactly at wick - offset by 80-100 pts beyond.

═══════════════════════════════════════════════════
 STEP 9 - TAKE PROFIT
═══════════════════════════════════════════════════
TP1 = entry ± (sl_dist × 2)
TP2 = entry ± (sl_dist × 4)
LONG:  TP1 and TP2 must be > {price:,.0f}
SHORT: TP1 and TP2 must be < {price:,.0f}
If TP wrong side, recalculate from current price.

═══════════════════════════════════════════════════
 STEP 10 - ACTIVE TRADE VALIDATION
═══════════════════════════════════════════════════
Same 4H direction → HOLD (reference swing prices).
Opposite direction → new signal + reason why flipped.
Unclear → HOLD with low confidence.

{conf_note}

═══════════════════════════════════════════════════
 OUTPUT - RAW JSON ONLY. NO MARKDOWN. NO TEXT BEFORE/AFTER.
═══════════════════════════════════════════════════

WAIT:
{{"signal":"WAIT","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"NEUTRAL","structure_4h":"NEUTRAL","entry_zone":"","confidence":"LOW","session":"{session}","reasoning":"Exact WAIT condition triggered with price references from summary.","trade_valid":null}}

HOLD:
{{"signal":"HOLD","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"BULLISH or BEARISH or NEUTRAL","structure_4h":"HH+HL or LH+LL or NEUTRAL","entry_zone":"","confidence":"HIGH or MEDIUM or LOW","session":"{session}","reasoning":"Why active trade still valid with swing point prices.","trade_valid":true}}

Trade:
{{"signal":"BUY or SELL","entry":<4H swing price>,"sl":<price>,"tp1":<price>,"tp2":<price>,"rr":"1:4.0","entry_type":"MARKET or PULLBACK","entry_note":"clear instruction with price","bias":"BULLISH or BEARISH","weekly_trend":"BULLISH or BEARISH or NEUTRAL","structure_4h":"HH+HL or LH+LL","entry_zone":"4H swing level and price","confidence":"HIGH or MEDIUM or LOW","session":"{session}","reasoning":"1)Weekly 2)4H swing high X swing low Y 3)Volume direction X bars ago 4)1H 5)5M 6)Entry at swing 7)SL=ATR_1H×1.5 8)TP=sl×2 and sl×4","trade_valid":null}}"""


def build_old_prompt_v7(summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note, session_note):
    return f"""{summary}{validate_ctx}{outcome_ctx}{news_ctx}

You are CLEXER - elite BTC trader (Binance fallback mode).
Current Price: {price:,.0f}
Use ONLY the data in the summary. Do not invent levels.

STEP 1 - WEEKLY: closes UP=BULLISH, DOWN=BEARISH.
STEP 2 - 4H TREND: HH+HL=BULLISH, LH+LL=BEARISH, flat=NEUTRAL.
  Find LAST 4H SWING HIGH and LAST 4H SWING LOW - these are entry levels.
STEP 3 - VOLUME: last big GREEN=buy pressure, RED=sell pressure. Confirms only.
STEP 4 - 1H: same=strong, neutral=ok, opposite=lower confidence (not a block).
STEP 5 - 5M: higher lows=bullish now, lower highs=bearish now. Conflict=lower confidence only.

SIGNAL:
LONG:  4H bullish + last big vol GREEN + 1H not bearish + price above last 4H swing low
SHORT: 4H bearish + last big vol RED  + 1H not bullish + price below last 4H swing high

WAIT only: 4H NEUTRAL | 4H+1H both opposite | no swing within 2000 pts
DO NOT WAIT for: low vol, 5M conflict, 1H lag, weekly neutral, missing vol candle

ENTRY: LONG=last 4H swing LOW | SHORT=last 4H swing HIGH
  Within 100 pts = MARKET | else = PULLBACK

SL: sl_dist = ATR_1H × 1.5 | Min 500 | Max 2500
  LONG SL = entry - sl_dist | SHORT SL = entry + sl_dist | offset round numbers by 50 pts

TP: TP1 = entry ± sl_dist×2 | TP2 = entry ± sl_dist×4
  LONG: both must be > {price:,.0f} | SHORT: both must be < {price:,.0f}

ACTIVE TRADE: same direction=HOLD | flipped=new signal
{conf_note}{session_note}

OUTPUT RAW JSON ONLY:

WAIT:  {{"signal":"WAIT","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"NEUTRAL","structure_4h":"NEUTRAL","entry_zone":"","confidence":"LOW","session":"{session}","reasoning":"exact condition triggered","trade_valid":null}}
HOLD:  {{"signal":"HOLD","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"BULLISH or BEARISH or NEUTRAL","structure_4h":"HH+HL or LH+LL or NEUTRAL","entry_zone":"","confidence":"HIGH or MEDIUM or LOW","session":"{session}","reasoning":"why valid with swing prices","trade_valid":true}}
Trade: {{"signal":"BUY or SELL","entry":<4H swing price>,"sl":<price>,"tp1":<price>,"tp2":<price>,"rr":"1:4.0","entry_type":"MARKET or PULLBACK","entry_note":"instruction with price","bias":"BULLISH or BEARISH","weekly_trend":"","structure_4h":"HH+HL or LH+LL","entry_zone":"","confidence":"HIGH or MEDIUM or LOW","session":"{session}","reasoning":"1)Weekly 2)4H swings 3)Volume 4)1H 5)5M 6)Entry 7)SL 8)TP","trade_valid":null}}"""


# --- CLAUDE ANALYSIS ----------------------------------------------------------
def analyze_with_claude(ticker, data, validate_trade=False):
    price = ticker["price"]; session = get_session()
    min_conf = required_confidence(); src = get_current_source()
    tv_on = is_tv_online()
    if BTC_PROMPT_MODE == "V7":
        prompt_mode = "V7+TV" if tv_on else "V7+Binance"
    else:
        prompt_mode = "V9+TV" if tv_on else "V9+BingX"
    print(f"  [CLAUDE] Mode:{prompt_mode} | BTC:{BTC_PROMPT_MODE} | MinConf:{min_conf} | Validate:{validate_trade}")

    screenshots = {}
    if tv_on:
        print("  [CLAUDE] Fetching screenshots...")
        screenshots = fetch_tv_screenshots()
    if not screenshots:
        # TV offline or returned nothing — generate charts from BingX candles via matplotlib
        try:
            charts = generate_all_charts(data, price)
            key_map = {"weekly": "W", "4h": "4H", "1h": "1H", "5m": "5"}
            screenshots = {key_map[k]: v for k, v in charts.items() if k in key_map and v}
            if screenshots:
                print(f"  [CLAUDE] Generated {len(screenshots)} matplotlib charts from BingX candles")
        except Exception as e:
            print(f"  [CLAUDE] matplotlib chart fallback error: {e}")

    indicators = {}
    if tv_on:
        indicators = fetch_tv_indicators()

    if SEND_CHARTS:
        if screenshots:
            for tf_key in CHART_TFS:
                img_b64 = screenshots.get(tf_key.upper())
                if not img_b64: continue
                try:
                    img_bytes = base64.b64decode(img_b64)
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                        data={"chat_id": TELEGRAM_CHANNEL_ID,
                              "caption": f"SMC Analysis - {tf_key.upper()} [TradingView Live]"},
                        files={"photo": (f"chart_{tf_key}.png", img_bytes, "image/png")}, timeout=20)
                    time.sleep(0.4)
                except Exception as e: print(f"  [CHARTS] {e}")
        else:
            try:
                charts = generate_all_charts(data, price)
                send_charts_to_channel({k: v for k, v in charts.items() if k in CHART_TFS})
            except Exception as e: print(f"  [CHARTS] matplotlib error: {e}")

    summary = build_smc_summary(data, ticker)
    indicator_ctx = build_indicator_context(indicators)
    full_summary = summary + indicator_ctx

    news_ctx = ""
    if latest_news_context:
        news_ctx = "\n\nRECENT MARKET NEWS:\n" + "\n".join(latest_news_context[-3:])
    outcome_ctx = ""
    if trade_outcomes:
        outcome_ctx = "\n\nRECENT TRADE OUTCOMES (last 3):\n"
        for o in trade_outcomes[-3:]:
            outcome_ctx += f"  {o['time']}: {o['signal']} @ {o['entry']} → {o['reason']} ({o['detail']})\n"
    validate_ctx = ""
    if validate_trade and active_trade["signal"]:
        t = active_trade
        validate_ctx = (f"\n\nACTIVE TRADE TO VALIDATE:\n"
            f"  Signal:{t['signal']}  Entry:{t['entry']:,.0f}  SL:{t['sl']:,.0f}  "
            f"TP1:{t['tp1']:,.0f}  TP2:{t['tp2']:,.0f}\n"
            f"  TP1 hit:{t['tp1_hit']}  Entry hit:{t['entry_hit']}\n\n"
            f"  If structure still supports → return HOLD\n"
            f"  If structure flipped → return new signal with reason\n")
    conf_note = ""
    if min_conf == "HIGH":   conf_note = "\n!!! 2+ SLs: HIGH confidence only."
    elif min_conf == "MEDIUM": conf_note = "\n!! 1 recent SL: MEDIUM+ only."
    session_note = ""
    if session == "NEW_YORK": session_note = "\n\nNY SESSION: 4H primary. Give signal if 4H clear."
    elif session == "LONDON": session_note = "\n\nLONDON SESSION: Strong breakouts. Signal if 4H+weekly agree."

    if BTC_PROMPT_MODE == "V7":
        # CLEXER_V7_CLASSIC: TV gets new prompt, Binance gets old prompt with session notes
        if tv_on:
            prompt = build_new_prompt_v7(full_summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note)
        else:
            prompt = build_old_prompt_v7(full_summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note, session_note)
    else:
        # CLEXER_V9_CURRENT: always new prompt regardless of TV status
        prompt = build_new_prompt_v9(full_summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note)

    content = []
    if screenshots:
        added = 0
        for tf in ["W", "4H", "1H", "5"]:
            img_b64 = screenshots.get(tf)
            if not img_b64: continue
            content.append({"type": "text", "text": f"=== {tf} TIMEFRAME CHART (TradingView Live) ==="})
            content.append({"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": img_b64}})
            added += 1
        if added:
            content.append({"type": "text", "text": f"\n=== {added} CHART IMAGES ABOVE ===\nAnalyze visually AND use numerical data below.\n"})
            print(f"  [CLAUDE] Sending {added} TV screenshots to Claude API")
    content.append({"type": "text", "text": prompt})

    max_retries = 2; raw = None
    for attempt in range(max_retries):
        try:
            msg = _claude_client().messages.create(
                model=SCAN_MODEL, max_tokens=1200,
                messages=[{"role": "user", "content": content}])
            _log_api_usage("btc_analysis", SCAN_MODEL,
                           msg.usage.input_tokens, msg.usage.output_tokens)
            raw = _claude_text(msg)
            if raw: break
            time.sleep(2)
        except Exception as e:
            print(f"  [CLAUDE ERROR] attempt {attempt+1}: {e}")
            if "image" in str(e).lower() and attempt == 0:
                print("  [CLAUDE] Retrying text-only...")
                content_text = [c for c in content if c["type"] == "text"]
                try:
                    msg = _claude_client().messages.create(
                        model=SCAN_MODEL, max_tokens=1200,
                        messages=[{"role": "user", "content": content_text}])
                    _log_api_usage("btc_analysis_textonly", SCAN_MODEL,
                                   msg.usage.input_tokens, msg.usage.output_tokens)
                    raw = _claude_text(msg)
                    if raw: break
                except Exception as e2: print(f"  [CLAUDE] text-only retry: {e2}")
            if attempt < max_retries-1: time.sleep(3)

    if not raw: return None
    signal = extract_json_from_response(raw)
    if signal is None:
        print(f"  [CLAUDE] JSON parse failed. Raw:\n{raw[:400]}"); return None

    try:
        sig_type = signal.get("signal", "")
        if sig_type == "HOLD":
            return {"_hold": True, "reasoning": signal.get("reasoning", "")}
        if sig_type == "WAIT":
            reason = signal.get("reasoning", ""); bias = signal.get("bias", "?")
            print(f"  [WAIT] {reason[:120]}")
            send_telegram(
                f"<b>Scan Complete - No Signal</b>\n\n"
                f"Price: <b>{price:,.2f}</b> ({ticker['change']:+.2f}%)\n"
                f"Session: {session} | Bias: {bias}\n"
                f"Source: {src} ({prompt_mode})\n"
                f"<i>{reason[:250]}</i>\n\n"
                f"Next scan in {SIGNAL_SCAN_INTERVAL//3600}h. /signal to force.\n\n"
                f"{ist_str()}\n<i>🛡️ Capital protected</i>")
            return None
        if sig_type not in ("BUY", "SELL"): return None

        entry = float(signal["entry"]); sl_raw = float(signal["sl"]); sl_dist = abs(entry-sl_raw)
        if sl_dist < 500:
            fix_dist = 650
            signal["sl"]  = round(entry-fix_dist if sig_type=="BUY" else entry+fix_dist, -1)
            sl_raw = float(signal["sl"]); sl_dist = abs(entry-sl_raw)
            signal["tp1"] = round(entry+sl_dist*2 if sig_type=="BUY" else entry-sl_dist*2, -1)
            signal["tp2"] = round(entry+sl_dist*4 if sig_type=="BUY" else entry-sl_dist*4, -1)
        if sl_dist > 3000: return None

        etype = signal.get("entry_type", "MARKET")
        if etype == "PULLBACK":
            if sig_type=="BUY" and float(signal["tp1"]) <= price:
                signal["tp1"] = round(price+sl_dist*2, -1); signal["tp2"] = round(price+sl_dist*4, -1)
            elif sig_type=="SELL" and float(signal["tp1"]) >= price:
                signal["tp1"] = round(price-sl_dist*2, -1); signal["tp2"] = round(price-sl_dist*4, -1)

        conf = signal.get("confidence", "LOW"); rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
        if rank.get(conf,1) < rank.get(min_conf,1):
            send_telegram(f"<b>Signal filtered - low confidence</b>\n\n"
                f"Claude found: <b>{sig_type}</b> @ {entry:,.0f}\n"
                f"Confidence: <b>{conf}</b> (required: {min_conf})\n\n"
                f"<i>/resetsl to lower bar. 🛡️ Capital protected</i>")
            return None

        tp2_dist = abs(entry-float(signal["tp2"]))
        signal["rr"] = f"1:{tp2_dist/sl_dist:.1f}" if sl_dist else "1:?"
        signal["data_source"] = src; signal["prompt_mode"] = prompt_mode
        print(f"  [OK] {sig_type} entry:{entry:,.0f} SL:{sl_raw:,.0f} ({sl_dist:.0f}pts) R:R:{signal['rr']} Conf:{conf}")
        return signal
    except Exception as e:
        print(f"  [CLAUDE PARSE ERROR] {e}"); import traceback; traceback.print_exc(); return None


# ═══════════════════════════════════════════════════════════════════════════════
#  B1 ANALYZER  — V7 prompts + BingX only (no SpacemanBTC, no TV indicators)
#  Used by /compare to test V7 logic against V9 side-by-side.
# ═══════════════════════════════════════════════════════════════════════════════

def b1_fetch_data(use_tv=False):
    """Fetch candle data for B1. use_tv=True reads from TV bridge, else BingX direct."""
    data = {}
    for key, lim, lb in [("weekly", 52, 5), ("4h", 200, 5), ("1h", 100, 5), ("5m", 50, 3)]:
        try:
            if use_tv and tv_bridge_state["online"]:
                df = tv_get_candles(key, lim)
                if df is not None and len(df) >= 2:
                    data[key] = (df, lb); continue
            df = bingx_get_btc_candles(key, lim)
            if df is None:
                df = binance_get_candles(key, lim)
            data[key] = (df, lb)
        except Exception as e:
            print(f"  [B1 DATA] {key}: {e}")
    return data

def b1_get_ticker(use_tv=False):
    if use_tv and tv_bridge_state["online"]:
        tk = tv_get_ticker()
        if tk: return tk
    tk = bingx_get_btc_ticker()
    if tk: return tk
    return binance_get_ticker()

def b1_analyze(ticker, data, use_tv=False):
    """B1 = V7 prompt logic + BingX data. No SpacemanBTC, no confidence filter, no channel posts."""
    price = ticker["price"]; session = get_session()
    src = "TradingView" if use_tv else "BingX"
    label = f"B1+{'TV' if use_tv else 'BingX'}"
    print(f"  [B1] {label} | price:{price:,.0f}")

    summary = build_smc_summary(data, ticker)

    session_note = ""
    if session == "NEW_YORK": session_note = "\n\nNY SESSION: 4H primary. Give signal if 4H clear."
    elif session == "LONDON": session_note = "\n\nLONDON SESSION: Strong breakouts. Signal if 4H+weekly agree."

    if use_tv:
        prompt = build_new_prompt_v7(summary, price, session, "", "", "", "")
    else:
        prompt = build_old_prompt_v7(summary, price, session, "", "", "", "", session_note)

    try:
        msg = _claude_client().messages.create(
            model=SCAN_MODEL, max_tokens=2000,
            system="You are a trading signal bot. Respond with ONLY a JSON object. No reasoning, no steps, no text before or after the JSON.",
            messages=[{"role": "user", "content": prompt}])
        _log_api_usage("btc_b1", SCAN_MODEL,
                       msg.usage.input_tokens, msg.usage.output_tokens)
        raw = _claude_text(msg)
    except Exception as e:
        print(f"  [B1 CLAUDE] {e}"); return None, label

    signal = extract_json_from_response(raw)
    if not signal:
        return {"signal": "ERROR", "raw": raw[:200]}, label

    signal["data_source"] = src
    signal["prompt_mode"] = label
    return signal, label

def _fmt_compare_result(sig, label):
    """Format one analysis result into a short text block."""
    if sig is None:
        return f"<b>[{label}]</b> — API error / no response"
    s = sig.get("signal", "?")
    if s == "ERROR":
        return f"<b>[{label}]</b> ❓ Parse error\n<code>{sig.get('raw','')[:120]}</code>"
    if s == "WAIT":
        return (f"<b>[{label}]</b> ⏸ WAIT\n"
                f"Bias: {sig.get('bias','?')} | Conf: {sig.get('confidence','?')}\n"
                f"<i>{sig.get('reasoning','')[:120]}</i>")
    if s == "HOLD":
        return (f"<b>[{label}]</b> 🔒 HOLD\n"
                f"4H: {sig.get('structure_4h','?')} | Conf: {sig.get('confidence','?')}\n"
                f"<i>{sig.get('reasoning','')[:120]}</i>")
    e = "🟢" if s == "BUY" else "🔴"
    entry = float(sig.get("entry", 0)); sl = float(sig.get("sl", 0)); tp1 = float(sig.get("tp1", 0)); tp2 = float(sig.get("tp2", 0))
    return (f"<b>[{label}]</b> {e} <b>{s}</b>\n"
            f"Entry: <b>{entry:,.0f}</b> ({sig.get('entry_type','?')})\n"
            f"SL: {sl:,.0f} | TP1: {tp1:,.0f} | TP2: {tp2:,.0f}\n"
            f"R:R: {sig.get('rr','?')} | Conf: {sig.get('confidence','?')}\n"
            f"4H: {sig.get('structure_4h','?')}\n"
            f"<i>{sig.get('reasoning','')[:150]}</i>")

# --- PRICE / DETECTION HELPERS ------------------------------------------------
def price_only_advice(price):
    t = active_trade; sig = t["signal"]; entry = t["entry"]; sl = t["sl"]; tp1 = t["tp1"]; tp2 = t["tp2"]
    tp2_dist = abs(entry-tp2) or 1
    if sig == "BUY":
        dist_to_sl = price-sl; dist_to_tp1 = tp1-price; dist_to_tp2 = tp2-price
        pct = (price-entry)/tp2_dist*100
    else:
        dist_to_sl = sl-price; dist_to_tp1 = price-tp1; dist_to_tp2 = price-tp2
        pct = (entry-price)/tp2_dist*100
    if pct >= 75:   advice = "HOLD - Consider trailing SL"
    elif pct >= 40: advice = "HOLD - Strong momentum"
    elif pct >= 10: advice = "HOLD - In profit"
    else:           advice = "WAIT - Near entry, watch momentum"
    tp1_status = "HIT - SL at breakeven" if t["tp1_hit"] else f"{abs(dist_to_tp1):.0f} pts away"
    return (f"<b>HOURLY CHECK</b>  {ist_str()}\n\n{sig} {SYMBOL}\n\n"
        f"Price:    <b>{price:,.2f}</b>\nEntry:    <b>{entry:,.0f}</b>\n"
        f"SL:       <b>{sl:,.0f}</b>  ({dist_to_sl:.0f} pts)\n"
        f"TP1:      <b>{tp1:,.0f}</b>  {tp1_status}\n"
        f"TP2:      <b>{tp2:,.0f}</b>  ({abs(dist_to_tp2):.0f} pts)\n"
        f"Progress: <b>{max(0,pct):.1f}%</b> to TP2\nAdvice:   <b>{advice}</b>\n"
        f"Source:   <b>{get_current_source()}</b>\n\n<i>🛡️ Capital protected</i>")

def required_confidence():
    n = trade_stats["consecutive_sl"]
    if n >= 2: return "HIGH"
    if n >= 1: return "MEDIUM"
    return "LOW"

def detect_stop_hunt(df_5m):
    t = active_trade
    if not t["signal"] or not t["entry_hit"]: return False
    sig = t["signal"]; sl = t["sl"]
    for i in range(-3, 0):
        row = df_5m.iloc[i]
        if sig=="BUY"  and row["low"]<sl  and row["close"]>sl and row["close"]-row["low"]>100:  return True
        if sig=="SELL" and row["high"]>sl and row["close"]<sl and row["high"]-row["close"]>100: return True
    return False

def detect_entry_missed(price):
    t = active_trade
    if t["entry_hit"] or t["entry_type"]!="PULLBACK": return False
    if t["signal"]=="BUY"  and price >= t["tp2"]: return True
    if t["signal"]=="SELL" and price <= t["tp2"]: return True
    return False

def detect_entry_invalidated(price, df_4h):
    t = active_trade
    if t["entry_hit"]: return False
    last_close = df_4h["close"].iloc[-1]
    if t["signal"]=="BUY"  and last_close < t["sl"]: return True
    if t["signal"]=="SELL" and last_close > t["sl"]: return True
    return False

def fetch_all_data():
    data = {}
    for key, lim, lb in [("weekly",52,5),("4h",200,5),("1h",100,5),("5m",50,3)]:
        df = get_candles(key, lim); data[key] = (df, lb); time.sleep(0.3)
        print(f"    {key}: {len(df)} candles  [{get_current_source()}]")
    return data

def check_price_status(price, high, low, df_5m=None):
    t = active_trade
    if not t["signal"]: return "NONE"
    sig, sl, tp1, tp2, entry = t["signal"], t["sl"], t["tp1"], t["tp2"], t["entry"]
    if not t["entry_hit"]:
        if sig=="BUY"  and high >= tp2: return "ENTRY_MISSED"
        if sig=="SELL" and low  <= tp2: return "ENTRY_MISSED"
        if sig=="BUY"  and low  <= sl:  return "SETUP_INVALID"
        if sig=="SELL" and high >= sl:  return "SETUP_INVALID"
        tol = abs(entry-sl)*0.3
        if (sig=="BUY" and price<=entry+tol) or (sig=="SELL" and price>=entry-tol):
            active_trade["entry_hit"] = True
        else: return "WAITING_ENTRY"
    if df_5m is not None and not t["sl_wicked"]:
        if detect_stop_hunt(df_5m): active_trade["sl_wicked"] = True; trade_stats["stop_hunts"] += 1; return "STOP_HUNT"
    if (sig=="SELL" and high>=sl)  or (sig=="BUY"  and low<=sl):   return "SL_HIT"
    if (sig=="SELL" and low<=tp2)  or (sig=="BUY"  and high>=tp2): return "TP2_HIT"
    if not t["tp1_hit"]:
        if (sig=="SELL" and low<=tp1) or (sig=="BUY" and high>=tp1): return "TP1_HIT"
    return "RUNNING"

import copytrade as ct
ct._pause_event = bot_paused

# --- TELEGRAM -----------------------------------------------------------------
_SETTINGS_FILE = os.path.join(os.getenv("DATA_DIR", "."), "settings.json")

def load_settings():
    global channel_paused, SEND_CHARTS, CHART_TFS, SEND_NEWS, SIGNAL_SCAN_INTERVAL, BTC_PROMPT_MODE, btc_analysis_enabled, SCAN1_AUTO_ENABLED, SCAN2_AUTO_ENABLED, TEST_SCAN_ENABLED, SCAN_MODEL, USE_AEROLINK, CONTACT_ADMIN_ENABLED, SIGNAL_CHANNEL_ENABLED, SIGNAL_CHANNEL_LINK, SCAN1_MODEL, SCAN1_AEROLINK, SCAN2_MODEL, SCAN2_AEROLINK, TEST_MODEL, TEST_AEROLINK, ZONE_ENTRY_ENABLED, CO_ADMIN_CHAT_ID, CO_ADMIN_ENABLED, ACTIVE_PROFILE, _SETTINGS_PROFILES, CHANNELS, FREE_SIGNAL_DAILY_LIMIT, TRAIL_SL_BTC, TRAIL_SL_SCAN1, TRAIL_SL_SCAN2
    try:
        if os.path.exists(_SETTINGS_FILE):
            d = json.load(open(_SETTINGS_FILE))
            channel_paused.update(d.get("channel_paused", {}))
            SEND_CHARTS           = d.get("send_charts",         SEND_CHARTS)
            CHART_TFS             = d.get("chart_tfs",           CHART_TFS)
            SEND_NEWS             = d.get("send_news",           SEND_NEWS)
            SIGNAL_SCAN_INTERVAL  = d.get("scan_interval",       SIGNAL_SCAN_INTERVAL)
            BTC_PROMPT_MODE       = d.get("btc_prompt_mode",     BTC_PROMPT_MODE)
            btc_analysis_enabled  = False  # always OFF on startup
            SCAN1_AUTO_ENABLED    = d.get("scan1_auto",          True)
            SCAN2_AUTO_ENABLED    = d.get("scan2_auto",          False)
            TEST_SCAN_ENABLED     = d.get("test_scan",           False)
            SCAN_MODEL            = d.get("scan_model",          SCAN_MODEL)
            USE_AEROLINK          = d.get("use_aerolink",        USE_AEROLINK)
            SCAN1_MODEL    = d.get("scan1_model", SCAN1_MODEL)
            SCAN2_MODEL    = d.get("scan2_model", SCAN2_MODEL)
            TEST_MODEL     = d.get("test_model",  TEST_MODEL)
            SCAN1_AEROLINK = d.get("scan1_aerolink", SCAN1_AEROLINK)
            SCAN2_AEROLINK = d.get("scan2_aerolink", SCAN2_AEROLINK)
            TEST_AEROLINK  = d.get("test_aerolink",  TEST_AEROLINK)
            ZONE_ENTRY_ENABLED = d.get("zone_entry_enabled", False)
            CO_ADMIN_CHAT_ID = d.get("co_admin_chat_id", "")
            CO_ADMIN_ENABLED = d.get("co_admin_enabled", False)
            ct.TP1_CLOSE_PCT = d.get("tp1_close_pct", ct.TP1_CLOSE_PCT)
            ACTIVE_PROFILE = d.get("active_profile", "mine")
            _SETTINGS_PROFILES = d.get("settings_profiles", {"mine": {}, "coadmin": {}})
            CHANNELS = d.get("channels", [])
            FREE_SIGNAL_DAILY_LIMIT = d.get("free_signal_daily_limit", 0)
            TRAIL_SL_BTC   = d.get("trail_sl_btc", False)
            TRAIL_SL_SCAN1 = d.get("trail_sl_scan1", False)
            TRAIL_SL_SCAN2 = d.get("trail_sl_scan2", False)
            CONTACT_ADMIN_ENABLED  = d.get("contact_admin_enabled",  True)
            SIGNAL_CHANNEL_ENABLED = d.get("signal_channel_enabled", True)
            SIGNAL_CHANNEL_LINK    = d.get("signal_channel_link",    "")
            ct.BTC_CT_ENABLED   = d.get("btc_ct_enabled",   True)
            ct.SCAN1_CT_ENABLED = d.get("scan1_ct_enabled", True)
            ct.SCAN2_CT_ENABLED = d.get("scan2_ct_enabled", True)
            print(f"[SETTINGS] Loaded — charts:{SEND_CHARTS} news:{SEND_NEWS} "
                  f"interval:{SIGNAL_SCAN_INTERVAL//3600}h "
                  f"btcmode:{BTC_PROMPT_MODE} "
                  f"model:{SCAN_MODEL} "
                  f"ch_paused:{channel_paused}")
    except Exception as e:
        print(f"[SETTINGS] Load error: {e}")

def save_settings():
    try:
        json.dump({
            "channel_paused":   channel_paused,
            "send_charts":      SEND_CHARTS,
            "chart_tfs":        CHART_TFS,
            "send_news":        SEND_NEWS,
            "scan_interval":    SIGNAL_SCAN_INTERVAL,
            "btc_prompt_mode":  BTC_PROMPT_MODE,
            "btc_analysis":     btc_analysis_enabled,
            "scan1_auto":       SCAN1_AUTO_ENABLED,
            "scan2_auto":       SCAN2_AUTO_ENABLED,
            "test_scan":        TEST_SCAN_ENABLED,
            "scan_model":       SCAN_MODEL,
            "use_aerolink":     USE_AEROLINK,
            "scan1_model":      SCAN1_MODEL,
            "scan2_model":      SCAN2_MODEL,
            "test_model":       TEST_MODEL,
            "scan1_aerolink":   SCAN1_AEROLINK,
            "scan2_aerolink":   SCAN2_AEROLINK,
            "test_aerolink":    TEST_AEROLINK,
            "zone_entry_enabled": ZONE_ENTRY_ENABLED,
            "co_admin_chat_id": CO_ADMIN_CHAT_ID,
            "co_admin_enabled": CO_ADMIN_ENABLED,
            "tp1_close_pct": ct.TP1_CLOSE_PCT,
            "active_profile": ACTIVE_PROFILE,
            "settings_profiles": _SETTINGS_PROFILES,
            "channels": CHANNELS,
            "free_signal_daily_limit": FREE_SIGNAL_DAILY_LIMIT,
            "trail_sl_btc": TRAIL_SL_BTC,
            "trail_sl_scan1": TRAIL_SL_SCAN1,
            "trail_sl_scan2": TRAIL_SL_SCAN2,
            "contact_admin_enabled":  CONTACT_ADMIN_ENABLED,
            "signal_channel_enabled": SIGNAL_CHANNEL_ENABLED,
            "signal_channel_link":    SIGNAL_CHANNEL_LINK,
            "btc_ct_enabled":   ct.BTC_CT_ENABLED,
            "scan1_ct_enabled": ct.SCAN1_CT_ENABLED,
            "scan2_ct_enabled": ct.SCAN2_CT_ENABLED,
        }, open(_SETTINGS_FILE, "w"), indent=2)
    except Exception as e:
        print(f"[SETTINGS] Save error: {e}")

channel_paused = {"1": False, "2": False}  # per-channel pause state

# Premium (Telegram Premium) animated emoji IDs — rendered via <tg-emoji emoji-id="…">
# HTML tag so they coexist with existing parse_mode="HTML" formatting. Falls back to
# the plain emoji glyph automatically for non-Premium viewers.
PREMIUM_EMOJI_MAP = {
    "🟢": "5215685881989442149", "🔴": "4926956800005112527",
    "🛑": "5366040905927113475", "🎯": "5461009483314517035",
    "🏆": "5188344996356448758", "✅": "6120713655366455614",
    "❌": "6120660741369369103", "🚫": "5240241223632954241",
    "🚨": "5395695537687123235", "🚀": "6221996895535896347",
    "💰": "6224365445445590974", "🤖": "5197252827247841976",
    "📊": "5231200819986047254", "📡": "6174682466356303760",
    "⏰": "5213349767672769194", "🕐": "5363857580777029543",
    "🕦": "5933544413740403607", "🛡": "6070930852647278292",
    "📌": "5193159135004211919", "💬": "5233376087777501917",
    "✨": "5325547803936572038", "🎉": "5208895581644140071",
    "🔺": "5980787993139481991", "🗂": "5332586662629227075",
    "▶️": "5264919878082509254", "👏": "5357052372600250759",
    "😭": "5339386257283764734", "💀": "5379930048478330552",
    "📣": "5215668805199473901", "🧠": "6120687391641440754",
    "⚠️": "5213181173026533794", "👑": "6120766436219555441",
    "👋": "5258029071207505708", "🏷️": "6016997440777883054",
    "🏷": "6016997440777883054",
    "⭐️": "5839390003238540432", "⭐": "5839390003238540432",
    "👇": "6222198028854367391",
    "⏳": "5215327832040811010", "🔄": "5978846612087114958",
    "⚙️": "5341715473882955310", "💵": "5215239948420003628",
    "⚡️": "5258203794772085854", "⚡": "5258203794772085854",
    "📋": "5886223731088431288",
    "🔧": "5413398757226076065", "🗑": "5445267414562389170",
    "🧪": "5411512278740640309", "⏸": "5359543311897998264",
    "👥": "4942888689131848546", "👤": "5818715087237549366",
    "🆓": "5364112491381006601", "🤝": "5395732581780040886",
    "🔀": "5289756243731162671", "🖼": "6298530025884884100",
    "📸": "5235837920081887219", "📱": "5819062970998590994",
    "📰": "5257952710983955418", "📥": "6073143860316344247",
    "🔗": "5271604874419647061",
}
PREMIUM_EMOJIS_ENABLED = True

def _apply_premium_emojis(text: str) -> str:
    """Wraps known emoji glyphs in <tg-emoji> so Premium users see the animated
    version; everyone else still sees the plain glyph (Telegram's own fallback)."""
    if not PREMIUM_EMOJIS_ENABLED or not text:
        return text
    for glyph, emoji_id in PREMIUM_EMOJI_MAP.items():
        if glyph in text:
            text = text.replace(glyph, f'<tg-emoji emoji-id="{emoji_id}">{glyph}</tg-emoji>')
    if "BingX" in text:
        text = text.replace("BingX", '<tg-emoji emoji-id="5289756243731162671">🔀</tg-emoji> BingX')
    return text

_STYLE_SUCCESS_HINTS = ("Turn ON", "🟢", "Yes, confirm", "Adopt", "💾 Save", "✅")
_STYLE_DANGER_HINTS  = ("Turn OFF", "🔴", "Cancel", "Remove", "Reset", "❌ Close", "🗑", "🚫", "❌")
# Exact buttons that should never get a color — plain settings/nav entries, not ON/OFF actions
_STYLE_NONE_LABELS = (
    "Set Custom SL", "Set Custom TP1", "Set Custom TP2", "TP1 Close %", "Trailing SL",
    "My Copy Trade", "Trade Control", "Copy Admin", "TV & Advanced", "Broadcast & Channels",
    "Contact/Channel Settings", "Active BTC + all scan trades", "Current BTC price",
    "London / NY / Sleep session", "Last 5 signals", "Other Actions",
)

_STYLE_ROTATION = ("primary", "success", "danger")  # Bot API 9.4 defines only these 3 — no orange/pink/custom hex exists

def _style_keyboard(markup, rotate=True):
    """Adds Bot API 9.4 button `style` — green for positive/confirm actions,
    red for destructive/off/cancel ones, and (when rotate=True, the default)
    blue/green/red cycling for everything else so menus aren't monotone.
    A small exact-label exclusion list opts specific buttons out entirely.
    Buttons that already set a style are left untouched."""
    if not markup or "inline_keyboard" not in markup:
        return markup
    _nav_i = 0
    for row in markup["inline_keyboard"]:
        for btn in row:
            if "text" not in btn:
                continue
            label = btn["text"]
            if "style" not in btn:
                if btn.get("callback_data") == "noop" or any(label.strip().endswith(n) for n in _STYLE_NONE_LABELS):
                    pass
                elif any(h in label for h in _STYLE_SUCCESS_HINTS):
                    btn["style"] = "success"
                elif any(h in label for h in _STYLE_DANGER_HINTS):
                    btn["style"] = "danger"
                elif rotate:
                    btn["style"] = _STYLE_ROTATION[_nav_i % 3]
                    _nav_i += 1
            if PREMIUM_EMOJIS_ENABLED and "icon_custom_emoji_id" not in btn:
                for glyph, emoji_id in PREMIUM_EMOJI_MAP.items():
                    if label.strip().startswith(glyph):
                        btn["icon_custom_emoji_id"] = emoji_id
                        # Icon already shows the glyph — drop the duplicate from the label
                        stripped = label.replace(glyph, "", 1).strip()
                        btn["text"] = stripped if stripped else label
                        break
    return markup

def send_telegram(text, include_ch2=True):
    success = False
    text = _apply_premium_emojis(text)
    channels = [("1", TELEGRAM_CHANNEL_ID), ("2", os.getenv("TELEGRAM_CHANNEL_ID_2",""))]
    for key, cid in channels:
        if not cid: continue
        if channel_paused.get(key): continue
        if key == "2" and not include_ch2: continue
        try:
            r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": text,
                      "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
            r.raise_for_status(); success = True
        except Exception as e: print(f"  [TG ERROR] {cid}: {e}")
    return success

def send_admin(text):
    """Send message to admin DM only (not channel)."""
    if not ADMIN_CHAT_ID: return
    text = _apply_premium_emojis(text)
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
        if not r.json().get("ok"):
            print(f"  [ADMIN MSG ERROR] Telegram rejected: {r.json().get('description')}")
    except Exception as e: print(f"  [ADMIN MSG ERROR] {e}")

_reply_capture: dict = {}  # cid → {"texts": [], "cat_id": str} when capturing for inline menu

def send_reply(chat_id, text, reply_markup=None):
    cid_str = str(chat_id)
    text = _apply_premium_emojis(text)
    reply_markup = _style_keyboard(reply_markup)
    if cid_str in _reply_capture:
        _reply_capture[cid_str]["texts"].append(text)
        if reply_markup:
            _reply_capture[cid_str]["markup"] = reply_markup
        return
    try:
        payload = {"chat_id": chat_id, "text": text,
                   "parse_mode": "HTML", "disable_web_page_preview": True}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json=payload, timeout=10)
        if not r.json().get("ok"):
            print(f"  [REPLY ERROR] Telegram rejected: {r.json().get('description')}")
    except Exception as e: print(f"  [REPLY ERROR] {e}")

def _build_vip_csv(vip_start: str, vip_end: str) -> bytes:
    """Filtered trade-log CSV for a VIP's membership window — excludes signal_time
    and entry_trigger_time columns per admin's request, only date-filters by them."""
    import csv, io
    try:
        d1, m1, y1 = vip_start.split("."); start_dt = datetime(int(y1), int(m1), int(d1))
        d2, m2, y2 = vip_end.split(".");   end_dt = datetime(int(y2), int(m2), int(d2), 23, 59, 59)
    except Exception:
        return b""
    if not os.path.exists(TRADE_LOG_CSV):
        return b""
    with open(TRADE_LOG_CSV, "r") as f:
        rows = list(csv.DictReader(f))
    out_rows = []
    for r in rows:
        st = (r.get("signal_time") or "").replace(" IST", "").strip()
        try:
            dt = datetime.strptime(st, "%Y-%m-%d %H:%M")
        except Exception:
            continue
        if start_dt <= dt <= end_dt:
            out_rows.append(r)
    if not out_rows:
        return b""
    fieldnames = [h for h in _CSV_HEADERS if h not in ("signal_time", "entry_trigger_time")]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    for r in out_rows:
        w.writerow({k: r.get(k, "") for k in fieldnames})
    return buf.getvalue().encode("utf-8")

def _send_vip_renew_reminder(cid: str, user: dict):
    _mkp = {"inline_keyboard": [[{"text": "💬 Contact Admin", "url": f"tg://user?id={ADMIN_CHAT_ID}"}]]} if ADMIN_CHAT_ID else None
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": int(cid),
                  "text": "⏰ <b>Your VIP expired</b>\n\nRenew within 24 hours or you'll be removed from "
                          "VIP and the VIP channel.",
                  "parse_mode": "HTML", "reply_markup": _mkp}, timeout=10)
    except Exception as e:
        print(f"  [VIP EXPIRE] reminder {cid}: {e}")

def _kick_from_vip_channels(cid: str):
    for c in CHANNELS:
        if c.get("tier") != "vip" or not c.get("id"):
            continue
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/banChatMember",
                json={"chat_id": c["id"], "user_id": int(cid)}, timeout=10)
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/unbanChatMember",
                json={"chat_id": c["id"], "user_id": int(cid), "only_if_banned": True}, timeout=10)
        except Exception as e:
            print(f"  [VIP EXPIRE] kick {cid} from {c.get('id')}: {e}")

def _expire_vip_user(cid: str, user: dict):
    vip_start = user.get("vip_start", ""); vip_end = user.get("vip_end", "")
    user["tier"] = "free"; user["vip_start"] = ""; user["vip_end"] = ""; user["vip_grace_notified_at"] = 0
    ct._set(cid, user)
    _kick_from_vip_channels(cid)
    _mkp = {"inline_keyboard": [[{"text": "💬 Contact Admin", "url": f"tg://user?id={ADMIN_CHAT_ID}"}]]} if ADMIN_CHAT_ID else None
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": int(cid), "text": "⏰ <b>Your VIP has expired</b>\n\nYou've been removed from the VIP channel. Contact admin to renew.",
                  "parse_mode": "HTML", "reply_markup": _mkp}, timeout=10)
    except Exception as e:
        print(f"  [VIP EXPIRE] notify {cid}: {e}")
    csv_bytes = _build_vip_csv(vip_start, vip_end)
    if csv_bytes:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
                data={"chat_id": int(cid), "caption": f"Your VIP trade history: {vip_start} to {vip_end}"},
                files={"document": ("vip_trades.csv", csv_bytes, "text/csv")}, timeout=30)
        except Exception as e:
            print(f"  [VIP EXPIRE] csv {cid}: {e}")

def _check_vip_expiries():
    """Runs hourly. The day after vip_end, sends a renew-or-be-removed reminder and
    starts a 24h grace clock — if still expired 24h later, actually downgrades and
    kicks them from any VIP channels. Renewing (a new /setvip) resets vip_end into
    the future, so this loop naturally stops flagging them once they're no longer expired."""
    today = (datetime.now(timezone.utc) + IST).date()
    for cid, user in list(ct._db.items()):
        if user.get("tier") != "vip" or not user.get("vip_end"):
            continue
        try:
            d, m, y = user["vip_end"].split(".")
            end_date = datetime(int(y), int(m), int(d)).date()
        except Exception:
            continue
        if today <= end_date:
            continue
        notified_at = user.get("vip_grace_notified_at")
        if not notified_at:
            user["vip_grace_notified_at"] = time.time()
            ct._set(cid, user)
            _send_vip_renew_reminder(cid, user)
        elif time.time() - notified_at >= 86400:
            _expire_vip_user(cid, user)

def send_to_user(chat_id, text, file_id=None, file_type=None):
    try:
        base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
        if file_type == "photo":
            r = requests.post(f"{base}/sendPhoto",
                json={"chat_id": chat_id, "photo": file_id, "caption": text, "parse_mode": "HTML"}, timeout=15)
        elif file_type == "document":
            r = requests.post(f"{base}/sendDocument",
                json={"chat_id": chat_id, "document": file_id, "caption": text, "parse_mode": "HTML"}, timeout=15)
        else:
            r = requests.post(f"{base}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                      "disable_web_page_preview": True}, timeout=10)
        _cid_int = int(chat_id)
        if r.status_code == 403 and "blocked" in r.text.lower():
            if _cid_int not in blocked_users:
                blocked_users.add(_cid_int); save_users()
        elif r.status_code == 200 and _cid_int in blocked_users:
            blocked_users.discard(_cid_int); save_users()  # they unblocked us
        return r.status_code == 200
    except Exception as e: print(f"  [USER SEND] {chat_id}: {e}"); return False

def do_broadcast(admin_chat_id, text, file_id=None, file_type=None, mode="all"):
    if mode == "users":
        targets = [u for u in registered_users if u not in blocked_users]
    elif mode == "channels":
        _ch2 = os.getenv("TELEGRAM_CHANNEL_ID_2", "")
        targets = [TELEGRAM_CHANNEL_ID] + ([_ch2] if _ch2 else [])
    else:
        targets = [u for u in registered_users if u not in blocked_users] + [TELEGRAM_CHANNEL_ID]
    ok = 0; fail = 0
    for cid in targets:
        if send_to_user(cid, text, file_id, file_type): ok += 1
        else: fail += 1
        time.sleep(0.05)
    send_reply(admin_chat_id, f"<b>Broadcast Done</b>\n{ok} delivered | {fail} failed\n\n<i>🛡️ Capital protected</i>")

# --- MESSAGE FORMATS ----------------------------------------------------------
def fmt_signal(s):
    e   = "🟢" if s["signal"]=="BUY" else "🔴"
    arr = "📈" if s["signal"]=="BUY" else "📉"
    ci  = {"HIGH":"🔥 HIGH","MEDIUM":"⚡ MED","LOW":"🌀 LOW"}.get(s.get("confidence",""),"")
    el  = f"🎯 Entry:  <b>{s['entry']:,.0f}</b>"
    if s.get("entry_type")=="PULLBACK" and s.get("entry_note"):
        el += f"\n   <i>{s['entry_note']}</i>"
    wk = s.get("weekly_trend",""); s4h = s.get("structure_4h","")
    ez = s.get("entry_zone","");   rs  = s.get("reasoning","")
    src = s.get("data_source", get_current_source()); mode = s.get("prompt_mode","?")
    return (f"{e} <b>{s['signal']} - {SYMBOL}</b>  {arr}  {ci}\n"
        f"🕐 {ist_str()}  |  🌍 {s.get('session',get_session())}\n\n"
        f"{el}\n"
        f"🛡️ SL       <b>{s['sl']:,.0f}</b>\n"
        f"💰 TP1     <b>{s['tp1']:,.0f}</b>\n"
        f"🏆 TP2     <b>{s['tp2']:,.0f}</b>\n"
        f"⚖️ R:R     <b>{s.get('rr','-')}</b>\n\n"
        + (f"🌐 Weekly: <i>{wk}</i>\n" if wk else "")
        + (f"📊 4H:     <i>{s4h}</i>\n" if s4h else "")
        + (f"📍 Zone:   <i>{ez}</i>\n"  if ez else "")
        + f"\n✨ <i>🛡️ Capital protected</i>")

def fmt_update(status, price=None):
    t = active_trade; entry = t.get("entry") or 0
    msgs = {
        "SL_HIT":         (
            f"🚨 <b>TRADE CLOSED — SL HIT</b> 🚨\n\n"
            f"❌ Loss taken on {t.get('signal','?')} @ {t.get('entry',0):,.0f}\n\n"
            f"💀 <i>MAA CHUD GYI TRADE KI TOH SHITT YRR</i> 😭\n\n"
            f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
            f"🔍 Waiting for next valid setup...\n\n"
            f"<i>🛡️ Capital protected</i>"
        ),
        "TP1_HIT":        (
            f"💰 <b>TP1 HIT — 50% CLOSED!</b> 🎉\n\n"
            f"🎊 <i>MAJA AAGYA BHAI YAYY!!!!</i>\n\n"
            f"✅ Half position closed at <b>{t.get('tp1',0):,.0f}</b> — profit secured!\n"
            f"🛡️ SL moved to Breakeven: <b>{entry:,.0f}</b>\n"
            f"🚀 Remaining 50% riding to TP2: <b>{t.get('tp2',0):,.0f}</b>\n\n"
            f"⚠️ <b>Do NOT close manually — bot is managing the rest</b>"
        ),
        "TP2_HIT":        (
            f"🏆 <b>TRADE CLOSED — TP2 HIT!</b> 🎊💵\n\n"
            f"🎊 <i>MAJA AAGYA BHAI YAYY!!!!</i>\n\n"
            f"✅ Full profit taken on {t.get('signal','?')} @ {t.get('tp2',0):,.0f}\n\n"
            f"⛔ <b>This is NOT a new signal — trade is fully closed</b>\n"
            f"🔍 Waiting for next valid setup..."
        ),
        "STOP_HUNT":      (
            f"🎣 <b>STOP HUNT DETECTED</b>\n\n"
            f"Price spiked below SL and closed back above.\n"
            f"✅ Still in {t.get('signal','?')} trade — position held.\n\n"
            f"⚠️ <b>No action needed — bot is managing this</b>"
        ),
        "SETUP_INVALID":  (
            f"⚠️ <b>TRADE CANCELLED — Setup Invalid</b>\n\n"
            f"Price closed past SL before entry was hit.\n"
            f"No position was opened.\n\n"
            f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
            f"⛔ <b>This is NOT a new signal</b>\n\n"
            f"🔍 Waiting for next valid setup..."
        ),
        "ENTRY_MISSED":   (
            f"😔 <b>TRADE CANCELLED — Entry Missed</b>\n\n"
            f"Price moved past entry zone <b>{entry:,.0f}</b> without filling.\n"
            f"No position was opened.\n\n"
            f"⛔ <b>DO NOT CHASE — do not open a trade now</b>\n"
            f"⛔ <b>This is NOT a new signal</b>\n\n"
            f"🔍 Waiting for next valid setup..."
        ),
        "STRUCTURE_FLIP": (
            f"🔄 <b>TRADE CLOSED — Structure Flipped</b>\n\n"
            f"Market structure changed — current {t.get('signal','?')} trade closed.\n\n"
            f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
            f"⛔ <b>Wait for the next signal from CLEXER</b>\n\n"
            f"🔍 Analysing new direction..."
        ),
        "WAITING_ENTRY":  (
            f"⏳ <b>Waiting Pullback</b>\n"
            f"🎯 Entry: <b>{entry:,.0f}</b>\n"
            f"🛑 SL:    <b>{t.get('sl',0):,.0f}</b>\n"
            f"🎯 TP1:   <b>{t.get('tp1',0):,.0f}</b>\n"
            f"🎯 TP2:   <b>{t.get('tp2',0):,.0f}</b>\n"
            + (f"📊 Current: <b>{price:,.0f}</b> ({abs((price or 0)-entry):,.0f} pts away)" if price else "")
        ),
    }
    return f"📣 <b>{SYMBOL} UPDATE</b>  🕐 {ist_str()}\n\n{msgs.get(status,'✅ Trade running')}\n\n✨ <i>🛡️ Capital protected</i>"

# --- TICK CHECK ---------------------------------------------------------------
def run_tick_check():
    if not active_trade["signal"]: return False
    try:
        ticker = get_ticker(); price = ticker["price"]
        t = active_trade; sig = t["signal"]; entry = t["entry"]; sl = t["sl"]; tp1 = t["tp1"]; tp2 = t["tp2"]

        # Only use candles since entry — pre-entry wicks must not trigger SL/TP
        # If entry was within the last 3 minutes, only use current price (no candle history)
        entry_ts  = t.get("entry_time", 0)
        mins_since_entry = (time.time() - entry_ts) / 60 if entry_ts else 999
        if mins_since_entry >= 1:
            candle_high, candle_low = get_recent_range(3)
            check_high = max(price, candle_high) if candle_high else price
            check_low  = min(price, candle_low)  if candle_low  else price
        else:
            # Too soon after entry — use live price only, not candle lows
            check_high = price
            check_low  = price

        if not t["entry_hit"]:
            tol = abs(entry-sl)*0.25
            # Use candle range so a spike that touched entry and reversed is caught
            entry_touched = (sig=="BUY"  and check_low  <= entry+tol) or \
                            (sig=="SELL" and check_high >= entry-tol)
            if entry_touched:
                active_trade["entry_hit"] = True
                save_active_trade()
                ct.on_entry_hit(entry, sl, tp2)
                send_telegram(
                    f"🚀 <b>ENTRY TRIGGERED!</b>  🕐 {ist_str()}\n\n"
                    f"{'🟢' if sig=='BUY' else '🔴'} <b>{sig} — {SYMBOL}</b>\n\n"
                    f"🎯 Entry:  <b>{entry:,.0f}</b>  |  📊 Price: <b>{price:,.2f}</b>\n"
                    f"🛡️ SL:     <b>{sl:,.0f}</b>  ({abs(price-sl):.0f} pts)\n"
                    f"💰 TP1:   <b>{tp1:,.0f}</b>\n"
                    f"🏆 TP2:   <b>{tp2:,.0f}</b>\n\n"
                    f"⚠️ <b>Trade is now LIVE — SL and TP active</b>\n\n"
                    f"✨ <i>🛡️ Capital protected</i>")
            return False

        _apply_trail_sl_btc(price)
        sl = active_trade["sl"]

        # TP2 — use candle high/low to catch spike
        tp2_hit = (sig=="BUY" and check_high >= tp2) or (sig=="SELL" and check_low <= tp2)
        if tp2_hit:
            trade_stats["total_tp2"] += 1; trade_stats["consecutive_sl"] = 0
            log_trade_outcome("TP2_HIT", f"closed at {tp2:,.0f}")
            send_telegram(f"🏆 <b>TP2 HIT!</b> 🎊💵  🕐 {ist_str()}\n\n"
                f"{'🟢' if sig=='BUY' else '🔴'} {sig} {SYMBOL}\n"
                f"🎯 Entry: {entry:,.0f} ✅ TP2: <b>{tp2:,.0f}</b>\n\n✨ <i>🛡️ Capital protected</i>")
            ct.on_tp2(entry, tp2); reset_trade(); return True

        # TP1 — use candle high/low
        if not t["tp1_hit"]:
            tp1_hit = (sig=="BUY" and check_high >= tp1) or (sig=="SELL" and check_low <= tp1)
            if tp1_hit:
                active_trade["tp1_hit"] = True; active_trade["sl"] = entry
                trade_stats["total_tp1"] += 1; trade_stats["consecutive_sl"] = 0
                save_active_trade()
                ct.on_tp1(entry, tp1)
                send_telegram(f"💰 <b>TP1 HIT!</b> 🎉  🕐 {ist_str()}\n\n"
                    f"{'🟢' if sig=='BUY' else '🔴'} {sig} {SYMBOL}\n"
                    f"✅ TP1: <b>{tp1:,.0f}</b>\n🛡️ SL moved to BE: <b>{entry:,.0f}</b>\n"
                    f"🚀 Riding TP2: <b>{tp2:,.0f}</b>...\n\n✨ <i>🛡️ Capital protected</i>")

        # SL — use candle low/high to catch wick SL hits
        sl_margin = 80
        sl_hit = (sig=="BUY"  and check_low  < sl - sl_margin) or \
                 (sig=="SELL" and check_high > sl + sl_margin)
        if sl_hit:
            trade_stats["total_sl"] += 1; trade_stats["consecutive_sl"] += 1
            n = trade_stats["consecutive_sl"]
            log_trade_outcome("SL_HIT", f"{n} in a row, low:{check_low:,.0f} sl:{sl:,.0f}")
            # Suppress ch2 if SL hit within 10 min of entry (stop hunt / quick SL)
            _entry_ts = t.get("entry_time", 0)
            _sl_in_ch2 = (time.time() - _entry_ts) > 600
            if n >= 3:
                trade_stats["cooldown_scans"] = 2
                send_telegram(
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {sig} @ {entry:,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 2 scans...\n\n<i>🛡️ Capital protected</i>", include_ch2=_sl_in_ch2)
            elif n == 2:
                trade_stats["cooldown_scans"] = 1
                send_telegram(
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {sig} @ {entry:,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 1 scan...\n\n<i>🛡️ Capital protected</i>", include_ch2=_sl_in_ch2)
            else:
                send_telegram(fmt_update("SL_HIT"), include_ch2=_sl_in_ch2)
            ct.on_sl(entry, sl); reset_trade(); return True
    except Exception as e: print(f"  [TICK ERROR] {e}")
    return False

# --- SCAN COIN MONITORING -----------------------------------------------------

# _tv_chart_lock is declared in bot_p1.py — used here to hold the TV chart during full scan sequence

def bingx_klines(symbol: str, interval: str, limit: int):
    """Module-level BingX klines fetch returning a pandas DataFrame or None."""
    try:
        import pandas as _pd
        kr = requests.get(
            "https://open-api.bingx.com/openApi/swap/v2/quote/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=12).json()
        rows = kr.get("data", [])
        if not rows: return None
        if isinstance(rows[0], (list, tuple)):
            df = _pd.DataFrame([[float(x) for x in row[:6]] for row in rows],
                               columns=["time","open","high","low","close","volume"])
        else:
            df = _pd.DataFrame([{
                "time": float(row.get("time",0) or row.get("openTime",0)),
                "open": float(row.get("open",0)), "high": float(row.get("high",0)),
                "low":  float(row.get("low",0)),  "close": float(row.get("close",0)),
                "volume": float(row.get("volume",0)),
            } for row in rows])
        return df if len(df) > 0 else None
    except Exception as e:
        print(f"  [BINGX KLINES] {symbol} {interval}: {e}")
        return None

def _scan_list(ver: int) -> list:
    """Return the active trades list for scan version 1 or 2."""
    return scan1_trades if ver == 1 else scan2_trades

def _all_active_scan_syms() -> set:
    """Return set of all symbols currently active across both scan lists."""
    return {t["symbol"] for t in scan1_trades + scan2_trades if t.get("symbol")}

def _log_scan_history(t: dict, result: str, close_price: float):
    """Append closed scan trade to scan_history (max 30)."""
    scan_history.append({
        "time":        ist_str(),
        "symbol":      t.get("symbol", "?"),
        "signal":      t.get("signal", "?"),
        "entry":       t.get("entry", 0),
        "sl":          t.get("sl", 0),
        "tp1":         t.get("tp1", 0),
        "tp2":         t.get("tp2", 0),
        "result":      result,          # TP1 / TP2 / SL / BE
        "close_price": close_price,
        "tp1_hit":     t.get("tp1_hit", False),
        "ver":         t.get("ver", 1),
    })
    if len(scan_history) > 30: scan_history.pop(0)
    save_state()

def _remove_scan_trade(ver: int, symbol: str):
    """Remove a specific symbol from a scan list and save state."""
    lst = _scan_list(ver)
    lst[:] = [t for t in lst if t.get("symbol") != symbol]
    save_state()

def reset_scan_trade():
    scan1_trades.clear(); save_state()

def _get_slot(ver: int) -> dict:
    """Return first active trade dict for a scan version, or empty dict."""
    lst = _scan_list(ver)
    return lst[0] if lst else {}

def get_bingx_price(symbol: str) -> float:
    try:
        r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
                         params={"symbol": symbol}, timeout=8).json()
        d = r.get("data", {})
        if isinstance(d, list): d = d[0] if d else {}
        p = float(d.get("lastPrice") or d.get("price") or 0)
        return p
    except Exception as e:
        print(f"  [SCAN PRICE] {symbol}: {e}")
        return 0.0

def fmt_scan_signal(t: dict) -> str:
    sym  = t["symbol"]; sig = t["signal"]
    entry = t["entry"]; sl = t["sl"]; tp1 = t["tp1"]; tp2 = t["tp2"]
    et   = t.get("entry_type","MARKET")
    ver  = t.get("ver", 1)
    sl_pct = abs(entry - sl) / entry * 100 if entry else 0
    coin = sym.replace("-USDT","").replace("USDT","")

    if et == "ZONE" and t.get("zone_lo") and t.get("zone_hi"):
        zone_lo, zone_hi = t["zone_lo"], t["zone_hi"]
        dir_lbl = "📉 Short Entry Zone" if sig == "SELL" else "📈 Long Entry Zone"
        sig_id = f"#ID{int(t.get('created_at', time.time()))}"
        return (
            f"📩 <b>#{coin}USDT</b>  Scan{ver} | Mid-Term\n\n"
            f"{dir_lbl}: <b>{min(zone_lo,zone_hi):,.4g} - {max(zone_lo,zone_hi):,.4g}</b>\n\n"
            f"⏳ Signal Details:\n"
            f"Target 1: <b>{tp1:,.4g}</b>\n"
            f"Target 2: <b>{tp2:,.4g}</b>\n\n"
            f"🔺 Stop-Loss: <b>{sl:,.4g}</b>\n"
            f"💡 After reaching the first target you can put the rest of the position to breakeven.\n\n"
            f"🔎 Signal ID: <i>{sig_id}</i>\n\n"
            f"✨ <i>🛡️ Capital protected</i>"
        )

    arrow = "🟢 LONG" if sig == "BUY" else "🔴 SHORT"
    return (
        f"<b>📣 #{coin}-USDT</b>\n"
        f"<b>{'─'*22}</b>\n\n"
        f" SCAN SIGNAL  |  <b>Scan{ver}</b>\n"
        f"  🕐 {ist_str()}\n\n"
        f"{arrow} — <b>MARKET ENTRY</b>\n\n"
        f"🎯 Entry: <b>{entry:,.4g}</b>\n"
        f"🛑 SL:    <b>{sl:,.4g}</b>  ({sl_pct:.1f}%)\n"
        f"💰 TP1:  <b>{tp1:,.4g}</b>\n"
        f"🏆 TP2:  <b>{tp2:,.4g}</b>\n\n"
        f"✨ <i>🛡️ Capital protected</i>"
    )

def fmt_scan_update(status: str, price: float = 0, t: dict = None) -> str:
    if t is None: t = scan_active_trade
    sym  = f"#{t.get('symbol','?')}"; sig = t.get("signal","?")
    ver_lbl = f"Scan{t.get('ver', 1)}"
    entry = t.get("entry") or 0; tp1 = t.get("tp1",0); tp2 = t.get("tp2",0)
    msgs = {
        "ENTRY_HIT": (
            f"🚀 <b>ENTRY TRIGGERED — {sym}</b>  |  <b>{ver_lbl}</b>  🕐 {ist_str()}\n\n"
            f"{'🟢' if sig=='BUY' else '🔴'} <b>{sig}</b>\n"
            f"🎯 Entry: <b>{entry:,.4g}</b>  |  📊 Price: <b>{price:,.4g}</b>\n"
            f"🛑 SL:    <b>{t.get('sl',0):,.4g}</b>\n"
            f"💰 TP1:  <b>{tp1:,.4g}</b>\n"
            f"🏆 TP2:  <b>{tp2:,.4g}</b>\n\n"
            f"⚠️ <b>Trade is now LIVE</b>\n\n✨ <i>🛡️ Capital protected</i>"
        ),
        "TP1_HIT": (
            f"💰 <b>TP1 HIT — {sym}!</b> 🎉  |  <b>{ver_lbl}</b>  🕐 {ist_str()}\n\n"
            f"🎊 <i>MAJA AAGYA BHAI YAYY!!!!</i>\n\n"
            f"{'🟢' if sig=='BUY' else '🔴'} {sig}\n"
            f"✅ TP1: <b>{tp1:,.4g}</b>\n"
            f"🛡️ SL moved to BE: <b>{entry:,.4g}</b>\n"
            f"🚀 Riding TP2: <b>{tp2:,.4g}</b>...\n\n✨ <i>🛡️ Capital protected</i>"
        ),
        "TP2_HIT": (
            f"🏆 <b>TP2 HIT — {sym}!</b> 🎊💵  |  <b>{ver_lbl}</b>  🕐 {ist_str()}\n\n"
            f"🎊 <i>MAJA AAGYA BHAI YAYY!!!!</i>\n\n"
            f"{'🟢' if sig=='BUY' else '🔴'} {sig}\n"
            f"✅ Full profit @ TP2: <b>{tp2:,.4g}</b>\n\n✨ <i>🛡️ Capital protected</i>"
        ),
        "SL_HIT": (
            (
                f"🛡️ <b>BE EXIT — {sym}</b>  |  <b>{ver_lbl}</b>  🕐 {ist_str()}\n\n"
                f"{'🟢' if sig=='BUY' else '🔴'} {sig}\n"
                f"✅ TP1 already hit — closed at entry <b>{entry:,.4g}</b>\n"
                f"📊 Result: <b>Breakeven</b> (no loss)\n\n"
                f"🔍 Waiting for next scan signal...\n\n✨ <i>🛡️ Capital protected</i>"
            ) if t.get("tp1_hit") else (
                f"🚨 <b>SL HIT — {sym}</b> 🚨  |  <b>{ver_lbl}</b>  🕐 {ist_str()}\n\n"
                f"💀 <i>MAA CHUD GYI TRADE KI TOH SHITT YRR</i> 😭\n\n"
                f"❌ Loss on {sig} @ {entry:,.4g}\n\n"
                f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                f"🔍 Waiting for next scan signal...\n\n✨ <i>🛡️ Capital protected</i>"
            )
        ),
        "ENTRY_MISSED": (
            f"😔 <b>ENTRY MISSED — {sym}</b>  |  <b>{ver_lbl}</b>  🕐 {ist_str()}\n\n"
            f"Price bypassed entry zone <b>{entry:,.4g}</b> without filling.\n"
            f"⛔ <b>DO NOT CHASE</b>\n\n✨ <i>🛡️ Capital protected</i>"
        ),
        "TIMEOUT": (
            f"⏰ <b>TIMEOUT — {sym}</b>  |  <b>{ver_lbl}</b>  🕐 {ist_str()}\n\n"
            f"{'🟢' if sig=='BUY' else '🔴'} {sig} still running after 12 hours — force-closed.\n"
            f"📊 Result: <b>{t.get('_timeout_pnl', '?')}</b>\n\n"
            f"🔍 Waiting for next scan signal...\n\n✨ <i>🛡️ Capital protected</i>"
        ),
        "WAITING_ENTRY": (
            f"⏳ <b>Waiting Entry — {sym}</b>  |  <b>{ver_lbl}</b>\n"
            f"🎯 Entry: <b>{entry:,.4g}</b>\n"
            f"🛑 SL:    <b>{t.get('sl',0):,.4g}</b>\n"
            f"💰 TP1:  <b>{tp1:,.4g}</b>\n"
            f"🏆 TP2:  <b>{tp2:,.4g}</b>\n"
            + (f"📊 Current: <b>{price:,.4g}</b> ({abs(price-entry)/entry*100:.2f}% away)" if price else "")
        ),
    }
    return msgs.get(status, f"✅ {sym} trade running")

def _wick_check_since_entry(sym: str, created_at: float):
    """Re-verify against 15m candles from the trade's entry time (rounded down
    to the nearest :00/:15/:30/:45) through now. Catches spikes a narrow
    1m/live-price window could've missed — e.g. after a bot restart or a gap
    between tick checks. Returns (high, low) or (None, None) if unavailable."""
    if not created_at:
        return None, None
    try:
        entry_dt = datetime.fromtimestamp(created_at, timezone.utc).replace(tzinfo=None) + IST
        rounded_min = (entry_dt.minute // 15) * 15
        start_dt = entry_dt.replace(minute=rounded_min, second=0, microsecond=0)
        elapsed_min = (now_ist() - start_dt).total_seconds() / 60
        n_candles = min(max(1, int(elapsed_min // 15) + 2), 100)
        df15 = bingx_klines(sym, "15m", n_candles)
        if df15 is not None and len(df15) > 0:
            return float(df15["high"].max()), float(df15["low"].min())
    except Exception as e:
        print(f"  [WICK CHECK] {sym} error: {e}")
    return None, None

def _tick_one(ver: int, t: dict) -> bool:
    """Tick check for one trade dict. Returns True if trade closed."""
    sym = t["symbol"]; sig = t["signal"]
    entry = t["entry"]; sl = t["sl"]; tp1 = t["tp1"]; tp2 = t["tp2"]
    try:
        _created_at = t.get("created_at")
        _age_hours  = (time.time() - _created_at) / 3600 if _created_at else 0

        # Hard cutoff — force-close any trade still running after 12 hours
        if _age_hours >= 12:
            price = get_bingx_price(sym)
            pnl = (price - entry) / entry * 100 * (1 if sig == "BUY" else -1) if price and entry else 0
            t["_timeout_pnl"] = f"{pnl:+.2f}%"
            _log_scan_history(t, f"TIMEOUT({pnl:+.2f}%)", price)
            send_telegram(fmt_scan_update("TIMEOUT", price, t))
            ct.on_scan_sl(sym)
            log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
                "timeout_time": _ist_str_now(), "result": f"TIMEOUT({pnl:+.2f}%)",
                "entry_price": entry, "sl_price": t.get("sl",0)})
            _remove_scan_trade(ver, sym); return True

        price = get_bingx_price(sym)
        if price <= 0: return False
        df1m = bingx_klines(sym, "1m", 3)
        if df1m is not None and len(df1m) > 0:
            check_high = max(price, float(df1m["high"].max()))
            check_low  = min(price, float(df1m["low"].min()))
        else:
            check_high = price; check_low = price

        # Comprehensive since-entry wick re-check — only kicks in once the trade
        # has been running 6+ hours, then re-runs at most every 4 hours after
        # that (not every tick) to avoid hammering the API for long-running trades.
        if _age_hours >= 6:
            _last_wick = t.get("last_wick_check", 0)
            if time.time() - _last_wick >= 4 * 3600:
                t["last_wick_check"] = time.time()
                _wick_high, _wick_low = _wick_check_since_entry(sym, _created_at)
                if _wick_high is not None:
                    check_high = max(check_high, _wick_high)
                    check_low  = min(check_low, _wick_low)
        print(f"  [SCAN{ver} {sym}] {sig} price:{price:.4g} H:{check_high:.4g} L:{check_low:.4g}")

        _apply_trail_sl(ver, t, price)
        sl = t["sl"]

        # All entries are MARKET — entry_hit is always True from creation.
        # Nothing to wait for. SL/TP monitoring starts immediately.
        if not t["entry_hit"]:
            # Shouldn't happen for MARKET trades, but safety fallback
            t["entry_hit"] = True
            send_telegram(fmt_scan_update("ENTRY_HIT", price, t))

        tp2_hit = (sig == "BUY" and check_high >= tp2) or (sig == "SELL" and check_low <= tp2)
        if tp2_hit:
            trade_stats["scan_tp2"] += 1; trade_stats["scan_tp1"] += (0 if t["tp1_hit"] else 1)
            trade_stats[f"scan{ver}_tp2"] += 1; trade_stats[f"scan{ver}_tp1"] += (0 if t["tp1_hit"] else 1)
            _log_scan_history(t, "TP2", price)
            send_telegram(fmt_scan_update("TP2_HIT", price, t))
            ct.on_scan_tp2(sym)
            log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
                "tp2_hit_time": _ist_str_now(), "result": "TP2",
                "entry_price": entry, "sl_price": t.get("sl",0), "tp2_price": tp2})
            _remove_scan_trade(ver, sym); return True

        if not t["tp1_hit"]:
            # Use current mark price only (not wick) — prevents false triggers from brief spikes
            tp1_hit = (sig == "BUY" and price >= tp1) or (sig == "SELL" and price <= tp1)
            if tp1_hit:
                t["tp1_hit"] = True
                t["sl"] = entry
                sl = entry
                trade_stats["scan_tp1"] += 1
                trade_stats[f"scan{ver}_tp1"] += 1
                send_telegram(fmt_scan_update("TP1_HIT", price, t))
                ct.on_scan_tp1(sym)
                log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
                    "tp1_hit_time": _ist_str_now(), "result": "TP1_partial",
                    "entry_price": entry, "sl_price": entry, "tp1_price": tp1})

        sl_margin = sl * 0.002
        sl_hit = (sig == "BUY"  and check_low  < sl - sl_margin) or \
                 (sig == "SELL" and check_high > sl + sl_margin)
        if sl_hit:
            trade_stats["scan_sl"] += 1
            trade_stats[f"scan{ver}_sl"] += 1
            result = "BE" if t["tp1_hit"] else "SL"
            _log_scan_history(t, result, price)
            send_telegram(fmt_scan_update("SL_HIT", price, t))
            ct.on_scan_sl(sym)
            log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
                "sl_hit_time": _ist_str_now(), "result": result,
                "entry_price": entry, "sl_price": t.get("sl",0)})
            _remove_scan_trade(ver, sym); return True

    except Exception as e:
        print(f"  [SCAN{ver} {sym} TICK ERROR] {e}")
    return False

def run_scan_tick_check() -> bool:
    any_closed = False
    for t in list(scan1_trades): any_closed |= _tick_one(1, t)
    for t in list(scan2_trades): any_closed |= _tick_one(2, t)
    return any_closed

# --- 1-HOUR PRICE CHECK -------------------------------------------------------
def run_price_check():
    if not active_trade["signal"]: return False
    try:
        ticker = get_ticker(); price = ticker["price"]
        # Only check price range SINCE entry — pre-entry wicks must not trigger SL/TP
        entry_ts = active_trade.get("entry_time")
        range_1h = get_price_range_since(60, since_ts=entry_ts)
        high_1h = range_1h["high"] or price; low_1h = range_1h["low"] or price
        print(f"  [1H] cur:{price:,.2f} H:{high_1h:,.2f} L:{low_1h:,.2f}")
        df_5m = get_candles("5m", 50); df_4h = get_candles("4h", 10)
        if detect_entry_missed(price):
            trade_stats["missed_entries"] += 1
            log_trade_outcome("ENTRY_MISSED", f"price bypassed entry {active_trade['entry']:,.0f}")
            ct.on_cancel_limits()
            send_telegram(fmt_update("ENTRY_MISSED")); reset_trade(); return True
        if not active_trade["entry_hit"] and detect_entry_invalidated(price, df_4h):
            log_trade_outcome("SETUP_INVALID", "4H closed past SL before entry")
            ct.on_cancel_limits()
            send_telegram(fmt_update("SETUP_INVALID")); reset_trade(); return True
        status = check_price_status(price, high_1h, low_1h, df_5m)
        print(f"  [1H] {active_trade['signal']} | {status}")
        if status == "TP2_HIT":
            trade_stats["total_tp2"] += 1; trade_stats["consecutive_sl"] = 0
            log_trade_outcome("TP2_HIT", "hit during 1H check")
            ct.on_tp2(active_trade.get("entry",0), active_trade.get("tp2",0)); send_telegram(fmt_update("TP2_HIT")); reset_trade(); return True
        elif status == "SL_HIT":
            trade_stats["total_sl"] += 1; trade_stats["consecutive_sl"] += 1
            n = trade_stats["consecutive_sl"]
            log_trade_outcome("SL_HIT", f"{n} in a row during 1H check")
            if n >= 3:
                trade_stats["cooldown_scans"] = 2
                send_telegram(
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {active_trade.get('signal','?')} @ {active_trade.get('entry',0):,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 2 scans...\n\n<i>🛡️ Capital protected</i>")
            elif n == 2:
                trade_stats["cooldown_scans"] = 1
                send_telegram(
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {active_trade.get('signal','?')} @ {active_trade.get('entry',0):,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 1 scan...\n\n<i>🛡️ Capital protected</i>")
            else:
                send_telegram(fmt_update("SL_HIT"))
            ct.on_sl(active_trade.get("entry",0), active_trade.get("sl",0)); reset_trade(); return True
        elif status == "TP1_HIT" and not active_trade["tp1_hit"]:
            active_trade["tp1_hit"] = True; active_trade["sl"] = active_trade["entry"]
            trade_stats["total_tp1"] += 1; trade_stats["consecutive_sl"] = 0
            save_active_trade()
            ct.on_tp1(active_trade["entry"], active_trade.get("tp1",0))
            send_telegram(fmt_update("TP1_HIT"))
        elif status in ("STOP_HUNT",):      send_telegram(fmt_update("STOP_HUNT"))
        elif status in ("ENTRY_MISSED","SETUP_INVALID"):
            log_trade_outcome(status, ""); ct.on_cancel_limits()
            send_telegram(fmt_update(status)); reset_trade(); return True
        elif status == "WAITING_ENTRY":
            active_trade["scan_count"] += 1; send_telegram(fmt_update("WAITING_ENTRY", price))
        elif status == "RUNNING":
            active_trade["scan_count"] += 1  # trade running, no message needed
    except Exception as e: print(f"  [1H ERROR] {e}")
    return False

# --- NEWS ---------------------------------------------------------------------
def get_article_image(entry):
    for field in ("media_content","media_thumbnail"):
        items = getattr(entry, field, []) or entry.get(field, [])
        if items:
            url = items[0].get("url","") if isinstance(items[0],dict) else ""
            if url.startswith("http"):
                try:
                    r = requests.get(url, timeout=8, headers={"User-Agent":"Mozilla/5.0"})
                    if r.status_code==200 and len(r.content)>2000: return r.content
                except: pass
    link = entry.get("link","")
    if not link: return None
    try:
        r = requests.get(link, timeout=8, headers={"User-Agent":"Mozilla/5.0 Chrome/120"})
        m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', r.text)
        if not m: m = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', r.text)
        if m:
            r2 = requests.get(m.group(1), timeout=8)
            if r2.status_code==200: return r2.content
    except: pass
    return None

def check_news(force=False):
    global latest_news_context
    if not SEND_NEWS and not force: return
    if not HAS_FEEDPARSER: return
    candidates = []; btc_kw = ["bitcoin","btc","crypto","fed","interest rate","sec","etf","regulation",
        "whale","halving","rally","crash","bull","bear","hack","cpi","bank","blackrock","coinbase","binance"]
    for src in NEWS_SOURCES:
        try:
            feed = feedparser.parse(src["url"]); added = 0
            for entry in feed.entries:
                title = (entry.get("title") or "").strip(); link = entry.get("link","")
                guid = entry.get("id", link or title)
                if not title or guid in posted_news_guids: continue
                pub = entry.get("published_parsed") or entry.get("updated_parsed")
                if pub and (time.time()-time.mktime(pub))/3600 > MAX_NEWS_AGE: continue
                raw_sum = entry.get("summary") or entry.get("description") or ""
                summary = re.sub(r"<[^>]+>","",raw_sum)[:400]
                if not any(kw in (title+" "+summary).lower() for kw in btc_kw): continue
                candidates.append({"title":title,"link":link,"guid":guid,"summary":summary,"source":src["name"],"entry":entry}); added+=1
        except Exception as e: print(f"    {src['name']}: {e}")
    if not candidates: return
    try: btc_price = get_ticker()["price"]
    except: btc_price = 0
    to_post = []
    for i in range(0, len(candidates), 10):
        batch = candidates[i:i+10]
        news_block = "\n\n".join(f"[{j}] {e['source']}\nTITLE: {e['title']}\nSUMMARY: {e['summary'][:200]}" for j,e in enumerate(batch))
        try:
            resp = _claude_client().messages.create(
                model="claude-haiku-4-5-20251001", max_tokens=600,
                messages=[{"role":"user","content":f"BTC: ${btc_price:,.0f}\n{news_block}\n\nReturn JSON array HIGH/MEDIUM impact only. Fields: index,impact(BULLISH/BEARISH/NEUTRAL),strength(HIGH/MEDIUM),reason. Empty [] if none. JSON only."}])
            _log_api_usage("news", "claude-haiku-4-5-20251001",
                           resp.usage.input_tokens, resp.usage.output_tokens)
            analyzed = json.loads(_claude_text(resp).replace("```json","").replace("```","").strip())
            for item in analyzed:
                idx = item.get("index",-1)
                if 0 <= idx < len(batch):
                    batch[idx].update({"impact":item.get("impact","NEUTRAL"),"strength":item.get("strength","LOW"),"reason":item.get("reason","")})
                    to_post.append(batch[idx])
        except Exception as e: print(f"  [NEWS CLAUDE] {e}")
    if not to_post: return
    to_post.sort(key=lambda x: 0 if x.get("strength")=="HIGH" else 1)
    latest_news_context = [f"• {e.get('impact','?')} ({e.get('strength','?')}): {e['title'][:80]} - {e.get('reason','')[:80]}" for e in to_post[:3]]
    for item in to_post[:MAX_NEWS_PER_RUN]:
        impact = item.get("impact","NEUTRAL"); emoji = "🟢" if impact=="BULLISH" else ("🔴" if impact=="BEARISH" else "⚪")
        msg_text = (f"<b>MARKET NEWS</b>\n\n{emoji} <b>{impact}</b> for BTC\n"
            f"<b>{item['title'][:120]}</b>\n\n<i>{item.get('reason','')}</i>\n\n"
            f"{item['source']}\n<a href='{item['link']}'>Read article</a>\n\n<i>🛡️ Capital protected · {ist_str()}</i>")
        try:
            img_bytes = get_article_image(item["entry"])
            if img_bytes and len(img_bytes)>2000:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                    data={"chat_id":TELEGRAM_CHANNEL_ID,"caption":msg_text,"parse_mode":"HTML"},
                    files={"photo":("news.jpg",img_bytes,"image/jpeg")}, timeout=20)
            else: send_telegram(msg_text)
            posted_news_guids.add(item["guid"])
            if len(posted_news_guids)>1000:
                old = list(posted_news_guids)[:200]
                for g in old: posted_news_guids.discard(g)
            time.sleep(1)
        except Exception as e: print(f"  [NEWS POST] {e}")

# --- /tvstatus ----------------------------------------------------------------
def cmd_tvstatus(chat_id):
    if not TV_BRIDGE_URL:
        send_reply(chat_id, "<b>TV Status</b>\n\nTV_BRIDGE_URL not set.\nRunning on <b>Binance</b>.\n\n<i>🛡️ Capital protected</i>"); return
    send_reply(chat_id, f"Checking...\n<code>{TV_BRIDGE_URL}</code>")
    now = time.time(); health = tv_ping()
    if not health:
        ls = tv_bridge_state.get("last_seen",0)
        since = f"{int((now-ls)//60)}m ago" if ls else "never"
        send_reply(chat_id, f"<b>TV Status</b>\n\n🔴 Bridge OFFLINE\nLast seen: {since}\n\nUsing: <b>Binance</b>\n\n<i>🛡️ Capital protected</i>"); return
    tv_bridge_state.update({"online":True,"last_seen":now,"cdp_ok":health.get("cdp_connected",False),
        "tv_version":health.get("tv_version",""),"cached_intervals":health.get("cached_intervals",[])})
    cdp_ok = health.get("cdp_connected",False); tv_version = health.get("tv_version","?")
    tv_symbol = health.get("symbol",SYMBOL); uptime = health.get("uptime_seconds",0)
    cached_ivs = health.get("cached_intervals",[])
    tk = tv_get_ticker(); price_ok = tk and tk.get("price",0)>0; price_val = tk["price"] if price_ok else 0
    df = tv_get_candles("1h",10); candles_ok = df is not None and len(df)>0
    def tick(ok): return "🟢" if ok else "🔴"
    overall = "<b>ALL SYSTEMS GO</b>" if (cdp_ok and price_ok and candles_ok) else "<b>PARTIAL</b>"
    uptime_str = f"{uptime//3600:.0f}h {(uptime%3600)//60:.0f}m" if uptime>=3600 else f"{uptime//60:.0f}m {uptime%60:.0f}s"
    send_reply(chat_id, f"<b>TV Status</b>\n\n{overall}\n\n"
        f"{tick(True)} Bridge reachable\n{tick(cdp_ok)} TradingView connected\n"
        f"{tick(price_ok)} Price feed" + (f" ({price_val:,.2f})" if price_ok else "") + "\n"
        f"{tick(candles_ok)} Candles" + (f" ({len(df)} bars 1H)" if candles_ok else "") + "\n\n"
        f"Cached: {', '.join(cached_ivs) if cached_ivs else 'none'}\n"
        f"TV: <code>{tv_version}</code> | Symbol: <code>{tv_symbol}</code>\n"
        f"Uptime: <b>{uptime_str}</b>\n\n{ist_str()}\n<i>🛡️ Capital protected</i>")

# --- COMMANDS -----------------------------------------------------------------
ADMIN_HELP = """<b>CLEXER V17.8.5 - Admin Commands</b>
--------------------

<b>BOT CONTROL</b>
/go - START scanning (required after deploy)
/pause - STOP scanning
/resume - Same as /go
/signal - Force scan now
/resetsl - Reset SL streak + cooldown
/setinterval 4 - Set scan interval (hours)
/btcmode on|off - Switch BTC prompt (V7 Classic / V9 Current)

<b>INFO</b>
/status - Bot status
/price - Live BTC price
/trade - Active trade
/history - Last 5 signals
/stats - Win/loss stats
/session - Current session
/tvstatus - TV connection
/force_reload - Clear TV bridge cache + 5min warmup (bridge only)

<b>TRADE CONTROL</b>
/close - Close BTC bot trade
/closetrade BTC - Close BTC for all copy users
/closetrade ETH - Close ETH for all copy users
/closetrade all - Close ALL positions (every coin)
/sltobe - SL to breakeven
/setsl 61500
/settp1 63000
/settp2 65000

<b>COIN RESEARCH &amp; SCAN</b>
/scan - Force-run Scan1 + Scan2 (best coins now)
/scan1 - Run Scan1 only (big movers)
/scan2 - Run Scan2 only (fresh momentum)
/closescan - Clear all active scan trades
/scantv on|off - Scan uses TV bridge (on) or BingX (off)
/coin ETHUSDT - Analyze any coin (Claude AI)
/compare - Run V9+B1 × BingX+TV side-by-side

<b>CHANNELS</b>
/channels - show status
/pausechannel 1 or 2
/resumechannel 1 or 2

<b>CHARTS (off by default)</b>
/images on|off
/setimages weekly,4h,1h,5m
/charts - Send all TF screenshots to DM
/chartson - Enable /charts feature
/chartsoff - Disable /charts (saves credits)

<b>NEWS (off by default)</b>
/news on|off
/latestnews

<b>MINI APP</b>
/miniapp pause [msg] - Put mini app in maintenance
/miniapp resume - Bring mini app back live

<b>COPY TRADE (ADMIN)</b>
/users - Copy trade users list
/allusers - Summary stats
/user ID - User detail + position
/kick ID - Remove user
/pauseuser ID - Pause/unpause user
/ctstatus - Failed copies + active signal
/ctretry ID - Retry for specific user
/ctclose - Close ALL copy positions
/ctclose ID - Close one user's position
/scancopy on|off - Enable/disable copy trade for /scan signals

/broadcast - Send to all
/help"""

FRIEND_HELP = """<b>CLEXER V17.8.5 Commands</b>
--------------------
/status - Bot status
/price - Live BTC price
/trade - Active trade
/history - Last 5 signals
/stats - Statistics
/session - Current session
/help - This menu

<b>COPY TRADE (BingX)</b>
/connect KEY SECRET - Link BingX
/disconnect - Remove keys
/copytrade on|off - Auto-copy
/setsize 50 - Margin per trade (USDT)
/setrisk 2 - Auto-leverage: max $2 loss per trade ⭐
/setleverage 10 - Manual leverage (overrides /setrisk)
/mytrade - Your open position
/mysize - Your settings
/myhistory - Trade history

<i>Note: 2 uses per command per hour</i>"""

FRIEND_COMMANDS = {"/start","/help","/status","/price","/trade","/history","/stats","/session"}

# False = scan uses BingX candles + matplotlib (default, no TV bridge needed)
# True  = scan uses TV bridge candles + TV screenshots (old behaviour)
SCAN_USE_TV = False

ADMIN_COMMANDS  = {"/go","/signal","/pause","/resume","/resetsl","/setinterval",
    "/close","/sltobe","/setsl","/settp1","/settp2","/tvstatus",
    "/broadcast","/users","/allusers","/user","/kick","/pauseuser",
    "/images","/setimages","/news","/latestnews",
    "/pausechannel","/resumechannel","/channels","/btcmode",
    "/scan","/scan1","/scan2","/scantoggle","/model","/gateway","/stop","/pause","/coin","/ctclose","/closetrade","/closescan","/scancopy","/readindicators","/checktvdata","/tvstudies","/calcstudies","/scantv",
    "/compare","/charts","/chartson","/chartsoff","/force_reload","/miniapp","/ctstatus","/ctretry","/btcanalysis","/demo","/synccheck","/report","/tradelog","/alt","/alt2","/altdemo","/adminlinks","/userstats","/aiconfig","/entrystyle","/coadmin","/tp1size","/freelimit","/channelmgmt","/trailsl"}

def handle_command(text, chat_id, message=None, sender_id=None):
    global SIGNAL_SCAN_INTERVAL, SEND_CHARTS, CHART_TFS, SEND_NEWS, last_force_scan_time, broadcast_pending, BTC_PROMPT_MODE, btc_analysis_enabled, ALT_SCAN_MINUTE, ALT_SCAN2_MINUTE, _auto_scan1_last_hour, _auto_scan2_last_hour, SCAN1_SCHEDULE, SCAN2_SCHEDULE, SCAN1_AUTO_ENABLED, SCAN2_AUTO_ENABLED, TEST_SCAN_ENABLED, SCAN_MODEL, USE_AEROLINK, SCAN1_TEST_SCHEDULE, CONTACT_ADMIN_ENABLED, SIGNAL_CHANNEL_ENABLED, SIGNAL_CHANNEL_LINK, FREE_SIGNAL_DAILY_LIMIT, CHANNELS
    _uname = (message or {}).get("from", {}).get("username")
    register_user(chat_id, _uname)
    parts = text.strip().split(); cmd = parts[0].lower().split("@")[0]
    # In groups, chat_id is the group — check sender_id for admin
    _check_id = sender_id if sender_id else chat_id
    is_admin = (str(_check_id)==str(ADMIN_CHAT_ID)) if ADMIN_CHAT_ID else True

    is_scanadmin = is_admin or (is_co_admin(_check_id) and cmd in _co_admin_allowed_commands())
    if cmd in ADMIN_COMMANDS and not is_scanadmin:
        send_reply(chat_id, "<b>Admin only.</b>\n\nUse /help to see your commands."); return

    if cmd == "/setvip" and is_admin and len(parts) < 2:
        send_vip_pick_screen(chat_id)
        return
    if cmd == "/setfree" and is_admin and len(parts) < 2:
        send_free_pick_screen(chat_id)
        return

    # -- Copy trade commands (user + admin) -----------------------------------
    if ct.is_ct_command(cmd, is_admin):
        uname = (message.get("from",{}).get("username","?") if message else "?")
        ct.handle(cmd, parts, chat_id, uname, send_reply, is_admin, scan_trades=scan1_trades+scan2_trades)
        return

    if cmd == "/synccheck" and is_admin:
        send_reply(chat_id, "🔍 Checking BingX vs bot state...")
        lines = ct.sync_check()
        # Parse __BTN__ markers into inline buttons
        text_lines = []; btn_rows = []
        for line in lines:
            if line.startswith("__BTN__"):
                row = []
                for item in line[7:].split("|"):
                    cb, *_ = item.split(":")
                    uid = item.split(":")[-1]
                    if cb.startswith("close_btc"):
                        row.append({"text": "❌ Close BTC", "callback_data": f"sync_close_btc:{uid}"})
                    elif cb.startswith("adopt_btc"):
                        row.append({"text": "✅ Adopt BTC", "callback_data": f"sync_adopt_btc:{uid}"})
                    elif cb.startswith("reset_ghost"):
                        row.append({"text": "🔄 Reset Ghost State", "callback_data": f"sync_reset_ghost:{uid}"})
                    elif cb.startswith("ctretry_"):
                        parts2 = cb.split("_"); sym = parts2[2] if len(parts2) > 2 else "?"
                        row.append({"text": f"✅ Adopt {sym}", "callback_data": f"sync_adopt_scan:{uid}:{sym}"})
                    elif cb.startswith("closescan_"):
                        sym = cb.replace("closescan_","")
                        row.append({"text": f"❌ Close {sym.replace('-USDT','')}", "callback_data": f"sync_close_scan:{uid}:{sym}"})
                if row: btn_rows.append(row)
            else:
                text_lines.append(line)
        markup = {"inline_keyboard": btn_rows} if btn_rows else None
        send_reply(chat_id, "<b>Sync Check Result</b>\n\n" + "\n".join(text_lines), reply_markup=markup)
        return

    if cmd in ("/start","/help"):
        _hm_from = (message or {}).get("from", {})
        _hm_uname = _hm_from.get("username") or _hm_from.get("first_name") or "there"
        _hm_uid = _hm_from.get("id", chat_id)
        send_help_menu(chat_id, is_admin, uname=_hm_uname, cid=_hm_uid)

    elif cmd in ("/go", "/resume"):
        bot_paused.clear(); bot_stopped.clear()
        send_go_screen(chat_id)

    elif cmd == "/demo" and is_scanadmin:
        """
        /demo btc buy entry 66000 tp1 67000 tp2 68000 sl 65500  → open fake BTC trade
        /demo tp1        → simulate TP1 hit  (SL→BE)
        /demo tp2        → simulate TP2 hit  (close all)
        /demo sl         → simulate SL hit   (close all)
        /demo btc sl 67000  → move SL to 67000
        /demo btc close     → force close all positions
        """
        raw = " ".join(parts[1:]).lower()
        if not raw:
            send_reply(chat_id,
                "<b>Simulate Demo Trade</b>\n\n"
                "Usage:\n"
                "<code>/demo btc buy entry 66000 tp1 67000 tp2 68000 sl 65500</code>\n"
                "<code>/demo tp1</code> — simulate TP1 hit\n"
                "<code>/demo tp2</code> — simulate TP2 hit\n"
                "<code>/demo sl</code> — simulate SL hit\n"
                "<code>/demo btc sl 67000</code> — move SL\n"
                "<code>/demo btc close</code> — force close\n\n"
                "<i>🛡️ Capital protected</i>"); return
        try:
            def _fmt(results):
                if results is None: return "Done."
                lines = [str(r) for r in results]
                return "\n".join(lines) if lines else "Done."

            if raw.split()[0] == "tp1":
                sig = ct._last_signal
                if not sig: send_reply(chat_id, "❌ No active demo signal. Open one first with /demo btc buy ..."); return
                ct.on_tp1(sig.get("entry",0), sig.get("tp1",0))
                send_reply(chat_id, f"<b>DEMO TP1 HIT</b>\nCheck Railway logs for SL placement result.")

            elif raw.split()[0] == "tp2":
                sig = ct._last_signal
                if not sig: send_reply(chat_id, "❌ No active demo signal."); return
                results = ct.on_tp2(sig.get("entry",0), sig.get("tp2",0))
                send_reply(chat_id, f"<b>DEMO TP2 HIT</b>\n{_fmt(results)}")

            elif raw in ("sl", "sl hit"):
                sig = ct._last_signal
                if not sig: send_reply(chat_id, "❌ No active demo signal."); return
                results = ct.on_sl(sig.get("entry",0), sig.get("sl",0))
                send_reply(chat_id, f"<b>DEMO SL HIT</b>\n{_fmt(results)}")

            elif raw == "btc close":
                results = ct.on_close_all()
                send_reply(chat_id, f"<b>DEMO CLOSE</b>\n{_fmt(results)}")

            elif raw.startswith("btc sl "):
                try:
                    new_sl = float(raw.split()[-1])
                    results = ct.on_sl_to_be(new_sl)
                    send_reply(chat_id, f"<b>DEMO SL MOVE → {new_sl:,.0f}</b>\n{_fmt(results)}")
                except Exception as e:
                    send_reply(chat_id, f"❌ btc sl error: {e}")

            elif raw.startswith("btc "):
                # Parse: btc buy/sell [entry X | market] sl X tp1 X tp2 X
                p = raw.split()
                side = p[1].upper()  # BUY or SELL
                vals = {}
                i = 2
                while i < len(p):
                    if p[i] in ("entry","sl","tp1","tp2") and i+1 < len(p):
                        try: vals[p[i]] = float(p[i+1]); i += 2
                        except: i += 1
                    elif p[i] == "market":
                        vals["market"] = True; i += 1
                    else:
                        i += 1
                sl  = vals.get("sl", 0); tp1 = vals.get("tp1", 0); tp2 = vals.get("tp2", 0)
                # entry: use provided value or fetch live price for market
                if "entry" in vals:
                    entry = vals["entry"]
                elif vals.get("market"):
                    ticker = get_ticker(); entry = ticker["price"]
                else:
                    entry = 0
                if not all([entry, sl, tp1, tp2]):
                    send_reply(chat_id, "Usage: /demo btc buy market sl 64000 tp1 67000 tp2 67500"); return
                fake_signal = {"side": side, "signal": side, "entry": entry, "sl": sl,
                               "tp1": tp1, "tp2": tp2, "price": entry,
                               "rr": f"1:{abs(tp2-entry)/abs(sl-entry):.1f}"}
                results = ct.on_signal(fake_signal, entry)
                send_reply(chat_id,
                    f"<b>DEMO TRADE OPEN</b>\n\n"
                    f"{side} entry:{entry:,.0f} SL:{sl:,.0f} TP1:{tp1:,.0f} TP2:{tp2:,.0f}\n\n"
                    + "\n".join(str(r) for r in (results or [])))
            else:
                send_reply(chat_id,
                    "<b>DEMO Commands</b>\n\n"
                    "<code>/demo btc buy entry 66000 tp1 67000 tp2 68000 sl 65500</code>\n"
                    "<code>/demo tp1</code> — TP1 hit → SL to BE\n"
                    "<code>/demo tp2</code> — TP2 hit → close all\n"
                    "<code>/demo sl</code>  — SL hit → close all\n"
                    "<code>/demo btc sl 67000</code> — move SL\n"
                    "<code>/demo btc close</code> — force close\n\n"
                    "<i>🛡️ Capital protected</i>")
        except Exception as e:
            send_reply(chat_id, f"❌ Demo error: {e}")

    elif cmd == "/pause":
        bot_paused.set(); bot_stopped.set()
        _ctrl_btns = {"inline_keyboard": [[
            {"text": "🟢 Resume",       "callback_data": "bot_go"},
            {"text": "🟠 Stop Scans",  "callback_data": "bot_stop"},
        ]]}
        send_reply(chat_id,
            f"⏸ <b>Bot PAUSED</b>\n\n"
            f"Everything frozen — scans, monitoring, alerts.\n"
            f"Use ▶️ Resume to restart.\n\n"
            f"<i>🛡️ Capital protected</i>", reply_markup=_ctrl_btns)

    elif cmd == "/stop":
        bot_stopped.set(); bot_paused.clear()
        _ctrl_btns = {"inline_keyboard": [[
            {"text": "🟢 Resume",       "callback_data": "bot_go"},
            {"text": "🔴 Pause All",    "callback_data": "bot_pause"},
        ]]}
        send_reply(chat_id,
            f"🛑 <b>Scans STOPPED</b>\n\n"
            f"✅ Trade monitoring still active\n"
            f"✅ Copytrade SL/TP still active\n"
            f"❌ New scans blocked\n"
            f"❌ BTC analysis blocked\n"
            f"❌ Demo blocked\n"
            f"❌ News blocked\n\n"
            f"<i>🛡️ Capital protected</i>", reply_markup=_ctrl_btns)

    elif cmd == "/btcanalysis":
        arg = parts[1].lower() if len(parts) > 1 else ""
        if arg in ("on", "off"):
            btc_analysis_enabled = (arg == "on")
            save_settings()
        _btca_mkp = {"inline_keyboard": [[
            {"text": "🟢 Enable Analysis",  "callback_data": "btca_on"},
            {"text": "🔴 Disable Analysis", "callback_data": "btca_off"},
        ]]}
        if btc_analysis_enabled:
            _btca_text = "📡 <b>BTC Analysis</b>  ✅ ON\n\nScheduled scans active.\n\n<i>🛡️ Capital protected</i>"
        else:
            _btca_text = "📡 <b>BTC Analysis</b>  ⏸ OFF\n\nScheduled scans paused.\n\n<i>🛡️ Capital protected</i>"
        send_reply(chat_id, _btca_text, reply_markup=_btca_mkp)

    elif cmd == "/tvstatus":
        cmd_tvstatus(chat_id)

    elif cmd == "/force_reload":
        if TV_BRIDGE_URL:
            try:
                r = requests.post(f"{TV_BRIDGE_URL}/reload", timeout=10)
                send_reply(chat_id, f"✅ Bridge reload triggered.\n\nTV bridge will reconnect + warm up candles (~5 min).\nUse /tvstatus to check progress.")
            except Exception as e:
                send_reply(chat_id, f"❌ Bridge unreachable: {e}\n\nMake sure ngrok tunnel is running.")
        else:
            send_reply(chat_id, "❌ TV_BRIDGE_URL not set — bridge not configured.")

    elif cmd == "/status":
        t = active_trade
        st = "⏸ PAUSED" if bot_paused.is_set() else ("🛑 STOPPED (scans off)" if bot_stopped.is_set() else "▶️ RUNNING")
        cd = f"Cooldown: {trade_stats['cooldown_scans']} scans\n" if trade_stats["cooldown_scans"] else ""
        ti = (f"{t['signal']} @ {t['entry']:,.0f}\nSL:{t['sl']:,.0f}  TP1:{t['tp1']:,.0f}  TP2:{t['tp2']:,.0f}\n"
            f"Entry:{'OK' if t['entry_hit'] else 'pending'}  TP1:{'OK' if t['tp1_hit'] else 'no'}"
            ) if t["signal"] else "No active trade"
        src = get_current_source()
        tv_status = ("ONLINE" if (tv_bridge_state["online"] and tv_bridge_state["cdp_ok"])
            else "Bridge OK - TV not connected" if tv_bridge_state["online"] else "OFFLINE - BingX fallback") if TV_BRIDGE_URL else "Not configured - BingX"
        scan_lines = ""
        for _ver, _lst in [(1, scan1_trades), (2, scan2_trades)]:
            for sc in _lst:
                scan_lines += (f"\n\n<b>Scan{_ver}:</b> {sc['signal']} {sc['symbol']}\n"
                    f"Entry:{sc['entry']:,.4g} {'✅' if sc.get('entry_hit') else '⏳'}  "
                    f"SL:{sc['sl']:,.4g}  TP1:{sc['tp1']:,.4g} {'✅' if sc.get('tp1_hit') else ''}")
        for _dlst in (demo_scan1_trades, demo_scan2_trades):
            for dc in _dlst:
                _cp = get_bingx_price(dc.get("symbol","")) if dc.get("symbol") else 0
                _pnl = (_cp - dc["entry"]) / dc["entry"] * 100 * (1 if dc["signal"]=="BUY" else -1) if _cp and dc.get("entry") else 0
                _dc_tp1 = "✅" if dc.get('tp1_hit') else f"{dc.get('tp1',0):,.4g}"
                scan_lines += (f"\n\n<b>[DEMO]</b> {dc['signal']} {dc.get('symbol','?')}\n"
                    f"Entry:{dc.get('entry',0):,.4g}  SL:{dc.get('sl',0):,.4g}  "
                    f"TP1:{_dc_tp1}  P/L:{_pnl:+.2f}%")
        _next_btc_scan, _next_scan1, _next_scan2 = _next_schedule_times()
        _next_btc_line = f"⏰ Next BTC scan:   <b>{_next_btc_scan} IST</b>\n" if btc_analysis_enabled else "⏰ Next BTC scan:   <b>OFF</b>\n"
        _next_s1_line  = f"⏰ Next Scan1:      <b>{_next_scan1}</b>\n" if (not bot_paused.is_set() and SCAN1_AUTO_ENABLED) else "⏰ Next Scan1:      <b>OFF</b>\n"
        _next_s2_line  = f"⏰ Next Scan2:      <b>{_next_scan2}</b>\n" if (not bot_paused.is_set() and SCAN2_AUTO_ENABLED) else "⏰ Next Scan2:      <b>OFF</b>\n"
        # Flags
        _btc_flag    = "✅ ON"  if btc_analysis_enabled              else "❌ OFF"
        _scan1_flag  = "✅ ON"  if not bot_paused.is_set()           else "❌ OFF"
        _scan2_flag  = "✅ ON"  if (not bot_paused.is_set() and SCAN2_AUTO_ENABLED) else "❌ OFF"
        _alt_flag    = f"Scan1 {_scan1_flag}  |  Scan2 {_scan2_flag}"
        _charts_flag = "✅ ON"  if SEND_CHARTS                       else "❌ OFF"
        _news_flag   = "✅ ON"  if SEND_NEWS                         else "❌ OFF"
        _btcmode_lbl = "V7 Classic" if BTC_PROMPT_MODE == "V7" else "V9 Current"
        _ctbtc_flag   = "✅ ON" if ct.BTC_CT_ENABLED   else "❌ OFF"
        _ctscan1_flag = "✅ ON" if ct.SCAN1_CT_ENABLED else "❌ OFF"
        _ctscan2_flag = "✅ ON" if ct.SCAN2_CT_ENABLED else "❌ OFF"
        _model_lbl   = "Opus 4.8" if SCAN_MODEL == "claude-opus-4-8" else "Fable 5"
        _gateway_lbl = "Aerolink" if USE_AEROLINK else "Direct"
        # Copy trade: per-user for non-admin, global active users count for admin
        _user_ct = ct._get(str(chat_id))
        _copy_flag = "✅ ON" if (_user_ct and _user_ct.get("copy_on")) else "❌ OFF"
        _tier_val = (_user_ct or {}).get("tier", "vip")
        _tier_tag = ("⭐ VIP" + (f" (until {_user_ct['vip_end']})" if _user_ct and _user_ct.get("vip_end") else "")) if _tier_val == "vip" else "🆓 FREE"
        _users_summary = _build_users_summary()
        send_reply(chat_id,
            f"<b>CLEXER V17.8.5</b>  |  {ist_str()}\n\n"
            f"🤖 Bot:        <b>{st}</b>\n"
            + (
                f"📡 BTC Scan:   <b>{_btc_flag}</b>  ({_btcmode_lbl})\n"
                f"🔍 Alt Scan:   {_alt_flag}\n"
                f"🧠 AI Model:   <b>{_model_lbl}</b>\n"
                f"🔌 Gateway:    <b>{_gateway_lbl}</b>\n"
                if is_admin else ""
            )
            + f"🔄 Copy Trade: <b>{_copy_flag}</b>\n"
            + (f"🏷 Tier:        <b>{_tier_tag}</b>\n" if _user_ct else "")
            + (
                f"📋 Copy Trade — BTC:{_ctbtc_flag} Scan1:{_ctscan1_flag} Scan2:{_ctscan2_flag}\n"
                f"🖼  Charts:     <b>{_charts_flag}</b>\n"
                f"📰 News:       <b>{_news_flag}</b>\n"
                if is_admin else ""
            )
            + f"\n{_next_btc_line}"
            f"{_next_s1_line}"
            f"{_next_s2_line}"
            f"\n📊 Session: {get_session()} | Conf: {required_confidence()} | SL streak: {trade_stats['consecutive_sl']}\n"
            + (_users_summary if is_admin else "")
            + (f"📡 Source: {src} | TV: {tv_status}\n" if is_admin else "")
            + (f"{cd}" if cd else "")
            + f"\n<b>BTC Trade:</b>\n{ti}"
            + scan_lines)

    elif cmd == "/price":
        try:
            tk = get_ticker()
            send_reply(chat_id, f"<b>BTCUSDT</b>\n\nPrice: <b>{tk['price']:,.2f}</b>\n"
                f"24h: {tk['change']:+.2f}% | Vol: ${tk['volume']/1e6:.1f}M\n"
                f"H:{tk['high24']:,.2f}  L:{tk['low24']:,.2f}\n"
                f"Source: {tk.get('source',get_current_source())}\n{ist_str()}")
        except Exception as e: send_reply(chat_id, f"Error: {e}")

    elif cmd == "/trade":
        parts_out = []
        # BTC trade
        t = active_trade
        if t["signal"]:
            try: tk = get_ticker(); pl = f"Current: <b>{tk['price']:,.2f}</b>\n"
            except: pl = ""
            parts_out.append(
                f"<b>BTC Trade</b>\n\n{t['signal']} - {SYMBOL}\n{pl}"
                f"Entry: <b>{t['entry']:,.0f}</b> {'✅' if t['entry_hit'] else '⏳ pending'}\n"
                f"SL:    <b>{t['sl']:,.0f}</b>\n"
                f"TP1:   <b>{t['tp1']:,.0f}</b> {'✅ HIT' if t['tp1_hit'] else '⏳ pending'}\n"
                f"TP2:   <b>{t['tp2']:,.0f}</b>\nType:  {t['entry_type']}\n"
                + (f"<i>{t['entry_note']}</i>" if t.get("entry_note") else "")
            )
        # Scan trades — all from both lists
        for _ver, _lst in [(1, scan1_trades), (2, scan2_trades)]:
            for sc in _lst:
                try:
                    sp = get_bingx_price(sc["symbol"])
                    spl = f"Current: <b>{sp:,.4g}</b>\n" if sp else ""
                except: spl = ""
                # Check tp1_hit from bot state OR from any copy user's state
                _tp1_hit = sc.get('tp1_hit') or ct.is_scan_tp1_hit(sc["symbol"])
                _sl_label = f"<b>{sc['sl']:,.4g}</b>" + (" ← BE" if _tp1_hit else "")
                parts_out.append(
                    f"<b>Scan{_ver} Trade</b>\n\n{sc['signal']} - {sc['symbol']}\n{spl}"
                    f"Entry: <b>{sc['entry']:,.4g}</b> {'✅' if sc.get('entry_hit') else '⏳ pending'}\n"
                    f"SL:    {_sl_label}\n"
                    f"TP1:   <b>{sc['tp1']:,.4g}</b> {'✅ HIT' if _tp1_hit else '⏳ pending'}\n"
                    f"TP2:   <b>{sc['tp2']:,.4g}</b>\nType:  {sc.get('entry_type','MARKET')}"
                )
        # Demo trades
        for _dlst in (demo_scan1_trades, demo_scan2_trades):
            for dc in _dlst:
                try: _dcp = get_bingx_price(dc.get("symbol","")); _dcpl = f"Current: <b>{_dcp:,.4g}</b>\n" if _dcp else ""
                except: _dcp = 0; _dcpl = ""
                _dpnl = (_dcp - dc["entry"]) / dc["entry"] * 100 * (1 if dc["signal"]=="BUY" else -1) if _dcp and dc.get("entry") else 0
                parts_out.append(
                    f"<b>[DEMO] SCALP V1</b>\n\n{dc['signal']} - {dc.get('symbol','?')}\n{_dcpl}"
                    f"Entry: <b>{dc.get('entry',0):,.4g}</b> ✅ (MARKET)\n"
                    f"SL:    <b>{dc.get('sl',0):,.4g}</b>\n"
                    f"TP1:   <b>{dc.get('tp1',0):,.4g}</b> {'✅ HIT' if dc.get('tp1_hit') else '⏳ pending'}\n"
                    f"TP2:   <b>{dc.get('tp2',0):,.4g}</b>\n"
                    f"BE SL: <b>{dc.get('be_sl',0):,.4g}</b>" + (" (active)" if dc.get('tp1_hit') and dc.get('be_sl') else "") + "\n"
                    f"P/L:   <b>{_dpnl:+.2f}%</b> | move_age: {dc.get('scan_ver','?')}"
                )
        if parts_out:
            send_reply(chat_id, "\n\n──────────\n\n".join(parts_out))
        else:
            send_reply(chat_id, "No active trade.")

    elif cmd == "/history":
        sub = parts[1].lower() if len(parts) > 1 else "btc"
        _hist_btns_rows = [[
            {"text": "📡 BTC",   "callback_data": "history_btc"},
            {"text": "🔍 Scan1", "callback_data": "history_scan1"},
            {"text": "🔍 Scan2", "callback_data": "history_scan2"},
        ]]
        if is_admin:
            _hist_btns_rows.append([{"text": "🗑 Reset History", "callback_data": "reset_signal_history"}])
        _hist_btns = {"inline_keyboard": _hist_btns_rows}
        if sub in ("scan1", "scan2"):
            ver = sub[-1]
            _sh = [s for s in scan_history if str(s.get("ver","1")) == ver]
            if not _sh:
                send_reply(chat_id, f"📜 <b>Scan{ver} History</b>\n\nNo signals yet.", reply_markup=_hist_btns); return
            lines = [f"📜 <b>Scan{ver} History (last 5)</b>"]
            for s in reversed(_sh[-5:]):
                res = s.get("result","?")
                em = "🏆" if res=="TP2" else ("💰" if res in ("TP1","BE") else "❌")
                lines.append(f"{em} {s['signal']} {s['symbol']} @ {s['entry']:,.4g}  → <b>{res}</b>\n"
                    f"   SL:{s['sl']:,.4g}  TP1:{s['tp1']:,.4g}  TP2:{s['tp2']:,.4g}\n   {s['time']}")
            send_reply(chat_id, "\n".join(lines), reply_markup=_hist_btns)
        else:
            if not signal_history:
                send_reply(chat_id, "📜 <b>BTC History</b>\n\nNo signals yet.", reply_markup=_hist_btns); return
            lines = ["📜 <b>BTC Signals (last 5)</b>"]
            for s in reversed(signal_history[-5:]):
                lines.append(f"{'🟢' if s['signal']=='BUY' else '🔴'} {s['signal']} @ {s['entry']:,.0f}  "
                    f"R:R:{s.get('rr','?')}  {s.get('confidence','?')}\n"
                    f"   SL:{s['sl']:,.0f}  TP1:{s['tp1']:,.0f}  TP2:{s['tp2']:,.0f}\n   {s['time']}")
            send_reply(chat_id, "\n".join(lines), reply_markup=_hist_btns)

    elif cmd == "/stats":
        ts = trade_stats
        btc_total = ts['total_tp1'] + ts['total_tp2'] + ts['total_sl'] or 1
        btc_wr = (ts['total_tp1'] + ts['total_tp2']) / btc_total * 100
        s1_total = ts['scan1_tp1'] + ts['scan1_tp2'] + ts['scan1_sl'] or 1
        s1_wr = (ts['scan1_tp1'] + ts['scan1_tp2']) / s1_total * 100
        s2_total = ts['scan2_tp1'] + ts['scan2_tp2'] + ts['scan2_sl'] or 1
        s2_wr = (ts['scan2_tp1'] + ts['scan2_tp2']) / s2_total * 100
        _stats_btns = {"inline_keyboard": [
            [{"text": "🔄 Refresh", "callback_data": "stats_win"}],
            [{"text": "🗑 Reset BTC",   "callback_data": "reset_btc_stats"},
             {"text": "🗑 Reset Scan1", "callback_data": "reset_scan1_stats"},
             {"text": "🗑 Reset Scan2", "callback_data": "reset_scan2_stats"}],
        ]} if is_admin else None
        send_reply(chat_id,
            f"<b>Statistics</b>\n\n"
            f"<b>BTC Trades</b>\n"
            f"Signals: {ts['total_signals']}\n"
            f"TP1: {ts['total_tp1']} | TP2: {ts['total_tp2']} | SL: {ts['total_sl']}\n"
            f"Win rate: <b>{btc_wr:.0f}%</b>\n"
            f"Stop hunts: {ts['stop_hunts']} | Missed: {ts['missed_entries']}\n"
            f"Consec SL: {ts['consecutive_sl']} | Cooldown: {ts['cooldown_scans']}\n\n"
            f"<b>Scan1 Trades</b>\n"
            f"Signals: {ts['scan1_signals']}\n"
            f"TP1: {ts['scan1_tp1']} | TP2: {ts['scan1_tp2']} | SL: {ts['scan1_sl']}\n"
            f"Win rate: <b>{s1_wr:.0f}%</b>\n\n"
            f"<b>Scan2 Trades</b>\n"
            f"Signals: {ts['scan2_signals']}\n"
            f"TP1: {ts['scan2_tp1']} | TP2: {ts['scan2_tp2']} | SL: {ts['scan2_sl']}\n"
            f"Win rate: <b>{s2_wr:.0f}%</b>", reply_markup=_stats_btns)

    elif cmd == "/session":
        s = get_session()
        send_reply(chat_id, f"<b>Session</b>\n\n{s} {'Active' if is_trading_hours() else 'Inactive'}\n\n"
            f"London:  07:30-16:30 IST\nNY:      18:30-01:00 IST\nSleep:   01:00-07:29 IST\n\n{ist_str()}")

    elif cmd == "/users":
        uname = (message.get("from",{}).get("username","?") if message else "?")
        ct.handle(cmd, parts, chat_id, uname, send_reply, is_admin, scan_trades=scan1_trades+scan2_trades)

    elif cmd == "/miniapp":
        if not is_admin: return
        _mini_btns = {"inline_keyboard": [[
            {"text": "▶️  Resume (Live)",       "callback_data": "miniapp_resume"},
            {"text": "⏸  Pause (Maintenance)", "callback_data": "miniapp_pause"}]]}
        sub = parts[1].lower() if len(parts) > 1 else ""
        if not sub:
            send_reply(chat_id,
                "<b>Mini App Control</b>\n\n<i>🛡️ Capital protected</i>", reply_markup=_mini_btns)
            return
        msg = " ".join(parts[2:]) if len(parts) > 2 else "Under Maintenance — back soon!"
        if "/" in msg:
            send_reply(chat_id, "⚠️ Maintenance message can't contain bot commands (users would see it). Rephrase without '/'.", reply_markup=_mini_btns)
            return
        if sub in ("pause", "off", "maintenance"):
            on = True
        elif sub in ("resume", "on", "live"):
            on = False; msg = "Live"
        else:
            send_reply(chat_id, "Usage:\n/miniapp pause\n/miniapp resume", reply_markup=_mini_btns); return
        if CLEXER_API_URL:
            try:
                hdrs = {"X-Push-Secret": PUSH_STATE_SECRET, "Content-Type": "application/json"} if PUSH_STATE_SECRET else {"Content-Type": "application/json"}
                requests.post(f"{CLEXER_API_URL}/maintenance", json={"on": on, "msg": msg}, headers=hdrs, timeout=5)
                if not on:
                    # Resuming — force a fresh state push so the mini app can't show a stale/ghost trade
                    save_state()
                send_reply(chat_id, f"🔧 Mini App {'⏸ PAUSED' if on else '▶️ RESUMED (state synced)'}\n\n<i>🛡️ Capital protected</i>", reply_markup=_mini_btns)
            except Exception as e:
                send_reply(chat_id, f"Error: {e}", reply_markup=_mini_btns)
        else:
            send_reply(chat_id, "CLEXER_API_URL not set", reply_markup=_mini_btns)

    elif cmd == "/close":
        t = active_trade
        if not t["signal"]: send_reply(chat_id, "No active trade.")
        else:
            info = f"{t['signal']} @ {t['entry']:,.0f}"
            log_trade_outcome("MANUAL_CLOSE", f"closed by admin")
            ct.on_close_all()
            reset_trade(); send_telegram(f"<b>Trade Closed</b>\n{info}\n\n<i>🛡️ Capital protected</i>")
            send_reply(chat_id, f"Closed: {info}"); force_scan.set()

    elif cmd == "/sltobe":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        else:
            old = active_trade["sl"]; active_trade["sl"] = active_trade["entry"]
            ct.on_sl_to_be(active_trade["entry"])
            send_telegram(f"<b>SL -> BE</b>  {old:,.0f} -> <b>{active_trade['entry']:,.0f}</b>\n\n<i>🛡️ Capital protected</i>")
            send_reply(chat_id, f"SL -> {active_trade['entry']:,.0f}")

    elif cmd == "/setsl":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /setsl 61500")
        else:
            try:
                v = float(parts[1].replace(",","")); old = active_trade["sl"]
                active_trade["sl"] = v
                ct.on_update_sl(v)
                send_telegram(f"<b>SL</b>  {old:,.0f} -> <b>{v:,.0f}</b>\n\n<i>🛡️ Capital protected</i>")
                send_reply(chat_id, f"SL = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /setsl 61500")

    elif cmd == "/settp1":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp1 63000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp1"] = v
                send_telegram(f"<b>TP1 -> {v:,.0f}</b>\n\n<i>🛡️ Capital protected</i>")
                send_reply(chat_id, f"TP1 = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /settp1 63000")

    elif cmd == "/settp2":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp2 65000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp2"] = v
                send_telegram(f"<b>TP2 -> {v:,.0f}</b>\n\n<i>🛡️ Capital protected</i>")
                send_reply(chat_id, f"TP2 = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /settp2 65000")

    elif cmd == "/signal":
        if bot_paused.is_set(): send_reply(chat_id, "Bot paused. /go first.")
        else:
            now = time.time(); elapsed = now-last_force_scan_time
            if elapsed<300 and last_force_scan_time>0: send_reply(chat_id, f"Cooldown: {int((300-elapsed)//60)+1} min left")
            else:
                last_force_scan_time = now
                send_reply(chat_id, "Forcing scan (~15-30s)..."); force_scan.set()

    elif cmd == "/compare":
        tv_live = is_tv_online()
        send_reply(chat_id,
            f"🔬 <b>Compare: 4 BTC Analyses Running in Parallel</b>\n\n"
            f"1️⃣ V9 + BingX\n2️⃣ V9 + TV {'✅' if tv_live else '❌ offline'}\n"
            f"3️⃣ B1 + BingX\n4️⃣ B1 + TV {'✅' if tv_live else '❌ offline'}\n\n"
            f"Results in ~30-60s...")
        def _run_compare(cid=chat_id):
            results = [None, None, None, None]
            errors  = ["", "", "", ""]
            def _r1():  # V9 + BingX
                try:
                    tk = bingx_get_btc_ticker() or binance_get_ticker()
                    d = {}
                    for key, lim, lb in [("weekly",52,5),("4h",200,5),("1h",100,5),("5m",50,3)]:
                        df = bingx_get_btc_candles(key, lim)
                        if df is None or len(df) < 2: df = binance_get_candles(key, lim)
                        d[key] = (df, lb)
                    # Temporarily spoof source for build_smc_summary
                    tk["source"] = "BingX"
                    # Build prompt using V9 prompt directly
                    price = tk["price"]; session = get_session()
                    summary = build_smc_summary(d, tk)
                    prompt = build_new_prompt_v9(summary, price, session, "", "", "", "")
                    msg = _claude_client().messages.create(
                        model=SCAN_MODEL, max_tokens=1000,
                        messages=[{"role":"user","content":prompt}])
                    _log_api_usage("compare_v9_bingx", SCAN_MODEL,
                                   msg.usage.input_tokens, msg.usage.output_tokens)
                    raw = _claude_text(msg)
                    sig = extract_json_from_response(raw) or {"signal":"ERROR","raw":raw[:200]}
                    if sig: sig["data_source"] = "BingX"; sig["prompt_mode"] = "V9+BingX"
                    results[0] = sig
                except Exception as e: errors[0] = str(e)
            def _r2():  # V9 + TV
                try:
                    if not tv_live: results[1] = {"signal":"SKIP","reason":"TV offline"}; return
                    tk = tv_get_ticker() or bingx_get_btc_ticker()
                    d = {}
                    for key, lim, lb in [("weekly",52,5),("4h",200,5),("1h",100,5),("5m",50,3)]:
                        df = tv_get_candles(key, lim)
                        if df is None or len(df) < 2: df = bingx_get_btc_candles(key, lim)
                        d[key] = (df, lb)
                    price = tk["price"]; session = get_session()
                    indicators = fetch_tv_indicators()
                    summary = build_smc_summary(d, tk) + build_indicator_context(indicators)
                    prompt = build_new_prompt_v9(summary, price, session, "", "", "", "")
                    msg = _claude_client().messages.create(
                        model=SCAN_MODEL, max_tokens=1000,
                        messages=[{"role":"user","content":prompt}])
                    _log_api_usage("compare_v9_tv", SCAN_MODEL,
                                   msg.usage.input_tokens, msg.usage.output_tokens)
                    raw = _claude_text(msg)
                    sig = extract_json_from_response(raw) or {"signal":"ERROR","raw":raw[:200]}
                    if sig: sig["data_source"] = "TV"; sig["prompt_mode"] = "V9+TV"
                    results[1] = sig
                except Exception as e: errors[1] = str(e)
            def _r3():  # B1 + BingX
                try:
                    tk = bingx_get_btc_ticker() or binance_get_ticker()
                    d = b1_fetch_data(use_tv=False)
                    sig, lbl = b1_analyze(tk, d, use_tv=False)
                    results[2] = sig
                except Exception as e: errors[2] = str(e)
            def _r4():  # B1 + TV
                try:
                    if not tv_live: results[3] = {"signal":"SKIP","reason":"TV offline"}; return
                    tk = tv_get_ticker() or bingx_get_btc_ticker()
                    d = b1_fetch_data(use_tv=True)
                    sig, lbl = b1_analyze(tk, d, use_tv=True)
                    results[3] = sig
                except Exception as e: errors[3] = str(e)

            threads = [threading.Thread(target=f, daemon=True) for f in [_r1,_r2,_r3,_r4]]
            for t in threads: t.start()
            for t in threads: t.join(timeout=90)

            try: price_now = (bingx_get_btc_ticker() or binance_get_ticker())["price"]
            except: price_now = 0

            labels = ["V9+BingX", "V9+TV", "B1+BingX", "B1+TV"]
            lines = [f"🔬 <b>BTC Compare Results</b>  {ist_str()}\n"
                     f"Price: <b>{price_now:,.2f}</b>\n{'─'*30}"]
            for i, (sig, lbl) in enumerate(zip(results, labels)):
                if errors[i]: lines.append(f"<b>[{lbl}]</b> ❌ {errors[i][:100]}")
                elif sig and sig.get("signal") == "SKIP": lines.append(f"<b>[{lbl}]</b> ⏭ TV offline")
                else: lines.append(_fmt_compare_result(sig, lbl))
                lines.append("─"*30)
            send_reply(cid, "\n".join(lines) + "\n\n<i>⚠️ For testing only — not financial advice</i>")
        threading.Thread(target=_run_compare, daemon=True).start()

    elif cmd == "/charts":
        global CHART_SNAP_ENABLED
        if not MINI_APP_URL:
            send_reply(chat_id, "❌ MINI_APP_URL not set in Railway env vars."); return
        if not CHART_SNAP_ENABLED:
            send_reply(chat_id, "📵 Chart snapshots are OFF. Send /chartson to enable."); return
        send_reply(chat_id, "📸 Taking chart screenshots (W, 4H, 1H, 5m)...\nThis takes ~30s")
        def _do_charts(cid=chat_id):
            try:
                shots = take_miniapp_screenshots()
            except Exception as e:
                send_reply(cid, f"❌ Screenshot crashed: {e}"); return
            if not shots:
                send_reply(cid, "❌ No screenshots returned — check Railway logs."); return
            for label, result in shots:
                if isinstance(result, str):
                    send_reply(cid, f"❌ [{label}] {result}"); continue
                try:
                    result.seek(0)
                    requests.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                        data={"chat_id": cid, "caption": f"📊 BTC {label}"},
                        files={"photo": (f"btc_{label}.png", result, "image/png")},
                        timeout=30)
                except Exception as e:
                    print(f"  [CHARTS] send {label} error: {e}")
        threading.Thread(target=_do_charts, daemon=True).start()

    elif cmd == "/chartson":
        CHART_SNAP_ENABLED = True
        send_reply(chat_id, "✅ Chart snapshots ON — /charts will work.")

    elif cmd == "/chartsoff":
        CHART_SNAP_ENABLED = False
        send_reply(chat_id, "📵 Chart snapshots OFF — /charts disabled (saves credits).")

    elif cmd == "/resetsl":
        trade_stats["consecutive_sl"] = 0; trade_stats["cooldown_scans"] = 0
        send_reply(chat_id, "SL streak + cooldown reset.")

    elif cmd == "/btcmode":
        _bmode_btns = {"inline_keyboard": [[
            {"text": "🔵  V7 Classic (on)",    "callback_data": "btcmode_v7"},
            {"text": "🔴  V9 Current (off)",   "callback_data": "btcmode_v9"}]]}
        mode_label = "🔵 V7 CLASSIC" if BTC_PROMPT_MODE == "V7" else "🟠 V9 CURRENT"
        if len(parts) < 2:
            send_reply(chat_id,
                f"<b>BTC Prompt Mode</b>\n\nCurrent: <b>{mode_label}</b>\n\n"
                f"<b>V7 Classic</b> — TV/BingX split, narrated scan, no Rule 8\n"
                f"<b>V9 Current</b> — CRITICAL header, silent scan, Rule 8 hard block\n\n"
                f"<i>🛡️ Capital protected</i>", reply_markup=_bmode_btns)
        elif parts[1].lower() == "on":
            BTC_PROMPT_MODE = "V7"; save_settings()
            send_reply(chat_id,
                f"<b>BTC Mode → 🔵 V7 CLASSIC</b> ✅\n\n"
                f"Narrated scan | min 2 pause candles | no Rule 8\n\n"
                f"<i>🛡️ Capital protected</i>", reply_markup=_bmode_btns)
        elif parts[1].lower() == "off":
            BTC_PROMPT_MODE = "V9"; save_settings()
            send_reply(chat_id,
                f"<b>BTC Mode → 🟠 V9 CURRENT</b> ✅\n\n"
                f"CRITICAL header | silent scan | Rule 8 hard block\n\n"
                f"<i>🛡️ Capital protected</i>", reply_markup=_bmode_btns)
        else:
            send_reply(chat_id, "Usage: /btcmode on|off", reply_markup=_bmode_btns)


    elif cmd == "/setinterval":
        if len(parts)<2: send_reply(chat_id, f"Current: {SIGNAL_SCAN_INTERVAL//3600}h\nUsage: /setinterval 4")
        else:
            try:
                h = float(parts[1])
                if h<1 or h>24: send_reply(chat_id, "1-24 hours only.")
                else: SIGNAL_SCAN_INTERVAL = int(h*3600); save_settings(); send_reply(chat_id, f"Scan interval -> {h}h")
            except: send_reply(chat_id, "Usage: /setinterval 4")

    elif cmd == "/images":
        _img_btns = {"inline_keyboard": [[
            {"text": "🟢  ON",  "callback_data": "images_on"},
            {"text": "🔴  OFF", "callback_data": "images_off"}]]}
        if len(parts)<2:
            send_reply(chat_id,
                f"<b>Chart Images</b>\n\nStatus: <b>{'✅ ON' if SEND_CHARTS else '❌ OFF'}</b>\n"
                f"TFs: <b>{', '.join(CHART_TFS).upper()}</b>\n\n<i>🛡️ Capital protected</i>",
                reply_markup=_img_btns)
        elif parts[1].lower()=="on":
            SEND_CHARTS = True; save_settings()
            send_reply(chat_id, f"✅ <b>Charts ON</b>\nTFs: {', '.join(CHART_TFS).upper()}\n\n<i>🛡️ Capital protected</i>", reply_markup=_img_btns)
        elif parts[1].lower()=="off":
            SEND_CHARTS = False; save_settings()
            send_reply(chat_id, "❌ <b>Charts OFF</b>\n\n<i>🛡️ Capital protected</i>", reply_markup=_img_btns)
        else: send_reply(chat_id, "Usage: /images on|off", reply_markup=_img_btns)

    elif cmd == "/setimages":
        _tf_btns = {"inline_keyboard": [
            [{"text": "📅  Weekly", "callback_data": "setimg:weekly"},
             {"text": "📊  4H",     "callback_data": "setimg:4h"}],
            [{"text": "📈  1H",     "callback_data": "setimg:1h"},
             {"text": "⏱  15M",    "callback_data": "setimg:15m"}],
            [{"text": "⚡  5M",     "callback_data": "setimg:5m"}]]}
        if len(parts)<2:
            send_reply(chat_id,
                f"<b>Chart Timeframes</b>\n\nCurrent: <b>{', '.join(CHART_TFS).upper()}</b>\n\n"
                f"<i>Tap to toggle a timeframe, or use:\n/setimages weekly,4h,1h,5m</i>",
                reply_markup=_tf_btns)
        else:
            valid = {"weekly","4h","1h","15m","5m"}
            chosen = [tf.strip().lower() for tf in parts[1].split(",") if tf.strip().lower() in valid]
            if not chosen: send_reply(chat_id, "No valid TFs. Use: weekly, 4h, 1h, 15m, 5m", reply_markup=_tf_btns)
            else: CHART_TFS = chosen; save_settings(); send_reply(chat_id, f"✅ Chart TFs: <b>{', '.join(CHART_TFS).upper()}</b>", reply_markup=_tf_btns)

    elif cmd == "/news":
        _news_btns = {"inline_keyboard": [[
            {"text": "🟢  ON",  "callback_data": "news_on"},
            {"text": "🔴  OFF", "callback_data": "news_off"}]]}
        if len(parts)<2:
            send_reply(chat_id,
                f"<b>Crypto News</b>\n\nStatus: <b>{'✅ ON' if SEND_NEWS else '❌ OFF'}</b>\n\n<i>🛡️ Capital protected</i>",
                reply_markup=_news_btns)
        elif parts[1].lower()=="on":
            SEND_NEWS = True; save_settings()
            send_reply(chat_id, f"✅ <b>News ON</b> — {len(NEWS_SOURCES)} sources\n\n<i>🛡️ Capital protected</i>", reply_markup=_news_btns)
        elif parts[1].lower()=="off":
            SEND_NEWS = False; save_settings()
            send_reply(chat_id, "❌ <b>News OFF</b>\n\n<i>🛡️ Capital protected</i>", reply_markup=_news_btns)
        else: send_reply(chat_id, "Usage: /news on|off", reply_markup=_news_btns)

    elif cmd == "/latestnews":
        send_reply(chat_id, "Fetching news (~15s)...")
        threading.Thread(target=check_news, args=(True,), daemon=True).start()

    elif cmd == "/broadcast":
        _bc_btns = {"inline_keyboard": [[
            {"text": "👥 Users Only",    "callback_data": "broadcast_mode:users"},
            {"text": "📢 Channels Only", "callback_data": "broadcast_mode:channels"},
        ], [
            {"text": "🌍 Both (Users + Channels)", "callback_data": "broadcast_mode:all"},
        ]]}
        send_reply(chat_id, "📢 <b>Broadcast Mode</b>\n\nWho should receive this message?", reply_markup=_bc_btns)

    elif cmd == "/adminlinks" and is_admin:
        send_adminlinks_screen(chat_id)

    elif cmd == "/userstats" and is_admin:
        send_userstats_screen(chat_id)

    elif cmd == "/aiconfig" and is_scanadmin:
        send_aiconfig_screen(chat_id)

    elif cmd == "/entrystyle" and is_scanadmin:
        send_entrystyle_screen(chat_id)

    elif cmd == "/coadmin" and is_admin:
        send_coadmin_screen(chat_id)

    elif cmd == "/trailsl" and is_scanadmin:
        send_trailsl_screen(chat_id)

    elif cmd == "/tp1size" and is_scanadmin:
        if len(parts) < 2:
            send_reply(chat_id,
                f"<b>TP1 Close %</b>\n\nCurrent: <b>{ct.TP1_CLOSE_PCT}%</b> closes at TP1, the rest rides to TP2.\n\n"
                f"Use the tap keypad or type a number 1–99.")
            return
        try:
            pct = float(parts[1])
            if pct < 1 or pct > 99:
                send_reply(chat_id, "TP1 close % must be between 1 and 99."); return
            ct.TP1_CLOSE_PCT = pct
            save_settings()
            send_reply(chat_id, f"<b>TP1 Close % Set</b>\n\n{pct}% closes at TP1, {100-pct}% rides to TP2.")
        except ValueError:
            send_reply(chat_id, "Please enter a valid number 1–99.")

    elif cmd == "/freelimit" and is_admin:
        if len(parts) < 2:
            send_reply(chat_id,
                f"<b>Free Channel Daily Limit</b>\n\nCurrent: <b>{FREE_SIGNAL_DAILY_LIMIT}</b> signals/day "
                f"(window 06:00–19:00 IST)\n\nUse the tap keypad or type a number 0–50.")
            return
        try:
            n = int(parts[1])
            if n < 0 or n > 50:
                send_reply(chat_id, "Limit must be 0–50."); return
            FREE_SIGNAL_DAILY_LIMIT = n
            save_settings()
            send_reply(chat_id, f"<b>Free Channel Daily Limit Set</b>\n\n{n} signals/day, 06:00–19:00 IST.")
        except ValueError:
            send_reply(chat_id, "Please enter a valid whole number.")

    elif cmd == "/channelmgmt" and is_admin:
        send_channelmgmt_screen(chat_id)

    elif cmd == "/channels":
        ch2 = os.getenv("TELEGRAM_CHANNEL_ID_2","")
        s1 = "PAUSED" if channel_paused["1"] else "ACTIVE"
        s2 = "PAUSED" if channel_paused["2"] else ("ACTIVE" if ch2 else "NOT SET")
        send_reply(chat_id,
            f"<b>Channel Status</b>\n\n"
            f"Channel 1: <b>{s1}</b>\n<code>{TELEGRAM_CHANNEL_ID}</code>\n\n"
            f"Channel 2: <b>{s2}</b>\n<code>{ch2 or 'not configured'}</code>\n\n"
            f"/pausechannel 1 or 2\n/resumechannel 1 or 2")

    elif cmd == "/pausechannel":
        def _ch_btns(action):
            s1 = "⏸ Paused" if channel_paused.get("1") else "✅ Live"
            s2 = "⏸ Paused" if channel_paused.get("2") else "✅ Live"
            return {"inline_keyboard": [[
                {"text": f"📢 Channel 1  {s1}", "callback_data": f"{action}:1"},
                {"text": f"📢 Channel 2  {s2}", "callback_data": f"{action}:2"},
            ]]}
        if len(parts) < 2 or parts[1] not in ("1","2"):
            send_reply(chat_id, "<b>⏸ Pause Channel</b>\n\nSelect channel:", reply_markup=_ch_btns("pausech")); return
        key = parts[1]
        channel_paused[key] = True
        save_settings()
        send_reply(chat_id, f"<b>Channel {key} PAUSED ⏸</b>", reply_markup=_ch_btns("pausech"))

    elif cmd == "/resumechannel":
        def _ch_btns_r(action):
            s1 = "⏸ Paused" if channel_paused.get("1") else "✅ Live"
            s2 = "⏸ Paused" if channel_paused.get("2") else "✅ Live"
            return {"inline_keyboard": [[
                {"text": f"📢 Channel 1  {s1}", "callback_data": f"{action}:1"},
                {"text": f"📢 Channel 2  {s2}", "callback_data": f"{action}:2"},
            ]]}
        if len(parts) < 2 or parts[1] not in ("1","2"):
            send_reply(chat_id, "<b>▶️ Resume Channel</b>\n\nSelect channel:", reply_markup=_ch_btns_r("resumech")); return
        key = parts[1]
        channel_paused[key] = False
        save_settings()
        send_reply(chat_id, f"<b>Channel {key} RESUMED ✅</b>", reply_markup=_ch_btns_r("resumech"))

    elif cmd == "/cancel":
        if chat_id in broadcast_pending: del broadcast_pending[chat_id]; send_reply(chat_id, "Cancelled.")
        else: send_reply(chat_id, "Nothing to cancel.")

    elif cmd == "/ctclose" and is_admin:
        uname = (message.get("from",{}).get("username","?") if message else "?")
        ct.handle(cmd, parts, chat_id, uname, send_reply, is_admin, scan_trades=scan1_trades+scan2_trades)

    elif cmd == "/closetrade" and is_scanadmin:
        if len(parts) < 2:
            send_reply(chat_id,
                "<b>Close Trade</b>\n\n"
                "Usage:\n"
                "<code>/closetrade BTC</code> — close BTC-USDT for all copy users\n"
                "<code>/closetrade ETH</code> — close ETH-USDT for all copy users\n"
                "<code>/closetrade SOL</code> — close SOL-USDT for all copy users\n"
                "<code>/closetrade all</code> — close ALL positions (every coin)\n\n"
                "<i>🛡️ Capital protected</i>"); return
        coin = parts[1].upper()
        if coin == "ALL":
            # Close every position on every symbol
            ct.on_close_all()
            if active_trade["signal"]:
                log_trade_outcome("MANUAL_CLOSE", "admin /closetrade all")
                reset_trade()
            # Clear all scan trades too
            scan1_count = len(scan1_trades)
            scan2_count = len(scan2_trades)
            scan1_trades.clear()
            scan2_trades.clear()
            save_state()
            send_telegram(
                f"🔴 <b>ALL TRADES CLOSED</b>  {ist_str()}\n\n"
                f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                f"⛔ <b>This is NOT a new signal</b>\n\n"
                f"Admin closed all positions.\n\n<i>🛡️ Capital protected</i>")
            send_reply(chat_id, f"✅ All positions closed.\nBTC trade reset + Scan1 ({scan1_count}) + Scan2 ({scan2_count}) trades cleared.")
        else:
            results = ct.close_coin_all(coin)
            # If it's BTC and we have an active BTC trade, also reset it
            if coin in ("BTC","BTCUSDT","BTC-USDT") and active_trade["signal"]:
                log_trade_outcome("MANUAL_CLOSE", f"admin /closetrade {coin}")
                reset_trade()
            reply = f"<b>Close {coin.upper()}-USDT</b>\n\n" + "\n".join(results)
            send_reply(chat_id, reply + "\n\n<i>🛡️ Capital protected</i>")

    elif cmd == "/closescan" and is_scanadmin:
        s1 = len(scan1_trades); s2 = len(scan2_trades)
        _syms = {t["symbol"] for t in scan1_trades + scan2_trades if t.get("symbol")}
        for _sym in _syms:
            ct.close_coin_all(_sym)
        scan1_trades.clear(); scan2_trades.clear(); save_state()
        send_reply(chat_id,
            f"✅ <b>Scan trades cleared</b>\n\n"
            f"Scan1: {s1} removed\nScan2: {s2} removed\nClosed on BingX: {', '.join(_syms) if _syms else 'none'}\n\n<i>🛡️ Capital protected</i>")

    elif cmd == "/alt" and is_scanadmin:
        _alt_btns = {"inline_keyboard": [[
            {"text": "🔁  Loop Mode (every hour)", "callback_data": "alt_loop:1"},
            {"text": "📋  Manual Times",           "callback_data": "alt_manual:1"},
        ], [
            {"text": "🔢  Tap to Pick Times", "callback_data": "tp_start:scan1"},
        ]]}
        _sched_str = "  ".join(f"{h}:{m:02d}" for h,m in SCAN1_SCHEDULE)
        if len(parts) < 2:
            send_reply(chat_id,
                f"⏰ <b>Scan1 Schedule</b>\n\n"
                f"Current times:\n<code>{_sched_str}</code>\n\n"
                f"<i>🛡️ Capital protected</i>", reply_markup=_alt_btns); return
        # /alt loop 2  → every hour at :02
        if parts[1].lower() == "loop" and len(parts) > 2:
            try: new_min = int(parts[2]); assert 0 <= new_min <= 59
            except: send_reply(chat_id, "❌ Usage: /alt loop 02"); return
            SCAN1_SCHEDULE = sorted(set((h, new_min) for h in range(24)))
            _scan1_triggered_today.clear()
            send_reply(chat_id, f"✅ <b>Scan1 → Loop Mode</b>\n\nRuns every hour at <b>:{new_min:02d}</b>\n\n<i>🛡️ Capital protected</i>", reply_markup=_alt_btns); return
        # /alt manual 2.02 2.23 14.25  → specific times (supports both . and : separator)
        if parts[1].lower() == "manual" and len(parts) > 2:
            new_slots = []; rejected = []
            for t in parts[2:]:
                try:
                    sep = "." if "." in t else ":"
                    h, m = t.split(sep); h, m = int(h), int(m)
                    if 0 <= h <= 23 and 0 <= m <= 59:
                        new_slots.append((h, m))
                    else:
                        rejected.append(t)
                except: rejected.append(t)
            if not new_slots:
                send_reply(chat_id, "❌ Type times like: <code>2.02 2.23 14.25 15.46</code>"); return
            SCAN1_SCHEDULE = sorted(set(new_slots))
            _scan1_triggered_today.clear()
            _times = "\n".join(f"• {h}:{m:02d} IST" for h,m in SCAN1_SCHEDULE)
            _rej_note = f"\n\n⚠️ Ignored invalid: <code>{' '.join(rejected)}</code>" if rejected else ""
            send_reply(chat_id, f"✅ <b>Scan1 → Manual Times</b>\n\n{_times}{_rej_note}\n\n<i>🛡️ Capital protected</i>", reply_markup=_alt_btns); return
        send_reply(chat_id, "❌ Tap a button below 👇", reply_markup=_alt_btns); return

    elif cmd == "/alt2" and is_scanadmin:
        _alt2_btns = {"inline_keyboard": [[
            {"text": "🔁  Loop Mode (every hour)", "callback_data": "alt_loop:2"},
            {"text": "📋  Manual Times",           "callback_data": "alt_manual:2"},
        ], [
            {"text": "🔢  Tap to Pick Times", "callback_data": "tp_start:scan2"},
        ]]}
        _sched2_str = "  ".join(f"{h}:{m:02d}" for h,m in SCAN2_SCHEDULE)
        if len(parts) < 2:
            send_reply(chat_id,
                f"⏰ <b>Scan2 Schedule</b>\n\n"
                f"Current times:\n<code>{_sched2_str}</code>\n\n"
                f"<i>🛡️ Capital protected</i>", reply_markup=_alt2_btns); return
        if parts[1].lower() == "loop" and len(parts) > 2:
            try: new_min = int(parts[2]); assert 0 <= new_min <= 59
            except: send_reply(chat_id, "❌ Usage: /alt2 loop 24"); return
            SCAN2_SCHEDULE = sorted(set((h, new_min) for h in range(24)))
            _auto_scan2_last_hour = -1
            send_reply(chat_id, f"✅ <b>Scan2 → Loop Mode</b>\n\nRuns every hour at <b>:{new_min:02d}</b>\n\n<i>🛡️ Capital protected</i>", reply_markup=_alt2_btns); return
        if parts[1].lower() == "manual" and len(parts) > 2:
            new_slots = []; rejected = []
            for t in parts[2:]:
                try:
                    sep = "." if "." in t else ":"
                    h, m = t.split(sep); h, m = int(h), int(m)
                    if 0 <= h <= 23 and 0 <= m <= 59:
                        new_slots.append((h, m))
                    else:
                        rejected.append(t)
                except: rejected.append(t)
            if not new_slots:
                send_reply(chat_id, "❌ Type times like: <code>12.24 15.24 19.24</code>"); return
            SCAN2_SCHEDULE = sorted(set(new_slots))
            _times = "\n".join(f"• {h}:{m:02d} IST" for h,m in SCAN2_SCHEDULE)
            _rej_note = f"\n\n⚠️ Ignored invalid: <code>{' '.join(rejected)}</code>" if rejected else ""
            send_reply(chat_id, f"✅ <b>Scan2 → Manual Times</b>\n\n{_times}{_rej_note}\n\n<i>🛡️ Capital protected</i>", reply_markup=_alt2_btns); return
        send_reply(chat_id, "❌ Tap a button below 👇", reply_markup=_alt2_btns); return

    elif cmd == "/altdemo" and is_scanadmin:
        _altd_btns = {"inline_keyboard": [[
            {"text": "📋  Manual Times", "callback_data": "alt_manual:3"},
        ], [
            {"text": "🔢  Tap to Pick Times", "callback_data": "tp_start:demo"},
        ]]}
        _sched_str = "  ".join(f"{h}:{m:02d}" for h,m in SCAN1_TEST_SCHEDULE)
        if len(parts) < 2:
            send_reply(chat_id,
                f"⏰ <b>Demo/Test Schedule</b>\n\n"
                f"Current times:\n<code>{_sched_str}</code>\n\n"
                f"<i>🛡️ Capital protected</i>", reply_markup=_altd_btns); return
        if parts[1].lower() == "manual" and len(parts) > 2:
            new_slots = []; rejected = []
            for t in parts[2:]:
                try:
                    sep = "." if "." in t else ":"
                    h, m = t.split(sep); h, m = int(h), int(m)
                    if 0 <= h <= 23 and 0 <= m <= 59:
                        new_slots.append((h, m))
                    else:
                        rejected.append(t)
                except: rejected.append(t)
            if not new_slots:
                send_reply(chat_id, "❌ Type times like: <code>2.02 2.23 14.25</code>"); return
            SCAN1_TEST_SCHEDULE = sorted(set(new_slots))
            _test_triggered_today.clear()
            _times = "\n".join(f"• {h}:{m:02d} IST" for h,m in SCAN1_TEST_SCHEDULE)
            _rej_note = f"\n\n⚠️ Ignored invalid: <code>{' '.join(rejected)}</code>" if rejected else ""
            send_reply(chat_id, f"✅ <b>Demo → Manual Times</b>\n\n{_times}{_rej_note}\n\n<i>🛡️ Capital protected</i>", reply_markup=_altd_btns); return
        send_reply(chat_id, "❌ Tap a button below 👇", reply_markup=_altd_btns); return

    elif cmd == "/tradelog" and (is_admin or is_co_admin(chat_id)):
        if not os.path.exists(TRADE_LOG_CSV):
            send_reply(chat_id, "📂 No trade history yet. Trades are logged automatically after first signal."); return
        try:
            import csv
            with open(TRADE_LOG_CSV, "r") as f:
                rows = list(csv.DictReader(f))
            if not rows:
                send_reply(chat_id, "📂 Trade log is empty."); return
            # Show last 10 rows as text summary
            lines = ["<b>Trade Log (last 10)</b>\n"]
            for r in rows[-10:]:
                coin = r.get("coin","?"); direction = r.get("direction","?")
                result = r.get("result","?"); sig_t = r.get("signal_time","")
                entry = r.get("entry_price",""); tp_type = r.get("type","")
                lines.append(f"[{tp_type}] {direction} {coin} @ {entry} → {result} | {sig_t}")
            send_reply(chat_id, "\n".join(lines) + f"\n\nTotal rows: {len(rows)}\nFile: trade_history.csv")
            # Send the actual CSV file
            with open(TRADE_LOG_CSV, "rb") as f:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
                    data={"chat_id": chat_id, "caption": "CLEXER Trade History"},
                    files={"document": ("trade_history.csv", f, "text/csv")}, timeout=30)
        except Exception as e:
            send_reply(chat_id, f"❌ Error reading trade log: {e}")
        return

    elif cmd == "/report" and is_admin:
        if not os.path.exists(API_COST_LOG):
            send_reply(chat_id, "📊 No API cost data yet. Data is logged from the next Claude call."); return
        try:
            import csv
            from collections import defaultdict
            with open(API_COST_LOG, "r") as f:
                rows = list(csv.DictReader(f))
            if not rows:
                send_reply(chat_id, "📊 API cost log is empty."); return
            # Group by date
            daily = defaultdict(lambda: {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0.0})
            for r in rows:
                d = r.get("date","?")
                daily[d]["calls"]        += 1
                daily[d]["input_tokens"] += int(r.get("input_tokens", 0))
                daily[d]["output_tokens"]+= int(r.get("output_tokens", 0))
                daily[d]["cost"]         += float(r.get("cost_usd", 0))
            # Build text summary (last 14 days)
            lines = ["📊 <b>Claude API Cost Report</b>\n"]
            total_cost = 0.0
            for date in sorted(daily.keys())[-14:]:
                d = daily[date]
                total_tok = d["input_tokens"] + d["output_tokens"]
                lines.append(
                    f"<b>{date}</b>  {d['calls']} calls  "
                    f"{total_tok:,} tokens  <b>${d['cost']:.4f}</b>")
                total_cost += d["cost"]
            lines.append(f"\n<b>Total (shown): ${total_cost:.4f}</b>")
            send_reply(chat_id, "\n".join(lines))
            # Send full CSV
            with open(API_COST_LOG, "rb") as f:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
                    data={"chat_id": chat_id, "caption": "CLEXER API Cost Log"},
                    files={"document": ("api_cost_log.csv", f, "text/csv")}, timeout=30)
        except Exception as e:
            send_reply(chat_id, f"❌ Error: {e}")
        return

    elif cmd == "/test" and is_scanadmin:
        global TEST_SCAN_ENABLED, _test_scan1_last_hour, _test_scan2_last_hour
        sub = parts[1].lower() if len(parts) > 1 else ""
        _test_btns = {"inline_keyboard": [
            [{"text": "🟢  ON", "callback_data": "test_on"}, {"text": "🔴  OFF", "callback_data": "test_off"}],
            [{"text": "🧪  Run Now", "callback_data": "test_run"}],
        ]}
        if sub == "on":
            TEST_SCAN_ENABLED = True
            _test_scan1_last_hour = -1; _test_scan2_last_hour = -1
            send_reply(chat_id,
                f"✅ <b>Test Mode ON</b> — CLEXER SCALP V1\n\n"
                f"Demo scans fire every hour at <b>:05 IST</b>\n"
                f"Signals tagged <b>[DEMO]</b> — no real trades placed.\n"
                f"Monitor checks TP1/SL/timeout every 30s.\n\n"
                f"<i>- CLEXER SCALP V1 TEST -</i>", reply_markup=_test_btns); return
        elif sub == "off":
            TEST_SCAN_ENABLED = False
            send_reply(chat_id,
                f"❌ <b>Test Mode OFF</b>\n\n"
                f"Demo scans stopped. Active demo trades still monitored until closed.\n\n"
                f"<i>- CLEXER SCALP V1 TEST -</i>", reply_markup=_test_btns); return
        elif sub == "run":
            send_reply(chat_id, "🧪 Triggering both demo scans now...")
            threading.Thread(target=lambda: _run_test_scan(chat_id, 1), daemon=True).start()
            time.sleep(3)
            threading.Thread(target=lambda: _run_test_scan(chat_id, 2), daemon=True).start()
            return
        else:
            # Show status
            all_demo = demo_scan1_trades + demo_scan2_trades
            state = "ON ✅" if TEST_SCAN_ENABLED else "OFF ❌"
            if not all_demo:
                trades_str = "No active demo trades."
            else:
                lines = []
                for t in all_demo:
                    sym   = t.get("symbol","?")
                    sig   = t.get("signal","?")
                    entry = t.get("entry",0)
                    sl    = t.get("sl",0)
                    tp1   = t.get("tp1",0)
                    tp1h  = "✅" if t.get("tp1_hit") else "⏳"
                    cp    = get_bingx_price(sym)
                    pnl   = (cp - entry) / entry * 100 * (1 if sig=="BUY" else -1) if entry and cp else 0
                    lines.append(f"{'🟢' if sig=='BUY' else '🔴'} {sym}  Entry:{entry:,.4g}  SL:{sl:,.4g}  TP1:{tp1h}  P/L:{pnl:+.2f}%")
                trades_str = "\n".join(lines)
            send_reply(chat_id,
                f"🧪 <b>Test Mode: {state}</b>\n"
                f"Fires at <b>:05 IST</b> hourly | Strategy: CLEXER SCALP V1\n\n"
                f"<b>Active Demo Trades ({len(all_demo)}):</b>\n"
                f"<pre>{trades_str}</pre>\n\n"
                f"<i>- CLEXER SCALP V1 TEST -</i>", reply_markup=_test_btns); return

    elif cmd in ("/scancopy", "/ctpause") and is_scanadmin:
        send_ctpause_screen(chat_id)

    elif cmd in ("/readindicators", "/checktvdata") and is_admin:
        if not TV_BRIDGE_URL or not tv_bridge_state["online"]:
            send_reply(chat_id, "❌ TV Bridge is offline."); return
        send_reply(chat_id, "🔍 <b>TV Data Audit starting…</b>\nFetching all data sources from TradingView bridge…")
        try:
            data     = fetch_tv_all_data()
            labels   = data.get("pine_labels", [])
            lines    = data.get("pine_lines",  [])
            boxes    = data.get("pine_boxes",  [])
            studies  = data.get("studies",     [])
            spaceman = fetch_spaceman_levels()
            ticker   = data.get("ticker",      {})
            candles  = data.get("candles",     {})

            # ── #3 SCREENSHOT ──────────────────────────────────────────────
            try:
                r3 = requests.get(f"{TV_BRIDGE_URL}/screenshot?interval=1H", timeout=20)
                shot_ok  = r3.status_code == 200 and r3.json().get("image_base64")
                shot_kb  = len(r3.json().get("image_base64","")) // 1024 if shot_ok else 0
            except Exception:
                shot_ok = False; shot_kb = 0

            # ── #4 INDICATORS ──────────────────────────────────────────────
            try:
                r4      = requests.get(f"{TV_BRIDGE_URL}/indicators", timeout=15)
                ind     = r4.json() if r4.status_code == 200 else {}
                clexer  = ind.get("clexer_sniper")
                poi     = ind.get("poi_vol_surge")
                raw_st  = ind.get("raw_studies", [])
                ind_ok  = bool(raw_st)
            except Exception:
                ind = {}; clexer = None; poi = None; raw_st = []; ind_ok = False

            # ── #5 PINE OBJECTS ────────────────────────────────────────────
            pine_ok = bool(labels or boxes)

            # ── #6 STUDIES ─────────────────────────────────────────────────
            studies_ok = bool(studies)

            # ── BUILD REPORT ───────────────────────────────────────────────
            msg = f"📊 <b>TV Data Audit</b>  🕐 {ist_str()}\n"
            msg += "─────────────────────────\n\n"

            # Candles + ticker (base)
            c_tfs = list(candles.keys())
            msg += f"✅ <b>#1 Candles:</b> {c_tfs} — {sum(len(v) for v in candles.values())} total bars\n"
            msg += f"{'✅' if ticker.get('price',0)>0 else '❌'} <b>#2 Ticker:</b> price={ticker.get('price',0):,.2f}\n\n"

            # #3 Screenshot
            msg += f"{'✅' if shot_ok else '❌'} <b>#3 Screenshot:</b> "
            if shot_ok:
                msg += f"{shot_kb}KB captured\n"
                msg += "  → <b>Used for:</b> sent to Claude as visual chart image (+10% accuracy)\n"
                msg += "  → <b>Alternative:</b> TradingView widget embed (Mini App chart tab already does this)\n"
                msg += "  → <b>Verdict:</b> Useful IF Claude vision improves signal quality. Remove if not.\n\n"
            else:
                msg += "NOT working (CDP issue or TV not open)\n\n"

            # #4 Indicators
            msg += f"{'✅' if ind_ok else '❌'} <b>#4 Indicators (DOM read):</b> {len(raw_st)} studies detected\n"
            if clexer:
                msg += f"  • CLEXER SNIPER: {str(clexer.get('value','?'))[:40]}\n"
            else:
                msg += "  • CLEXER SNIPER: ❌ not detected\n"
            if poi:
                msg += f"  • POI Vol Surge: {str(poi.get('value','?'))[:40]}\n"
            else:
                msg += "  • POI Vol Surge: ❌ not detected\n"
            msg += f"  → <b>SpacemanBTC levels:</b> "
            if spaceman.get("all_levels"):
                lvls = spaceman["all_levels"]
                msg += f"{len(lvls)} levels\n"
                for lv in lvls[:5]:
                    msg += f"    • {lv['label']}: {lv['price']:,.2f}\n"
                msg += "  → <b>Source: BingX OHLCV</b> ✅ (calculated from BingX candles directly — TV bridge NOT needed for this)\n\n"
            else:
                msg += "❌ 0 levels — BingX candle fetch may have failed\n\n"

            # #5 Pine Objects
            msg += f"{'✅' if pine_ok else '❌'} <b>#5 Pine Objects (labels/boxes):</b>\n"
            msg += f"  • Pine Labels: {len(labels)} (BOS/CHoCH/swing labels)\n"
            msg += f"  • Pine Boxes:  {len(boxes)} (OB/FVG zones)\n"
            if pine_ok:
                msg += "  → <b>Used for:</b> feeding OB/FVG zone prices into Claude analysis\n"
                msg += "  → <b>Alternative:</b> Calculate OB/FVG directly from raw OHLCV candles in bot (no TV needed). Library: pure Python, no indicator required.\n"
                msg += "  → <b>Verdict:</b> Can be replaced with candle-based OB/FVG detection.\n\n"
            else:
                msg += "  → Pine objects returning 0 — indicator not on chart or CDP can't read it.\n\n"

            # #6 Studies
            msg += f"{'✅' if studies_ok else '❌'} <b>#6 Studies (legend values):</b> {len(studies)} found\n"
            if studies_ok:
                for s in studies[:4]:
                    msg += f"  • {s.get('name','?')}\n"
                msg += "  → <b>Used for:</b> reading RSI/EMA/MACD values from chart legend\n"
                msg += "  → <b>Alternative:</b> Calculate RSI/EMA/MACD directly from OHLCV candles using pandas-ta or ta-lib (no TV needed, works on Railway).\n"
                msg += "  → <b>Verdict:</b> Fully replaceable. Remove TV dependency for these.\n\n"
            else:
                msg += "  → No studies detected from chart legend.\n\n"

            msg += "─────────────────────────\n"
            msg += "<b>Summary:</b>\n"
            msg += "• SpacemanBTC levels → BingX ✅ (no TV needed)\n"
            msg += "• Studies (#6) → replaceable with pandas-ta\n"
            msg += "• Pine boxes (#5) → replaceable with candle OB/FVG calc\n"
            msg += "• Screenshot (#3) → only reason to keep TV bridge\n"
            msg += "• Indicators (#4) → only if CLEXER SNIPER shown above is ✅"
            send_reply(chat_id, msg)
        except Exception as e:
            send_reply(chat_id, f"❌ Audit error: {e}")

    elif cmd == "/tvstudies" and is_admin:
        # Read RSI/EMA/MACD from TradingView chart legend via tv_bridge indicators endpoint
        if not TV_BRIDGE_URL or not tv_bridge_state["online"]:
            send_reply(chat_id, "❌ TV Bridge is offline."); return
        send_reply(chat_id, "📡 Reading indicators from TV chart legend…")
        try:
            import requests as _req
            ticker_r = _req.get(f"{TV_BRIDGE_URL}/ticker", timeout=10).json()
            price    = ticker_r.get("price", 0)

            # Try /indicators endpoint — reads raw legend text from chart DOM
            ind_r = _req.get(f"{TV_BRIDGE_URL}/indicators", timeout=10).json()
            inds  = ind_r if isinstance(ind_r, list) else ind_r.get("indicators", ind_r.get("data", []))

            # Also get studies for names
            try:
                st_r   = _req.get(f"{TV_BRIDGE_URL}/studies", timeout=10).json()
                studies = st_r if isinstance(st_r, list) else st_r.get("studies", [])
            except Exception:
                studies = []

            msg = f"📊 <b>TV Indicators (Chart Legend)</b>  🕐 {ist_str()}\n"
            msg += f"Price: <b>{price:,.2f}</b>\n\n"

            # Show raw indicator text from legend
            if inds:
                msg += f"<b>Raw legend values ({len(inds)} items):</b>\n"
                for item in inds[:20]:
                    if isinstance(item, dict):
                        name = item.get("name", item.get("title", "?"))
                        val  = item.get("value", item.get("values", item.get("output", "")))
                        if isinstance(val, list):
                            val = "  ".join(str(v) for v in val[:5])
                        msg += f"• <b>{name}</b>: {val}\n"
                    else:
                        msg += f"• {item}\n"
            elif studies:
                msg += f"<b>{len(studies)} indicators on chart:</b>\n"
                for s in studies:
                    name = s.get("name", "?")
                    vals = s.get("values", s.get("value", "—"))
                    if isinstance(vals, list):
                        vals = "  ".join(str(v) for v in vals[:5]) or "—"
                    msg += f"• <b>{name}</b>: {vals}\n"
                msg += "\n⚠️ <b>Values are empty because RSI/EMA/MACD are not on your chart.</b>\n"
                msg += "Add RSI, EMA, MACD to TradingView chart for legend readings.\n"
            else:
                msg += "❌ No indicators found from TV bridge.\n"

            msg += "\n<i>Run /calcstudies to get RSI/EMA/MACD calculated from BingX candles.</i>"
            send_reply(chat_id, msg)
        except Exception as e:
            send_reply(chat_id, f"❌ Error reading TV indicators: {e}")

    elif cmd == "/calcstudies" and is_admin:
        # Calculate RSI/EMA/MACD from BingX candles using pure pandas (no extra library)
        send_reply(chat_id, "🧮 Calculating studies from BingX candles…")
        try:
            import pandas as _pd

            def get_df(interval, limit=200):
                # get_candles returns a DataFrame with columns: open, high, low, close, vol
                df = get_candles(interval, limit)
                if df is None or len(df) < 10:
                    return None
                return df

            def calc(df, label):
                if df is None or len(df) < 30:
                    return f"❌ {label}: not enough candles ({len(df) if df is not None else 0})\n"
                c = df["close"].astype(float)
                h = df["high"].astype(float)
                l = df["low"].astype(float)
                v = df["vol"].astype(float) if "vol" in df.columns else _pd.Series([0]*len(c), index=c.index)

                # RSI 14
                delta = c.diff()
                gain  = delta.clip(lower=0).rolling(14).mean()
                loss  = (-delta.clip(upper=0)).rolling(14).mean()
                rsi   = (100 - 100 / (1 + gain / loss.replace(0, 1e-9))).iloc[-1]

                # EMA
                ema20  = c.ewm(span=20,  adjust=False).mean().iloc[-1]
                ema50  = c.ewm(span=50,  adjust=False).mean().iloc[-1]
                ema200 = c.ewm(span=200, adjust=False).mean().iloc[-1] if len(c) >= 200 else None

                # MACD 12,26,9
                ema12   = c.ewm(span=12, adjust=False).mean()
                ema26   = c.ewm(span=26, adjust=False).mean()
                macd    = (ema12 - ema26).iloc[-1]
                signal  = (ema12 - ema26).ewm(span=9, adjust=False).mean().iloc[-1]
                hist    = macd - signal

                # ATR 14
                tr   = _pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
                atr  = tr.rolling(14).mean().iloc[-1]

                # Volume
                vol_sma = v.rolling(20).mean().iloc[-1]
                vol_rel = v.iloc[-1] / vol_sma if vol_sma > 0 else 0

                trend = "ABOVE" if c.iloc[-1] > ema50 else "BELOW"

                out  = f"<b>{label}</b>\n"
                out += f"  RSI(14):  <b>{rsi:.1f}</b>  {'⚡ OB' if rsi > 70 else '🔵 OS' if rsi < 30 else '—'}\n"
                out += f"  EMA20:    <b>{ema20:,.2f}</b>\n"
                out += f"  EMA50:    <b>{ema50:,.2f}</b>  Price {trend} EMA50\n"
                if ema200:
                    out += f"  EMA200:   <b>{ema200:,.2f}</b>\n"
                out += f"  MACD:     <b>{macd:+.2f}</b>  Sig: {signal:+.2f}  Hist: {hist:+.2f}  {'📈' if hist > 0 else '📉'}\n"
                out += f"  ATR(14):  <b>{atr:,.2f}</b>\n"
                out += f"  Volume:   {v.iloc[-1]:,.0f}  ({vol_rel:.1f}x avg)\n"
                return out

            msg = f"🧮 <b>Calculated Studies — BingX Candles</b>  🕐 {ist_str()}\n\n"
            msg += calc(get_df("1h",  200), "1H Timeframe") + "\n"
            msg += calc(get_df("4h",  100), "4H Timeframe") + "\n"
            msg += calc(get_df("5m",   50), "5M Timeframe")
            msg += "\n\n<i>Source: BingX OHLCV — TV bridge not needed.</i>"
            msg += "\n<i>Run /tvstudies to compare with chart legend.</i>"
            send_reply(chat_id, msg)
        except Exception as e:
            send_reply(chat_id, f"❌ Calc error: {e}")

    elif cmd == "/scantv" and is_admin:
        global SCAN_USE_TV
        args = text.strip().split()
        if len(args) < 2 or args[1].lower() not in ("on","off"):
            status = "🟢 ON (TV Bridge)" if SCAN_USE_TV else "🔵 OFF (BingX)"
            send_reply(chat_id,
                f"📡 <b>Scan TV Mode</b>\n\n"
                f"Current: <b>{status}</b>\n\n"
                f"• /scantv on  — scan uses TV bridge (candles + screenshots)\n"
                f"• /scantv off — scan uses BingX only (default, no TV needed)")
            return
        new_val = args[1].lower() == "on"
        SCAN_USE_TV = new_val
        if new_val:
            tv_ok = is_tv_online()
            send_reply(chat_id,
                f"✅ <b>Scan TV Mode: ON</b>\n\n"
                f"Scan will now use TV bridge for candles + screenshots.\n"
                f"TV Bridge status: {'🟢 Online' if tv_ok else '🔴 Offline — start it before scanning'}")
        else:
            send_reply(chat_id,
                f"✅ <b>Scan TV Mode: OFF</b>\n\n"
                f"Scan now uses BingX candles directly.\n"
                f"TV bridge not needed. Works anytime.")

    elif cmd in ("/scan", "/scan1", "/scan2", "/scantoggle") and is_scanadmin:
        if cmd == "/scantoggle":
            _arg = parts[1].lower() if len(parts) > 1 else ""
            if _arg == "scan1on":
                SCAN1_AUTO_ENABLED = True
            elif _arg == "scan1off":
                SCAN1_AUTO_ENABLED = False
            elif _arg == "scan2on":
                SCAN2_AUTO_ENABLED = True
            elif _arg == "scan2off":
                SCAN2_AUTO_ENABLED = False
            elif _arg == "teston":
                TEST_SCAN_ENABLED = True
            elif _arg == "testoff":
                TEST_SCAN_ENABLED = False
            if _arg: save_settings()
            _s1 = "✅ ON" if SCAN1_AUTO_ENABLED else "❌ OFF"
            _s2 = "✅ ON" if SCAN2_AUTO_ENABLED else "❌ OFF"
            _ts = "✅ ON" if TEST_SCAN_ENABLED else "❌ OFF"
            _mkp = {"inline_keyboard": [
                [{"text": f"🔍 Scan1  {_s1}", "callback_data": "noop"}, {"text": "🟢 ON", "callback_data": "scantoggle:scan1on"}, {"text": "🔴 OFF", "callback_data": "scantoggle:scan1off"}],
                [{"text": f"🔍 Scan2  {_s2}", "callback_data": "noop"}, {"text": "🟢 ON", "callback_data": "scantoggle:scan2on"}, {"text": "🔴 OFF", "callback_data": "scantoggle:scan2off"}],
                [{"text": f"🧪 Demo   {_ts}", "callback_data": "noop"}, {"text": "🟢 ON", "callback_data": "scantoggle:teston"},  {"text": "🔴 OFF", "callback_data": "scantoggle:testoff"}],
            ]}
            send_reply(chat_id,
                f"<b>Scan Toggle</b>\n\n"
                f"🔍 Scan1 Auto  —  <b>{_s1}</b>\n"
                f"🔍 Scan2 Auto  —  <b>{_s2}</b>\n"
                f"🧪 Demo Trade  —  <b>{_ts}</b>\n\n"
                f"<i>🛡️ Capital protected</i>", reply_markup=_mkp)
            return

        if cmd == "/scan":
            # Force-run both scan1 and scan2 back-to-back
            send_reply(chat_id, "📡 Force-running Scan1 + Scan2 (~3 min total)...")
            threading.Thread(target=lambda: handle_command("/scan1", chat_id), daemon=True).start()
            threading.Thread(target=lambda: handle_command("/scan2", chat_id), daemon=True).start()
            return
        ver = 1 if cmd == "/scan1" else 2
        lbl = "V1 (big movers)" if ver == 1 else "V2 (fresh momentum)"
        send_reply(chat_id, f"📡 Scanning BingX — {lbl} (~60s)...")
        def _do_scan(cid=chat_id, scan_ver=ver):
            try:
                import traceback as _tb, pandas as _pd

                # ── helpers ────────────────────────────────────────────────────
                get_klines = bingx_klines   # use module-level function

                def check_4h_structure(df):
                    """Returns BULLISH, BEARISH, or NEUTRAL based on swing highs/lows + close trend."""
                    if df is None or len(df) < 8: return "NEUTRAL"
                    h   = df["high"].values[-15:]
                    l   = df["low"].values[-15:]
                    cls = df["close"].values[-15:]
                    sh = []; sl = []
                    for i in range(1, len(h)-1):
                        if h[i] > h[i-1] and h[i] > h[i+1]: sh.append(h[i])
                        if l[i] < l[i-1] and l[i] < l[i+1]: sl.append(l[i])
                    swing_result = "NEUTRAL"
                    if len(sh) >= 2 and len(sl) >= 2:
                        if sh[-1] > sh[-2] and sl[-1] > sl[-2]: swing_result = "BULLISH"
                        if sh[-1] < sh[-2] and sl[-1] < sl[-2]: swing_result = "BEARISH"
                    # Overall close trend: compare last close vs midpoint close
                    mid = cls[len(cls)//2]
                    if mid > 0:
                        trend_pct = (cls[-1] - mid) / mid * 100
                        if trend_pct < -5:   close_trend = "BEARISH"
                        elif trend_pct > 5:  close_trend = "BULLISH"
                        else:                close_trend = "NEUTRAL"
                    else:
                        close_trend = "NEUTRAL"
                    # If swing and close trend agree → return that
                    if swing_result == close_trend: return swing_result
                    # If swing says BULLISH but closes are clearly falling → trust closes
                    if swing_result == "BULLISH" and close_trend == "BEARISH": return "BEARISH"
                    if swing_result == "BEARISH" and close_trend == "BULLISH": return "BULLISH"
                    # One is NEUTRAL → use whichever has a signal
                    if swing_result != "NEUTRAL": return swing_result
                    if close_trend != "NEUTRAL":  return close_trend
                    return "NEUTRAL"

                def is_momentum(df):
                    """True if last 1-2 4H candles moved >5%."""
                    if df is None or len(df) < 2: return False
                    for i in [-1,-2]:
                        r = df.iloc[i]
                        if r["open"] > 0 and abs(r["close"]-r["open"])/r["open"]*100 > 5:
                            return True
                    return False

                # ── Step 1: Get all BingX tickers, score = |change| × volume ──
                r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
                                 timeout=15).json()
                skip = {"USDC","BUSD","DAI","TUSD","FDUSD","USDP","FRAX","USDT","BTC","BTCDOM"}
                movers = []
                for t in r.get("data",[]):
                    sym = t.get("symbol","")
                    if not sym.endswith("-USDT"): continue
                    base = sym.replace("-USDT","")
                    if base in skip: continue
                    vol  = float(t.get("quoteVolume",0) or t.get("volume",0) or 0)
                    chg  = float(t.get("priceChangePercent",0) or 0)
                    px   = float(t.get("lastPrice",0) or 0)
                    # Hard filters — reject ghost/phantom listings
                    if vol < 2_000_000: continue          # min $2M real volume
                    if px <= 0: continue
                    if abs(chg) > 200: continue           # >200% change = bad data / scam pump
                    import re as _re
                    # Reject tickers that look like structured products / RWAs / phantom symbols
                    if _re.search(r'\d', base): continue          # ANY digit in base = phantom (e.g. NCSKSPCX2USD)
                    if len(base) > 10: continue                   # real coins: BTC ETH SOL etc — max 10 chars
                    if "USD" in base: continue                    # base already contains USD = garbage symbol
                    if not _re.match(r'^[A-Z]+$', base): continue # must be all uppercase letters only
                    import math as _math
                    if scan_ver == 1:
                        # V1 — original: rewards biggest movers (high % change × volume)
                        score = (abs(chg) ** 1.5) * (_math.sqrt(vol / 1e6))
                    else:
                        # V2 — fresh momentum: skip coins already extended >15%
                        if abs(chg) > 15: continue
                        freshness = 1.0 if 2 <= abs(chg) <= 10 else 0.6
                        score = _math.sqrt(vol / 1e6) * (abs(chg) ** 0.8) * freshness
                    movers.append({"sym":sym,"base":base,"price":px,
                                   "change":chg,"vol_m":round(vol/1e6,1),"score":score})
                movers.sort(key=lambda x: x["score"], reverse=True)
                top10 = movers[:10]

                if not top10:
                    send_reply(cid, "❌ No coins found from BingX ticker (API issue?)"); return

                # Show top 10 so user can verify real data was fetched
                top_lines = "\n".join(
                    f"{i+1}. {t['base']:8s} {t['change']:+.2f}%  Vol:${t['vol_m']}M  Score:{t['score']:.0f}"
                    for i, t in enumerate(top10))
                send_reply(cid, f"📈 <b>Top 10 by score (|change|×vol):</b>\n<pre>{top_lines}</pre>")

                # ── Step 2: Check 4H structure for each, pick best ─────────────
                send_reply(cid, f"📊 Fetching 4H candles & checking structure...")
                structured = []   # coins with clean 4H structure
                all_with_data = []
                kline_ok = 0
                for t in top10:
                    df4 = get_klines(t["sym"], "4h", 30)
                    if df4 is not None: kline_ok += 1
                    struct = check_4h_structure(df4)
                    mom    = is_momentum(df4)
                    t["structure"] = struct
                    t["momentum"]  = mom
                    t["df4h"]      = df4
                    all_with_data.append(t)
                    if struct != "NEUTRAL":
                        structured.append(t)
                    print(f"  [SCAN] {t['base']}: klines={'OK' if df4 is not None else 'FAIL'} score={t['score']:.0f} struct={struct} mom={mom}")

                struct_lines = "\n".join(
                    f"{'✅' if t['structure']!='NEUTRAL' else '⬜'} {t['base']:8s} {t['structure']:8s} {'⚡' if t['momentum'] else ''}"
                    for t in all_with_data)
                send_reply(cid,
                    f"🔍 Structure results ({kline_ok}/10 kline OK):\n<pre>{struct_lines}</pre>\n"
                    f"{'✅ Found structured coins!' if structured else '⚠️ All NEUTRAL — picking highest score'}")

                # Build candidate order: structured first (by score), then rest
                candidate_order = structured + [c for c in all_with_data if c not in structured]
                btc_price = get_ticker()["price"]

                # ── Block if TV mode ON but bridge offline ────────────────────
                if SCAN_USE_TV and not is_tv_online():
                    send_reply(cid,
                        f"📴 <b>TradingView Offline — Scan{scan_ver} blocked</b>\n\n"
                        f"TV mode is ON. Start TV bridge or run /scantv off to use BingX mode.\n\n<i>🛡️ Capital protected</i>")
                    return

                # ── Check slot availability (scan1=6 slots, scan2=6 slots) ──────
                _max_slots = 6
                my_list = _scan_list(scan_ver)
                if len(my_list) >= _max_slots:
                    send_reply(cid,
                        f"🚫 <b>Scan{scan_ver} slots full ({_max_slots}/{_max_slots})</b>\n\n" +
                        "\n".join(f"  {'🟢' if x['signal']=='BUY' else '🔴'} {x['symbol']}" for x in my_list) +
                        f"\n\nWaiting for a trade to close before scanning again.\n\n<i>🛡️ Capital protected</i>")
                    return

                # ── Remind about running trades ───────────────────────────────
                if my_list:
                    lines = "\n".join(
                        f"{'🟢' if x['signal']=='BUY' else '🔴'} {x['signal']} {x['symbol']} "
                        f"| Entry:{x['entry']:,.4g} SL:{x['sl']:,.4g} "
                        f"TP1:{'✅' if x.get('tp1_hit') else '⏳'} TP2:⏳"
                        for x in my_list
                    )
                    send_reply(cid, f"📌 <b>Scan{scan_ver} running trades:</b>\n<pre>{lines}</pre>")

                mode_label = "TV Bridge" if SCAN_USE_TV else "BingX"
                send_reply(cid, f"🔍 Trying up to 3 coins for MARKET entry ({mode_label})...")

                # ── Try candidates one by one until Claude approves one ────────
                MAX_TRIES = 3
                signal_placed = False
                tried = []
                skip_log = []   # tracks why each coin was skipped
                api_fail_count = 0  # coins skipped because Claude itself failed 3x — not a real "no signal"

                for candidate in candidate_order[:MAX_TRIES + 3]:  # a few extras in case of skips
                    if signal_placed: break
                    if len(tried) >= MAX_TRIES: break

                    chosen_base = candidate["base"]
                    chosen_sym  = candidate["sym"]
                    cp          = candidate["price"]

                    # Skip if already in active trade
                    if chosen_sym in _all_active_scan_syms():
                        skip_log.append(f"⏭ {chosen_sym}: already in active trade")
                        print(f"  [SCAN] Skip {chosen_sym} — already active")
                        continue

                    tried.append(chosen_sym)
                    conf_note = "" if candidate.get("structure") != "NEUTRAL" else " (no clear structure)"
                    send_reply(cid, f"🎯 Trying #{len(tried)}: <b>{chosen_sym}</b>{conf_note} — fetching candles...")

                    # ── Step 3: Fetch candles ─────────────────────────────────
                    _tv_data_source = "BingX"
                    scan_screenshots = {}
                    tv_switched = False

                    if SCAN_USE_TV and is_tv_online():
                        with _tv_chart_lock:
                            tv_sym = f"BINGX:{chosen_base}USDT.P"
                            tv_switched = tv_set_symbol(tv_sym) or tv_set_symbol(f"{chosen_base}USDT")
                            print(f"  [SCAN] TV switch {chosen_sym}: {'OK' if tv_switched else 'FAIL'}")
                            if tv_switched and is_tv_online():
                                _t4 = tv_get_candles_for(tv_sym,"4h",60, live_price=cp)
                                _t1 = tv_get_candles_for(tv_sym,"1h",40, live_price=cp)
                                _t5 = tv_get_candles_for(tv_sym,"5m",30, live_price=cp)
                                if _t4 is None or _t1 is None:
                                    _c4 = candidate.get("df4h")
                                    df_4h = _c4 if _c4 is not None else get_klines(chosen_sym,"4h",60)
                                    df_1h = get_klines(chosen_sym,"1h",40)
                                    df_5m = get_klines(chosen_sym,"5m",30)
                                    _tv_data_source = "BingX(TV-fallback)"
                                else:
                                    df_4h = _t4; df_1h = _t1
                                    df_5m = _t5 if _t5 is not None else get_klines(chosen_sym,"5m",30)
                                    scan_screenshots = fetch_tv_screenshots()
                                    _tv_data_source = "TradingView"
                            else:
                                _c4 = candidate.get("df4h")
                                df_4h = _c4 if _c4 is not None else get_klines(chosen_sym,"4h",60)
                                df_1h = get_klines(chosen_sym,"1h",40)
                                df_5m = get_klines(chosen_sym,"5m",30)
                                _tv_data_source = "BingX(TV-switch-fail)"
                            tv_set_symbol("BINGX:BTCUSDT.P")
                    else:
                        _c4 = candidate.get("df4h")
                        df_4h = _c4 if _c4 is not None else get_klines(chosen_sym,"4h",60)
                        df_1h = get_klines(chosen_sym,"1h",40)
                        df_5m = get_klines(chosen_sym,"5m",30)
                        _tv_data_source = "BingX"

                    print(f"  [SCAN] {chosen_sym} candle source: {_tv_data_source}")

                    # ── Data integrity check ───────────────────────────────────
                    def _integrity_ok(d4, d1, d5, live_px, label=""):
                        for tf_name, df in [("4H", d4), ("1H", d1), ("5M", d5)]:
                            if df is None or len(df) == 0: continue
                            last_close = float(df["close"].iloc[-1])
                            diff = abs(last_close - live_px) / live_px * 100
                            if diff > 8.0:
                                print(f"  [INTEGRITY{label}] ❌ {tf_name} mismatch {chosen_sym}: close={last_close:.4g} live={live_px:.4g} diff={diff:.1f}%")
                                return False
                        return True

                    if not _integrity_ok(df_4h, df_1h, df_5m, cp, f" {chosen_sym}"):
                        skip_log.append(f"⚠️ {chosen_sym}: candle data mismatch (integrity fail)")
                        print(f"  [SCAN] {chosen_sym} integrity fail — trying next coin")
                        continue   # skip to next candidate

                    # ── Step 4: Build data summary ─────────────────────────────
                    smc = (f"=== {chosen_sym} DATA SUMMARY ===\n"
                           f"Price: {cp:,.6g}\n"
                           f"24h Change: {candidate['change']:+.2f}%\n"
                           f"Volume (24h): ${candidate['vol_m']}M\n"
                           f"4H Structure: {candidate['structure']}\n"
                           f"Momentum move (>5% candle): {'YES' if candidate['momentum'] else 'NO'}\n"
                           f"Candle source: {_tv_data_source}\n")

                    if df_4h is not None and len(df_4h) >= 10:
                        h4 = df_4h
                        highs4=h4["high"].values; lows4=h4["low"].values; cls4=h4["close"].values; ops4=h4["open"].values
                        tr4=[max(h4["high"].iloc[i]-h4["low"].iloc[i],abs(h4["high"].iloc[i]-cls4[i-1]),abs(h4["low"].iloc[i]-cls4[i-1])) for i in range(1,min(20,len(h4)))]
                        atr4=sum(tr4)/len(tr4) if tr4 else 0
                        sh=[]; sl_p=[]
                        for i in range(1,len(highs4)-1):
                            if highs4[i]>highs4[i-1] and highs4[i]>highs4[i+1]: sh.append(highs4[i])
                            if lows4[i]<lows4[i-1] and lows4[i]<lows4[i+1]: sl_p.append(lows4[i])
                        vols4=h4["volume"].values[-10:]; bidx=int(vols4.argmax())
                        bdir="GREEN" if cls4[-10+bidx]>ops4[-10+bidx] else "RED"
                        last2=[f"{i}: open={ops4[i]:,.4g} close={cls4[i]:,.4g} high={highs4[i]:,.4g} low={lows4[i]:,.4g} move={abs(cls4[i]-ops4[i])/ops4[i]*100:.2f}%" for i in [-2,-1]]
                        smc+=(f"\n--- 4H CANDLES ---\nLast 2:\n  {last2[0]}\n  {last2[1]}\n"
                              f"4H ATR: {atr4:,.4g}\nSwing highs: {[round(x,4) for x in sh[-4:]]}\n"
                              f"Swing lows: {[round(x,4) for x in sl_p[-4:]]}\n"
                              f"Last 5 closes: {[round(x,4) for x in cls4[-5:].tolist()]}\n"
                              f"Big vol candle: {bdir} ({10-bidx} bars ago)\n")

                    if df_1h is not None and len(df_1h) >= 5:
                        h1=df_1h; c1=h1["close"].values; h1_h=h1["high"].values; h1_l=h1["low"].values
                        atr1=[max(h1["high"].iloc[i]-h1["low"].iloc[i],abs(h1["high"].iloc[i]-c1[i-1]),abs(h1["low"].iloc[i]-c1[i-1])) for i in range(1,min(15,len(h1)))]
                        atr1v=sum(atr1)/len(atr1) if atr1 else 0
                        sh1=[]; sl1=[]
                        for i in range(1,len(h1_h)-1):
                            if h1_h[i]>h1_h[i-1] and h1_h[i]>h1_h[i+1]: sh1.append(h1_h[i])
                            if h1_l[i]<h1_l[i-1] and h1_l[i]<h1_l[i+1]: sl1.append(h1_l[i])
                        smc+=(f"\n--- 1H CANDLES ---\nATR_1H: {atr1v:,.4g}\n"
                              f"1H swing highs: {[round(x,4) for x in sh1[-4:]]}\n"
                              f"1H swing lows: {[round(x,4) for x in sl1[-4:]]}\n"
                              f"Last 5 closes: {[round(x,4) for x in c1[-5:].tolist()]}\n")

                    if df_5m is not None and len(df_5m) >= 5:
                        c5=df_5m["close"].values; h5=df_5m["high"].values; l5=df_5m["low"].values
                        last10_5m=[f"  [{i}] H:{h5[i]:,.4g} L:{l5[i]:,.4g} C:{c5[i]:,.4g}" for i in range(max(-10,-len(c5)),0)]
                        smc+=f"\n--- 5M (last 10 candles, newest last) ---\n"+"\n".join(last10_5m)+"\n"

                    # ── Step 4b: Pre-filter — block post-pump exhaustion ───────
                    if BTC_PROMPT_MODE != "V7" and df_4h is not None and len(df_4h) >= 10:
                        _c4v=df_4h["close"].values; _o4v=df_4h["open"].values
                        _last_move_pct=(_c4v[-1]-_o4v[-1])/_o4v[-1]*100
                        _gain_10=(_c4v[-1]-_c4v[-10])/_c4v[-10]*100
                        _skip_reason=None
                        if _last_move_pct < -8 and _gain_10 > 30:
                            _skip_reason=f"post-pump rejection {_last_move_pct:.1f}% after +{_gain_10:.0f}% rally"
                        elif _last_move_pct < -10:
                            _skip_reason=f"large rejection candle {_last_move_pct:.1f}%"
                        elif _gain_10 > 40 and _last_move_pct < 0:
                            _skip_reason=f"parabolic +{_gain_10:.0f}% with red close"
                        if _skip_reason:
                            skip_log.append(f"🚫 {chosen_sym}: pre-filter blocked ({_skip_reason})")
                            print(f"  [SCAN] {chosen_sym} pre-filter blocked: {_skip_reason} — trying next")
                            send_reply(cid, f"⏸ {chosen_sym} blocked ({_skip_reason}) — trying next coin...")
                            continue   # try next candidate

                    # ── Step 5: Claude analysis — IS THIS COIN READY NOW? ──────
                    if BTC_PROMPT_MODE == "V7":
                        analysis_prompt = f"""{smc}
BTC: ${btc_price:,.0f} | Session: {get_session()} | Current price: {cp:,.6g}

You are CLEXER. Analyze {chosen_sym} for MARKET entry at current price {cp:,.6g}.
Entry is ALWAYS market price — no pullback, no limit orders.
Reply ONLY with the output block below. No steps, no working, no extra text.

RULES (apply internally, do not output):
- 4H: HH+HL=BULLISH, LH+LL=BEARISH, unclear=WAIT
- 1H must agree with 4H or be neutral. Opposite=WAIT.
- 5M last 10 candles: higher lows forming=BUY, lower highs forming=SELL, choppy=WAIT
- SL = lowest 5M low (BUY) or highest 5M high (SELL) from last 5 candles. Min 1.5% max 4% of entry. +0.3% buffer.
- TP1 = entry ± sl_dist×1.5. TP2 = entry ± sl_dist×3
- If any condition unclear → Signal: WAIT

OUTPUT (copy exactly, replace bracketed values):
Signal: BUY / SELL / WAIT
Entry: {cp:,.6g}
Entry_Type: MARKET
SL: [number only]
TP1: [number only]
TP2: [number only]
R:R: [number only]
Confidence: HIGH / MED / LOW
Reasoning: [one line]"""
                        _max_tokens = 200
                    else:
                        analysis_prompt = f"""{smc}
BTC: ${btc_price:,.0f} | Session: {get_session()} | Current price: {cp:,.6g}

You are CLEXER. Analyze {chosen_sym}. Decide: is this coin ready for MARKET entry RIGHT NOW?
If not → WAIT. Do not force. Another coin will be tried. Go directly to output.

RULES:
1. 4H trend: HH+HL=BULLISH, LH+LL=BEARISH, unclear=WAIT
2. 1H: must agree with 4H or be neutral. Opposite=WAIT
3. 5M NOW: higher lows forming=BUY ready, lower highs forming=SELL ready, choppy/mixed=WAIT
4. Entry = {cp:,.6g} (MARKET, fills now)
5. SL = lowest low of last 3-5 x 5M candles (BUY) or highest high (SELL). Min 1.5%, Max 4%. +0.3% buffer.
6. TP1 = entry ± sl_dist×1.5. TP2 = entry ± sl_dist×3
7. Confidence: HIGH=all 3 TFs agree clearly. MED=4H+1H agree, 5M forming. LOW=only 4H clear.
8. HARD BLOCK→WAIT: last 4H candle <-6%, price fell >10% in 2 candles, 4H/1H opposite, 5M choppy.

OUTPUT ONLY (no steps, no working, replace bracketed values):
Signal: BUY / SELL / WAIT
Entry: {cp:,.6g}
Entry_Type: MARKET
SL: [number only]
TP1: [number only]
TP2: [number only]
R:R: [number only]
Confidence: HIGH / MED / LOW
Reasoning: [one line]"""
                        _max_tokens = 200

                    content = []
                    if scan_screenshots:
                        for tf in ["4H","1H","5"]:
                            img_b64 = scan_screenshots.get(tf)
                            if not img_b64: continue
                            content.append({"type":"text","text":f"=== {chosen_sym} {tf} CHART ==="})
                            content.append({"type":"image","source":{"type":"base64","media_type":"image/png","data":img_b64}})
                    content.append({"type":"text","text":analysis_prompt})

                    analysis = ""
                    _claude_ok = False
                    _last_claude_err = ""
                    for _attempt in range(3):
                        try:
                            r2 = _claude_client(f"scan{scan_ver}").messages.create(
                                model=_ai_model(f"scan{scan_ver}"), max_tokens=_max_tokens,
                                messages=[{"role":"user","content":content}])
                            _log_api_usage(f"scan{scan_ver}_{chosen_sym}", _ai_model(f"scan{scan_ver}"),
                                           r2.usage.input_tokens, r2.usage.output_tokens)
                            analysis = _claude_text(r2)
                            _claude_ok = True
                            break
                        except Exception as _ce:
                            _last_claude_err = str(_ce)
                            print(f"  [SCAN] Claude attempt {_attempt+1} FAIL: {_last_claude_err}")
                            if _attempt < 2:
                                time.sleep(10)
                    if not _claude_ok:
                        print(f"  [SCAN] {chosen_sym}: Claude failed 3 times — skipping coin")
                        api_fail_count += 1
                        skip_log.append(f"🔴 {chosen_sym}: Claude API call failed 3x — NOT analyzed (last error: {_last_claude_err[:120]})")
                        continue

                    import re as _re
                    _analysis_clean = analysis.replace(",", "")  # strip thousand-sep commas before parsing
                    def _parse(label):
                        m = _re.search(rf"{label}[:\s]+([0-9.]+)", _analysis_clean, _re.IGNORECASE)
                        return float(m.group(1)) if m else 0.0
                    sig_m = _re.search(r"Signal[:\s]+(BUY|SELL|WAIT)", analysis, _re.IGNORECASE)
                    scan_signal_val = sig_m.group(1).upper() if sig_m else "WAIT"

                    emoji = "🟢" if candidate["change"] >= 0 else "🔴"
                    tv_src = "TV" if tv_switched else "BingX"
                    send_reply(cid,
                        f"{emoji} <b>#{chosen_sym}</b> #{len(tried)}  <b>Scan{scan_ver}</b>  {ist_str()}\n\n"
                        f"Price: <b>${cp:,.6g}</b> ({candidate['change']:+.2f}%) | {tv_src}\n\n"
                        f"<pre>{analysis[:900]}</pre>\n\n"
                        f"🛡️ <i>Capital protected</i>")

                    if scan_signal_val == "WAIT":
                        # Extract reasoning from Claude's analysis for the skip log
                        _wait_reason = ""
                        _r_match = _re.search(r"[Rr]easoning[:\s]+(.+)", analysis)
                        if _r_match: _wait_reason = _r_match.group(1).strip()[:80]
                        skip_log.append(f"⏸ {chosen_sym}: Claude → WAIT" + (f" ({_wait_reason})" if _wait_reason else ""))
                        print(f"  [SCAN] {chosen_sym} → WAIT — trying next coin")
                        continue   # try next candidate

                    # ── Dedup: skip if other scan version already signaled this coin this cycle ──
                    with _scan_cycle_lock:
                        if chosen_sym in _scan_cycle_placed:
                            skip_log.append(f"⏭ {chosen_sym}: already signaled by other scan this cycle")
                            print(f"  [SCAN] {chosen_sym} already signaled by other scan this cycle — skipping")
                            continue
                        _scan_cycle_placed.add(chosen_sym)

                    # ── BUY or SELL — place trade ──────────────────────────────
                    scan_entry = cp   # always live price
                    scan_sl    = _parse("SL")
                    entry_type = "MARKET"

                    # Zone entry: the actual order price is the LOWER bound of the zone,
                    # not the midpoint — e.g. a 13-18 zone places the order at 13.
                    _etype = "ZONE" if ZONE_ENTRY_ENABLED else "MARKET"
                    _zone_lo = _zone_hi = None
                    if _etype == "ZONE":
                        _zone_lo = round(scan_entry * (1 - _ZONE_BAND_PCT), 6)
                        _zone_hi = round(scan_entry * (1 + _ZONE_BAND_PCT), 6)
                        scan_entry = _zone_lo  # order placed at the zone's lower bound

                    if scan_sl > 0:
                        sl_dist = abs(scan_entry - scan_sl)
                        sl_pct  = sl_dist / scan_entry * 100
                        if sl_pct < 1.0:
                            skip_log.append(f"⚠️ {chosen_sym}: SL {sl_pct:.2f}% < 1.0% — too tight, skipping")
                            print(f"  [SCAN] {chosen_sym}: SL {sl_pct:.2f}% < 1.0% — skipping")
                            continue
                        if sl_pct > 5.0:
                            skip_log.append(f"⚠️ {chosen_sym}: SL {sl_pct:.2f}% > 5.0% — too loose, skipping")
                            print(f"  [SCAN] {chosen_sym}: SL {sl_pct:.2f}% > 5.0% — skipping")
                            continue
                        scan_tp1 = round(scan_entry + sl_dist*1.5 if scan_signal_val=="BUY"
                                         else scan_entry - sl_dist*1.5, 6)
                        scan_tp2 = round(scan_entry + sl_dist*3.0 if scan_signal_val=="BUY"
                                         else scan_entry - sl_dist*3.0, 6)

                        sd = {"signal":scan_signal_val,"entry":scan_entry,
                              "sl":scan_sl,"tp1":scan_tp1,"tp2":scan_tp2,"entry_type":_etype}
                        slot_data = {
                            "symbol":chosen_sym,"signal":scan_signal_val,
                            "entry":scan_entry,"sl":scan_sl,"tp1":scan_tp1,"tp2":scan_tp2,
                            "entry_type":_etype,"zone_lo":_zone_lo,"zone_hi":_zone_hi,"tp1_hit":False,
                            "entry_hit":True,"created_at":time.time(),"ver":scan_ver,
                        }
                        _scan_list(scan_ver).append(slot_data)
                        trade_stats["scan_signals"] += 1
                        trade_stats[f"scan{scan_ver}_signals"] += 1
                        save_state()
                        _share_free = _free_quota_available()
                        if _share_free: _consume_free_quota()
                        send_telegram(fmt_scan_signal(slot_data))
                        send_to_tier_channels(fmt_scan_signal(slot_data), _share_free)
                        log_trade_event({"type": f"scan{scan_ver}", "coin": chosen_sym,
                            "direction": scan_signal_val, "signal_time": _ist_str_now(),
                            "entry_price": scan_entry, "sl_price": scan_sl,
                            "tp1_price": scan_tp1, "tp2_price": scan_tp2,
                            "entry_trigger_time": _ist_str_now(), "result": "open"})
                        sd["ver"] = scan_ver
                        ct_results = ct.on_scan_signal(sd, chosen_sym, cp, _share_free)
                        send_reply(cid, f"📋 <b>Copy Trade ({chosen_sym}):</b>\n"+"\n".join(ct_results[:5]))
                        # Send skip summary explaining why previous coins were skipped
                        if skip_log:
                            send_reply(cid,
                                f"📋 <b>Why {chosen_sym} was picked:</b>\n\n"
                                + "\n".join(skip_log) + f"\n\n✅ <b>{chosen_sym}</b>: Claude → {scan_signal_val} — signal placed")
                        signal_placed = True
                    else:
                        skip_log.append(f"⚠️ {chosen_sym}: could not parse SL from Claude output")
                        print(f"  [SCAN] {chosen_sym} — could not parse SL, trying next")
                        continue

                # ── No coin found after trying all candidates ──────────────────
                if not signal_placed:
                    tried_str = ", ".join(tried) if tried else "none"
                    if api_fail_count > 0 and api_fail_count >= len(tried):
                        # Every single candidate failed at the API call — this is NOT
                        # "no clean setup", it's Claude/the gateway not responding at all.
                        send_reply(cid,
                            f"🔴 <b>Claude API failed — no analysis ran</b>  {ist_str()}\n\n"
                            f"Tried {len(tried)} coin(s): <b>{tried_str}</b> — every one failed 3 retry "
                            f"attempts at the Claude API call itself. <b>No chart was actually analyzed.</b>\n\n"
                            + "\n".join(skip_log[-len(tried):]) +
                            f"\n\n⚠️ Check your AI Model / Gateway settings (Aerolink may be blocked) — "
                            f"switch to Direct if this keeps happening.\n"
                            f"Next auto-scan runs at :{ALT_SCAN_MINUTE:02d} IST.\n\n"
                            f"<i>🛡️ Capital protected</i>")
                    else:
                        send_reply(cid,
                            f"⏸ <b>No signal found</b>  {ist_str()}\n\n"
                            f"Tried {len(tried)} coin(s): <b>{tried_str}</b>\n\n"
                            f"None had clear 4H+1H+5M alignment for MARKET entry right now.\n"
                            f"Next auto-scan runs at :{ALT_SCAN_MINUTE:02d} IST.\n\n"
                            f"<i>🛡️ Capital protected</i>")

            except Exception as e:
                send_reply(cid, f"❌ Scan error: {e}")
                import traceback as _tb2; print(_tb2.format_exc())
        threading.Thread(target=lambda: _do_scan(cid=chat_id, scan_ver=ver), daemon=True).start()

    elif cmd == "/model" and is_admin:
        _arg = parts[1].lower() if len(parts) > 1 else ""
        if _arg in ("opus", "4.8", "4-8"):
            SCAN_MODEL = "claude-opus-4-8"
        elif _arg in ("fable", "fable5", "5"):
            SCAN_MODEL = "claude-fable-5"
        if _arg: save_settings()
        _is_opus  = SCAN_MODEL == "claude-opus-4-8"
        _is_fable = SCAN_MODEL == "claude-fable-5"
        _mkp = {"inline_keyboard": [[
            {"text": ("✅ " if _is_opus else "") + "Opus 4.8 ($15/$75)",  "callback_data": "model:opus"},
            {"text": ("✅ " if _is_fable else "") + "Fable 5 ($10/$50)",  "callback_data": "model:fable"},
        ]]}
        send_reply(chat_id,
            f"<b>🧠 AI Model</b>\n\n"
            f"Active: <b>{SCAN_MODEL}</b>\n\n"
            f"Opus 4.8  — $15 in / $75 out per 1M tokens\n"
            f"Fable 5   — $10 in / $50 out per 1M tokens (~33% cheaper)\n\n"
            f"Used for all scan/BTC/coin analysis calls.\n\n"
            f"<i>🛡️ Capital protected</i>", reply_markup=_mkp)

    elif cmd == "/gateway" and is_admin:
        _arg = parts[1].lower() if len(parts) > 1 else ""
        if _arg == "direct":
            USE_AEROLINK = False
        elif _arg == "aerolink":
            if not AEROLINK_API_KEY:
                send_reply(chat_id, "⚠️ AEROLINK_API_KEY not set in Railway env vars. Add it first, then switch.")
                return
            USE_AEROLINK = True
        if _arg: save_settings()
        _is_direct = not USE_AEROLINK
        _mkp = {"inline_keyboard": [[
            {"text": ("✅ " if _is_direct else "") + "Direct (Anthropic)", "callback_data": "gateway:direct"},
            {"text": ("✅ " if USE_AEROLINK else "") + "Aerolink Gateway",  "callback_data": "gateway:aerolink"},
        ]]}
        send_reply(chat_id,
            f"<b>🔌 API Gateway</b>\n\n"
            f"Active: <b>{'Aerolink Gateway' if USE_AEROLINK else 'Direct (Anthropic)'}</b>\n\n"
            f"Direct — uses your own ANTHROPIC_API_KEY straight to Anthropic.\n"
            f"Aerolink — uses a separate AEROLINK_API_KEY through capi.aerolink.lat.\n"
            f"Your real Anthropic key is never sent to Aerolink — the two keys stay fully separate.\n\n"
            f"<i>🛡️ Capital protected</i>", reply_markup=_mkp)

    elif cmd == "/coin" and is_scanadmin:
        if len(parts) < 2:
            send_reply(chat_id,
                "🪙 <b>Coin Lookup</b>\n\n"
                "Just type a coin's name — e.g. <code>eth</code>, <code>sol</code>, <code>avax</code> — "
                "and the bot finds it on BingX and analyzes it for you.\n\n"
                "🔎 If more than one coin shares that name (e.g. two different Broccoli tokens), "
                "the bot shows you all the matches so you can tell it exactly which one you want.\n\n"
                "<i>🛡️ Capital protected</i>"); return
        query = parts[1].upper().strip()
        send_reply(chat_id, f"🔍 Searching for <b>{query}</b> on BingX...")
        def _do_coin(cid=chat_id, q=query):
            try:
                # Fetch all BingX perpetual contracts
                r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts",
                                 timeout=12).json()
                all_contracts = r.get("data", [])
                # Match query against symbol (BTC-USDT format on BingX)
                q_bare = q.replace("USDT","").replace("-","").replace("_","")
                matches = []
                for c in all_contracts:
                    sym = c.get("symbol","")   # e.g. "BTC-USDT"
                    base = sym.replace("-USDT","").replace("-","")
                    if q_bare == base or q_bare == sym.replace("-",""):
                        matches.append(sym)
                # Also try partial if exact empty
                if not matches:
                    matches = [c.get("symbol","") for c in all_contracts
                               if q_bare in c.get("symbol","").replace("-","")]

                if not matches:
                    send_reply(cid,
                        f"❌ <b>{q}</b> not found on BingX perpetuals.\n\n"
                        f"Try: /coin ETH  /coin SOL  /coin BNB\n\n"
                        f"<i>🛡️ Capital protected</i>"); return

                if len(matches) > 1:
                    # Multiple matches — fetch prices and show list
                    lines = [f"<b>Multiple matches for '{q}'</b>\n\nChoose and run /coin SYMBOL:\n"]
                    for sym in matches[:10]:
                        try:
                            pr = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/price",
                                              params={"symbol": sym}, timeout=5).json()
                            p = float((pr.get("data") or {}).get("price", 0))
                            lines.append(f"• <code>/coin {sym.replace('-','')}</code>  ${p:,.4f}")
                        except:
                            lines.append(f"• <code>/coin {sym.replace('-','')}</code>")
                    send_reply(cid, "\n".join(lines) + "\n\n<i>🛡️ Capital protected</i>"); return

                # Exact match — fetch ticker then analyze
                sym = matches[0]   # e.g. "ETH-USDT"
                pr = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
                                  params={"symbol": sym}, timeout=10).json()
                if pr.get("code") != 0:
                    send_reply(cid, f"❌ Could not fetch ticker for {sym}: {pr.get('msg','?')}"); return
                d    = pr.get("data", {})
                price  = float(d.get("lastPrice", 0))
                change = float(d.get("priceChangePercent", 0))
                high24 = float(d.get("highPrice", 0))
                low24  = float(d.get("lowPrice",  0))
                vol    = float(d.get("volume", 0))

                # Ask Claude for brief analysis
                resp = _claude_client().messages.create(
                    model=SCAN_MODEL, max_tokens=700,
                    messages=[{"role": "user", "content":
                        f"Analyze {sym} for a short-term futures trade:\n"
                        f"Current Price: ${price:,.6g}\n"
                        f"24h Change: {change:+.2f}%\n"
                        f"24h High: ${high24:,.6g}  |  24h Low: ${low24:,.6g}\n"
                        f"24h Volume: ${vol:,.0f}\n"
                        f"BTC: ${get_ticker()['price']:,.0f} ({get_session()} session)\n\n"
                        f"Give:\n1. Bias: LONG / SHORT / WAIT\n"
                        f"2. Entry zone\n3. SL zone\n4. TP target\n"
                        f"5. Confidence: HIGH / MED / LOW\n"
                        f"6. Reasoning (2-3 lines max)\n\n"
                        f"Be practical and concise. No fluff."}])
                _log_api_usage(f"coin_{sym}", SCAN_MODEL,
                               resp.usage.input_tokens, resp.usage.output_tokens)
                analysis = _claude_text(resp)
                emoji = "🟢" if change >= 0 else "🔴"
                send_reply(cid,
                    f"{emoji} <b>{sym} Analysis</b>  {ist_str()}\n\n"
                    f"Price:  <b>${price:,.6g}</b>  ({change:+.2f}%)\n"
                    f"24H:   H:${high24:,.6g}  L:${low24:,.6g}\n"
                    f"Vol:   ${vol/1e6:.1f}M\n\n"
                    f"<b>Claude Analysis:</b>\n<i>{analysis[:900]}</i>\n\n"
                    f"<i>🛡️ Capital protected</i>")
            except Exception as e:
                send_reply(cid, f"❌ Error: {e}")
                import traceback; traceback.print_exc()
        threading.Thread(target=_do_coin, daemon=True).start()

    else:
        send_reply(chat_id, f"Unknown: {cmd}\n/help")

def handle_broadcast_message(chat_id, message):
    text = message.get("text") or message.get("caption") or ""
    photo = message.get("photo"); doc = message.get("document")
    file_id = None; file_type = None
    if photo:   file_id = photo[-1]["file_id"]; file_type = "photo"
    elif doc:   file_id = doc["file_id"];       file_type = "document"
    if not text and not file_id: send_reply(chat_id, "Empty. /cancel to abort."); return
    mode = broadcast_pending.get(chat_id, {}).get("mode", "all")
    del broadcast_pending[chat_id]
    _mode_label = {"users": "registered users", "channels": "channels", "all": "users + channels"}[mode]
    send_reply(chat_id, f"📢 Broadcasting to {_mode_label}...")
    threading.Thread(target=do_broadcast, args=(chat_id, text, file_id, file_type, mode), daemon=True).start()

# ─── HELP BUTTON MENU ────────────────────────────────────────────────────────
# Each category: (label, admin_only, [(cmd, emoji, short_description), ...])
_HELP_CATS = {
    "monitor": (
        "📊 Status & Info", False, [
            ("/status",   "📊", "Full bot status & positions"),
            ("/trade",    "📈", "Active BTC + all scan trades"),
            ("/price",    "💲", "Current BTC price"),
            ("/session",  "🕐", "London / NY / Sleep session"),
            ("/history",  "📜", "Last 5 signals"),
            ("/stats",    "🏆", "Win rate & trade statistics"),
        ]
    ),
    # copyuser, tradecontrol, scan, copyadmin, settings, tv, and broadcast are all
    # registered in _NESTED_CATS below — their command lists live in the matching
    # _XXX_SUBCATS dict (rooms), not here. Only the label/admin-only flag is used
    # for these; an empty list is intentional, not a bug.
    "copyuser":     ("💰 My Copy Trade",       False, []),
    "tradecontrol": ("🎯 Trade Control",       True,  []),
    "scan":         ("🔍 Scan Control",        True,  []),
    "copyadmin":    ("👥 Copy Admin",          True,  []),
    "settings":     ("⚙️ Settings",            True,  []),
    "tv":           ("📡 TV & Advanced",       True,  []),
    "broadcast":    ("📢 Broadcast & Channels", True,  []),
}

# ─── "My Copy Trade" is split into sub-sections (main gate → door) ────────────
# Each entry: subcat_id -> (label, [(cmd, emoji, bold_title, slim_description), ...])
_COPYUSER_SUBCATS = {
    "connect": ("🔗 Connection", [
        ("/connect",    "🔗", "Connect Account",    "Link your BingX API key so the bot can copy trades for you."),
        ("/disconnect", "🔌", "Disconnect Account", "Remove your BingX API key. Open positions stay open — manage them manually."),
    ]),
    "controls": ("⚙️ Trading Controls", [
        ("/copytrade", "🔄", "Copy Trading On/Off", "Turns copying ON or OFF. When ON, every signal is copied to your account automatically."),
        ("/nocopy",    "🚫", "Block a Coin",         "Skip one coin. It won't be copied, even when everything else is ON."),
    ]),
    "sizing": ("💰 Position Sizing", [
        ("/setsize",     "💵", "Margin Per Trade", "How much money (USDT) goes into each trade."),
        ("/setleverage", "⚡", "Manual Leverage",   "Pick your own leverage (like 10x) — same on every trade."),
        ("/setrisk",     "🛡", "Auto-Risk Mode",    "You set a max loss amount, e.g. $2. The bot then picks the right leverage for you on every trade so you never lose more than that."),
    ]),
    "reports": ("📊 Reports", [
        ("/mytrade",   "📋", "Open Position", "Shows your current live position on BingX, if any."),
        ("/mysize",    "⚙️", "My Settings",   "Your current margin, leverage, and exposure per trade."),
        ("/myhistory", "📊", "P&L History",   "Your past copy-traded results — wins, losses, total P&L."),
    ]),
}

# ─── "Scan Control" is split into sub-sections (main gate → door) ─────────────
_SCAN_SUBCATS = {
    "toggles": ("⚙️ On/Off Switches", [
        ("/scantoggle",  "⚙️", "Scan1/Scan2/Demo",  "Turn each of the three auto-scan pipelines on or off individually."),
        ("/btcanalysis", "📡", "BTC Analysis",       "Turn the scheduled BTC signal analysis on or off."),
        ("/scancopy",    "📋", "Copy Trade By Type", "Turn auto-copy on or off separately for BTC, Scan1, and Scan2 signals."),
    ]),
    "system": ("🧠 AI & Gateway", [
        ("/aiconfig", "🧠", "AI Model & Gateway", "Set model + gateway independently for BTC, Scan1, Scan2, and Test/Demo."),
        ("/entrystyle", "🎯", "Scan Entry Style", "Choose Market (instant) or Zone (limit order at a price range) entries for Scan1/Scan2."),
    ]),
    "schedule": ("⏰ Schedule Editor", [
        ("/alt",     "⏰", "Scan1 Times",       "Edit the exact hour:minute slots Scan1 fires at."),
        ("/alt2",    "⏰", "Scan2 Times",       "Edit the exact hour:minute slots Scan2 fires at."),
        ("/altdemo", "⏰", "Demo/Test Times",   "Edit the exact hour:minute slots the demo scan fires at."),
    ]),
    "run": ("🔍 Run Now", [
        ("/scan",   "🔍", "Force Scan1 + Scan2", "Runs both scans immediately, outside their schedule."),
        ("/scan1",  "1️⃣", "Force Scan1 Only",    "Runs Scan1 immediately."),
        ("/scan2",  "2️⃣", "Force Scan2 Only",    "Runs Scan2 immediately."),
        ("/signal", "⚡", "Force BTC Scan",       "Runs a BTC signal analysis immediately, outside the schedule."),
        ("/test",   "🧪", "Run Demo Scan",       "Fires a demo/test scan now — signals only, no real trades."),
        ("/demo",   "🎭", "Simulate Demo Trade", "Manually simulate one demo trade for testing."),
    ]),
    "lookup": ("🪙 Coin Lookup", [
        ("/coin", "🪙", "Coin Lookup", "Type any coin's name and the bot finds and analyzes it for you."),
    ]),
}

# ─── "Trade Control" is split into sub-sections (main gate → door) ────────────
_TRADECONTROL_SUBCATS = {
    "levels": ("🎯 SL / TP Levels", [
        ("/sltobe",  "🛡", "Move SL to Breakeven", "Pick any open trade (BTC, Scan1 or Scan2) and lock its stop-loss at entry price — no more loss possible."),
        ("/setsl",   "🔧", "Set Custom SL",        "Pick any open trade, then tap in a new stop-loss price for it."),
        ("/settp1",  "🎯", "Set Custom TP1",       "Pick any open trade, then tap in a new first take-profit price for it."),
        ("/settp2",  "🏆", "Set Custom TP2",       "Pick any open trade, then tap in a new final take-profit price for it."),
        ("/tp1size", "📐", "TP1 Close %",          "How much of the position closes at TP1 (default 50%) — the rest rides to TP2."),
        ("/trailsl", "🛡️", "Trailing SL",          "At the halfway point to TP1, auto-move SL to the halfway point toward entry — locks in more capital early."),
    ]),
    "close": ("❌ Close Positions", [
        ("/close",      "❌", "Close BTC Trade",     "Manually closes the currently active BTC trade right now."),
        ("/closetrade", "❌", "Close a Coin",         "Pick any open trade (BTC, Scan1 or Scan2) to close it — on BingX for every copy user, and in the bot."),
        ("/closescan",  "🗑", "Clear All Scan Trades","Force-closes every open Scan1/Scan2 trade on BingX for all copy users, then clears them from the bot."),
    ]),
    "actions": ("⚡ Other Actions", [
        ("/resetsl", "🔄", "Reset SL Streak", "Clears the consecutive-SL counter and any active cooldown."),
    ]),
}

# ─── "Copy Admin" is split into sub-sections (main gate → door) ───────────────
_COPYADMIN_SUBCATS = {
    "directory": ("👥 User Directory", [
        ("/allusers",  "👥", "All Users Summary", "Quick overview of every copy-trade user and their status."),
        ("/users",     "📋", "List with Status",  "Full list of all users showing connected/copy-on/paused state."),
        ("/user",      "👤", "One User's Detail", "Look up a single user's full copy-trade configuration."),
        ("/userstats", "📊", "User Stats",         "Total users, how many are using copy trade, and who has blocked the bot — by username."),
    ]),
    "manage": ("🛡 Moderation", [
        ("/kick",      "🚫", "Remove User",       "Disconnects a user and cancels any pending orders for them."),
        ("/pauseuser", "⏸", "Pause / Unpause",   "Pause or resume a specific user's copy trading without removing them."),
    ]),
    "tiers": ("⭐ VIP & Tiers", [
        ("/setvip",    "⭐", "Promote to VIP",     "Give a user VIP for a date range — they get every signal (tap or type the dates)."),
        ("/setfree",   "🆓", "Set to Free",        "Downgrade a user to Free tier — they only copy the signals shared to the free channel."),
    ]),
    "coadmin": ("🤝 Co-Admin", [
        ("/coadmin",   "🤝", "Co-Admin",          "Give one trusted user Scan + Trade Control access — no user management, billing, or resets."),
    ]),
    "sync": ("🔄 Sync & Recovery", [
        ("/ctstatus",  "🔍", "Failed Users",      "Shows users whose copy trade failed, plus the active signal."),
        ("/ctretry",   "🔄", "Retry Failed Copy", "Re-attempts a copy trade that previously failed for a user."),
        ("/ctclose",   "❌", "Close Positions",   "Force-closes a user's copy-traded positions."),
        ("/synccheck", "🔄", "BingX vs Bot Sync", "Compares live BingX positions against what the bot thinks is open."),
    ]),
}

# ─── "TV & Advanced" is split into sub-sections (main gate → door) ────────────
_TV_SUBCATS = {
    "bridge": ("📡 Bridge Status", [
        ("/tvstatus",     "📡", "Bridge Status",     "Shows whether the TradingView bridge connection is online."),
        ("/force_reload", "🔄", "Reload Bridge",     "Reconnects the TradingView bridge if it's stuck or offline."),
        ("/scantv",       "🔀", "TV Data Toggle",    "Turn TradingView chart data on or off for scans (falls back to BingX when off)."),
    ]),
    "indicators": ("📊 Indicators & Analysis", [
        ("/tvstudies",   "📊", "Read TV Indicators", "Pulls current RSI/EMA/MACD values straight from TradingView."),
        ("/calcstudies", "🧮", "Calculate (BingX)",  "Calculates the same indicators locally from BingX candle data."),
        ("/compare",     "⚖️", "4-Way BTC Compare",  "Runs 4 parallel BTC analyses (TV vs BingX data) side by side."),
    ]),
}

# ─── "Settings" is split into sub-sections (main gate → door) ─────────────────
_SETTINGS_SUBCATS = {
    "botcontrol": ("▶️ Bot Control", [
        ("/go",    "▶️", "Resume Bot",   "Starts scanning again after a pause."),
        ("/pause", "⏸", "Pause Bot",    "Freezes everything — no new scans, signals, or trades."),
    ]),
    "btcsettings": ("📡 BTC Settings", [
        ("/btcmode",      "🔀", "Prompt Mode V7/V9",  "Switch which BTC analysis prompt version is used."),
        ("/btcanalysis",  "📡", "Toggle Analysis",     "Turn scheduled BTC signal analysis on or off."),
        ("/setinterval",  "⏰", "Scan Interval",       "Set how many hours between each BTC analysis scan."),
    ]),
    "charts": ("🖼 Charts & Images", [
        ("/chartson",  "📸", "Enable Charts",   "Turn on chart snapshot generation for signals."),
        ("/chartsoff", "🚫", "Disable Charts",  "Turn off chart snapshots — saves API credits."),
        ("/images",    "🖼", "Images On/Off",   "Enable or disable chart images being sent at all."),
        ("/setimages", "🖼", "Chart Timeframes","Choose which timeframes appear in generated charts."),
    ]),
    "extras": ("📰 Extras", [
        ("/news",    "📰", "News Feed",       "Turn the crypto news feed on or off."),
        ("/miniapp", "📱", "Mini App Status", "Pause or resume the mini app (maintenance mode)."),
    ]),
    "data": ("📊 Data & Reports", [
        ("/tradelog", "📥", "Trade History CSV", "Download the full trade log (BTC + Scan1 + Scan2) as a CSV file."),
        ("/report",   "📊", "API Cost Report",   "Daily Claude API token usage and cost breakdown, across every feature."),
    ]),
}

# ─── "Broadcast & Channels" is split into sub-sections (main gate → door) ─────
_BROADCAST_SUBCATS = {
    "messaging": ("📢 Messaging", [
        ("/broadcast",   "📢", "Message All Users", "Send a message to every registered user of the bot."),
        ("/latestnews",  "📰", "Fetch Latest News",  "Pull and post the latest crypto news right now."),
    ]),
    "channels": ("📡 Channel Control", [
        ("/channels",      "📡", "Channel Status",    "Show the current status of all connected signal channels."),
        ("/pausechannel",  "⏸", "Pause a Channel",   "Stop signals from being sent to a specific channel."),
        ("/resumechannel", "▶️", "Resume a Channel",  "Re-enable signals for a specific channel."),
        ("/channelmgmt",   "⭐", "VIP / Free Channels","Add/remove any number of VIP or Free channels and set the free daily signal limit."),
    ]),
}

# ─── Tap-to-pick time keypad (digit entry for Scan1/Scan2/Demo schedules) ─────
_TP_LABELS  = {"scan1": "Scan1", "scan2": "Scan2", "demo": "Demo/Test"}
_TP_APPLYCMD = {"scan1": "/alt manual", "scan2": "/alt2 manual", "demo": "/altdemo manual"}
_TP_BACKCAT  = {"scan1": "scan", "scan2": "scan", "demo": "scan"}

def _tp_render(chat_id, cid, msg_id):
    st = _tp_state.get(str(cid))
    if not st:
        return
    digits = st["digits"]
    times  = st["times"]
    label  = _TP_LABELS[st["target"]]

    slots = [str(d) for d in digits] + ["_"] * (4 - len(digits))
    entering = f"{slots[0]}{slots[1]} : {slots[2]}{slots[3]}"
    complete = len(digits) == 4

    saved_str = "  ".join(f"{h}:{m:02d}" for h, m in times) if times else "(none yet)"
    text = (
        f"🔢 <b>Pick {label} Times</b>\n\n"
        f"Saved so far: <code>{saved_str}</code>\n\n"
        f"Entering: <code>{entering}</code>{'  ✅' if complete else ''}\n\n"
        f"<i>Tap digits to build HH:MM (24h). First 2 = hour, last 2 = minute.</i>"
    )

    rows = []
    if not complete:
        rows.append([{"text": str(n), "callback_data": f"tp_d:{n}"} for n in (1, 2, 3)])
        rows.append([{"text": str(n), "callback_data": f"tp_d:{n}"} for n in (4, 5, 6)])
        rows.append([{"text": str(n), "callback_data": f"tp_d:{n}"} for n in (7, 8, 9)])
        rows.append([{"text": "0", "callback_data": "tp_d:0"}])
        rows.append([
            {"text": "◀️ Previous", "callback_data": "tp_prev"},
            {"text": "🚫 Back",     "callback_data": "tp_back"},
        ])
    else:
        rows.append([
            {"text": "➡️ Next",     "callback_data": "tp_next"},
            {"text": "💾 Save",     "callback_data": "tp_save"},
        ])
        rows.append([
            {"text": "◀️ Previous", "callback_data": "tp_prev"},
            {"text": "🚫 Back",     "callback_data": "tp_back"},
        ])

    markup = {"inline_keyboard": rows}
    _help_edit_or_send(chat_id, text, markup, message_id=msg_id)

def _toggle_cmd(cmd_text, chat_id, cid, msg_id, cat_id):
    """Run a command, capture its reply, and edit the current message in-place
    with the result + a Back button — instead of sending a brand-new message.
    The Back button target is auto-resolved to the immediate subcategory the
    command lives in (falls back to the given cat_id if it can't be found)."""
    cid_str = str(cid)
    _reply_capture[cid_str] = {"texts": [], "cat_id": cat_id}
    handle_command(cmd_text, chat_id, {}, sender_id=cid)
    captured = _reply_capture.pop(cid_str, {})
    result_text = "\n\n".join(captured.get("texts", [])) or "✅ Done"
    if len(result_text) > 4000:
        result_text = result_text[:4000] + "\n\n<i>...truncated</i>"
    _base_cmd = cmd_text.split()[0]
    _back_cb, _ = _find_back_target(_base_cmd)
    _back_row = [{"text": "◀️  Back", "callback_data": _back_cb}]
    cap_mkp = captured.get("markup")
    if cap_mkp and "inline_keyboard" in cap_mkp:
        merged = cap_mkp["inline_keyboard"] + [_back_row]
    else:
        merged = [_back_row]
    _help_edit_or_send(chat_id, result_text, {"inline_keyboard": merged}, message_id=msg_id)

def _help_edit_or_send(chat_id, text, markup, message_id=None, rotate=True):
    base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    text = _apply_premium_emojis(text)
    markup = _style_keyboard(markup, rotate=rotate)
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
               "reply_markup": markup, "disable_web_page_preview": True}
    if message_id:
        payload["message_id"] = message_id
        try:
            r = requests.post(f"{base}/editMessageText", json=payload, timeout=10)
            if not r.json().get("ok") and "message is not modified" not in r.json().get("description", ""):
                print(f"  [HELP EDIT ERROR] Telegram rejected: {r.json().get('description')}")
                requests.post(f"{base}/sendMessage", json=payload, timeout=10)
        except Exception as e:
            print(f"  [HELP EDIT ERROR] {e}")
            requests.post(f"{base}/sendMessage", json=payload, timeout=10)
    else:
        r = requests.post(f"{base}/sendMessage", json=payload, timeout=10)
        if not r.json().get("ok"):
            print(f"  [HELP SEND ERROR] Telegram rejected: {r.json().get('description')}")

def _np_render(chat_id, cid, msg_id):
    st = _np_state.get(str(cid))
    if not st:
        return
    cfg = _NP_CONFIG[st["target"]]
    digits = st["digits"]
    value_str = digits if digits else "0"
    text = (
        f"🔢 <b>Set {cfg['label']}</b>\n\n"
        f"Entering: <code>{value_str}</code> {cfg['unit']}\n\n"
        f"<i>Tap digits to build the value{' — use . for cents, e.g. 0.5' if cfg['decimals'] else ''}.</i>")
    rows = [
        [{"text": str(n), "callback_data": f"np_d:{n}"} for n in (1, 2, 3)],
        [{"text": str(n), "callback_data": f"np_d:{n}"} for n in (4, 5, 6)],
        [{"text": str(n), "callback_data": f"np_d:{n}"} for n in (7, 8, 9)],
    ]
    zero_row = [{"text": "0", "callback_data": "np_d:0"}]
    if cfg["decimals"]:
        zero_row.append({"text": ".", "callback_data": "np_d:."})
    rows.append(zero_row)
    rows.append([{"text": "⌨️ Type Instead", "callback_data": "np_manual"}])
    rows.append([{"text": "◀️ Erase", "callback_data": "np_prev"}, {"text": "🚫 Back", "callback_data": "np_back"}])
    if digits:
        rows.insert(-2, [{"text": "💾 Save", "callback_data": "np_save"}])
    _help_edit_or_send(chat_id, text, {"inline_keyboard": rows}, message_id=msg_id)

def _ask_confirm(chat_id, cid, action_id, label, back_cb, message_id=None):
    """Show a Yes/Cancel confirmation before running a destructive action."""
    _pending_confirm[cid] = {"action": action_id, "back_cb": back_cb}
    mkp = {"inline_keyboard": [[
        {"text": "✅ Yes, confirm", "callback_data": "confirm_yes", "style": "success"},
        {"text": "❌ Cancel",       "callback_data": "confirm_no",  "style": "danger"},
    ]]}
    text = f"⚠️ <b>Are you sure?</b>\n\n{label}\n\n<i>This cannot be undone.</i>"
    _help_edit_or_send(chat_id, text, mkp, message_id=message_id)

def _strip_html(s: str) -> str:
    for tag in ("<b>", "</b>", "<i>", "</i>"):
        s = s.replace(tag, "")
    return s.replace("&gt;", ">").replace("&lt;", "<")

def _run_confirmed_action(action_id, chat_id, cid, msg_id, back_cb):
    """Executes the action a user just confirmed via Yes. Returns a short result string —
    caller shows it as a popup and navigates back to the previous screen (no extra tap)."""
    ts = trade_stats
    if action_id == "reset_btc_stats":
        for k in ("total_sl","total_tp1","total_tp2","total_signals","missed_entries","stop_hunts","consecutive_sl","cooldown_scans"):
            ts[k] = 0
        save_state()
        result_text = "✅ <b>BTC stats reset.</b>"
    elif action_id == "reset_scan1_stats":
        for k in ("scan1_sl","scan1_tp1","scan1_tp2","scan1_signals"):
            ts[k] = 0
        save_state()
        result_text = "✅ <b>Scan1 stats reset.</b>"
    elif action_id == "reset_scan2_stats":
        for k in ("scan2_sl","scan2_tp1","scan2_tp2","scan2_signals"):
            ts[k] = 0
        save_state()
        result_text = "✅ <b>Scan2 stats reset.</b>"
    elif action_id == "reset_signal_history":
        signal_history.clear(); scan_history.clear(); save_state()
        result_text = "✅ <b>Signal history cleared.</b>\n\nCSV trade log untouched."
    elif action_id.startswith("reset_pnl:"):
        uid = action_id.split(":", 1)[1]
        ct.reset_history(uid)
        result_text = "✅ <b>Your P&L history has been reset.</b>"
    elif action_id == "closescan":
        s1 = len(scan1_trades); s2 = len(scan2_trades)
        _syms = {t["symbol"] for t in scan1_trades + scan2_trades if t.get("symbol")}
        for _sym in _syms:
            ct.close_coin_all(_sym)
        scan1_trades.clear(); scan2_trades.clear(); save_state()
        result_text = f"✅ <b>Scan trades cleared</b>\n\nScan1: {s1} removed\nScan2: {s2} removed\nClosed on BingX: {', '.join(_syms) if _syms else 'none'}"
    elif action_id.startswith("disconnect:"):
        uid = action_id.split(":", 1)[1]
        handle_command("/disconnect", chat_id, {}, sender_id=int(uid))
        result_text = "✅ <b>Account disconnected.</b>"
    elif action_id.startswith("kick:"):
        uid = action_id.split(":", 1)[1]
        handle_command(f"/kick {uid}", chat_id, {}, sender_id=cid)
        result_text = f"✅ <b>User {uid} removed.</b>"
    elif action_id.startswith("nocopy_blk:"):
        coin = action_id.split(":", 1)[1]
        handle_command(f"/nocopy {coin}", chat_id, {}, sender_id=cid)
        result_text = f"✅ <b>{coin} blocked from auto-copy.</b>"
    elif action_id.startswith("sltobe:"):
        _, kind, symbol, idx_str = action_id.split(":", 3)
        idx = int(idx_str)
        if kind == "btc":
            if active_trade.get("signal"):
                active_trade["sl"] = active_trade["entry"]
                ct.on_sl_to_be(active_trade["entry"]); save_state()
                send_telegram(f"<b>SL -&gt; BE</b>  {symbol} -&gt; <b>{active_trade['entry']:,.4f}</b>\n\n<i>🛡️ Capital protected</i>")
                result_text = f"✅ <b>{symbol} SL moved to breakeven</b> ({active_trade['entry']:,.4f})"
            else:
                result_text = f"⚠️ {symbol} trade no longer open."
        else:
            lst = scan1_trades if kind == "scan1" else scan2_trades
            if 0 <= idx < len(lst) and lst[idx].get("symbol") == symbol:
                lst[idx]["sl"] = lst[idx]["entry"]
                ct.scan_sl_to_be(symbol, lst[idx]["entry"]); save_state()
                send_telegram(f"<b>SL -&gt; BE</b>  {symbol} -&gt; <b>{lst[idx]['entry']:,.4f}</b>\n\n<i>🛡️ Capital protected</i>")
                result_text = f"✅ <b>{symbol} SL moved to breakeven</b> ({lst[idx]['entry']:,.4f})"
            else:
                result_text = f"⚠️ {symbol} trade no longer open."
    elif action_id.startswith("closetrade:"):
        _, kind, symbol, idx_str = action_id.split(":", 3)
        idx = int(idx_str)
        ct.close_coin_all(symbol)
        if kind == "btc":
            if active_trade.get("signal"):
                log_trade_outcome("MANUAL_CLOSE", "admin trade-picker close")
                reset_trade()
        else:
            lst = scan1_trades if kind == "scan1" else scan2_trades
            if 0 <= idx < len(lst) and lst[idx].get("symbol") == symbol:
                lst.pop(idx); save_state()
        result_text = f"✅ <b>{symbol} closed</b> — on BingX for all copy users + removed from the bot."
    elif action_id == "remove_channel_link":
        global SIGNAL_CHANNEL_LINK
        SIGNAL_CHANNEL_LINK = ""; save_settings()
        result_text = "✅ Channel link removed."
    elif action_id.startswith("ctpause:"):
        _, which = action_id.split(":", 1)
        if which == "btc":
            ct.set_btc_ct(not ct.BTC_CT_ENABLED)
            result_text = f"✅ BTC copy trade turned {'ON' if ct.BTC_CT_ENABLED else 'OFF'}."
        elif which == "scan1":
            ct.set_scan1_ct(not ct.SCAN1_CT_ENABLED)
            result_text = f"✅ Scan1 copy trade turned {'ON' if ct.SCAN1_CT_ENABLED else 'OFF'}."
        else:
            ct.set_scan2_ct(not ct.SCAN2_CT_ENABLED)
            result_text = f"✅ Scan2 copy trade turned {'ON' if ct.SCAN2_CT_ENABLED else 'OFF'}."
    else:
        result_text = "✅ Done."
    return result_text

def send_adminlinks_screen(chat_id, message_id=None):
    _ca_flag = "✅ ON" if CONTACT_ADMIN_ENABLED else "❌ OFF"
    _sc_flag = "✅ ON" if SIGNAL_CHANNEL_ENABLED else "❌ OFF"
    rows = [
        [{"text": f"💬 Contact Admin  {_ca_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "adminlinks_ca_on"},
         {"text": "🔴 Turn OFF", "callback_data": "adminlinks_ca_off"}],
        [{"text": f"📡 Signal Channel  {_sc_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "adminlinks_sc_on"},
         {"text": "🔴 Turn OFF", "callback_data": "adminlinks_sc_off"}],
    ]
    if SIGNAL_CHANNEL_LINK:
        rows.append([{"text": "🗑 Remove Channel Link", "callback_data": "adminlinks_remove_channel"}])
        channel_line = f"Current link: {SIGNAL_CHANNEL_LINK}"
    else:
        rows.append([{"text": "🔗 Connect Channel", "callback_data": "adminlinks_connect_channel"}])
        channel_line = "No channel link set yet."
    rows.append([{"text": "◀️  Back to Menu", "callback_data": "help_main"}])
    text = (
        f"<b>🔗 Contact / Channel Settings</b>\n\n"
        f"Controls whether users see the <b>Contact Admin</b> and <b>Signal Channel</b>\n"
        f"buttons on their main /help menu.\n\n"
        f"{channel_line}")
    _help_edit_or_send(chat_id, text, {"inline_keyboard": rows}, message_id=message_id)

def send_userstats_screen(chat_id, message_id=None):
    # Negative chat_ids are groups/channels, not individual users — exclude them.
    _n_total  = len([u for u in registered_users if int(u) > 0])
    _n_active = len([u for u in ct.active_ids() if int(u) > 0])
    _n_blocked = len([u for u in blocked_users if int(u) > 0])
    rows = [
        [{"text": f"👥 Total Users ({_n_total})", "callback_data": "userstats_total"}],
        [{"text": f"🟢 Using Copy Trade ({_n_active})", "callback_data": "userstats_active"}],
        [{"text": f"🚫 Blocked Bot ({_n_blocked})", "callback_data": "userstats_blocked"}],
        [{"text": "◀️  Back to Menu", "callback_data": "help_main"}],
    ]
    _help_edit_or_send(chat_id,
        "📊 <b>User Stats</b>\n\nTap a category to see the users with DM links.",
        {"inline_keyboard": rows}, message_id=message_id)

def send_userstats_list(chat_id, kind, message_id=None):
    # Negative chat_ids are groups/channels, not individual users — exclude them.
    if kind == "total":
        ids = [u for u in registered_users if int(u) > 0]; title = "👥 Total Users"
    elif kind == "active":
        ids = [u for u in ct.active_ids() if int(u) > 0]; title = "🟢 Using Copy Trade"
    else:
        ids = [u for u in blocked_users if int(u) > 0]; title = "🚫 Blocked Bot"
    text = _render_user_list_text(title, ids)
    _help_edit_or_send(chat_id, text,
        {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": "userstats_open"}]]},
        message_id=message_id)

_AICFG_LABELS = {"btc": "₿ BTC", "scan1": "🔍 Scan1", "scan2": "🔍 Scan2", "test": "🧪 Test/Demo"}

def send_aiconfig_screen(chat_id, message_id=None):
    rows = []
    for kind, label in _AICFG_LABELS.items():
        gw  = "Aerolink" if _ai_aerolink(kind) else "Direct"
        mdl = "Opus 4.8" if _ai_model(kind) == "claude-opus-4-8" else "Fable 5"
        rows.append([{"text": f"{label}: {gw} · {mdl}", "callback_data": f"aicfg_open:{kind}"}])
    rows.append([{"text": "◀️  Back to Menu", "callback_data": "help_main"}])
    _help_edit_or_send(chat_id,
        "<b>🧠 AI Model & Gateway — Per Scan Type</b>\n\n"
        "BTC, Scan1, Scan2, and Test/Demo each pick their own model + gateway independently.\n"
        "Tap a type below to change its combo.",
        {"inline_keyboard": rows}, message_id=message_id)

def send_aiconfig_type_screen(chat_id, kind, message_id=None):
    label = _AICFG_LABELS.get(kind, kind)
    cur_model = _ai_model(kind); cur_aero = _ai_aerolink(kind)
    def mark(m, a): return "✅ " if (cur_model == m and cur_aero == a) else ""
    rows = [
        [{"text": f"{mark('claude-opus-4-8', False)}Direct · Opus 4.8",    "callback_data": f"aicfg_set:{kind}:direct:opus"}],
        [{"text": f"{mark('claude-fable-5', False)}Direct · Fable 5",     "callback_data": f"aicfg_set:{kind}:direct:fable"}],
        [{"text": f"{mark('claude-opus-4-8', True)}Aerolink · Opus 4.8",   "callback_data": f"aicfg_set:{kind}:aerolink:opus"}],
        [{"text": f"{mark('claude-fable-5', True)}Aerolink · Fable 5",    "callback_data": f"aicfg_set:{kind}:aerolink:fable"}],
        [{"text": "◀️  Back", "callback_data": "aicfg_open"}],
    ]
    _help_edit_or_send(chat_id, f"<b>{label} — AI Model &amp; Gateway</b>\n\nChoose a combo:",
        {"inline_keyboard": rows}, message_id=message_id)

def send_entrystyle_screen(chat_id, message_id=None):
    _is_market = not ZONE_ENTRY_ENABLED
    rows = [
        [{"text": f"{'✅ ' if _is_market else ''}📍 Market Entry", "callback_data": "entrystyle:market"}],
        [{"text": f"{'✅ ' if not _is_market else ''}📩 Zone Entry",  "callback_data": "entrystyle:zone"}],
        [{"text": "◀️  Back to Menu", "callback_data": "help_main"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>🎯 Scan Entry Style</b>\n\n"
        "<b>Market Entry</b> — places the trade instantly at the current price.\n\n"
        "<b>Zone Entry</b> — shows a price range (like a signal-channel style zone) and "
        "places a single LIMIT order at the zone's midpoint for every copy user. "
        "The order only fills if price actually trades back into that zone — if it never "
        "does, the position stays unfilled on BingX (this applies to Scan1/Scan2 only).",
        {"inline_keyboard": rows}, message_id=message_id)

def send_channel_picker_screen(chat_id, message_id=None):
    rows = [
        [{"text": "🆓 Free Channel", "callback_data": "chanpick:free"}],
        [{"text": "⭐ VIP Channel",  "callback_data": "chanpick:vip"}],
        [{"text": "◀️  Back", "callback_data": "help_main"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>📡 Signal Channels</b>\n\nChoose which one you want to join:",
        {"inline_keyboard": rows}, message_id=message_id)

def send_channel_picker_result(chat_id, tier, message_id=None):
    if tier == "free":
        chans = [c for c in CHANNELS if c.get("tier") == "free" and c.get("id")]
        if not chans:
            text = "🆓 <b>Free Channel</b>\n\nNo free channel is set up yet — check back later."
            rows = [[{"text": "◀️  Back", "callback_data": "chanpick_open"}]]
        else:
            text = "🆓 <b>Free Channel</b>\n\nA limited number of signals per day, shared with everyone:"
            rows = [[{"text": c.get("label") or "Join", "url": c["link"]}] for c in chans if c.get("link")]
            if not rows:
                text += "\n\n<i>No public join link set for it yet — ask the admin.</i>"
            rows.append([{"text": "◀️  Back", "callback_data": "chanpick_open"}])
    else:
        _u = ct._get(str(chat_id))
        _is_vip = bool(_u and _u.get("tier") == "vip")
        vip_chans = [c for c in CHANNELS if c.get("tier") == "vip" and c.get("link")]
        if _is_vip and vip_chans:
            text = (
                "⭐ <b>VIP Channel</b>\n\n"
                "You're VIP — tap below to request to join. Your request is approved automatically.")
            rows = [[{"text": c.get("label") or "Join", "url": c["link"]}] for c in vip_chans]
            rows.append([{"text": "◀️  Back", "callback_data": "chanpick_open"}])
        else:
            text = (
                "⭐ <b>VIP Channel</b>\n\n"
                "Get every signal, no limits — BTC, Scan1, and Scan2, the moment they fire.\n\n"
                "VIP access is activated by the admin. Tap below to request it.")
            rows = [
                [{"text": "💬 Contact Admin for VIP", "url": f"tg://user?id={ADMIN_CHAT_ID}"}] if ADMIN_CHAT_ID else [],
            ]
            if CO_ADMIN_ENABLED and CO_ADMIN_CHAT_ID:
                _co_uname = user_usernames.get(str(CO_ADMIN_CHAT_ID))
                _co_url = f"https://t.me/{_co_uname}" if _co_uname else f"tg://user?id={CO_ADMIN_CHAT_ID}"
                rows.append([{"text": "💬 Contact Co-Admin for VIP", "url": _co_url}])
            rows.append([{"text": "◀️  Back", "callback_data": "chanpick_open"}])
            rows = [r for r in rows if r]
    _help_edit_or_send(chat_id, text, {"inline_keyboard": rows}, message_id=message_id)

def send_channelmgmt_screen(chat_id, message_id=None):
    rows = []
    for i, c in enumerate(CHANNELS):
        label = c.get("label") or (("⭐ VIP" if c.get("tier") == "vip" else "🆓 Free") + f" · {c.get('id','?')}")
        rows.append([{"text": label, "callback_data": "noop"}])
        rows.append([{"text": "🗑 Remove", "callback_data": f"chrm_remove:{i}"}])
    rows.append([{"text": "➕ Add VIP Channel", "callback_data": "chrm_add:vip"}])
    rows.append([{"text": "➕ Add Free Channel", "callback_data": "chrm_add:free"}])
    rows.append([{"text": f"🔢 Free Daily Limit: {FREE_SIGNAL_DAILY_LIMIT}", "callback_data": "freelimit_open"}])
    rows.append([{"text": "◀️  Back", "callback_data": "broadcast_sub:channels"}])
    _help_edit_or_send(chat_id,
        "<b>📡 Channels — VIP / Free</b>\n\n"
        "Add as many VIP or Free channels as you want. VIP channels get every signal. "
        "Free channels only get up to your daily limit, between 06:00–19:00 IST — "
        "free-tier bot users copy exactly the same signals the free channels got.",
        {"inline_keyboard": rows}, message_id=message_id)

def send_trailsl_screen(chat_id, message_id=None):
    _btc_flag   = "✅ ON" if TRAIL_SL_BTC   else "❌ OFF"
    _scan1_flag = "✅ ON" if TRAIL_SL_SCAN1 else "❌ OFF"
    _scan2_flag = "✅ ON" if TRAIL_SL_SCAN2 else "❌ OFF"
    rows = [
        [{"text": f"₿ BTC  {_btc_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "trailsl_btc_on"},
         {"text": "🔴 Turn OFF", "callback_data": "trailsl_btc_off"}],
        [{"text": f"🔍 Scan1  {_scan1_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "trailsl_scan1_on"},
         {"text": "🔴 Turn OFF", "callback_data": "trailsl_scan1_off"}],
        [{"text": f"🔍 Scan2  {_scan2_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "trailsl_scan2_on"},
         {"text": "🔴 Turn OFF", "callback_data": "trailsl_scan2_off"}],
        [{"text": "◀️  Back", "callback_data": "tradecontrol_sub:levels"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>🛡️ Trailing SL</b>\n\n"
        "Once price reaches the halfway point to TP1, SL automatically moves to the halfway "
        "point between the original SL and entry — locking in more capital before TP1 even hits.\n\n"
        "Example: Entry 10, TP1 18, SL 6 → at price 14, SL moves to 8.\n\n"
        "Turn on independently for BTC, Scan1, and Scan2.",
        {"inline_keyboard": rows}, message_id=message_id)

def send_coadmin_screen(chat_id, message_id=None):
    global CO_ADMIN_CHAT_ID
    _flag = "✅ ON" if CO_ADMIN_ENABLED else "❌ OFF"
    _uname = user_usernames.get(str(CO_ADMIN_CHAT_ID), "")
    _who = f"@{_uname}" if _uname else (str(CO_ADMIN_CHAT_ID) if CO_ADMIN_CHAT_ID else "not set")
    _profile_lbl = "👤 My Settings" if ACTIVE_PROFILE == "mine" else "🤝 Co-Admin's Settings"
    _switch_to_lbl = "🤝 Co-Admin's" if ACTIVE_PROFILE == "mine" else "👤 My"
    rows = [
        [{"text": f"Co-Admin: {_who}  {_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON", "callback_data": "coadmin_on"}, {"text": "🔴 Turn OFF", "callback_data": "coadmin_off"}],
        [{"text": "👤 Choose Co-Admin", "callback_data": "coadmin_pick"}],
        [{"text": f"Active: {_profile_lbl}", "callback_data": "noop"}],
        [{"text": f"🔀 Switch to {_switch_to_lbl} Settings", "callback_data": "profile_switch"}],
        [{"text": "◀️  Back to Menu", "callback_data": "help_main"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>🤝 Co-Admin</b>\n\n"
        "Gives one trusted person control of Scan Control + Trade Control — force scans, "
        "BTC/Scan1/Scan2 on-off, AI model &amp; gateway per type, entry style, TP1%, schedules, "
        "SL/TP/close on any trade, and the Trade History CSV. They still can't see/manage users, "
        "see billing, reset anything, broadcast, or touch this Co-Admin screen. Their contact "
        "shows next to Contact Admin while ON.\n\n"
        "<b>🔀 Switch Settings</b> swaps between two remembered configs — yours and the "
        "co-admin's — of everything above (model, gateway, entry style, TP1%, schedules, "
        "on/off toggles). Switching saves your current setup before loading the other one, "
        "so nothing is lost either way.",
        {"inline_keyboard": rows}, message_id=message_id)

def send_coadmin_pick_screen(chat_id, message_id=None):
    ids = [u for u in registered_users if int(u) > 0]
    if not ids:
        _help_edit_or_send(chat_id, "No registered users yet.",
            {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": "coadmin_open"}]]}, message_id=message_id)
        return
    rows = []
    for uid in ids:
        uname = user_usernames.get(str(uid))
        label = f"@{uname}" if uname else f"ID {uid}"
        rows.append([{"text": label, "callback_data": f"coadmin_set:{uid}"}])
    rows.append([{"text": "◀️  Back", "callback_data": "coadmin_open"}])
    _help_edit_or_send(chat_id, "<b>👤 Choose Co-Admin</b>\n\nTap the user to grant Trade History CSV access:",
        {"inline_keyboard": rows}, message_id=message_id)

def send_vip_pick_screen(chat_id, message_id=None):
    ids = [u for u in registered_users if int(u) > 0]
    if not ids:
        _help_edit_or_send(chat_id, "No registered users yet.",
            {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": "help_cat:copyadmin"}]]}, message_id=message_id)
        return
    rows = []
    for uid in ids:
        uname = user_usernames.get(str(uid))
        _u_ct = ct._get(str(uid))
        tier_tag = ""
        if _u_ct:
            tier_tag = "  ⭐" if _u_ct.get("tier", "vip") == "vip" and _u_ct.get("connected") else ("  🆓" if _u_ct.get("connected") else "")
        label = (f"@{uname}" if uname else f"ID {uid}") + tier_tag
        rows.append([{"text": label, "callback_data": f"vip_pick:{uid}"}])
    rows.append([{"text": "◀️  Back", "callback_data": "help_cat:copyadmin"}])
    _help_edit_or_send(chat_id,
        "⭐ <b>Promote to VIP</b>\n\nChoose any registered user — they don't need to have connected BingX yet:",
        {"inline_keyboard": rows}, message_id=message_id)

def send_free_pick_screen(chat_id, message_id=None):
    ids = [u for u in registered_users if int(u) > 0]
    if not ids:
        _help_edit_or_send(chat_id, "No registered users yet.",
            {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": "help_cat:copyadmin"}]]}, message_id=message_id)
        return
    rows = []
    for uid in ids:
        uname = user_usernames.get(str(uid))
        _u_ct = ct._get(str(uid))
        tier_tag = ""
        if _u_ct:
            tier_tag = "  ⭐" if _u_ct.get("tier", "vip") == "vip" and _u_ct.get("connected") else ("  🆓" if _u_ct.get("connected") else "")
        label = (f"@{uname}" if uname else f"ID {uid}") + tier_tag
        rows.append([{"text": label, "callback_data": f"free_set:{uid}"}])
    rows.append([{"text": "◀️  Back", "callback_data": "help_cat:copyadmin"}])
    _help_edit_or_send(chat_id,
        "🆓 <b>Demote to Free</b>\n\nChoose any registered user:",
        {"inline_keyboard": rows}, message_id=message_id)

def send_ctpause_screen(chat_id, message_id=None):
    _btc_flag   = "✅ ON" if ct.BTC_CT_ENABLED   else "❌ OFF"
    _scan1_flag = "✅ ON" if ct.SCAN1_CT_ENABLED else "❌ OFF"
    _scan2_flag = "✅ ON" if ct.SCAN2_CT_ENABLED else "❌ OFF"
    rows = [
        [{"text": f"₿ BTC Copy Trade  {_btc_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "ctbtc_on"},
         {"text": "🔴 Turn OFF", "callback_data": "ctbtc_off"}],
        [{"text": f"🔍 Scan1 Copy Trade  {_scan1_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "ctscan1_on"},
         {"text": "🔴 Turn OFF", "callback_data": "ctscan1_off"}],
        [{"text": f"🔍 Scan2 Copy Trade  {_scan2_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "ctscan2_on"},
         {"text": "🔴 Turn OFF", "callback_data": "ctscan2_off"}],
        [{"text": "◀️  Back to Menu", "callback_data": "help_main"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>📋 Copy Trade — By Type</b>\n\n"
        "Turn auto-copy on or off separately for BTC, Scan1, and Scan2 signals.\n"
        "OFF for a type means no user's account copies those trades — analysis/signals still post as normal.",
        {"inline_keyboard": rows}, message_id=message_id)

def send_go_screen(chat_id, message_id=None):
    """Renders the /go 'Bot RUNNING' screen. Its model/gateway buttons toggle
    in-place (go_model:/go_gateway:) instead of opening the separate detail screens."""
    _go_is_opus  = SCAN_MODEL == "claude-opus-4-8"
    _go_is_fable = SCAN_MODEL == "claude-fable-5"
    _go_model_lbl   = "Opus 4.8" if _go_is_opus else "Fable 5"
    _go_gateway_lbl = "Aerolink" if USE_AEROLINK else "Direct"
    _go_next_btc, _go_next_s1, _go_next_s2 = _next_schedule_times()
    _go_btc_line = f"⏰ Next BTC scan: <b>{_go_next_btc} IST</b>\n" if btc_analysis_enabled else "⏰ Next BTC scan: <b>OFF</b>\n"
    _go_s1_line  = f"⏰ Next Scan1: <b>{_go_next_s1}</b>\n" if SCAN1_AUTO_ENABLED else "⏰ Next Scan1: <b>OFF</b>\n"
    _go_s2_line  = f"⏰ Next Scan2: <b>{_go_next_s2}</b>\n" if SCAN2_AUTO_ENABLED else "⏰ Next Scan2: <b>OFF</b>\n"
    _ctrl_btns = {"inline_keyboard": [[
        {"text": "🔴 Pause All",    "callback_data": "bot_pause"},
        {"text": "🟠 Stop Scans",  "callback_data": "bot_stop"},
    ], [
        {"text": ("✅ " if _go_is_opus else "") + "🧠 Opus 4.8",  "callback_data": "go_model:opus"},
        {"text": ("✅ " if _go_is_fable else "") + "🧠 Fable 5",  "callback_data": "go_model:fable"},
    ], [
        {"text": ("✅ " if not USE_AEROLINK else "") + "🔌 Direct",   "callback_data": "go_gateway:direct"},
        {"text": ("✅ " if USE_AEROLINK else "") + "🔌 Aerolink", "callback_data": "go_gateway:aerolink"},
    ], [
        {"text": "◀️  Back", "callback_data": "help_main"},
    ]]}
    text = (
        f"▶️ <b>Bot RUNNING</b>\n\n"
        f"All scans, monitoring and alerts active.\n\n"
        f"🧠 Model:  <b>{_go_model_lbl}</b>\n"
        f"🔌 Gateway: <b>{_go_gateway_lbl}</b>\n\n"
        f"{_go_btc_line}"
        f"{_go_s1_line}"
        f"{_go_s2_line}\n"
        f"<i>🛡️ Capital protected</i>")
    if message_id:
        _help_edit_or_send(chat_id, text, _ctrl_btns, message_id=message_id)
    else:
        send_reply(chat_id, text, reply_markup=_ctrl_btns)

def send_help_menu(chat_id, is_admin, message_id=None, uname=None, cid=None):
    _sees_scanadmin_cats = is_admin or is_co_admin(cid if cid is not None else chat_id)
    rows = []
    for cat_id, (label, admin_only, _) in _HELP_CATS.items():
        if admin_only and not is_admin:
            if cat_id in ("scan", "tradecontrol") and _sees_scanadmin_cats:
                pass  # co-admin can see these two rooms
            else:
                continue
        rows.append([{"text": label, "callback_data": f"help_cat:{cat_id}"}])
    _extra_row = []
    if CONTACT_ADMIN_ENABLED and ADMIN_CHAT_ID:
        _extra_row.append({"text": "💬 Contact Admin", "url": f"tg://user?id={ADMIN_CHAT_ID}"})
    if CO_ADMIN_ENABLED and CO_ADMIN_CHAT_ID:
        _co_uname = user_usernames.get(str(CO_ADMIN_CHAT_ID))
        _co_url = f"https://t.me/{_co_uname}" if _co_uname else f"tg://user?id={CO_ADMIN_CHAT_ID}"
        _extra_row.append({"text": "💬 Contact Co-Admin", "url": _co_url})
    if SIGNAL_CHANNEL_ENABLED and SIGNAL_CHANNEL_LINK:
        _extra_row.append({"text": "📡 Signal Channel", "url": SIGNAL_CHANNEL_LINK})
    if _extra_row:
        rows.append(_extra_row)
    rows.append([{"text": "🆓 Free Channel", "callback_data": "chanpick:free"},
                 {"text": "⭐ VIP Channel",  "callback_data": "chanpick:vip"}])
    if is_admin:
        rows.append([{"text": "🔗 Contact/Channel Settings", "callback_data": "adminlinks_open"}])
    markup = {"inline_keyboard": rows}
    _is_co = is_co_admin(cid) if cid is not None else False
    role = "👑 Admin" if is_admin else ("🤝 Co-Admin" if _is_co else "👤 User")
    _greeting = f"👋 Welcome back, <b>{uname}</b>!\n\n" if uname else ""
    _pnl_line = ""
    _tier_line = ""
    if cid is not None:
        _u_ct = ct._get(str(cid))
        if _u_ct and _u_ct.get("connected"):
            _h = _u_ct.get("history", {})
            _pnl = _h.get("total_pnl", 0.0)
            _pnl_s = f"+${_pnl:.2f} 🟢" if _pnl > 0 else (f"-${abs(_pnl):.2f} 🔴" if _pnl < 0 else "$0.00")
            _pnl_line = f"💰 Your Copy Trade P&L: <b>{_pnl_s}</b>\n\n"
        if _u_ct:
            _tier_val = _u_ct.get("tier", "vip")
            _tag = ("⭐ VIP" + (f" (until {_u_ct['vip_end']})" if _u_ct.get("vip_end") else "")) if _tier_val == "vip" else "🆓 FREE"
            _tier_line = f"🏷 Your Tier: <b>{_tag}</b>\n\n"
    text = (
        f"✨ <b>CLEXER V17.8.5 — Help Menu</b>  {role}\n\n"
        f"{_greeting}{_tier_line}{_pnl_line}"
        "Tap a category to see commands 👇"
    )
    cid_str = str(chat_id)
    if message_id:
        # Editing existing message (e.g. Back button press) — no dedup needed
        _help_edit_or_send(chat_id, text, markup, message_id, rotate=True)
    else:
        # New /help command — delete previous help message first (clean chat)
        old_msg_id = _last_help_msg.get(cid_str)
        if old_msg_id:
            try:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage",
                    json={"chat_id": chat_id, "message_id": old_msg_id}, timeout=5)
            except Exception:
                pass
        # Send fresh help menu and track its message_id
        base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
        payload = {"chat_id": chat_id, "text": _apply_premium_emojis(text), "parse_mode": "HTML",
                   "reply_markup": _style_keyboard(markup, rotate=True), "disable_web_page_preview": True}
        try:
            r = requests.post(f"{base}/sendMessage", json=payload, timeout=10)
            new_msg_id = r.json().get("result", {}).get("message_id")
            if new_msg_id:
                _last_help_msg[cid_str] = new_msg_id
        except Exception:
            pass

# Categories that are a "main gate" — show sub-sections instead of a flat list.
# Maps cat_id -> (subcats dict, callback prefix used for that subcat's buttons)
_NESTED_CATS = {"copyuser": (_COPYUSER_SUBCATS, "copyuser_sub"), "scan": (_SCAN_SUBCATS, "scan_sub"),
                 "tradecontrol": (_TRADECONTROL_SUBCATS, "tradecontrol_sub"),
                 "copyadmin": (_COPYADMIN_SUBCATS, "copyadmin_sub"),
                 "settings": (_SETTINGS_SUBCATS, "settings_sub"),
                 "broadcast": (_BROADCAST_SUBCATS, "broadcast_sub"),
                 "tv": (_TV_SUBCATS, "tv_sub")}

def _find_back_target(cmd_text):
    """Find the correct 'Back' callback_data for a command — the immediate
    subcategory it lives in if nested, else the top-level category."""
    for cat_id, (subcats, cb_prefix) in _NESTED_CATS.items():
        for sub_id, (_, cmds) in subcats.items():
            if any(c == cmd_text for c, _, _, _ in cmds):
                return f"{cb_prefix}:{sub_id}", cat_id
    cat_id = next((cid_ for cid_, (_, _, cmds_) in _HELP_CATS.items()
                   if any(c == cmd_text for c, _, _ in cmds_)), "monitor")
    return f"help_cat:{cat_id}", cat_id

def _navigate_to(back_cb, chat_id, cid, msg_id, is_admin):
    """Renders whatever screen a 'back' callback_data string points to —
    lets code jump straight to a menu without the user tapping it themselves."""
    if back_cb == "help_main":
        send_help_menu(chat_id, is_admin, message_id=msg_id, cid=cid)
    elif back_cb.startswith("help_cat:"):
        send_help_category(chat_id, back_cb.split(":", 1)[1], is_admin, message_id=msg_id)
    elif back_cb.startswith("copyuser_sub:"):
        send_copyuser_subcat(chat_id, back_cb.split(":", 1)[1], cid, message_id=msg_id)
    elif back_cb.startswith("scan_sub:"):
        send_scan_subcat(chat_id, back_cb.split(":", 1)[1], message_id=msg_id)
    elif back_cb.startswith("tradecontrol_sub:"):
        send_tradecontrol_subcat(chat_id, back_cb.split(":", 1)[1], message_id=msg_id)
    elif back_cb.startswith("copyadmin_sub:"):
        _send_generic_subcat(chat_id, _COPYADMIN_SUBCATS, back_cb.split(":", 1)[1], "copyadmin", message_id=msg_id)
    elif back_cb.startswith("settings_sub:"):
        _send_generic_subcat(chat_id, _SETTINGS_SUBCATS, back_cb.split(":", 1)[1], "settings", message_id=msg_id)
    elif back_cb.startswith("broadcast_sub:"):
        _send_generic_subcat(chat_id, _BROADCAST_SUBCATS, back_cb.split(":", 1)[1], "broadcast", message_id=msg_id)
    elif back_cb.startswith("tv_sub:"):
        _send_generic_subcat(chat_id, _TV_SUBCATS, back_cb.split(":", 1)[1], "tv", message_id=msg_id)
    elif back_cb == "adminlinks_open":
        send_adminlinks_screen(chat_id, message_id=msg_id)
    elif back_cb == "userstats_open":
        send_userstats_screen(chat_id, message_id=msg_id)
    elif back_cb == "aicfg_open":
        send_aiconfig_screen(chat_id, message_id=msg_id)
    elif back_cb.startswith("aicfg_open:"):
        send_aiconfig_type_screen(chat_id, back_cb.split(":", 1)[1], message_id=msg_id)
    elif back_cb == "entrystyle_open":
        send_entrystyle_screen(chat_id, message_id=msg_id)
    elif back_cb == "coadmin_open":
        send_coadmin_screen(chat_id, message_id=msg_id)
    elif back_cb == "trailsl_open":
        send_trailsl_screen(chat_id, message_id=msg_id)
    elif back_cb == "channelmgmt_open":
        send_channelmgmt_screen(chat_id, message_id=msg_id)
    elif back_cb == "chanpick_open":
        send_channel_picker_screen(chat_id, message_id=msg_id)
    elif back_cb.startswith("trdpick_open:"):
        _action = back_cb.split(":", 1)[1]
        _orig_back = _TRDPICK_BACKCB.get(str(cid), "help_cat:monitor")
        _send_trade_pick_screen(chat_id, cid, _action, msg_id, _orig_back)
    else:
        send_help_menu(chat_id, is_admin, message_id=msg_id, cid=cid)

def send_help_category(chat_id, cat_id, is_admin, message_id=None):
    entry = _HELP_CATS.get(cat_id)
    if not entry:
        return
    label, admin_only, cmds = entry
    if admin_only and not is_admin and not (cat_id in ("scan", "tradecontrol") and is_co_admin(chat_id)):
        return

    if cat_id in _NESTED_CATS:
        subcats, cb_prefix = _NESTED_CATS[cat_id]
        rows = [[{"text": sub_label, "callback_data": f"{cb_prefix}:{sub_id}"}]
                for sub_id, (sub_label, _) in subcats.items()]
        rows.append([{"text": "◀️  Back to Menu", "callback_data": "help_main"}])
        markup = {"inline_keyboard": rows}
        text = f"<b>{label}</b>\n\n<i>Pick a section 👇</i>"
        _help_edit_or_send(chat_id, text, markup, message_id)
        return

    rows = []
    for cmd, emoji, desc in cmds:
        rows.append([{"text": f"{emoji}  {desc}", "callback_data": f"help_cmd:{cmd}"}])
    rows.append([{"text": "◀️  Back to Menu", "callback_data": "help_main"}])
    markup = {"inline_keyboard": rows}
    text = f"<b>{label}</b>\n\n<i>Tap any command to run it instantly 👇</i>"
    _help_edit_or_send(chat_id, text, markup, message_id)

def send_copyuser_subcat(chat_id, sub_id, user_cid, message_id=None):
    entry = _COPYUSER_SUBCATS.get(sub_id)
    if not entry:
        return
    label, cmds = entry
    user = ct._get(str(user_cid))
    connected = bool(user and user.get("connected"))

    rows = []
    desc_lines = []
    for cmd, emoji, title, desc in cmds:
        # Connection section: only show the button that applies right now
        if sub_id == "connect":
            if cmd == "/connect" and connected:
                continue
            if cmd == "/disconnect" and not connected:
                continue
        rows.append([{"text": f"{emoji}  {title}", "callback_data": f"help_cmd:{cmd}"}])
        desc_lines.append(f"<b>{emoji} {title}</b>\n<i>{desc}</i>")
    rows.append([{"text": "◀️  Back", "callback_data": "help_cat:copyuser"}])
    markup = {"inline_keyboard": rows}
    text = f"<b>{label}</b>\n\n" + "\n\n".join(desc_lines)
    _help_edit_or_send(chat_id, text, markup, message_id)

def send_scan_subcat(chat_id, sub_id, message_id=None):
    entry = _SCAN_SUBCATS.get(sub_id)
    if not entry:
        return
    label, cmds = entry
    rows = []
    desc_lines = []
    for cmd, emoji, title, desc in cmds:
        rows.append([{"text": f"{emoji}  {title}", "callback_data": f"help_cmd:{cmd}"}])
        desc_lines.append(f"<b>{emoji} {title}</b>\n<i>{desc}</i>")
    rows.append([{"text": "◀️  Back", "callback_data": "help_cat:scan"}])
    markup = {"inline_keyboard": rows}
    text = f"<b>{label}</b>\n\n" + "\n\n".join(desc_lines)
    _help_edit_or_send(chat_id, text, markup, message_id)

def send_tradecontrol_subcat(chat_id, sub_id, message_id=None):
    entry = _TRADECONTROL_SUBCATS.get(sub_id)
    if not entry:
        return
    label, cmds = entry
    rows = []
    desc_lines = []
    for cmd, emoji, title, desc in cmds:
        rows.append([{"text": f"{emoji}  {title}", "callback_data": f"help_cmd:{cmd}"}])
        desc_lines.append(f"<b>{emoji} {title}</b>\n<i>{desc}</i>")
    rows.append([{"text": "◀️  Back", "callback_data": "help_cat:tradecontrol"}])
    markup = {"inline_keyboard": rows}
    text = f"<b>{label}</b>\n\n" + "\n\n".join(desc_lines)
    _help_edit_or_send(chat_id, text, markup, message_id)

def _send_generic_subcat(chat_id, subcats, sub_id, back_cat, message_id=None):
    entry = subcats.get(sub_id)
    if not entry:
        return
    label, cmds = entry
    rows = []
    desc_lines = []
    for cmd, emoji, title, desc in cmds:
        rows.append([{"text": f"{emoji}  {title}", "callback_data": f"help_cmd:{cmd}"}])
        desc_lines.append(f"<b>{emoji} {title}</b>\n<i>{desc}</i>")
    rows.append([{"text": "◀️  Back", "callback_data": f"help_cat:{back_cat}"}])
    markup = {"inline_keyboard": rows}
    text = f"<b>{label}</b>\n\n" + "\n\n".join(desc_lines)
    _help_edit_or_send(chat_id, text, markup, message_id)

# ─── END HELP MENU ────────────────────────────────────────────────────────────


def command_listener():
    global last_update_id
    print("[CMD] Listener started")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook", timeout=10)
    except: pass
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"offset": last_update_id+1, "timeout": 20, "allowed_updates": ["message","callback_query","chat_join_request"]}, timeout=25)
            data = r.json()
            if not data.get("ok"): time.sleep(5); continue
            for upd in data.get("result", []):
                last_update_id = upd["update_id"]

                # Handle inline button callbacks
                cb = upd.get("callback_query")
                if cb:
                    cb_data     = cb.get("data","")
                    cb_cid      = cb["from"]["id"]
                    _cb_fname   = cb["from"].get("first_name") or cb["from"].get("username") or "User"
                    cb_uname    = cb["from"].get("username") or _cb_fname
                    cb_mention  = f'<a href="tg://user?id={cb_cid}">{_cb_fname}</a>'
                    cb_msg      = cb.get("message", {})
                    cb_msg_id   = cb_msg.get("message_id")
                    cb_chat_id  = cb_msg.get("chat", {}).get("id", cb_cid)
                    cb_is_admin = str(cb_cid) == str(ADMIN_CHAT_ID)
                    cb_is_scanadmin = cb_is_admin or is_co_admin(cb_cid)
                    register_user(cb_cid, cb["from"].get("username"))

                    # Pressing ANY button cancels a stale pending text-input prompt
                    # (e.g. Loop Mode / Manual Times) so a later stray message can't
                    # get misapplied to an abandoned flow. Handlers that need fresh
                    # pending_input set it themselves right after this.
                    pending_input.pop(cb_cid, None)

                    # Check admin-only BEFORE answering, so we can show alert popup
                    _is_admin_btn = False
                    if cb_data.startswith("help_cmd:"):
                        _ct = cb_data.split(":", 1)[1]
                        for _, (_cl, _cadm, _ccmds) in _HELP_CATS.items():
                            if _cadm and any(_ct == _c for _c, _, _ in _ccmds):
                                _is_admin_btn = True; break

                    if _is_admin_btn and not cb_is_admin:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                            json={"callback_query_id": cb["id"],
                                  "text": "😅 Bhai yeh admin only button hai! /help bhejo apne commands ke liye 👇",
                                  "show_alert": True}, timeout=5)
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": cb_chat_id,
                                  "text": f"😅 {cb_mention} Bhai yeh button admin only hai!\n\nTum /help send karo, main tumhare liye user commands deta hu 👇",
                                  "parse_mode": "HTML"}, timeout=10)
                        continue

                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                  json={"callback_query_id": cb["id"]}, timeout=5)

                    # Help menu navigation
                    if cb_data == "help_main":
                        send_help_menu(cb_chat_id, cb_is_admin, message_id=cb_msg_id, uname=cb_uname, cid=cb_cid)
                    elif cb_data.startswith("help_cat:"):
                        cat_id = cb_data.split(":", 1)[1]
                        send_help_category(cb_chat_id, cat_id, cb_is_admin, message_id=cb_msg_id)
                    elif cb_data.startswith("copyuser_sub:"):
                        sub_id = cb_data.split(":", 1)[1]
                        send_copyuser_subcat(cb_chat_id, sub_id, cb_cid, message_id=cb_msg_id)
                    elif cb_data.startswith("scan_sub:") and cb_is_scanadmin:
                        sub_id = cb_data.split(":", 1)[1]
                        send_scan_subcat(cb_chat_id, sub_id, message_id=cb_msg_id)
                    elif cb_data.startswith("tradecontrol_sub:") and cb_is_scanadmin:
                        sub_id = cb_data.split(":", 1)[1]
                        send_tradecontrol_subcat(cb_chat_id, sub_id, message_id=cb_msg_id)
                    elif cb_data.startswith("copyadmin_sub:") and cb_is_admin:
                        sub_id = cb_data.split(":", 1)[1]
                        _send_generic_subcat(cb_chat_id, _COPYADMIN_SUBCATS, sub_id, "copyadmin", message_id=cb_msg_id)
                    elif cb_data.startswith("settings_sub:") and cb_is_admin:
                        sub_id = cb_data.split(":", 1)[1]
                        _send_generic_subcat(cb_chat_id, _SETTINGS_SUBCATS, sub_id, "settings", message_id=cb_msg_id)
                    elif cb_data.startswith("broadcast_sub:") and cb_is_admin:
                        sub_id = cb_data.split(":", 1)[1]
                        _send_generic_subcat(cb_chat_id, _BROADCAST_SUBCATS, sub_id, "broadcast", message_id=cb_msg_id)
                    elif cb_data.startswith("tv_sub:") and cb_is_admin:
                        sub_id = cb_data.split(":", 1)[1]
                        _send_generic_subcat(cb_chat_id, _TV_SUBCATS, sub_id, "tv", message_id=cb_msg_id)

                    elif cb_data.startswith("tp_start:") and cb_is_scanadmin:
                        target = cb_data.split(":", 1)[1]
                        _tp_state[str(cb_cid)] = {"target": target, "digits": [], "times": [], "msg_id": cb_msg_id}
                        _tp_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data.startswith("tp_d:") and cb_is_scanadmin:
                        st = _tp_state.get(str(cb_cid))
                        if st and len(st["digits"]) < 4:
                            st["digits"].append(cb_data.split(":", 1)[1])
                            _tp_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data == "tp_prev" and cb_is_scanadmin:
                        st = _tp_state.get(str(cb_cid))
                        if st and st["digits"]:
                            st["digits"].pop()
                            _tp_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data == "tp_next" and cb_is_scanadmin:
                        st = _tp_state.get(str(cb_cid))
                        if st and len(st["digits"]) == 4:
                            h = int("".join(st["digits"][0:2])); m = int("".join(st["digits"][2:4]))
                            if 0 <= h <= 23 and 0 <= m <= 59:
                                st["times"].append((h, m))
                                st["digits"] = []
                                _tp_render(cb_chat_id, cb_cid, cb_msg_id)
                            else:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": "⚠️ Invalid time — hour must be 00-23, minute 00-59",
                                          "show_alert": True}, timeout=5)
                    elif cb_data == "tp_save" and cb_is_scanadmin:
                        st = _tp_state.get(str(cb_cid))
                        if st:
                            if len(st["digits"]) == 4:
                                h = int("".join(st["digits"][0:2])); m = int("".join(st["digits"][2:4]))
                                if 0 <= h <= 23 and 0 <= m <= 59:
                                    st["times"].append((h, m))
                                else:
                                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                        json={"callback_query_id": cb["id"], "text": "⚠️ Invalid time — hour must be 00-23, minute 00-59",
                                              "show_alert": True}, timeout=5)
                                    continue
                            if not st["times"]:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": "⚠️ Add at least one time first", "show_alert": True}, timeout=5)
                                continue
                            times_str = " ".join(f"{h}.{m:02d}" for h, m in st["times"])
                            applycmd = _TP_APPLYCMD[st["target"]]
                            del _tp_state[str(cb_cid)]
                            _toggle_cmd(f"{applycmd} {times_str}", cb_chat_id, cb_cid, cb_msg_id, _TP_BACKCAT[st["target"]])
                    elif cb_data == "tp_back" and cb_is_scanadmin:
                        st = _tp_state.pop(str(cb_cid), None)
                        cat = _TP_BACKCAT.get(st["target"], "scan") if st else "scan"
                        send_help_category(cb_chat_id, cat, cb_is_admin, message_id=cb_msg_id)

                    elif cb_data.startswith("np_d:"):
                        st = _np_state.get(str(cb_cid))
                        if st:
                            cfg = _NP_CONFIG[st["target"]]
                            ch = cb_data.split(":", 1)[1]
                            if ch == "." and (not cfg["decimals"] or "." in st["digits"]):
                                pass
                            elif len(st["digits"]) < 10:
                                st["digits"] += ch
                                _np_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data == "np_prev":
                        st = _np_state.get(str(cb_cid))
                        if st and st["digits"]:
                            st["digits"] = st["digits"][:-1]
                            _np_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data == "np_save":
                        st = _np_state.get(str(cb_cid))
                        if st:
                            cfg = _NP_CONFIG[st["target"]]
                            _digits = st["digits"]
                            try:
                                value = float(_digits) if _digits and _digits != "." else None
                            except ValueError:
                                value = None
                            if value is None:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": "⚠️ Enter a value first", "show_alert": True}, timeout=5)
                                continue
                            if not cfg["decimals"]:
                                value = int(value)
                            back_cb = st["back_cb"]
                            del _np_state[str(cb_cid)]
                            _toggle_cmd(f"{cfg['cmd']} {value}", cb_chat_id, cb_cid, cb_msg_id, back_cb)
                    elif cb_data == "np_back":
                        st = _np_state.pop(str(cb_cid), None)
                        back_cb = st["back_cb"] if st else "help_cat:copyuser"
                        _navigate_to(back_cb, cb_chat_id, cb_cid, cb_msg_id, cb_is_admin)
                    elif cb_data == "np_manual":
                        st = _np_state.pop(str(cb_cid), None)
                        if st:
                            cfg = _NP_CONFIG[st["target"]]
                            pending_input[cb_cid] = {"cmd": cfg["cmd"], "msg_id": cb_msg_id, "cat_id": st["back_cb"]}
                            _help_edit_or_send(cb_chat_id,
                                f"⌨️ <b>Type {cfg['label']}</b>\n\nSend the value as a message (e.g. <code>{'2.5' if cfg['decimals'] else '50'}</code>):",
                                {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": st["back_cb"]}]]},
                                message_id=cb_msg_id)

                    # ── Trade picker (SL/TP/close on any open trade) ──────────
                    elif cb_data.startswith("trdpick_open:") and cb_is_scanadmin:
                        _action = cb_data.split(":", 1)[1]
                        _orig_back = _TRDPICK_BACKCB.get(str(cb_cid), "help_cat:monitor")
                        _send_trade_pick_screen(cb_chat_id, cb_cid, _action, cb_msg_id, _orig_back)
                    elif cb_data.startswith("trdpick:") and cb_is_scanadmin:
                        _, _action, _kind, _idx_str = cb_data.split(":", 3)
                        _idx = int(_idx_str)
                        _trades = _all_open_trades()
                        _match = next((t for t in _trades if t["kind"] == _kind and t["idx"] == _idx), None)
                        if not _match:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                json={"callback_query_id": cb["id"], "text": "⚠️ That trade isn't open anymore", "show_alert": True}, timeout=5)
                            continue
                        _list_back = f"trdpick_open:{_action}"
                        if _action == "sltobe":
                            _ask_confirm(cb_chat_id, cb_cid, f"sltobe:{_kind}:{_match['symbol']}:{_idx}",
                                f"Move SL to breakeven for {_match['symbol']}?", _list_back, message_id=cb_msg_id)
                        elif _action == "closetrade":
                            _ask_confirm(cb_chat_id, cb_cid, f"closetrade:{_kind}:{_match['symbol']}:{_idx}",
                                f"Close {_match['symbol']}? This closes it on BingX for every copy user AND removes it from the bot.", _list_back, message_id=cb_msg_id)
                        else:
                            _pp_state[str(cb_cid)] = {"action": _action, "kind": _kind, "symbol": _match["symbol"],
                                                       "idx": _idx, "digits": "", "back_cb": _list_back}
                            _pp_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data.startswith("pp_d:"):
                        st = _pp_state.get(str(cb_cid))
                        if st:
                            ch = cb_data.split(":", 1)[1]
                            if ch == "." and "." in st["digits"]:
                                pass
                            elif len(st["digits"]) < 15:
                                st["digits"] += ch
                                _pp_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data == "pp_prev":
                        st = _pp_state.get(str(cb_cid))
                        if st and st["digits"]:
                            st["digits"] = st["digits"][:-1]
                            _pp_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data == "pp_back":
                        st = _pp_state.pop(str(cb_cid), None)
                        back_cb = st["back_cb"] if st else "help_cat:monitor"
                        _navigate_to(back_cb, cb_chat_id, cb_cid, cb_msg_id, cb_is_admin)
                    elif cb_data == "pp_manual":
                        st = _pp_state.pop(str(cb_cid), None)
                        if st:
                            label = {"setsl": "Stop Loss", "settp1": "TP1", "settp2": "TP2"}[st["action"]]
                            pending_input[cb_cid] = {"cmd": "_pp_manual", "msg_id": cb_msg_id, "cat_id": st["back_cb"], "pp": st}
                            _help_edit_or_send(cb_chat_id,
                                f"⌨️ <b>Type {label} — {st['symbol']}</b>\n\nSend the price as a message (e.g. <code>62000</code>):",
                                {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": st["back_cb"]}]]},
                                message_id=cb_msg_id)
                    elif cb_data == "pp_save":
                        st = _pp_state.get(str(cb_cid))
                        if st:
                            _digits = st["digits"]
                            try:
                                price = float(_digits) if _digits and _digits != "." else None
                            except ValueError:
                                price = None
                            if price is None or price <= 0:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": "⚠️ Enter a valid price greater than 0", "show_alert": True}, timeout=5)
                                continue
                            _ok, _reason = _apply_trade_price_edit(st["action"], st["kind"], st["symbol"], st["idx"], price)
                            if not _ok and _reason and "no longer open" not in _reason:
                                # Bad price relative to entry/direction — let them retry, don't discard the picker
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": f"⚠️ {_reason}", "show_alert": True}, timeout=5)
                                continue
                            _back_cb = st["back_cb"]
                            del _pp_state[str(cb_cid)]
                            if _ok:
                                send_telegram(f"<b>{st['symbol']} {st['action'].upper()} -&gt; {price:,.6f}</b>\n\n<i>🛡️ Capital protected</i>")
                            _msg = f"✅ <b>{st['symbol']} updated to {price:,.6f}</b>" if _ok else f"⚠️ {_reason or st['symbol'] + ' trade no longer open.'}"
                            _help_edit_or_send(cb_chat_id, _msg,
                                {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": _back_cb}]]}, message_id=cb_msg_id)

                    elif cb_data.startswith("help_cmd:"):
                        cmd_text = cb_data.split(":", 1)[1]
                        # Check if non-admin is pressing an admin-only button
                        _INPUT_PROMPTS = {
                            "/connect":     "🔗 <b>Connect BingX — Step 1/2</b>\n\nPlease type your <b>API Key</b>:",
                        }
                        # Find which subcategory (or top-level category) this command belongs to (for Back button)
                        _back_cb, _cmd_cat = _find_back_target(cmd_text)
                        _back_markup = {"inline_keyboard": [[
                            {"text": "◀️  Back", "callback_data": _back_cb}]]}

                        _CONFIRM_FIRST = {
                            "/closescan":  ("closescan", "Clear ALL open Scan1 + Scan2 trades? This closes them in the bot immediately."),
                            "/disconnect": (f"disconnect:{cb_cid}", "Disconnect your BingX account? Your API keys will be removed (open positions stay open — manage them manually)."),
                        }
                        _NP_TARGETS = {"/setsize": "setsize", "/setleverage": "setleverage", "/setrisk": "setrisk", "/tp1size": "tp1size", "/freelimit": "freelimit"}
                        _SCREEN_CMDS = {"/adminlinks": send_adminlinks_screen, "/userstats": send_userstats_screen,
                                        "/coadmin": send_coadmin_screen, "/channelmgmt": send_channelmgmt_screen}
                        _SCAN_SCREEN_CMDS = {"/scancopy": send_ctpause_screen, "/ctpause": send_ctpause_screen,
                                             "/aiconfig": send_aiconfig_screen, "/entrystyle": send_entrystyle_screen,
                                             "/trailsl": send_trailsl_screen}
                        _TRDPICK_TARGETS = {"/sltobe": "sltobe", "/setsl": "setsl", "/settp1": "settp1",
                                            "/settp2": "settp2", "/closetrade": "closetrade"}
                        if cmd_text in _SCREEN_CMDS and cb_is_admin:
                            _SCREEN_CMDS[cmd_text](cb_chat_id, message_id=cb_msg_id)
                        elif cmd_text in _SCAN_SCREEN_CMDS and cb_is_scanadmin:
                            _SCAN_SCREEN_CMDS[cmd_text](cb_chat_id, message_id=cb_msg_id)
                        elif cmd_text in _TRDPICK_TARGETS and cb_is_scanadmin:
                            _TRDPICK_BACKCB[str(cb_cid)] = _back_cb
                            _send_trade_pick_screen(cb_chat_id, cb_cid, _TRDPICK_TARGETS[cmd_text], cb_msg_id, _back_cb)
                        elif cmd_text in _CONFIRM_FIRST:
                            _action_id, _label = _CONFIRM_FIRST[cmd_text]
                            _ask_confirm(cb_chat_id, cb_cid, _action_id, _label, _back_cb, message_id=cb_msg_id)
                        elif cmd_text in _NP_TARGETS:
                            _np_state[str(cb_cid)] = {"target": _NP_TARGETS[cmd_text], "digits": "", "back_cb": _back_cb}
                            _np_render(cb_chat_id, cb_cid, cb_msg_id)
                        elif cmd_text in _INPUT_PROMPTS:
                            pending_input[cb_cid] = {"cmd": cmd_text, "step": "api_key", "msg_id": cb_msg_id, "cat_id": _back_cb} if cmd_text == "/connect" else {"cmd": cmd_text, "msg_id": cb_msg_id, "cat_id": _back_cb}
                            _help_edit_or_send(cb_chat_id, _INPUT_PROMPTS[cmd_text], _back_markup, message_id=cb_msg_id)
                        else:
                            # Capture the command output and edit message in-place
                            cid_str = str(cb_cid)
                            _reply_capture[cid_str] = {"texts": [], "cat_id": _cmd_cat}
                            handle_command(cmd_text, cb_chat_id, {}, sender_id=cb_cid)
                            captured = _reply_capture.pop(cid_str, {})
                            result_text = "\n\n".join(captured.get("texts", [])) or f"✅ Done: {cmd_text}"
                            if len(result_text) > 4000:
                                result_text = result_text[:4000] + "\n\n<i>...truncated</i>"
                            # Merge captured inline buttons + Back button
                            cap_mkp = captured.get("markup")
                            if cap_mkp and "inline_keyboard" in cap_mkp:
                                merged_rows = cap_mkp["inline_keyboard"] + _back_markup["inline_keyboard"]
                            else:
                                merged_rows = _back_markup["inline_keyboard"]
                            _help_edit_or_send(cb_chat_id, result_text, {"inline_keyboard": merged_rows}, message_id=cb_msg_id)

                    # ── Copytrade ON/OFF ─────────────────────────────────────
                    elif cb_data in ("copytrade_on", "copytrade_off"):
                        _toggle_cmd(f"/copytrade {'on' if cb_data=='copytrade_on' else 'off'}", cb_chat_id, cb_cid, cb_msg_id, "copyuser")

                    # ── Mysize quick-set buttons ──────────────────────────────
                    elif cb_data in ("mysize_setsize", "mysize_setlev", "mysize_setrisk"):
                        _map = {"mysize_setsize": "setsize", "mysize_setlev": "setleverage", "mysize_setrisk": "setrisk"}
                        _cmd_map = {"mysize_setsize": "/setsize", "mysize_setlev": "/setleverage", "mysize_setrisk": "/setrisk"}
                        _ms_back_cb, _ = _find_back_target(_cmd_map[cb_data])
                        _np_state[str(cb_cid)] = {"target": _map[cb_data], "digits": "", "back_cb": _ms_back_cb}
                        _np_render(cb_chat_id, cb_cid, cb_msg_id)

                    # ── Nocopy coin block/unblock ─────────────────────────────
                    elif cb_data.startswith("nocopy_blk:"):
                        _nc_coin = cb_data.split(":", 1)[1]
                        _nc_back_cb, _ = _find_back_target("/nocopy")
                        _ask_confirm(cb_chat_id, cb_cid, f"nocopy_blk:{_nc_coin}",
                            f"Block {_nc_coin} from being auto-copied?", _nc_back_cb, message_id=cb_msg_id)
                    elif cb_data.startswith("nocopy_clr:"):
                        _nc_coin = cb_data.split(":", 1)[1]
                        _nc_cmd = f"/nocopy clear {_nc_coin}"
                        _nc_back_cb, _ = _find_back_target("/nocopy")
                        cid_str = str(cb_cid)
                        _reply_capture[cid_str] = {"texts": [], "cat_id": _nc_back_cb}
                        handle_command(_nc_cmd, cb_chat_id, {}, sender_id=cb_cid)
                        captured = _reply_capture.pop(cid_str, {})
                        result_text = "\n\n".join(captured.get("texts", [])) or "✅ Done"
                        cap_mkp = captured.get("markup")
                        if cap_mkp and "inline_keyboard" in cap_mkp:
                            _help_edit_or_send(cb_chat_id, result_text, cap_mkp, message_id=cb_msg_id)
                        else:
                            _help_edit_or_send(cb_chat_id, result_text, {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": _nc_back_cb}]]}, message_id=cb_msg_id)
                    elif cb_data == "nocopy_type":
                        _nc_back_cb, _ = _find_back_target("/nocopy")
                        pending_input[cb_cid] = {"cmd": "/nocopy", "msg_id": cb_msg_id, "cat_id": _nc_back_cb}
                        _help_edit_or_send(cb_chat_id, "⌨️ <b>Type Coin Name</b>\n\nEnter the coin (e.g. <code>SOL</code>):",
                            {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": _nc_back_cb}]]},
                            message_id=cb_msg_id)

                    # ── Images ON/OFF ─────────────────────────────────────────
                    elif cb_data in ("images_on", "images_off"):
                        _toggle_cmd(f"/images {'on' if cb_data=='images_on' else 'off'}", cb_chat_id, cb_cid, cb_msg_id, "settings")

                    # ── Setimages TF buttons ──────────────────────────────────
                    elif cb_data.startswith("setimg:"):
                        tf = cb_data.split(":", 1)[1]
                        global CHART_TFS
                        if tf in CHART_TFS:
                            CHART_TFS = [t for t in CHART_TFS if t != tf]
                        else:
                            CHART_TFS.append(tf)
                        save_settings()
                        _tf_btns2 = {"inline_keyboard": [
                            [{"text": f"{'✅' if 'weekly' in CHART_TFS else '📅'}  Weekly", "callback_data": "setimg:weekly"},
                             {"text": f"{'✅' if '4h' in CHART_TFS else '📊'}  4H",         "callback_data": "setimg:4h"}],
                            [{"text": f"{'✅' if '1h' in CHART_TFS else '📈'}  1H",          "callback_data": "setimg:1h"},
                             {"text": f"{'✅' if '15m' in CHART_TFS else '⏱'}  15M",        "callback_data": "setimg:15m"}],
                            [{"text": f"{'✅' if '5m' in CHART_TFS else '⚡'}  5M",          "callback_data": "setimg:5m"}]]}
                        _help_edit_or_send(cb_chat_id,
                            f"<b>Chart Timeframes</b>\n\nActive: <b>{', '.join(CHART_TFS).upper() or 'none'}</b>\n\n<i>Tap to toggle ✅ = active</i>",
                            _tf_btns2, message_id=cb_msg_id)

                    # ── News ON/OFF ───────────────────────────────────────────
                    elif cb_data in ("news_on", "news_off"):
                        _toggle_cmd(f"/news {'on' if cb_data=='news_on' else 'off'}", cb_chat_id, cb_cid, cb_msg_id, "settings")

                    # ── BTC Mode V7/V9 ────────────────────────────────────────
                    elif cb_data in ("btcmode_v7", "btcmode_v9"):
                        _toggle_cmd(f"/btcmode {'on' if cb_data=='btcmode_v7' else 'off'}", cb_chat_id, cb_cid, cb_msg_id, "settings")

                    # ── Copy Trade by type: BTC / Scan1 / Scan2 ON/OFF ────────
                    elif cb_data == "ctbtc_on" and cb_is_scanadmin:
                        ct.set_btc_ct(True); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "ctbtc_off" and cb_is_scanadmin:
                        ct.set_btc_ct(False); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "ctscan1_on" and cb_is_scanadmin:
                        ct.set_scan1_ct(True); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "ctscan1_off" and cb_is_scanadmin:
                        ct.set_scan1_ct(False); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "ctscan2_on" and cb_is_scanadmin:
                        ct.set_scan2_ct(True); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "ctscan2_off" and cb_is_scanadmin:
                        ct.set_scan2_ct(False); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)

                    # ── Miniapp pause/resume ──────────────────────────────────
                    elif cb_data in ("miniapp_pause", "miniapp_resume"):
                        _toggle_cmd(f"/miniapp {'pause' if cb_data=='miniapp_pause' else 'resume'}", cb_chat_id, cb_cid, cb_msg_id, "settings")

                    elif cb_data in ("btca_on", "btca_off") and cb_is_scanadmin:
                        global btc_analysis_enabled
                        btc_analysis_enabled = (cb_data == "btca_on")
                        save_settings()
                        _btca_mkp = {"inline_keyboard": [[
                            {"text": "🟢 Enable Analysis",  "callback_data": "btca_on"},
                            {"text": "🔴 Disable Analysis", "callback_data": "btca_off"},
                        ]]}
                        if btc_analysis_enabled:
                            _btca_text = "📡 <b>BTC Analysis</b>  ✅ ON\n\nScheduled scans active.\n\n<i>🛡️ Capital protected</i>"
                        else:
                            _btca_text = "📡 <b>BTC Analysis</b>  ⏸ OFF\n\nScheduled scans paused.\n\n<i>🛡️ Capital protected</i>"
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
                            json={"chat_id": cb_chat_id, "message_id": cb_msg_id,
                                  "text": _apply_premium_emojis(_btca_text), "parse_mode": "HTML",
                                  "reply_markup": _style_keyboard(_btca_mkp)}, timeout=10)
                    elif cb_data in ("history_btc", "history_scan1", "history_scan2"):
                        sub = cb_data.replace("history_", "")
                        _toggle_cmd(f"/history {sub}", cb_chat_id, cb_cid, cb_msg_id, "monitor")
                    elif cb_data == "stats_win":
                        _toggle_cmd("/stats", cb_chat_id, cb_cid, cb_msg_id, "monitor")
                    elif cb_data in ("reset_btc_stats", "reset_scan1_stats", "reset_scan2_stats") and cb_is_admin:
                        _labels = {"reset_btc_stats": "Reset all BTC trade statistics?",
                                   "reset_scan1_stats": "Reset all Scan1 trade statistics?",
                                   "reset_scan2_stats": "Reset all Scan2 trade statistics?"}
                        _ask_confirm(cb_chat_id, cb_cid, cb_data, _labels[cb_data], "help_cat:monitor", message_id=cb_msg_id)
                    elif cb_data == "reset_signal_history" and cb_is_admin:
                        _ask_confirm(cb_chat_id, cb_cid, "reset_signal_history",
                            "Clear the Last 5 Signals history (BTC + Scan1 + Scan2)? The CSV trade log is not affected.",
                            "help_cat:monitor", message_id=cb_msg_id)
                    elif cb_data == "myhistory_reset":
                        _ask_confirm(cb_chat_id, cb_cid, f"reset_pnl:{cb_cid}",
                            "Reset your copy-trade P&L history? Your connection and settings stay unchanged.",
                            "help_cat:copyuser", message_id=cb_msg_id)
                    elif cb_data == "confirm_yes":
                        pc = _pending_confirm.pop(cb_cid, None)
                        if pc:
                            _result = _run_confirmed_action(pc["action"], cb_chat_id, cb_cid, cb_msg_id, pc["back_cb"])
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                json={"callback_query_id": cb["id"], "text": _strip_html(_result)[:190], "show_alert": True}, timeout=5)
                            _navigate_to(pc["back_cb"], cb_chat_id, cb_cid, cb_msg_id, cb_is_admin)
                    elif cb_data == "adminlinks_open" and cb_is_admin:
                        send_adminlinks_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "userstats_open" and cb_is_admin:
                        send_userstats_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "userstats_total" and cb_is_admin:
                        send_userstats_list(cb_chat_id, "total", message_id=cb_msg_id)
                    elif cb_data == "userstats_active" and cb_is_admin:
                        send_userstats_list(cb_chat_id, "active", message_id=cb_msg_id)
                    elif cb_data == "userstats_blocked" and cb_is_admin:
                        send_userstats_list(cb_chat_id, "blocked", message_id=cb_msg_id)
                    elif cb_data == "aicfg_open" and cb_is_scanadmin:
                        send_aiconfig_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data.startswith("aicfg_open:") and cb_is_scanadmin:
                        send_aiconfig_type_screen(cb_chat_id, cb_data.split(":", 1)[1], message_id=cb_msg_id)
                    elif cb_data.startswith("aicfg_set:") and cb_is_scanadmin:
                        global SCAN_MODEL, USE_AEROLINK, SCAN1_MODEL, SCAN1_AEROLINK, SCAN2_MODEL, SCAN2_AEROLINK, TEST_MODEL, TEST_AEROLINK
                        _, _kind, _gw, _mdl = cb_data.split(":", 3)
                        _model_val = "claude-opus-4-8" if _mdl == "opus" else "claude-fable-5"
                        _aero_val = (_gw == "aerolink")
                        if _kind == "btc":
                            SCAN_MODEL = _model_val; USE_AEROLINK = _aero_val
                        elif _kind == "scan1":
                            SCAN1_MODEL = _model_val; SCAN1_AEROLINK = _aero_val
                        elif _kind == "scan2":
                            SCAN2_MODEL = _model_val; SCAN2_AEROLINK = _aero_val
                        else:
                            TEST_MODEL = _model_val; TEST_AEROLINK = _aero_val
                        save_settings()
                        send_aiconfig_type_screen(cb_chat_id, _kind, message_id=cb_msg_id)
                    elif cb_data == "trailsl_btc_on" and cb_is_scanadmin:
                        global TRAIL_SL_BTC
                        TRAIL_SL_BTC = True; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "trailsl_btc_off" and cb_is_scanadmin:
                        TRAIL_SL_BTC = False; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "trailsl_scan1_on" and cb_is_scanadmin:
                        global TRAIL_SL_SCAN1
                        TRAIL_SL_SCAN1 = True; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "trailsl_scan1_off" and cb_is_scanadmin:
                        TRAIL_SL_SCAN1 = False; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "trailsl_scan2_on" and cb_is_scanadmin:
                        global TRAIL_SL_SCAN2
                        TRAIL_SL_SCAN2 = True; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "trailsl_scan2_off" and cb_is_scanadmin:
                        TRAIL_SL_SCAN2 = False; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "coadmin_open" and cb_is_admin:
                        send_coadmin_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "coadmin_on" and cb_is_admin:
                        global CO_ADMIN_ENABLED
                        CO_ADMIN_ENABLED = True; save_settings()
                        send_coadmin_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "coadmin_off" and cb_is_admin:
                        CO_ADMIN_ENABLED = False; save_settings()
                        send_coadmin_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "freelimit_open" and cb_is_admin:
                        _np_state[str(cb_cid)] = {"target": "freelimit", "digits": "", "back_cb": "channelmgmt_open"}
                        _np_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data == "chanpick_open":
                        send_channel_picker_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data.startswith("chanpick:"):
                        send_channel_picker_result(cb_chat_id, cb_data.split(":", 1)[1], message_id=cb_msg_id)
                    elif cb_data.startswith("chrm_add:") and cb_is_admin:
                        _tier = cb_data.split(":", 1)[1]
                        pending_input[cb_cid] = {"cmd": "_chrm_add", "msg_id": cb_msg_id, "cat_id": "channelmgmt_open", "tier": _tier}
                        _help_edit_or_send(cb_chat_id,
                            f"➕ <b>Add {'VIP' if _tier=='vip' else 'Free'} Channel</b>\n\n"
                            f"Add this bot as admin to the channel, then send its <b>public link</b>, e.g.\n"
                            f"<code>https://t.me/yourchannel</code>\n\n"
                            f"If it's a <b>private</b> channel (invite-link only, no public username), send its "
                            f"numeric ID instead — forward any message from it to @userinfobot to get that ID.",
                            {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": "channelmgmt_open"}]]},
                            message_id=cb_msg_id)
                    elif cb_data.startswith("chrm_remove:") and cb_is_admin:
                        _idx = int(cb_data.split(":", 1)[1])
                        if 0 <= _idx < len(CHANNELS):
                            CHANNELS.pop(_idx); save_settings()
                        send_channelmgmt_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "coadmin_pick" and cb_is_admin:
                        send_coadmin_pick_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "profile_switch" and cb_is_admin:
                        global ACTIVE_PROFILE
                        _SETTINGS_PROFILES[ACTIVE_PROFILE] = _snapshot_scan_settings()
                        ACTIVE_PROFILE = "coadmin" if ACTIVE_PROFILE == "mine" else "mine"
                        _apply_scan_settings(_SETTINGS_PROFILES.get(ACTIVE_PROFILE, {}))
                        save_settings()
                        send_coadmin_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data.startswith("coadmin_set:") and cb_is_admin:
                        global CO_ADMIN_CHAT_ID
                        CO_ADMIN_CHAT_ID = cb_data.split(":", 1)[1]
                        save_settings()
                        send_coadmin_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data.startswith("entrystyle:") and cb_is_scanadmin:
                        global ZONE_ENTRY_ENABLED
                        ZONE_ENTRY_ENABLED = (cb_data.split(":", 1)[1] == "zone")
                        save_settings()
                        send_entrystyle_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "adminlinks_ca_on" and cb_is_admin:
                        global CONTACT_ADMIN_ENABLED, SIGNAL_CHANNEL_ENABLED, SIGNAL_CHANNEL_LINK
                        CONTACT_ADMIN_ENABLED = True; save_settings()
                        send_adminlinks_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "adminlinks_ca_off" and cb_is_admin:
                        CONTACT_ADMIN_ENABLED = False; save_settings()
                        send_adminlinks_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "adminlinks_sc_on" and cb_is_admin:
                        SIGNAL_CHANNEL_ENABLED = True; save_settings()
                        send_adminlinks_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "adminlinks_sc_off" and cb_is_admin:
                        SIGNAL_CHANNEL_ENABLED = False; save_settings()
                        send_adminlinks_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "adminlinks_remove_channel" and cb_is_admin:
                        _ask_confirm(cb_chat_id, cb_cid, "remove_channel_link",
                            "Remove the connected channel link? Users won't see the Signal Channel button until you set a new one.",
                            "help_main", message_id=cb_msg_id)
                    elif cb_data == "adminlinks_connect_channel" and cb_is_admin:
                        pending_input[cb_cid] = {"cmd": "_adminlinks_set_channel", "msg_id": cb_msg_id, "cat_id": "help_main"}
                        _help_edit_or_send(cb_chat_id,
                            "🔗 <b>Connect Channel</b>\n\nPaste the channel's invite link (e.g. <code>https://t.me/yourchannel</code>):",
                            {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": "adminlinks_open"}]]},
                            message_id=cb_msg_id)

                    elif cb_data.startswith("broadcast_mode:") and cb_is_admin:
                        _mode = cb_data.split(":", 1)[1]
                        broadcast_pending[cb_chat_id] = {"step": "waiting_message", "mode": _mode}
                        _mode_lbl = {"users": "Users Only", "channels": "Channels Only", "all": "Both"}[_mode]
                        _help_edit_or_send(cb_chat_id,
                            f"📢 <b>Broadcast — {_mode_lbl}</b>\n\nSend message now (text/image/PDF).\n\n<i>/cancel to abort</i>",
                            None, message_id=cb_msg_id)
                    elif cb_data == "confirm_no":
                        pc = _pending_confirm.pop(cb_cid, None)
                        _back_cb = pc["back_cb"] if pc else "help_main"
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                            json={"callback_query_id": cb["id"], "text": "❌ Cancelled — nothing changed."}, timeout=5)
                        _navigate_to(_back_cb, cb_chat_id, cb_cid, cb_msg_id, cb_is_admin)
                    elif cb_data.startswith("sync_close_btc:"):
                        uid = cb_data.split(":")[1]
                        handle_command(f"/ctclose {uid}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data.startswith("sync_adopt_btc:"):
                        uid = cb_data.split(":")[1]
                        handle_command(f"/ctretry {uid}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data.startswith("sync_reset_ghost:"):
                        uid = cb_data.split(":")[1]
                        handle_command(f"/ctsync {uid}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data.startswith("sync_adopt_scan:"):
                        _, uid, sym = cb_data.split(":")
                        handle_command(f"/ctretry {uid} {sym}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data.startswith("sync_close_scan:"):
                        _, uid, sym = cb_data.split(":")
                        handle_command(f"/closetrade {sym.replace('-USDT','')}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data == "bot_go" and cb_is_admin:
                        _toggle_cmd("/go", cb_chat_id, cb_cid, cb_msg_id, "settings")
                    elif cb_data == "bot_pause" and cb_is_admin:
                        _toggle_cmd("/pause", cb_chat_id, cb_cid, cb_msg_id, "settings")
                    elif cb_data == "bot_stop" and cb_is_admin:
                        _toggle_cmd("/stop", cb_chat_id, cb_cid, cb_msg_id, "settings")
                    elif cb_data.startswith("scantoggle:"):
                        if cb_is_admin:
                            _toggle_cmd(f"/scantoggle {cb_data.split(':')[1]}", cb_chat_id, cb_cid, cb_msg_id, "scan")
                    elif cb_data.startswith("model:") and cb_is_admin:
                        _marg = cb_data.split(":")[1]
                        if _marg == "opus":  SCAN_MODEL = "claude-opus-4-8"
                        elif _marg == "fable": SCAN_MODEL = "claude-fable-5"
                        save_settings()
                        _is_opus  = SCAN_MODEL == "claude-opus-4-8"
                        _is_fable = SCAN_MODEL == "claude-fable-5"
                        _model_mkp = {"inline_keyboard": [
                            [{"text": ("✅ " if _is_opus else "") + "Opus 4.8 ($15/$75)",  "callback_data": "model:opus"},
                             {"text": ("✅ " if _is_fable else "") + "Fable 5 ($10/$50)",  "callback_data": "model:fable"}],
                            [{"text": "◀️  Back", "callback_data": "help_cat:scan"}],
                        ]}
                        _model_text = (
                            f"<b>🧠 AI Model</b>\n\n"
                            f"Active: <b>{SCAN_MODEL}</b>\n\n"
                            f"Opus 4.8  — $15 in / $75 out per 1M tokens\n"
                            f"Fable 5   — $10 in / $50 out per 1M tokens (~33% cheaper)\n\n"
                            f"Used for all scan/BTC/coin analysis calls.\n\n"
                            f"<i>🛡️ Capital protected</i>")
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
                            json={"chat_id": cb_chat_id, "message_id": cb_msg_id,
                                  "text": _apply_premium_emojis(_model_text), "parse_mode": "HTML",
                                  "reply_markup": _style_keyboard(_model_mkp)}, timeout=10)

                    elif cb_data.startswith("gateway:") and cb_is_admin:
                        _garg = cb_data.split(":")[1]
                        if _garg == "direct":
                            USE_AEROLINK = False
                        elif _garg == "aerolink":
                            if not AEROLINK_API_KEY:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"],
                                          "text": "⚠️ AEROLINK_API_KEY not set in Railway env vars.",
                                          "show_alert": True}, timeout=5)
                                continue
                            USE_AEROLINK = True
                        save_settings()
                        _gw_mkp = {"inline_keyboard": [
                            [{"text": ("✅ " if not USE_AEROLINK else "") + "Direct (Anthropic)", "callback_data": "gateway:direct"},
                             {"text": ("✅ " if USE_AEROLINK else "") + "Aerolink Gateway",  "callback_data": "gateway:aerolink"}],
                            [{"text": "◀️  Back", "callback_data": "help_cat:scan"}],
                        ]}
                        _gw_text = (
                            f"<b>🔌 API Gateway</b>\n\n"
                            f"Active: <b>{'Aerolink Gateway' if USE_AEROLINK else 'Direct (Anthropic)'}</b>\n\n"
                            f"Direct — uses your own ANTHROPIC_API_KEY straight to Anthropic.\n"
                            f"Aerolink — uses a separate AEROLINK_API_KEY through capi.aerolink.lat.\n"
                            f"Your real Anthropic key is never sent to Aerolink — the two keys stay fully separate.\n\n"
                            f"<i>🛡️ Capital protected</i>")
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
                            json={"chat_id": cb_chat_id, "message_id": cb_msg_id,
                                  "text": _apply_premium_emojis(_gw_text), "parse_mode": "HTML",
                                  "reply_markup": _style_keyboard(_gw_mkp)}, timeout=10)

                    elif cb_data.startswith("go_model:") and cb_is_admin:
                        _garg = cb_data.split(":")[1]
                        if _garg == "opus":  SCAN_MODEL = "claude-opus-4-8"
                        elif _garg == "fable": SCAN_MODEL = "claude-fable-5"
                        save_settings()
                        send_go_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data.startswith("go_gateway:") and cb_is_admin:
                        _garg = cb_data.split(":")[1]
                        if _garg == "direct":
                            USE_AEROLINK = False
                        elif _garg == "aerolink":
                            if not AEROLINK_API_KEY:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"],
                                          "text": "⚠️ AEROLINK_API_KEY not set in Railway env vars.",
                                          "show_alert": True}, timeout=5)
                                continue
                            USE_AEROLINK = True
                        save_settings()
                        send_go_screen(cb_chat_id, message_id=cb_msg_id)

                    elif cb_data == "noop":
                        pass
                    elif cb_data == "test_on" and cb_is_scanadmin:
                        _toggle_cmd("/test on", cb_chat_id, cb_cid, cb_msg_id, "scan")
                    elif cb_data == "test_off" and cb_is_scanadmin:
                        _toggle_cmd("/test off", cb_chat_id, cb_cid, cb_msg_id, "scan")
                    elif cb_data == "test_run" and cb_is_scanadmin:
                        _toggle_cmd("/test run", cb_chat_id, cb_cid, cb_msg_id, "scan")
                    elif cb_data.startswith("pausech:"):
                        _toggle_cmd(f"/pausechannel {cb_data.split(':')[1]}", cb_chat_id, cb_cid, cb_msg_id, "broadcast")
                    elif cb_data.startswith("resumech:"):
                        _toggle_cmd(f"/resumechannel {cb_data.split(':')[1]}", cb_chat_id, cb_cid, cb_msg_id, "broadcast")
                    elif cb_data.startswith("userinfo:"):
                        uid = cb_data.split(":")[1]
                        handle_command(f"/user {uid}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data.startswith("kick:"):
                        uid = cb_data.split(":")[1]
                        _kick_back, _ = _find_back_target("/kick")
                        _ask_confirm(cb_chat_id, cb_cid, f"kick:{uid}",
                            f"Remove user {uid}? This disconnects them and cancels any pending orders.",
                            _kick_back, message_id=cb_msg_id)
                    elif cb_data.startswith("pauseuser:"):
                        uid = cb_data.split(":")[1]
                        handle_command(f"/pauseuser {uid}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data.startswith("free_set:") and cb_is_admin:
                        uid = cb_data.split(":", 1)[1]
                        _tgt_uname = user_usernames.get(str(uid), str(uid))
                        handle_command(f"/setfree {uid} {_tgt_uname}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data.startswith("vip_pick:") and cb_is_admin:
                        uid = cb_data.split(":", 1)[1]
                        _vip_state[str(cb_cid)] = {"uid": uid, "stage": "start", "digits": "", "start": ""}
                        _vip_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data.startswith("vip_d:") and cb_is_admin:
                        st = _vip_state.get(str(cb_cid))
                        if st and len(st["digits"]) < 8:
                            st["digits"] += cb_data.split(":", 1)[1]
                            _vip_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data == "vip_prev" and cb_is_admin:
                        st = _vip_state.get(str(cb_cid))
                        if st and st["digits"]:
                            st["digits"] = st["digits"][:-1]
                            _vip_render(cb_chat_id, cb_cid, cb_msg_id)
                    elif cb_data == "vip_back" and cb_is_admin:
                        _vip_state.pop(str(cb_cid), None)
                        _navigate_to("help_cat:copyadmin", cb_chat_id, cb_cid, cb_msg_id, cb_is_admin)
                    elif cb_data == "vip_manual" and cb_is_admin:
                        st = _vip_state.pop(str(cb_cid), None)
                        if st:
                            label = "Start Date" if st["stage"] == "start" else "End Date"
                            pending_input[cb_cid] = {"cmd": "_vip_manual_date", "msg_id": cb_msg_id, "cat_id": "help_cat:copyadmin", "vip": st}
                            _help_edit_or_send(cb_chat_id,
                                f"⌨️ <b>Type VIP {label}</b>\n\nSend as <code>DD.MM.YYYY</code> (e.g. <code>17.08.2026</code>):",
                                {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": "help_cat:copyadmin"}]]},
                                message_id=cb_msg_id)
                    elif cb_data == "vip_save" and cb_is_admin:
                        st = _vip_state.get(str(cb_cid))
                        if st and len(st["digits"]) == 8:
                            date_str = _digits_to_date(st["digits"])
                            if st["stage"] == "start":
                                st["start"] = date_str; st["stage"] = "end"; st["digits"] = ""
                                _vip_render(cb_chat_id, cb_cid, cb_msg_id)
                            else:
                                _vip_state.pop(str(cb_cid), None)
                                _tgt_uname = user_usernames.get(str(st["uid"]), str(st["uid"]))
                                handle_command(f"/setvip {st['uid']} {st['start']} {date_str} {_tgt_uname}", cb_chat_id, {}, sender_id=cb_cid)
                                _navigate_to("help_cat:copyadmin", cb_chat_id, cb_cid, cb_msg_id, cb_is_admin)
                    elif cb_data.startswith("ctretry:"):
                        uid = cb_data.split(":")[1]
                        handle_command(f"/ctretry {uid}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data.startswith("ctclose:"):
                        uid = cb_data.split(":")[1]
                        handle_command(f"/ctclose {uid}", cb_chat_id, {}, sender_id=cb_cid)
                    elif cb_data.startswith("alt_loop:") or cb_data.startswith("alt_manual:"):
                        _mode, _ver = cb_data.split(":")
                        _cmd = "/alt" if _ver == "1" else "/alt2"
                        _alt_back_cb, _ = _find_back_target(_cmd)
                        if _mode == "alt_loop":
                            pending_input[cb_cid] = {"cmd": _cmd, "step": "loop", "msg_id": None, "cat_id": _alt_back_cb}
                            send_reply(cb_chat_id,
                                f"🔁 <b>Loop Mode — Scan{_ver}</b>\n\n"
                                f"Type the minute <b>(0–59)</b>:\n"
                                f"Bot will run every hour at that minute.\n\n"
                                f"<i>Example: type <code>2</code> → runs at 1:02, 2:02, 3:02, 4:02...</i>")
                        else:
                            pending_input[cb_cid] = {"cmd": _cmd, "step": "manual", "msg_id": None, "cat_id": _alt_back_cb}
                            send_reply(cb_chat_id,
                                f"📋 <b>Manual Times — Scan{_ver}</b>\n\n"
                                f"Type your specific times separated by spaces:\n\n"
                                f"<i>Example: <code>2.02 2.23 14.25 15.26 15.46</code></i>")
                    continue

                # Auto-approve/decline VIP channel join requests — only current VIP
                # tier users get let in automatically; everyone else is declined.
                jr = upd.get("chat_join_request")
                if jr:
                    _jr_chat_id = jr["chat"]["id"]
                    _jr_user_id = jr["from"]["id"]
                    _jr_uname   = jr["from"].get("username", "?")
                    _is_vip_chan = any(str(c.get("id","")).lstrip("@") == str(_jr_chat_id) or
                                       str(c.get("id","")) == f"@{jr['chat'].get('username','')}"
                                       for c in CHANNELS if c.get("tier") == "vip")
                    _u = ct._get(str(_jr_user_id))
                    _requester_is_vip = bool(_u and _u.get("tier") == "vip")
                    try:
                        if _is_vip_chan and _requester_is_vip:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/approveChatJoinRequest",
                                json={"chat_id": _jr_chat_id, "user_id": _jr_user_id}, timeout=10)
                            send_to_user(_jr_user_id, "✅ <b>Welcome to the VIP channel!</b> Your request was auto-approved.")
                            print(f"  [VIP CHANNEL] approved @{_jr_uname} ({_jr_user_id})")
                        elif _is_vip_chan:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/declineChatJoinRequest",
                                json={"chat_id": _jr_chat_id, "user_id": _jr_user_id}, timeout=10)
                            _mkp = {"inline_keyboard": [[{"text": "💬 Contact Admin for VIP", "url": f"tg://user?id={ADMIN_CHAT_ID}"}]]} if ADMIN_CHAT_ID else None
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                json={"chat_id": _jr_user_id,
                                      "text": "⭐ This channel is VIP-only. Contact admin to activate VIP first.",
                                      "reply_markup": _mkp}, timeout=10)
                            print(f"  [VIP CHANNEL] declined @{_jr_uname} ({_jr_user_id}) — not VIP")
                    except Exception as e:
                        print(f"  [VIP CHANNEL] join request error: {e}")
                    continue

                msg = upd.get("message",{}); text = msg.get("text","") or ""
                cid = msg.get("chat",{}).get("id"); uname = msg.get("from",{}).get("username","?")
                sender_uid = msg.get("from",{}).get("id")
                if not cid: continue

                print(f"  [CMD] @{uname} ID:{cid}: {text[:50]}")
                register_user(cid, uname if uname != "?" else None)
                if cid in broadcast_pending and not text.startswith("/"):
                    handle_broadcast_message(cid, msg); continue
                # Pending input — user typed value after tapping a button
                if cid in pending_input and not text.startswith("/"):
                    pi = pending_input[cid]
                    _pi_msg_id  = pi.get("msg_id")
                    _pi_cat_id  = pi.get("cat_id") or "monitor"
                    # "cat_id" is either a full callback_data (contains ":") or a bare
                    # top-level category id (legacy) — normalize both to a callback_data
                    _pi_back_cb = _pi_cat_id if ":" in _pi_cat_id else f"help_cat:{_pi_cat_id}"
                    _back_mkp   = {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": _pi_back_cb}]]}

                    if pi["cmd"] == "/connect":
                        if pi.get("step") == "secret":
                            api_key = pi["api_key"]
                            api_secret = text.strip()
                            del pending_input[cid]
                            print(f"  [CMD] pending input resolved: /connect *** ***")
                            cid_str = str(cid)
                            _reply_capture[cid_str] = {"texts": [], "cat_id": _pi_cat_id}
                            handle_command(f"/connect {api_key} {api_secret}", cid, msg)
                            captured = _reply_capture.pop(cid_str, {})
                            result_text = "\n\n".join(captured.get("texts", [])) or "✅ Connected"
                            if _pi_msg_id:
                                _help_edit_or_send(cid, result_text, _back_mkp, message_id=_pi_msg_id)
                            else:
                                send_reply(cid, result_text, reply_markup=_back_mkp)
                        else:
                            pending_input[cid] = {"cmd": "/connect", "step": "secret",
                                                  "api_key": text.strip(), "msg_id": _pi_msg_id, "cat_id": _pi_cat_id}
                            _help_edit_or_send(cid,
                                "🔑 <b>Connect BingX — Step 2/2</b>\n\nNow type your <b>Secret Key</b>:",
                                _back_mkp, message_id=_pi_msg_id)
                    elif pi["cmd"] == "_adminlinks_set_channel":
                        del pending_input[cid]
                        SIGNAL_CHANNEL_LINK = text.strip()
                        save_settings()
                        send_adminlinks_screen(cid, message_id=_pi_msg_id)
                    elif pi["cmd"] == "_chrm_add":
                        _parts_in = text.strip().split()
                        _id_tok = next((p for p in _parts_in if p.lstrip("-").isdigit()), None)
                        _link_tok = next((p for p in _parts_in if p.startswith("http")), "")
                        if not _id_tok and _link_tok:
                            # Public channel link (t.me/username) — Telegram lets a bot send
                            # via "@username" directly, no separate numeric ID needed.
                            _uname_part = _link_tok.rstrip("/").split("/")[-1]
                            if _uname_part and not _uname_part.startswith(("+", "joinchat")):
                                _id_tok = f"@{_uname_part.lstrip('@')}"
                        if not _id_tok:
                            _help_edit_or_send(cid,
                                "⚠️ <b>That looks like a private invite link with no public username</b>\n\n"
                                "Forward any message from the channel to @userinfobot to get its numeric ID "
                                "(looks like <code>-1001234567890</code>), then send it here — optionally "
                                "with the link after it.",
                                _back_mkp, message_id=_pi_msg_id)
                        else:
                            del pending_input[cid]
                            _n = sum(1 for c in CHANNELS if c.get("tier") == pi["tier"]) + 1
                            _friendly = f"{'⭐ VIP' if pi['tier']=='vip' else '🆓 Free'} Tier {_n}"
                            CHANNELS.append({"id": _id_tok, "tier": pi["tier"], "label": _friendly, "link": _link_tok})
                            save_settings()
                            send_channelmgmt_screen(cid, message_id=_pi_msg_id)
                    elif pi["cmd"] == "_vip_manual_date":
                        del pending_input[cid]
                        vst = pi["vip"]
                        _txt = text.strip()
                        import re as _vre
                        if not _vre.match(r"^\d{2}\.\d{2}\.\d{4}$", _txt):
                            send_reply(cid, "⚠️ Format must be DD.MM.YYYY, e.g. 17.08.2026", reply_markup=_back_mkp)
                        elif vst["stage"] == "start":
                            _vip_state[str(cid)] = {"uid": vst["uid"], "stage": "end", "digits": "", "start": _txt}
                            _vip_render(cid, cid, _pi_msg_id)
                        else:
                            _tgt_uname = user_usernames.get(str(vst["uid"]), str(vst["uid"]))
                            handle_command(f"/setvip {vst['uid']} {vst['start']} {_txt} {_tgt_uname}", cid, {}, sender_id=cid)
                            _navigate_to("help_cat:copyadmin", cid, cid, _pi_msg_id, True)
                    elif pi["cmd"] == "_pp_manual":
                        del pending_input[cid]
                        ppst = pi["pp"]
                        try:
                            price = float(text.strip())
                        except ValueError:
                            price = None
                        if price is None or price <= 0:
                            send_reply(cid, "⚠️ Enter a valid price greater than 0.", reply_markup=_back_mkp)
                        else:
                            _ok, _reason = _apply_trade_price_edit(ppst["action"], ppst["kind"], ppst["symbol"], ppst["idx"], price)
                            if not _ok and _reason and "no longer open" not in _reason:
                                send_reply(cid, f"⚠️ {_reason}", reply_markup=_back_mkp)
                            else:
                                if _ok:
                                    send_telegram(f"<b>{ppst['symbol']} {ppst['action'].upper()} -&gt; {price:,.6f}</b>\n\n<i>🛡️ Capital protected</i>")
                                _msg = f"✅ <b>{ppst['symbol']} updated to {price:,.6f}</b>" if _ok else f"⚠️ {_reason or ppst['symbol'] + ' trade no longer open.'}"
                                if _pi_msg_id:
                                    _help_edit_or_send(cid, _msg, _back_mkp, message_id=_pi_msg_id)
                                else:
                                    send_reply(cid, _msg, reply_markup=_back_mkp)
                    else:
                        del pending_input[cid]
                        _step = pi.get("step", "")
                        full_cmd = f"{pi['cmd']} {_step} {text.strip()}" if _step in ("loop","manual") else f"{pi['cmd']} {text.strip()}"
                        print(f"  [CMD] pending input resolved: {full_cmd}")
                        cid_str = str(cid)
                        _reply_capture[cid_str] = {"texts": [], "cat_id": _pi_cat_id}
                        handle_command(full_cmd, cid, msg)
                        captured = _reply_capture.pop(cid_str, {})
                        result_text = "\n\n".join(captured.get("texts", [])) or "✅ Done"
                        if len(result_text) > 4000:
                            result_text = result_text[:4000] + "\n\n<i>...truncated</i>"
                        cap_mkp = captured.get("markup")
                        if cap_mkp and "inline_keyboard" in cap_mkp:
                            final_mkp = {"inline_keyboard": cap_mkp["inline_keyboard"] + _back_mkp["inline_keyboard"]}
                        else:
                            final_mkp = _back_mkp
                        if _pi_msg_id:
                            _help_edit_or_send(cid, result_text, final_mkp, message_id=_pi_msg_id)
                        else:
                            send_reply(cid, result_text, reply_markup=final_mkp)
                    continue
                if text.startswith("/"): handle_command(text, cid, msg, sender_id=sender_uid)
        except Exception as e: print(f"  [CMD] {e}")
        time.sleep(2)

# --- MAIN ---------------------------------------------------------------------
# ── Scan1 fixed schedule (IST HH:MM) ─────────────────────────────────────────
SCAN1_SCHEDULE: list[tuple[int,int]] = sorted(set([
    # AM
    (3,2),(5,23),
    # PM
    (12,23),(13,45),
]))
# Scan2 keeps the full original schedule (independent of Scan1)
SCAN2_SCHEDULE: list[tuple[int,int]] = sorted(set([
    # AM
    (1,2),(1,23),(2,2),(2,23),(3,2),(3,23),(4,2),(4,23),(5,2),(5,23),
    (6,2),(6,23),(8,2),(8,23),(11,2),(11,23),
    # PM
    (12,3),(12,23),(13,7),(13,23),(14,7),(14,23),(15,7),(15,23),
    (16,7),(16,23),(17,9),(17,23),(19,4),(19,23),(20,7),(20,23),(21,7),(21,23),
]))
# Demo/Test fires 1 min after each Scan2 slot (uses full schedule, not trimmed Scan1)
SCAN1_TEST_SCHEDULE: list[tuple[int,int]] = [
    ((h + (m+1)//60) % 24, (m+1) % 60) for h,m in SCAN2_SCHEDULE
]
_scan1_triggered_today: set[tuple[int,int]] = set()   # (hour,minute) pairs run today
_test_triggered_today:  set[tuple[int,int]] = set()
_last_midnight_date = None   # for midnight reset

ALT_SCAN_MINUTE  = 2        # kept for /alt command reference — not used for auto-trigger
ALT_SCAN2_MINUTE = 24       # scan2 — disabled for auto-trigger (SCAN2_AUTO_ENABLED=False)
SCAN2_AUTO_ENABLED = False   # set True to re-enable scan2 auto
_auto_scan_last_hour  = -1  # legacy
_scan_cycle_placed = set()  # coins signaled this cycle — prevents scan1+scan2 picking same coin
_scan_cycle_lock   = __import__("threading").Lock()

def _run_auto_scan(cid, scan_ver=2):
    """Auto-scan entry point — called from main loop at IST :02."""
    global _scan_cycle_placed
    lbl = "V1" if scan_ver == 1 else "V2"
    send_admin(f"🔄 <b>Auto-Scan {lbl}</b>  {ist_str()}\n\nScheduled scan starting (~60s)...\n\n<i>🛡️ Capital protected</i>")
    # Clear cycle dedup set when scan1 starts (scan1 always starts first)
    if scan_ver == 1:
        with _scan_cycle_lock:
            _scan_cycle_placed.clear()
    cmd = "/scan1" if scan_ver == 1 else "/scan2"
    handle_command(cmd, cid)

# ══════════════════════════════════════════════════════════
# TEST MODE — CLEXER SCALP v1 (demo only, no copytrade)
# ══════════════════════════════════════════════════════════

def _move_age_1h(candles_1h: list, direction: str) -> int:
    """Count 1H candles since last confirmed swing point. Returns 999 if no swing found."""
    n = 2
    last = len(candles_1h) - 1 - n
    for i in range(last, n - 1, -1):
        c = candles_1h[i]
        neighbors = (i-2, i-1, i+1, i+2)
        if direction == "long":
            if all(c["low"] < candles_1h[j]["low"] for j in neighbors):
                return (len(candles_1h) - 1) - i
        else:
            if all(c["high"] > candles_1h[j]["high"] for j in neighbors):
                return (len(candles_1h) - 1) - i
    return 999  # no swing found → treat as old/exhausted → skip

def _build_scalp_v1_prompt(symbol: str, cp: float, smc: str, vol_24h: float, change_24h: float,
                           struct: str = "", age: int = 0, age_4h: int = 0) -> str:
    return f"""{smc}
Current Price: {cp:,.6g}
Volume 24h: ${vol_24h/1e6:.1f}M
Change 24h: {change_24h:+.2f}%

You are CLEXER SCALP V1. Analyze {symbol} for a short-term scalp trade.

HARD GATES — already pre-filtered before this prompt (do not recheck):
1. 4H structure: BULLISH→LONG only, BEARISH→SHORT only (already confirmed: {struct.upper()})
2. change_24h <= 40% (already checked: {abs(change_24h):.1f}%)
3. move_age_1h <= 5 (already checked: {age} candles)
4. move_age_4h <= 8 — 4H trend ≤ 8 candles old (already checked: {age_4h} candles)
Remaining gate to check: RR >= 2.0 after computing SL

ENTRY: MARKET at current price {cp:,.6g}

STOP LOSS RULE (critical — read carefully):
Step 1: Try 15M candles first. Find the most recent swing LOW (LONG) or swing HIGH (SHORT).
        A swing low = a 15M candle whose low is lower than both its left and right neighbor.
        A swing high = a 15M candle whose high is higher than both its left and right neighbor.
        Check last 10 candles. Needs 1 confirmed left and 1 confirmed right neighbor — edge candles don't count.
Step 2: If no valid 15M swing → try 5M candles. Same fractal rule, last 10 candles.
Step 3: If NO swing found in either timeframe → Signal: WAIT. Never invent a percentage.
Step 4: If swing found → sl_dist_pct = abs(entry - swing_level) / entry × 100
        If sl_dist_pct < 1.0% → Signal: WAIT (structure too tight)
        If sl_dist_pct > 3.0% → Signal: WAIT (structure too loose)
        Otherwise → SL = swing_level. Valid trade.

RULE: The 1.0%-3.0% band filters real structure — it never generates a level.
      "No valid structure in range" always means WAIT. There is no fallback.

For SwingLevel output:
- If swing found AND used for the trade:  SwingLevel: 0.4400 (accepted: 2.91% in band)
- If swing found BUT rejected (too tight): SwingLevel: 0.5267 (rejected: 0.34% < 1.0%)
- If swing found BUT rejected (too loose): SwingLevel: 0.1049 (rejected: 5.70% > 3.0%)
- If NO swing candle found at all:         SwingLevel: NONE
Never write "rejected" on a level that was used. Never write "accepted" on a level that was rejected.

TAKE PROFIT (only compute if SL is valid):
sl_dist = abs(entry - SL)
TP1 = entry ± (sl_dist × 2.0)
TP2 = entry ± (sl_dist × 3.75)

OUTPUT ONLY — no working, no steps, no bullet points, just the block below. One Signal line only — never emit two Signal lines.
Put Signal through SL_pct first so a token cutoff never eats the trade numbers:
Signal: BUY / SELL / WAIT
Entry: {cp:,.6g}
SwingLevel: [see rules above — accepted/rejected/NONE tag required]
SL: [price, or — if WAIT]
TP1: [price, or — if WAIT]
TP2: [price, or — if WAIT]
RR: [e.g. 2.0 / 3.75 for both legs, or — if WAIT]
SL_pct: [SL distance as % of entry, or — if WAIT]
Reasoning: [one line — name the exact swing candle level used]"""

_demo_monitor_lock = __import__("threading").Lock()

def _demo_monitor_loop():
    """Background thread: monitors demo trades every 30s. No copytrade — only TG alerts."""
    import re as _re
    while True:
        try:
            time.sleep(30)
            if bot_paused.is_set(): continue
            now = time.time()
            for demo_list in (demo_scan1_trades, demo_scan2_trades):
                to_remove = []
                for t in list(demo_list):
                    sym    = t.get("symbol","")
                    sig    = t.get("signal","")
                    entry  = float(t.get("entry", 0))
                    sl     = float(t.get("sl", 0))
                    tp1    = float(t.get("tp1", 0))
                    tp2    = float(t.get("tp2", 0))
                    tp1hit = t.get("tp1_hit", False)
                    be_sl  = float(t.get("be_sl", 0))
                    created = float(t.get("created_at", now))

                    if not sym or not entry: continue
                    cp = get_bingx_price(sym)
                    if cp <= 0: continue

                    timeout_hit = (now - created) >= 3600  # 1H timeout

                    if sig == "BUY":
                        sl_hit  = cp <= (be_sl if tp1hit and be_sl else sl)
                        tp1_now = not tp1hit and tp1 > 0 and cp >= tp1
                        tp2_now = tp1hit and tp2 > 0 and cp >= tp2
                    else:
                        sl_hit  = cp >= (be_sl if tp1hit and be_sl else sl)
                        tp1_now = not tp1hit and tp1 > 0 and cp <= tp1
                        tp2_now = tp1hit and tp2 > 0 and cp <= tp2

                    coin = sym.replace("-USDT","")
                    arrow = "🟢" if sig == "BUY" else "🔴"
                    if tp2_now:
                        log_trade_event({"type":"demo","coin":sym,"direction":sig,
                            "tp2_hit_time":_ist_str_now(),"result":"TP2",
                            "entry_price":entry,"sl_price":sl,"tp1_price":tp1,"tp2_price":tp2})
                        send_telegram(
                            f"{arrow} <b>[DEMO] {coin}-USDT — TP2 HIT ✅</b>\n"
                            f"──────────────────────\n"
                            f"Price @ TP2: <b>{cp:,.6g}</b>\n"
                            f"Entry: {entry:,.6g} → TP2: {tp2:,.6g}\n"
                            f"Result: <b>FULL WIN</b>")
                        to_remove.append(t)
                    elif sl_hit:
                        lbl = "BE SL" if tp1hit else "SL"
                        result = "BREAKEVEN" if tp1hit else "LOSS"
                        log_trade_event({"type":"demo","coin":sym,"direction":sig,
                            "sl_hit_time":_ist_str_now(),"result":result,
                            "entry_price":entry,"sl_price":be_sl if tp1hit and be_sl else sl,
                            "tp1_price":tp1,"tp2_price":tp2})
                        send_telegram(
                            f"{arrow} <b>[DEMO] {coin}-USDT — {lbl} HIT ❌</b>\n"
                            f"──────────────────────\n"
                            f"Price @ {lbl}: <b>{cp:,.6g}</b>\n"
                            f"Entry: {entry:,.6g} | {lbl}: {be_sl if tp1hit and be_sl else sl:,.6g}\n"
                            f"Result: <b>{result}</b>")
                        to_remove.append(t)
                    elif tp1_now:
                        be_sl_price = round(entry * 1.001 if sig == "SELL" else entry * 0.999, 6)
                        t["tp1_hit"] = True
                        t["be_sl"]   = be_sl_price
                        log_trade_event({"type":"demo","coin":sym,"direction":sig,
                            "tp1_hit_time":_ist_str_now(),"result":"TP1_partial",
                            "entry_price":entry,"sl_price":be_sl_price,"tp1_price":tp1,"tp2_price":tp2})
                        send_telegram(
                            f"{arrow} <b>[DEMO] {coin}-USDT — TP1 HIT 🎯</b>\n"
                            f"──────────────────────\n"
                            f"Price @ TP1: <b>{cp:,.6g}</b>\n"
                            f"50% closed. BE SL → <b>{be_sl_price:,.6g}</b>\n"
                            f"Runner TP2: {tp2:,.6g}")
                    elif timeout_hit:
                        pnl = (cp - entry) / entry * 100 * (1 if sig == "BUY" else -1)
                        log_trade_event({"type":"demo","coin":sym,"direction":sig,
                            "timeout_time":_ist_str_now(),"result":f"TIMEOUT({pnl:+.2f}%)",
                            "entry_price":entry,"sl_price":sl,"tp1_price":tp1,"tp2_price":tp2})
                        send_telegram(
                            f"{arrow} <b>[DEMO] {coin}-USDT — TIMEOUT ⏰</b>\n"
                            f"──────────────────────\n"
                            f"1H elapsed — no TP1/SL hit.\n"
                            f"Exit @ <b>{cp:,.6g}</b> | P/L: <b>{pnl:+.2f}%</b>\n"
                            f"Entry: {entry:,.6g}")
                        to_remove.append(t)

                with _demo_monitor_lock:
                    for t in to_remove:
                        if t in demo_scan1_trades: demo_scan1_trades.remove(t)
                        if t in demo_scan2_trades: demo_scan2_trades.remove(t)
        except Exception as _e:
            print(f"  [DEMO MONITOR] Error: {_e}")

def _run_test_scan(cid, scan_ver: int):
    """CLEXER SCALP v1 test scan. Sends [DEMO] signal to TG. No copytrade."""
    import re as _re, math as _math
    lbl = "S1" if scan_ver == 1 else "S2"
    send_admin(f"🧪 <b>[TEST] Scalp V1 Scan{lbl}</b>  {ist_str()}\n\nDemo scan starting...\n\n<i>- CLEXER TEST -</i>")

    demo_list = demo_scan1_trades if scan_ver == 1 else demo_scan2_trades
    _max_demo_slots = 6
    with _demo_monitor_lock:
        if len(demo_list) >= _max_demo_slots:
            send_admin(f"🚫 <b>[TEST] Scan{lbl} slots full ({_max_demo_slots}/{_max_demo_slots})</b>\n\nAll demo slots occupied. Waiting for close.\n\n<i>- CLEXER TEST -</i>")
            return

    try:
        # Fetch BingX ticker
        r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
                         timeout=15).json()
        skip = {"USDC","BUSD","DAI","TUSD","FDUSD","USDP","FRAX","USDT","BTC","BTCDOM"}
        movers = []
        for t in r.get("data", []):
            sym = t.get("symbol","")
            if not sym.endswith("-USDT"): continue
            base = sym.replace("-USDT","")
            if base in skip: continue
            vol  = float(t.get("quoteVolume",0) or t.get("volume",0) or 0)
            chg  = float(t.get("priceChangePercent",0) or 0)
            px   = float(t.get("lastPrice",0) or 0)
            if vol < 5_000_000: continue   # SCALP V1 gate: $5M floor
            if px <= 0: continue
            if abs(chg) > 40: continue     # SCALP V1 gate: exhausted
            if abs(chg) > 200: continue
            import re as _re2
            if _re2.search(r'\d', base): continue
            if len(base) > 10: continue
            if "USD" in base: continue
            if not _re2.match(r'^[A-Z]+$', base): continue
            # Skip coins already in real or demo trades
            existing_syms = (
                [t2.get("symbol","") for t2 in scan1_trades + scan2_trades] +
                [t2.get("symbol","") for t2 in demo_scan1_trades + demo_scan2_trades]
            )
            if sym in existing_syms: continue
            if scan_ver == 1:
                score = (abs(chg) ** 1.5) * (_math.sqrt(vol / 1e6))
            else:
                if abs(chg) > 15: continue
                freshness = 1.0 if 2 <= abs(chg) <= 10 else 0.6
                score = _math.sqrt(vol / 1e6) * (abs(chg) ** 0.8) * freshness
            movers.append({"sym":sym,"base":base,"price":px,"change":chg,"vol_m":round(vol/1e6,1),"score":score,"vol":vol})

        movers.sort(key=lambda x: x["score"], reverse=True)
        top10 = movers[:10]

        if not top10:
            send_admin(f"[TEST] ❌ No coins found from BingX for scan{lbl}."); return

        # Check 4H structure
        import pandas as _pd
        def _check_4h_struct(df):
            if df is None or len(df) < 8: return "NEUTRAL"
            h = df["high"].values[-15:]; l = df["low"].values[-15:]; cls = df["close"].values[-15:]
            sh = []; sl_s = []
            for i in range(1, len(h)-1):
                if h[i] > h[i-1] and h[i] > h[i+1]: sh.append(h[i])
                if l[i] < l[i-1] and l[i] < l[i+1]: sl_s.append(l[i])
            swing = "NEUTRAL"
            if len(sh) >= 2 and len(sl_s) >= 2:
                if sh[-1] > sh[-2] and sl_s[-1] > sl_s[-2]: swing = "BULLISH"
                if sh[-1] < sh[-2] and sl_s[-1] < sl_s[-2]: swing = "BEARISH"
            mid = cls[len(cls)//2]
            trend = "NEUTRAL"
            if mid > 0:
                tp = (cls[-1] - mid) / mid * 100
                if tp < -5: trend = "BEARISH"
                elif tp > 5: trend = "BULLISH"
            if swing == trend: return swing
            if swing == "BULLISH" and trend == "BEARISH": return "BEARISH"
            if swing == "BEARISH" and trend == "BULLISH": return "BULLISH"
            return swing if swing != "NEUTRAL" else trend
        for t in top10:
            df4 = bingx_klines(t["sym"], "4h", 30)
            t["structure"] = _check_4h_struct(df4)
            t["df4h"] = df4
        structured = [t for t in top10 if t["structure"] != "NEUTRAL"]
        candidate_order = structured + [c for c in top10 if c not in structured]

        signal_placed = False
        tried = []
        for candidate in candidate_order:
            if signal_placed: break
            chosen_sym  = candidate["sym"]
            chosen_base = candidate["base"]
            cp          = candidate["price"]
            tried.append(chosen_sym)

            # Fetch candles
            _cached_4h = candidate.get("df4h")
            df_4h = _cached_4h if _cached_4h is not None else bingx_klines(chosen_sym, "4h", 60)
            df_1h = bingx_klines(chosen_sym, "1h", 40)
            df_15m = bingx_klines(chosen_sym, "15m", 30)
            df_5m = bingx_klines(chosen_sym, "5m", 30)
            if df_4h is None or df_1h is None or df_5m is None:
                continue

            # move_age_1h gate — determine direction first from 4H structure
            struct = candidate["structure"]
            if struct == "BULLISH":
                direction = "long"
            elif struct == "BEARISH":
                direction = "short"
            else:
                continue  # NEUTRAL → skip

            # Build 1H candle dicts for move_age calculation
            h1_candles = [{"high": float(row["high"]), "low": float(row["low"])}
                          for _, row in df_1h.iterrows()]
            age = _move_age_1h(h1_candles, direction)
            if age > 5:
                print(f"  [TEST] {chosen_sym}: move_age_1h={age} > 5 — too old, skipping")
                continue

            # HTF exhaustion guard — 4H move_age must be ≤ 8 candles (32h)
            # Prevents entering a "fresh 1H leg" inside a multi-day exhausted 4H move
            h4_candles = [{"high": float(row["high"]), "low": float(row["low"])}
                          for _, row in df_4h.iterrows()]
            age_4h = _move_age_1h(h4_candles, direction)  # same fractal logic on 4H
            if age_4h > 8:
                print(f"  [TEST] {chosen_sym}: move_age_4h={age_4h} > 8 — HTF exhausted, skipping")
                continue

            # Build data summary (same as main scan)
            smc = (f"=== {chosen_sym} DATA SUMMARY ===\n"
                   f"Price: {cp:,.6g}\n"
                   f"24h Change: {candidate['change']:+.2f}%\n"
                   f"Volume (24h): ${candidate['vol_m']}M\n"
                   f"4H Structure: {struct}\n"
                   f"move_age_1h: {age} candles (gate: ≤5)\n"
                   f"move_age_4h: {age_4h} candles (gate: ≤8)\n")

            if df_4h is not None and len(df_4h) >= 10:
                h4 = df_4h
                cls4 = h4["close"].values; ops4 = h4["open"].values
                highs4 = h4["high"].values; lows4 = h4["low"].values
                sh = []; sl_p = []
                for i in range(1, len(highs4)-1):
                    if highs4[i]>highs4[i-1] and highs4[i]>highs4[i+1]: sh.append(highs4[i])
                    if lows4[i]<lows4[i-1] and lows4[i]<lows4[i+1]: sl_p.append(lows4[i])
                smc += (f"4H Swing highs: {[round(x,4) for x in sh[-4:]]}\n"
                        f"4H Swing lows: {[round(x,4) for x in sl_p[-4:]]}\n")

            if df_1h is not None and len(df_1h) >= 5:
                h1 = df_1h; cls1 = h1["close"].values; ops1 = h1["open"].values
                last2_1h = [f"open={ops1[i]:,.4g} close={cls1[i]:,.4g}" for i in [-2,-1]]
                smc += f"1H last 2: {last2_1h[0]} | {last2_1h[1]}\n"

            if df_15m is not None and len(df_15m) >= 5:
                h15 = df_15m; lows15 = h15["low"].values; highs15 = h15["high"].values
                smc += (f"15M last 10 lows:  {[round(x,4) for x in lows15[-10:]]}\n"
                        f"15M last 10 highs: {[round(x,4) for x in highs15[-10:]]}\n")

            if df_5m is not None and len(df_5m) >= 5:
                h5 = df_5m; cls5 = h5["close"].values; lows5 = h5["low"].values; highs5 = h5["high"].values
                smc += (f"5M last 10 lows:  {[round(x,4) for x in lows5[-10:]]}\n"
                        f"5M last 10 highs: {[round(x,4) for x in highs5[-10:]]}\n"
                        f"5M last close: {cls5[-1]:,.6g}\n")

            analysis_prompt = _build_scalp_v1_prompt(chosen_sym, cp, smc, candidate["vol"], candidate["change"],
                                                     struct=struct, age=age, age_4h=age_4h)

            # Claude analysis
            analysis = ""; _claude_ok = False
            for _attempt in range(3):
                try:
                    r2 = _claude_client("test").messages.create(
                        model=_ai_model("test"), max_tokens=500,
                        messages=[{"role":"user","content":analysis_prompt}])
                    _log_api_usage(f"demo_{chosen_sym}", _ai_model("test"),
                                   r2.usage.input_tokens, r2.usage.output_tokens)
                    analysis = _claude_text(r2)
                    _claude_ok = True; break
                except Exception as _ce:
                    print(f"  [TEST] Claude attempt {_attempt+1} FAIL: {_ce}")
                    if _attempt < 2: time.sleep(10)
            if not _claude_ok:
                print(f"  [TEST] {chosen_sym}: Claude failed 3 times — skipping"); continue

            _ac = analysis.replace(",","")
            def _p(label):
                m = _re.search(rf"{label}[:\s]+([0-9.]+)", _ac, _re.IGNORECASE)
                return float(m.group(1)) if m else 0.0
            _all_sigs = [s.upper() for s in _re.findall(r"Signal[:\s]+(BUY|SELL|WAIT)", analysis, _re.IGNORECASE)]
            if not _all_sigs or "WAIT" in _all_sigs:
                scan_signal_val = "WAIT"
            else:
                scan_signal_val = _all_sigs[-1]  # take last non-WAIT signal

            # Send analysis preview — show only the final Signal block to avoid confusion
            # Find last occurrence of "Signal:" and show from there
            _last_sig_idx = analysis.rfind("Signal:")
            _preview = analysis[_last_sig_idx:].strip() if _last_sig_idx >= 0 else analysis.strip()
            emoji = "🟢" if candidate["change"] >= 0 else "🔴"
            send_admin(
                f"{emoji} <b>[TEST] {chosen_sym}</b>  {ist_str()}\n\n"
                f"Price: <b>{cp:,.6g}</b> ({candidate['change']:+.2f}%)\n"
                f"move_age: {age} candles\n\n"
                f"<pre>{_preview[:600]}</pre>\n\n"
                f"<i>- CLEXER SCALP V1 TEST -</i>")

            if scan_signal_val == "WAIT":
                print(f"  [TEST] {chosen_sym} → WAIT")
                continue

            scan_sl_raw = _p("SL")
            # Also parse SwingLevel for transparency
            swing_m = _re.search(r"SwingLevel[:\s]+([0-9.]+)", analysis.replace(",",""), _re.IGNORECASE)
            swing_level_str = swing_m.group(1) if swing_m else "NONE"

            # If Claude reported no swing or couldn't parse SL → skip
            if swing_level_str == "NONE" or scan_sl_raw <= 0:
                print(f"  [TEST] {chosen_sym}: no swing level found — WAIT"); continue

            scan_entry = cp
            sl_dist = abs(scan_entry - scan_sl_raw)
            sl_pct  = sl_dist / scan_entry * 100

            # Structure SL must be 1.0%–3.0% — reject both sides, never stretch
            if sl_pct < 1.0:
                print(f"  [TEST] {chosen_sym}: structure SL {sl_pct:.2f}% < 1.0% — too tight, skipping"); continue
            if sl_pct > 3.0:
                print(f"  [TEST] {chosen_sym}: structure SL {sl_pct:.2f}% > 3.0% — too loose, skipping"); continue

            scan_sl  = round(scan_entry - sl_dist if scan_signal_val == "BUY" else scan_entry + sl_dist, 6)
            scan_tp1 = round(scan_entry + sl_dist * 2.0 if scan_signal_val == "BUY" else scan_entry - sl_dist * 2.0, 6)
            scan_tp2 = round(scan_entry + sl_dist * 3.75 if scan_signal_val == "BUY" else scan_entry - sl_dist * 3.75, 6)

            arrow = "🟢 LONG" if scan_signal_val == "BUY" else "🔴 SHORT"
            coin  = chosen_sym.replace("-USDT","")
            demo_msg = (
                f"<b>📣 [DEMO] {coin}-USDT</b>\n"
                f"<b>{'─'*22}</b>\n\n"
                f" TEST SIGNAL — SCALP V1\n\n"
                f"{arrow} — <b>MARKET ENTRY</b>\n\n"
                f"🎯 Entry:      <b>{scan_entry:,.4g}</b>\n"
                f"🛑 SL:         <b>{scan_sl:,.4g}</b>  ({sl_pct:.1f}%)\n"
                f"📌 SwingLevel: <b>{swing_level_str}</b>\n"
                f"💰 TP1:       <b>{scan_tp1:,.4g}</b>\n"
                f"🏆 TP2:       <b>{scan_tp2:,.4g}</b>\n"
                f"📊 RR:        <b>1:2.0 (TP1) / 1:3.75 (TP2)</b>\n"
                f"⏰ Timeout: 1H | move_age: {age}c"
            )
            send_telegram(demo_msg)

            slot_data = {
                "symbol": chosen_sym, "signal": scan_signal_val,
                "entry": scan_entry, "sl": scan_sl, "tp1": scan_tp1, "tp2": scan_tp2,
                "tp1_hit": False, "be_sl": 0, "created_at": time.time(),
                "scan_ver": scan_ver,
            }
            with _demo_monitor_lock:
                demo_list.append(slot_data)
            log_trade_event({"type":"demo","coin":chosen_sym,"direction":scan_signal_val,
                "signal_time":_ist_str_now(),"entry_price":scan_entry,
                "sl_price":scan_sl,"tp1_price":scan_tp1,"tp2_price":scan_tp2,
                "entry_trigger_time":_ist_str_now(),"result":"open"})
            signal_placed = True
            print(f"  [TEST] {chosen_sym} {scan_signal_val} demo signal placed — scan{lbl}")

        if not signal_placed:
            tried_str = ", ".join(tried) if tried else "none"
            send_admin(
                f"⏸ <b>[TEST] No demo signal</b>  {ist_str()}\n\n"
                f"Tried: <b>{tried_str}</b>\n\n"
                f"All WAIT or failed gates (age/SL/RR).\n\n<i>- CLEXER SCALP V1 TEST -</i>")

    except Exception as e:
        send_admin(f"❌ [TEST] Scan error: {e}")
        import traceback as _tb3; print(_tb3.format_exc())

def main():
    global last_signal_scan_time, last_price_check_time, last_tick_time, last_news_check_time, last_scan_tick_time

    print(f"[CLEXER V17.8.5] Starting | {SYMBOL}")
    print(f"  TV Bridge: {TV_BRIDGE_URL or 'NOT SET - Binance-only'}")
    print(f"  Starting PAUSED - send /go to start scanning")
    load_users()
    ct.load()
    load_settings()
    load_active_trade()

    # Start PAUSED - user must send /go
    bot_paused.set()

    # Force the mini app live on every boot — the backend that serves it
    # (CLEXER_API_URL) resets to maintenance-on with its own default message
    # on every restart, so we override that here instead of waiting for
    # an admin to manually send /miniapp resume after each deploy.
    if CLEXER_API_URL:
        try:
            _hdrs = {"X-Push-Secret": PUSH_STATE_SECRET, "Content-Type": "application/json"} if PUSH_STATE_SECRET else {"Content-Type": "application/json"}
            requests.post(f"{CLEXER_API_URL}/maintenance", json={"on": False, "msg": "Live"}, headers=_hdrs, timeout=5)
            print("  Mini app: forced LIVE on startup")
        except Exception as e:
            print(f"  Mini app: could not force live on startup — {e}")

    if TV_BRIDGE_URL:
        print("  Checking TV bridge...")
        if tv_update_state():
            cdp = "TV connected" if tv_bridge_state["cdp_ok"] else "TV not connected yet"
            print(f"  Bridge ONLINE - {cdp}")
        else:
            print("  TV bridge OFFLINE - Binance fallback")

    def _vip_expiry_loop():
        time.sleep(60)
        while True:
            try:
                _check_vip_expiries()
            except Exception as e:
                print(f"[VIP] expiry check error: {e}")
            time.sleep(3600)  # hourly is plenty for a date-based expiry
    threading.Thread(target=_vip_expiry_loop, daemon=True).start()

    threading.Thread(target=command_listener, daemon=True).start()
    threading.Thread(target=_demo_monitor_loop, daemon=True).start()

    # Start SL/TP monitor — checks all copy users' positions every 1 hour
    ct.start_monitor_loop(notify_fn=send_admin, interval_hours=1)

    # Startup sync check — alert admin if any orphan positions exist
    def _startup_sync():
        time.sleep(10)  # wait for db to load
        lines = ct.sync_check()
        has_orphan = any("ORPHAN" in l or "GHOST" in l for l in lines)
        if has_orphan:
            text_lines = [l for l in lines if not l.startswith("__BTN__")]
            btn_rows = []
            for line in lines:
                if not line.startswith("__BTN__"): continue
                row = []
                for item in line[7:].split("|"):
                    cb = item.split(":")[0]; uid = item.split(":")[-1]
                    if cb.startswith("close_btc"):
                        row.append({"text": "❌ Close BTC", "callback_data": f"sync_close_btc:{uid}"})
                    elif cb.startswith("adopt_btc"):
                        row.append({"text": "✅ Adopt BTC", "callback_data": f"sync_adopt_btc:{uid}"})
                    elif cb.startswith("reset_ghost"):
                        row.append({"text": "🔄 Reset Ghost", "callback_data": f"sync_reset_ghost:{uid}"})
                    elif cb.startswith("ctretry_"):
                        sym = cb.split("_")[2] if len(cb.split("_")) > 2 else "?"
                        row.append({"text": f"✅ Adopt {sym}", "callback_data": f"sync_adopt_scan:{uid}:{sym}"})
                    elif cb.startswith("closescan_"):
                        sym = cb.replace("closescan_","")
                        row.append({"text": f"❌ Close {sym.replace('-USDT','')}", "callback_data": f"sync_close_scan:{uid}:{sym}"})
                if row: btn_rows.append(row)
            markup = {"inline_keyboard": btn_rows} if btn_rows else None
            send_reply(ADMIN_CHAT_ID, "🚨 <b>STARTUP SYNC ALERT</b>\n\n" + "\n".join(text_lines), reply_markup=markup)
    threading.Thread(target=_startup_sync, daemon=True).start()

    # Startup message → admin DM only
    tv_line = ""
    if TV_BRIDGE_URL:
        if tv_bridge_state["online"] and tv_bridge_state["cdp_ok"]:   tv_line = "TV: ONLINE ✅\n"
        elif tv_bridge_state["online"]:                                 tv_line = "TV: Bridge OK - TV not connected\n"
        else:                                                           tv_line = "TV: OFFLINE - Binance fallback\n"
    else:
        tv_line = "TV: Not configured - Binance-only\n"

    send_admin(
        f"<b>CLEXER V17.8.5 Deployed</b>\n"
        f"---------------------\n"
        f"{tv_line}"
        f"Charts: {'ON' if SEND_CHARTS else 'OFF (default)'}\n"
        f"News: {'ON' if SEND_NEWS else 'OFF (default)'}\n\n"
        f"⚠️ <b>Bot is PAUSED</b>\n"
        f"Send /go to start scanning.\n"
        f"---------------------\n"
        f"<i>🛡️ Capital protected</i>")

    MAIN_TICK = 5   # loop runs every 5s — ticker checked every TICK_INTERVAL=10s

    while True:
        try:
            if bot_paused.is_set():
                time.sleep(MAIN_TICK); continue

            # ── Weekend sleep: Fri 22:00 IST → Sun 23:00 IST ──────────────────
            global _weekend_sleep_notified
            if is_weekend_sleep():
                if not _weekend_sleep_notified:
                    _weekend_sleep_notified = True
                    send_admin("😴 <b>Weekend Sleep Mode</b>\n\nAll bot activity paused.\nFri 10 PM → Sun 11 PM IST.\nOpen trades are safe — BingX orders still active.\n\n<i>🛡️ Capital protected</i>")
                time.sleep(60); continue
            elif _weekend_sleep_notified:
                _weekend_sleep_notified = False
                send_admin("✅ <b>Weekend Sleep Ended</b>\n\nBot resuming all activity.\n\n<i>🛡️ Capital protected</i>")

            now = time.time(); forced = force_scan.is_set()
            if forced: force_scan.clear()

            h_str = now_ist().strftime('%H:%M IST'); mode_str = "NEW+TV" if is_tv_online() else "NEW+BingX"
            print(f"\n[{h_str}] {get_session()}{' FORCED' if forced else ''} | Src:{get_current_source()} | Mode:{mode_str}")

            # TV bridge health check
            if TV_BRIDGE_URL and (now-tv_bridge_state["last_check"]) >= tv_bridge_state["check_interval"]:
                was_online = tv_bridge_state["online"]
                is_online  = tv_update_state()
                if was_online and not is_online:
                    print("  TV OFFLINE - Binance fallback")
                    # Admin DM only - not channel
                    send_admin(f"<b>TradingView Offline</b>\n\nSwitched to Binance (OLD prompt).\n\n<i>🛡️ Capital protected</i>")
                elif not was_online and is_online:
                    print("  TV back ONLINE")
                    send_admin(f"<b>TradingView Back Online</b>\n\nSwitched back to TradingView (NEW prompt).\n\n<i>🛡️ Capital protected</i>")

            # News
            if (now-last_news_check_time) >= NEWS_CHECK_INTERVAL and SEND_NEWS:
                last_news_check_time = now
                threading.Thread(target=check_news, daemon=True).start()

            # ── Scan schedule ──────────────────────────────────────────────────
            global _last_midnight_date, _scan1_triggered_today, _test_triggered_today
            _ist_now = now_ist()
            _today = _ist_now.date()
            # Reset daily trigger sets at midnight
            if _last_midnight_date != _today:
                _last_midnight_date = _today
                _scan1_triggered_today.clear()
                _test_triggered_today.clear()

            _btc_fixed_hours = {7, 11, 15, 19, 23}
            _cur_hm = (_ist_now.hour, _ist_now.minute)

            # Scan1: fixed schedule
            if SCAN1_AUTO_ENABLED and not bot_paused.is_set() and not bot_stopped.is_set() and _cur_hm in SCAN1_SCHEDULE and _cur_hm not in _scan1_triggered_today:
                _scan1_triggered_today.add(_cur_hm)
                print(f"  [AUTO-SCAN1] {_ist_now.strftime('%H:%M')} IST")
                if ADMIN_CHAT_ID:
                    threading.Thread(target=lambda: _run_auto_scan(ADMIN_CHAT_ID, scan_ver=1), daemon=True).start()

            # Scan2: same schedule as Scan1
            if SCAN2_AUTO_ENABLED and not bot_paused.is_set() and not bot_stopped.is_set():
                if _cur_hm in SCAN2_SCHEDULE and (_cur_hm, 2) not in _scan1_triggered_today:
                    _scan1_triggered_today.add((_cur_hm, 2))
                    print(f"  [AUTO-SCAN2] {_ist_now.strftime('%H:%M')} IST")
                    if ADMIN_CHAT_ID:
                        threading.Thread(target=lambda: _run_auto_scan(ADMIN_CHAT_ID, scan_ver=2), daemon=True).start()

            # Test demo: fires 1 min after each scan1 time (if TEST_SCAN_ENABLED)
            if TEST_SCAN_ENABLED and not bot_paused.is_set() and not bot_stopped.is_set() and _cur_hm in SCAN1_TEST_SCHEDULE and _cur_hm not in _test_triggered_today:
                _test_triggered_today.add(_cur_hm)
                print(f"  [TEST-SCAN] Demo scan at {_ist_now.strftime('%H:%M')} IST (1min after scan1)")
                if ADMIN_CHAT_ID:
                    threading.Thread(target=lambda: _run_test_scan(ADMIN_CHAT_ID, 1), daemon=True).start()

            # Sleep hours
            if not forced and is_ist_sleep():
                if active_trade["signal"] or scan1_trades or scan2_trades:
                    pass   # still watch entry/SL/TP even during sleep hours
                else:
                    time.sleep(60); continue  # no trade — sleep a full minute

            # 1-min tick — BTC
            if ((now-last_tick_time) >= TICK_INTERVAL or forced) and active_trade["signal"]:
                last_tick_time = now
                if run_tick_check():
                    forced = True; last_signal_scan_time = 0

            # 1-min tick — all active scan trades (scan1 + scan2 lists)
            if ((now-last_scan_tick_time) >= TICK_INTERVAL) and (scan1_trades or scan2_trades):
                last_scan_tick_time = now
                run_scan_tick_check()

            # 1-hour price check
            if (now-last_price_check_time) >= PRICE_CHECK_INTERVAL and active_trade["signal"]:
                last_price_check_time = now
                if run_price_check():
                    forced = True; last_signal_scan_time = 0

            # BTC scan due? — fixed times: 7:21, 11:21, 15:21, 19:21, 23:21 IST only
            # (or forced via /signal)
            _btc_scan_hours = {7, 11, 15, 19, 23}
            _btc_ist = now_ist()
            _btc_scan_due = (
                _btc_ist.minute == 21 and
                _btc_ist.hour in _btc_scan_hours and
                last_signal_scan_time < (now - 3600)  # once per window (don't re-run same minute)
            )
            if not forced and (not _btc_scan_due or not btc_analysis_enabled or bot_stopped.is_set()):
                time.sleep(MAIN_TICK); continue

            # Cooldown
            if trade_stats["cooldown_scans"] > 0 and not forced:
                trade_stats["cooldown_scans"] -= 1
                if trade_stats["cooldown_scans"] == 0:
                    send_telegram("✅ <b>Cooldown over - scanning now!</b> 🔍\n\n✨ <i>🛡️ Capital protected</i>")
                last_signal_scan_time = now; time.sleep(MAIN_TICK); continue

            # -- FULL CLAUDE SCAN ----------------------------------------------
            last_signal_scan_time = now
            print("  Fetching candles...")
            ticker = get_ticker(); price = ticker["price"]
            print(f"  BTC: {price:,.2f} | {ticker['change']:+.2f}% | {get_session()} | {get_current_source()}")

            if active_trade["signal"]:
                t = active_trade
                send_admin(
                    f"<b>4H Scan - Active Trade</b>  {ist_str()}\n\n"
                    f"{t['signal']} @ {t['entry']:,.0f}\n"
                    f"SL:{t['sl']:,.0f} | TP1:{t['tp1']:,.0f} | TP2:{t['tp2']:,.0f}\n"
                    f"Current: {price:,.2f}\n"
                    f"Entry: {'YES' if t['entry_hit'] else 'pending'} | TP1: {'YES' if t['tp1_hit'] else 'no'}\n\n"
                    f"Analyzing...\n<i>🛡️ Capital protected</i>")

            data = fetch_all_data()

            if not active_trade["signal"]:
                signal = analyze_with_claude(ticker, data, validate_trade=False)
                if signal and not signal.get("_hold"):
                    _share_free = _free_quota_available()
                    if _share_free: _consume_free_quota()
                    send_telegram(fmt_signal(signal)); send_to_tier_channels(fmt_signal(signal), _share_free)
                    set_trade(signal)
                    results = ct.on_signal(signal, price, _share_free)
                    # MARKET orders filled instantly — send entry confirmation immediately
                    if signal.get("entry_type", "MARKET") == "MARKET":
                        send_telegram(
                            f"🚀 <b>ENTRY TRIGGERED!</b>  🕐 {ist_str()}\n\n"
                            f"{'🟢' if signal['signal']=='BUY' else '🔴'} <b>{signal['signal']} {SYMBOL}</b>\n"
                            f"🎯 Entry: <b>{signal['entry']:,.0f}</b>  ✅ MARKET FILLED\n"
                            f"🛑 SL:    <b>{signal['sl']:,.0f}</b>\n"
                            f"💰 TP1:   <b>{signal['tp1']:,.0f}</b>\n"
                            f"🏆 TP2:   <b>{signal['tp2']:,.0f}</b>\n\n"
                            f"✨ <i>🛡️ Capital protected</i>"
                        )
                    active = ct.active_count()
                    if active == 0:
                        send_admin(f"⚠️ <b>Copy Trade</b>\n\nNo active copy users — signal NOT copied to BingX.\n\nUse /users to check.")
                    else:
                        ok   = [r for r in results if r.startswith("✅")]
                        fail = [r for r in results if r.startswith("❌")]
                        msg  = f"📋 <b>Copy Trade Report</b>\n\n"
                        msg += f"Signal: {signal['signal']} @ {signal['entry']:,.0f}\n\n"
                        if ok:   msg += "\n".join(ok) + "\n"
                        if fail: msg += "\n" + "\n".join(fail) + "\n"
                        msg += f"\n✅ {len(ok)} executed | ❌ {len(fail)} failed"
                        send_admin(msg)
                    print(f"  [SIGNAL SENT] {signal['signal']} R:R:{signal.get('rr','?')}")
            else:
                t = active_trade
                signal = analyze_with_claude(ticker, data, validate_trade=True)
                if signal is None:
                    if forced:
                        send_telegram(f"<b>Trade Status: HOLD</b>  {ist_str()}\n\n"
                            f"{t['signal']} @ {t['entry']:,.0f}\nStructure intact.\n"
                            f"TP2: <b>{t['tp2']:,.0f}</b>\n\n<i>🛡️ Capital protected</i>")
                elif signal.get("_hold"):
                    send_admin(f"<b>Trade Validated - HOLD</b>  {ist_str()}\n\n"
                        f"{t['signal']} @ {t['entry']:,.0f}\n"
                        f"SL:{t['sl']:,.0f} | TP1:{t['tp1']:,.0f} | TP2:{t['tp2']:,.0f}\n\n"
                        f"<i>{signal.get('reasoning','Structure intact')[:250]}</i>\n\n<i>🛡️ Capital protected</i>")
                elif signal["signal"] != t["signal"]:
                    # Only flip if entry has already been hit — never flip a pending trade
                    if not t["entry_hit"]:
                        print(f"  [FLIP BLOCKED] Entry not hit yet — holding {t['signal']} @ {t['entry']:,.0f}")
                        send_admin(f"<b>Flip Blocked</b>\n\nClaude wanted to flip {t['signal']} -> {signal['signal']} but entry not hit yet.\nHolding original trade.\n\n<i>🛡️ Capital protected</i>")
                    else:
                        flip_reason = signal.get("reasoning","Structure flipped")
                        log_trade_outcome("STRUCTURE_FLIP", flip_reason[:100])
                        send_telegram(f"🔄 <b>STRUCTURE FLIP!</b> 🚨  🕐 {ist_str()}\n\n"
                            f"❌ Closing: {t['signal']} @ {t['entry']:,.0f}\n"
                            f"💡 Why: <i>{flip_reason[:200]}</i>\n\n"
                            f"{'🟢' if signal['signal']=='BUY' else '🔴'} New: <b>{signal['signal']} @ {signal['entry']:,.0f}</b>\n\n✨ <i>🛡️ Capital protected</i>")
                        ct.on_close_all()
                        reset_trade(); time.sleep(1)
                        _share_free = _free_quota_available()
                        if _share_free: _consume_free_quota()
                        send_telegram(fmt_signal(signal)); send_to_tier_channels(fmt_signal(signal), _share_free)
                        set_trade(signal)
                        ct.on_signal(signal, price, _share_free)
                else:
                    if forced:
                        send_telegram(f"<b>Trade Update</b>  {ist_str()}\n\n"
                            f"Old: {t['signal']} @ {t['entry']:,.0f}\n"
                            f"New: {signal['signal']} @ {signal['entry']:,.0f}\n"
                            f"Bias confirmed.\n\n<i>🛡️ Capital protected</i>")
                    log_trade_outcome("REPLACED","same direction, updated levels")
                    ct.on_close_all()
                    reset_trade(); time.sleep(1)
                    _share_free = _free_quota_available()
                    if _share_free: _consume_free_quota()
                    send_telegram(fmt_signal(signal)); send_to_tier_channels(fmt_signal(signal), _share_free)
                    set_trade(signal)
                    results = ct.on_signal(signal, price, _share_free)
                    ok = [r for r in results if r.startswith("✅")]
                    fail = [r for r in results if r.startswith("❌")]
                    send_admin(f"📋 <b>Copy Trade - Flip Signal</b>\n\n{signal['signal']} @ {signal['entry']:,.0f}\n✅ {len(ok)} | ❌ {len(fail)}\n{''.join(fail)}")

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("<b>CLEXER V17.8.5 Stopped</b>\n\n<i>- CLEXER -</i>"); break
        except Exception as e:
            print(f"  [MAIN ERROR] {e}"); import traceback; traceback.print_exc()

        time.sleep(MAIN_TICK)

if __name__ == "__main__":
    main()
