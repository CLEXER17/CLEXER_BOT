"""
CLEXER Signal Bot V17.8.5
"""

import os, time, json, base64, requests, anthropic, threading, re, subprocess, html as _html, random, string as _string, math

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
    import websocket as _ws_client   # websocket-client — powers the free liquidation feed
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False

# --- CONFIG -------------------------------------------------------------------
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY",   "")
AEROLINK_API_KEY    = os.getenv("AEROLINK_API_KEY",    "")   # separate key issued by aerolink.lat — never mix with ANTHROPIC_API_KEY
AEROLINK_API_KEY_2  = os.getenv("AEROLINK_API_KEY_2",  "")   # backup Aerolink key — used automatically on retry if the primary fails
AEROLINK_API_KEY_3  = os.getenv("AEROLINK_API_KEY_3",  "")   # 3rd Aerolink key slot — rotated in on further retries, empty until set
AEROLINK_API_KEY_4  = os.getenv("AEROLINK_API_KEY_4",  "")   # 4th Aerolink key slot — rotated in on further retries, empty until set
AEROLINK_API_KEY_5  = os.getenv("AEROLINK_API_KEY_5",  "")   # 5th Aerolink key slot — rotated in on further retries, empty until set
AEROLINK_API_KEY_6  = os.getenv("AEROLINK_API_KEY_6",  "")   # 6th Aerolink key slot — rotated in on further retries, empty until set
AEROLINK_API_KEY_7  = os.getenv("AEROLINK_API_KEY_7",  "")   # 7th Aerolink key slot — rotated in on further retries, empty until set
AEROLINK_API_KEY_8  = os.getenv("AEROLINK_API_KEY_8",  "")   # 8th Aerolink key slot — rotated in on further retries, empty until set
AEROLINK_API_KEY_9  = os.getenv("AEROLINK_API_KEY_9",  "")   # 9th Aerolink key slot — rotated in on further retries, empty until set
AEROLINK_API_KEY_10 = os.getenv("AEROLINK_API_KEY_10", "")   # 10th Aerolink key slot — rotated in on further retries, empty until set
AEROLINK_BASE_URL   = os.getenv("AEROLINK_BASE_URL",   "https://capi.aerolink.lat/")
AGENTROUTER_AUTH_TOKEN = os.getenv("AGENTROUTER_AUTH_TOKEN", "")   # TEST ONLY — re-testing whether Railway's AgentRouter failures still reproduce. Not wired into any live scan.
AGENTROUTER_BASE_URL   = os.getenv("AGENTROUTER_BASE_URL",   "https://agentrouter.org/")
GEMINI_API_KEY      = os.getenv("GEMINI_API_KEY",      "")   # free-tier key from aistudio.google.com — powers /chat
CHAT_MODEL = "google"   # "google" | "sonnet" | "opus" — /chat's text engine, admin-only via /model, defaults to Gemini
_CHAT_MODEL_IDS = {"sonnet": "claude-sonnet-5", "opus": "claude-opus-4-8"}
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN",  "")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")
ADMIN_CHAT_ID       = os.getenv("ADMIN_CHAT_ID",       "")
TV_BRIDGE_URL       = os.getenv("TV_BRIDGE_URL", "").rstrip("/")
MINI_APP_URL        = os.getenv("MINI_APP_URL", "").rstrip("/")   # Railway mini app URL for chart screenshots
CRYPTO_PAY_API_TOKEN = os.getenv("CRYPTO_PAY_API_TOKEN", "")   # @CryptoBot Crypto Pay API token
STARS_PER_USD = float(os.getenv("STARS_PER_USD", "62.5"))   # Telegram's real Stars rate: 100 Stars ≈ $1.60, i.e. $1 ≈ 62.5 Stars

SYMBOL               = "BTCUSDT"
TICK_INTERVAL        = 5     # price check every 5s when trade active
PRICE_CHECK_INTERVAL = 3600
SIGNAL_SCAN_INTERVAL = 14400
BINANCE_BASE         = "https://api1.binance.com/api/v3"
IST                  = timedelta(hours=5, minutes=30)

SEND_CHARTS       = False   # OFF by default - /images on to enable
CHART_SNAP_ENABLED = True   # /chartson /chartsoff toggle
CHART_TFS         = ["weekly", "4h", "1h", "5m"]
SEND_NEWS         = False
LIQUIDATION_MIN_USD    = 100000   # only post liquidations at/above this size
LIQUIDATION_POST_COOLDOWN = 20    # seconds — min gap between posts, so a liquidation cascade doesn't spam the channel

tv_bridge_state = {
    "online": False, "cdp_ok": False, "last_seen": 0,
    "last_check": 0, "fail_count": 0, "source": "BINANCE",
    "tv_version": "", "tv_symbol": "", "cached_intervals": [],
    "check_interval": 60,
}

def now_ist():  return datetime.now(timezone.utc) + IST
def ist_str():  return now_ist().strftime("%d %b %Y  %I:%M %p IST")

def _next_special_time(kind: str) -> str:
    """Next SPECIAL-only Scan1/Scan2 time (the ones that actually reach VIP/Free) —
    used for any user-facing display, so regular users never see the internal
    non-special/testing grid times mixed in."""
    times = sorted(_SCAN_SPECIAL.get(kind, set()))
    if not times:
        return "—"
    _now_hm = (now_ist().hour, now_ist().minute)
    fut = [(h, m) for h, m in times if (h, m) > _now_hm]
    if fut:
        h, m = fut[0]
        return f"{h}:{m:02d} IST"
    h, m = times[0]
    return f"{h}:{m:02d} IST (tomorrow)"

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

WEEKEND_SLEEP_ENABLED = True   # /ws on|off — off = bot keeps running straight through Fri-Sun
STATS_VISIBLE_TO_USERS = True  # /statsaccess on|off — off hides /stats from regular users (admin/co-admin always keep it)

def is_weekend_sleep() -> bool:
    """True from Friday 22:00 IST to Sunday 23:00 IST — full bot pause.
    Admin can disable this entirely via /ws off to let the bot run
    straight through the weekend instead."""
    if not WEEKEND_SLEEP_ENABLED:
        return False
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
demo_history          = []   # closed TS1/TS2 (demo) trades — same shape as scan_history, "dver" instead of "ver"
trade_outcomes        = []
force_scan            = threading.Event()
bot_paused            = threading.Event()  # PAUSE: freezes everything
bot_stopped           = threading.Event()  # STOP: blocks new scans only, monitoring continues
btc_analysis_enabled  = False  # OFF by default — /btcanalysis on to enable
SCAN_MODEL             = "claude-opus-4-8"  # BTC's model — switch via /model button or /gateway (BTC has no special/unverified/nonspecial split, always verified)
USE_AEROLINK           = False  # BTC's gateway — switch via /gateway button

# /aiconfig — full grid: each of Scan1/Scan2/TS1/TS2 picks its OWN model+gateway
# independently PER classification (verified/unverified/nonspecial), 12 slots
# total. BTC is not part of this grid (see SCAN_MODEL/USE_AEROLINK above).
def _aicfg_default_tier(): return {"model": "claude-opus-4-8", "aerolink": False}
AICFG_GRID = {
    kind: {tier: _aicfg_default_tier() for tier in ("verified", "unverified", "nonspecial")}
    for kind in ("scan1", "scan2", "test1", "test2")
}
ZONE_ENTRY_ENABLED = False  # Scan1/Scan2 entry style — MARKET (instant) vs ZONE (limit order at a price range's midpoint). Set via /entrystyle
_ZONE_BAND_PCT = 0.008      # zone width — ±0.8% around the computed entry price
CO_ADMIN_CHAT_ID  = ""    # a single trusted friend who gets ONE extra permission: /tradelog. No user mgmt, no billing, no resets, no broadcast.
CO_ADMIN_ENABLED  = False # ON = the co-admin permission is active AND their contact button shows next to Contact Admin
TRAIL_SL_BTC   = False  # Trailing SL — halfway to TP1, move SL to halfway toward entry. Set via /trailsl
TRAIL_SL_SCAN1 = False
TRAIL_SL_SCAN2 = False
TRAIL_SL_DEMO1 = False
TRAIL_SL_DEMO2 = False

def _apply_trail_sl(ver: int, t: dict, price: float):
    """Fixed 50/50 rule, two phases:
    Phase 1 (pre-TP1): once price reaches the halfway point to TP1, move SL to
    the halfway point between the original SL and entry.
    Phase 2 (post-TP1): SL sits at breakeven after TP1 — once price reaches the
    halfway point between that breakeven SL and TP2, move SL up to that point,
    locking in more than just breakeven before TP2 itself hits.
    Each phase runs once (trail_sl_moved / trail_sl2_moved guards it), and each
    replaces the OTHER phase's still-open trailing message (deleted at TP1 hit
    for phase 1's, and at TP2/SL close for phase 2's — see _delete_trail_sl_messages).
    ver: 1=Scan1, 2=Scan2, 3=Demo1, 4=Demo2 — ct.update_scan_sl() looks the symbol
    up across all slot prefixes (including demo1/demo2), so this also moves any
    real copy-user SL that's mirroring a demo trade."""
    enabled = {1: TRAIL_SL_SCAN1, 2: TRAIL_SL_SCAN2, 3: TRAIL_SL_DEMO1, 4: TRAIL_SL_DEMO2}.get(ver, False)
    if not enabled:
        return
    sig = t["signal"]; tag = f"S{ver}" if ver <= 2 else f"TS{ver - 2}"
    if not t.get("tp1_hit"):
        if t.get("trail_sl_moved"):
            return
        entry = t["entry"]; tp1 = t["tp1"]
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
        _msg = (
            f"🛡️ <b>Trailing SL — #{t['symbol']}</b>  {tag}\n\n"
            f"Price reached halfway to TP1 — SL moved <code>{orig_sl:,.4g}</code> → <code>{new_sl:,.4g}</code> to lock in more capital.")
        _locked_msg = (
            f"🛡️ <b>Trailing SL — #{t['symbol']}</b>  {tag}\n\n"
            f"Price reached halfway to TP1 — SL moved BE to lock in more capital.")
        _trail_ids = send_lifecycle_reply(_msg, t.get("reply_map"), include_ch2=False,
            tier_routed=bool(t.get("tier_routed")), share_free=t.get("share_free", True), locked_text=_locked_msg)
        t["trail_sl_msg_ids"] = _trail_ids or {}
        for _k, _v in (_trail_ids or {}).items():
            if _k.startswith("free:"): _track_free_sl(t.get("sig_id",""), _k.split(":", 1)[1], "trailing_mid", _v)
    else:
        if t.get("trail_sl2_moved"):
            return
        be_sl = t["sl"]; tp2 = t["tp2"]
        midpoint2 = (be_sl + tp2) / 2
        hit2 = (sig == "BUY" and price >= midpoint2) or (sig == "SELL" and price <= midpoint2)
        if not hit2:
            return
        t["sl"] = midpoint2
        t["trail_sl2_moved"] = True
        ct.update_scan_sl(t["symbol"], midpoint2)
        save_state()
        _msg = (
            f"🛡️ <b>Trailing SL (Post-TP1) — #{t['symbol']}</b>  {tag}\n\n"
            f"Price reached halfway to TP2 — SL moved <code>{be_sl:,.4g}</code> → <code>{midpoint2:,.4g}</code> to lock in more profit.")
        _locked_msg = (
            f"🛡️ <b>Trailing SL (Post-TP1) — #{t['symbol']}</b>  {tag}\n\n"
            f"Price reached halfway to TP2 — SL moved up to lock in more profit.")
        _trail_ids = send_lifecycle_reply(_msg, t.get("reply_map"), include_ch2=False,
            tier_routed=bool(t.get("tier_routed")), share_free=t.get("share_free", True), locked_text=_locked_msg)
        t["trail_sl_msg_ids"] = _trail_ids or {}
        for _k, _v in (_trail_ids or {}).items():
            if _k.startswith("free:"): _track_free_sl(t.get("sig_id",""), _k.split(":", 1)[1], "trailing_mid2", _v)

def _apply_trail_sl_btc(price: float):
    if not TRAIL_SL_BTC:
        return
    sig = active_trade["signal"]
    if not active_trade.get("tp1_hit"):
        if active_trade.get("trail_sl_moved"):
            return
        entry = active_trade["entry"]; tp1 = active_trade["tp1"]
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
        _msg = (
            f"🛡️ <b>Trailing SL — BTC</b>\n\n"
            f"Price reached halfway to TP1 — SL moved <code>{orig_sl:,.0f}</code> → <code>{new_sl:,.0f}</code> to lock in more capital.")
        _locked_msg = (
            f"🛡️ <b>Trailing SL — BTC</b>\n\n"
            f"Price reached halfway to TP1 — SL moved BE to lock in more capital.")
        _trail_ids = send_lifecycle_reply(_msg, active_trade.get("reply_map"), include_ch2=False,
            tier_routed=True, share_free=active_trade.get("share_free", True), locked_text=_locked_msg)
        active_trade["trail_sl_msg_ids"] = _trail_ids or {}
        for _k, _v in (_trail_ids or {}).items():
            if _k.startswith("free:"): _track_free_sl(active_trade.get("sig_id",""), _k.split(":", 1)[1], "trailing_mid", _v)
    else:
        if active_trade.get("trail_sl2_moved"):
            return
        be_sl = active_trade["sl"]; tp2 = active_trade["tp2"]
        midpoint2 = (be_sl + tp2) / 2
        hit2 = (sig == "BUY" and price >= midpoint2) or (sig == "SELL" and price <= midpoint2)
        if not hit2:
            return
        active_trade["sl"] = midpoint2
        active_trade["trail_sl2_moved"] = True
        ct.on_update_sl(midpoint2)
        save_active_trade()
        _msg = (
            f"🛡️ <b>Trailing SL (Post-TP1) — BTC</b>\n\n"
            f"Price reached halfway to TP2 — SL moved <code>{be_sl:,.0f}</code> → <code>{midpoint2:,.0f}</code> to lock in more profit.")
        _locked_msg = (
            f"🛡️ <b>Trailing SL (Post-TP1) — BTC</b>\n\n"
            f"Price reached halfway to TP2 — SL moved up to lock in more profit.")
        _trail_ids = send_lifecycle_reply(_msg, active_trade.get("reply_map"), include_ch2=False,
            tier_routed=True, share_free=active_trade.get("share_free", True), locked_text=_locked_msg)
        active_trade["trail_sl_msg_ids"] = _trail_ids or {}
        for _k, _v in (_trail_ids or {}).items():
            if _k.startswith("free:"): _track_free_sl(active_trade.get("sig_id",""), _k.split(":", 1)[1], "trailing_mid2", _v)
    for _k, _v in (_trail_ids or {}).items():
        if _k.startswith("free:"): _track_free_sl(active_trade.get("sig_id",""), _k.split(":", 1)[1], "trailing_mid", _v)

def _delete_trail_sl_messages(t: dict):
    """TP1 hit makes the earlier 'Trailing SL moved to X' note stale (SL is
    about to move again, to breakeven) — delete it from every channel it was
    posted to instead of leaving an outdated message sitting in the feed."""
    _ids = t.get("trail_sl_msg_ids") or {}
    if not _ids:
        return
    for _k, _mid in _ids.items():
        if _k == "ch1": _cid = TELEGRAM_CHANNEL_ID
        elif _k == "ch2": _cid = os.getenv("TELEGRAM_CHANNEL_ID_2", "")
        else: _cid = _k.split(":", 1)[1]
        if not _cid: continue
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage",
                json={"chat_id": _cid, "message_id": _mid}, timeout=10)
        except Exception as e:
            print(f"  [TRAIL SL] delete msg error: {e}")
    t["trail_sl_msg_ids"] = {}
# ─── VIP / Free channels + user tiers ──────────────────────────────────────
CHANNELS: list = []  # [{"id": str, "tier": "vip"/"free", "label": str}, ...] — any number of each
FREE_SIGNAL_DAILY_LIMIT = 40   # % of each day's verified/special signals also shared to Free (0-100)
_free_signal_tracker = {"date": "", "total": 0, "shared": 0}  # resets automatically when the IST date rolls over

def _save_free_tracker():
    """Persists _free_signal_tracker to local disk AND the central store — same
    fix as _save_daily_buckets(): a pure in-memory counter silently resets to
    0/0 on every Railway redeploy (and, in the multi-server setup, is never
    shared between the standby and active instances), which is exactly why the
    Free Channel Share % screen kept showing 0/0 shared even while signals
    were actively being posted to Free from whichever process fired them."""
    try:
        with open(os.path.join(DATA_DIR, "free_signal_tracker.json"), "w") as f:
            json.dump(_free_signal_tracker, f)
    except Exception as e:
        print(f"[FREE TRACKER] local save error: {e}")
    # Active-server gate — see save_settings()'s comment: a standby/abandoned
    # server keeps running this same loop with its own local counters, and
    # would otherwise silently clobber the active server's real numbers.
    if CLEXER_API_URL and is_active_server():
        try:
            _kv_push("free_signal_tracker", _free_signal_tracker)
        except Exception as e:
            print(f"[FREE TRACKER] central push error: {e}")

def _load_free_tracker():
    global _free_signal_tracker
    try:
        d = None
        path = os.path.join(DATA_DIR, "free_signal_tracker.json")
        if CLEXER_API_URL:
            r = _central_get("/kv/free_signal_tracker")
            if r is not None and r.ok:
                d = _kv_pick_newer(path, r.json(), "FREE TRACKER")
        if d is None and os.path.exists(path):
            with open(path) as f:
                d = json.load(f)
        if d is not None:
            _free_signal_tracker = {"date": d.get("date",""), "total": d.get("total",0), "shared": d.get("shared",0)}
            print(f"[FREE TRACKER] Restored {_free_signal_tracker}")
    except Exception as e:
        print(f"[FREE TRACKER] load error: {e}")

def _channels_by_tier(tier: str) -> list:
    return [c["id"] for c in CHANNELS if c.get("tier") == tier and c.get("id")]

def _in_free_window() -> bool:
    now = datetime.now(timezone.utc) + IST
    return 6 <= now.hour < 19  # 06:00–19:00 IST

def _free_quota_available() -> bool:
    """FREE_SIGNAL_DAILY_LIMIT rule (now a %, not a raw count): out of every
    day's verified/special signals, that % also gets shared to Free (e.g. 40%
    with 10 verified fires that day -> 4 shown in Free). Every call counts
    toward that day's total (one call per verified fire), and returns True
    (share it) only while doing so keeps shared/total at or under the %  —
    this naturally spreads the share across the whole day instead of front-
    or back-loading it."""
    global _free_signal_tracker
    now = datetime.now(timezone.utc) + IST
    today = now.strftime("%Y-%m-%d")
    if _free_signal_tracker.get("date") != today:
        _free_signal_tracker = {"date": today, "total": 0, "shared": 0}
    _free_signal_tracker["total"] += 1
    _save_free_tracker()
    if not _in_free_window():
        return False
    return _free_signal_tracker["shared"] < math.ceil(_free_signal_tracker["total"] * (FREE_SIGNAL_DAILY_LIMIT / 100.0))

def _consume_free_quota():
    _free_signal_tracker["shared"] = _free_signal_tracker.get("shared", 0) + 1
    _save_free_tracker()

_BOT_USERNAME = None

def _get_bot_username():
    global _BOT_USERNAME
    if _BOT_USERNAME:
        return _BOT_USERNAME
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=10)
        rj = r.json()
        if rj.get("ok"):
            _BOT_USERNAME = rj["result"]["username"]
    except Exception as e:
        print(f"  [GETME ERROR] {e}")
    return _BOT_USERNAME

def _send_via_true_forward(text: str, dest_chat_id, tag: str, with_bot_button: bool = False) -> bool:
    """Sends via plain sendMessage. NOTE: this used to stage-then-forwardMessage
    to "preserve" premium/custom emoji — that assumption was wrong. /testreply
    proved the opposite empirically: forwardMessage strips premium emoji, while
    plain sendMessage (with the <tg-emoji> entities _apply_premium_emojis already
    wrapped the text in, by the time it reaches here) renders them correctly.
    Kept the same name/signature so every existing caller needed no changes.
    If with_bot_button, sends a small follow-up message with an 'Open Bot' link."""
    base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    try:
        r = requests.post(f"{base}/sendMessage",
            json={"chat_id": dest_chat_id, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True}, timeout=10)
        rj = r.json()
        if not rj.get("ok"):
            print(f"  [SEND] {tag} {dest_chat_id} rejected: {rj.get('description')}")
            return False
        if with_bot_button:
            _uname = _get_bot_username()
            if _uname:
                r_btn = requests.post(f"{base}/sendMessage",
                    json={"chat_id": dest_chat_id, "text": "👇 Copy this trade automatically",
                          "reply_markup": {"inline_keyboard": [[
                              {"text": "🤖 Open Bot", "url": f"https://t.me/{_uname}", "style": "primary"}]]}},
                    timeout=10)
                if not r_btn.json().get("ok"):
                    print(f"  [SEND BUTTON] {tag} {dest_chat_id} failed: {r_btn.json().get('description')}")
        return rj.get("result", {}).get("message_id") or True
    except Exception as e:
        print(f"  [SEND] {tag} {dest_chat_id}: {e}")
        return False


def _pin_message(chat_id, message_id, disable_notification: bool = True):
    """Pins a message in a channel/group — requires the bot to be an admin
    there with 'pin messages' rights. Silent no-op on failure (e.g. bot isn't
    admin) rather than raising, since pinning is a nice-to-have, not critical."""
    if not message_id:
        return
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/pinChatMessage",
            json={"chat_id": chat_id, "message_id": message_id,
                  "disable_notification": disable_notification}, timeout=10)
        if not r.json().get("ok"):
            print(f"  [PIN] {chat_id} msg {message_id} failed: {r.json().get('description')}")
    except Exception as e:
        print(f"  [PIN] {chat_id} msg {message_id}: {e}")

def _send_plain_reply(chat_id, text: str, reply_to=None, reply_markup=None):
    """Direct sendMessage (no forward trick) — premium emoji fall back to plain
    glyphs, but this lets the message reply to an earlier one (reply_to = that
    message's message_id). Returns the new message_id, or None on failure."""
    text = _apply_premium_emojis(text)
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_to:
        payload["reply_parameters"] = {"message_id": reply_to, "allow_sending_without_reply": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json=payload, timeout=10)
        rj = r.json()
        if rj.get("ok"):
            return rj["result"]["message_id"]
        print(f"  [PLAIN REPLY] {chat_id} rejected: {rj.get('description')}")
    except Exception as e:
        print(f"  [PLAIN REPLY] {chat_id}: {e}")
    return None

def send_entry_signal(text: str, include_ch2: bool = True, tier_routed: bool = False, share_free: bool = True,
                       locked_text: str = None, sig_id: str = None) -> dict:
    """Sends a trade's entry signal via plain sendMessage (confirmed via /testreply
    to render premium emoji correctly — the old forward-relay trick actually LOSES
    them despite the original assumption it preserved them), and captures each
    destination's message_id. Store the returned dict on the trade (t["reply_map"] = ...)
    so later send_lifecycle_reply() calls can thread TP1/TP2/SL/Trailing-SL/timeout
    messages as genuine replies to this entry post in every channel it went to.

    locked_text + sig_id: when both given AND share_free is False (i.e. this
    signal is genuinely VIP-exclusive — previously Free got NOTHING for these
    at entry, only a generic "VIP hit TP1" teaser later via _notify_free_late),
    the FREE channel now gets this redacted variant instead, plus an Unlock
    button deep-linking to the bot's DM (/start unlock_<sig_id>). When
    share_free is True, the signal was always meant to be free — Free keeps
    getting the real, unredacted `text`, completely unchanged. VIP and the
    legacy channels always get the real `text` either way."""
    ids = {}
    channels = [("1", TELEGRAM_CHANNEL_ID), ("2", os.getenv("TELEGRAM_CHANNEL_ID_2",""))]
    for key, cid in channels:
        if not cid: continue
        if channel_paused.get(key): continue
        if key == "2" and not include_ch2: continue
        mid = _send_plain_reply(cid, text)
        if mid: ids[f"ch{key}"] = mid
    if tier_routed:
        for cid in _channels_by_tier("vip"):
            mid = _send_plain_reply(cid, text)
            if mid: ids[f"vip:{cid}"] = mid
        if share_free:
            for cid in _channels_by_tier("free"):
                mid = _send_plain_reply(cid, text)
                if mid: ids[f"free:{cid}"] = mid
        elif locked_text and sig_id:
            _free_markup = None
            _uname = _get_bot_username()
            if _uname:
                _free_markup = {"inline_keyboard": [[
                    # Telegram deep-link start params only allow [A-Za-z0-9_-] — sig_id
                    # is "#CLEXxxxxxx", so the "#" must be stripped here (it would
                    # otherwise be parsed as a URL fragment and never reach the bot at
                    # all) and re-added when /start parses it back (see handle_command).
                    {"text": "🔓 Unlock Signal", "url": f"https://t.me/{_uname}?start=unlock_{sig_id.lstrip('#')}", "style": "primary"}]]}
            for cid in _channels_by_tier("free"):
                mid = _send_plain_reply(cid, locked_text, reply_markup=_free_markup)
                if mid: ids[f"free:{cid}"] = mid
    return ids

def _tp_buttons():
    """Open Bot + Get VIP buttons — attached to every TP1/TP2 message, per
    admin request, so users can act right from the win notification. Get VIP
    deep-links straight into the /vip offer screen (same t.me/bot?start=vip
    pattern used elsewhere) rather than just opening a blank DM."""
    row = []
    _uname = _get_bot_username()
    if _uname:
        row.append({"text": "🤖 Open Bot", "url": f"https://t.me/{_uname}", "style": "primary"})
        row.append({"text": "👑 Get VIP", "url": f"https://t.me/{_uname}?start=vip", "style": "primary"})
    return {"inline_keyboard": [row]} if row else None

def send_lifecycle_reply(text: str, reply_map: dict, include_ch2: bool = True, tier_routed: bool = False, share_free: bool = True, reply_markup=None,
                          locked_text: str = None):
    """Sends a TP1/TP2/SL/Trailing-SL/timeout follow-up as a genuine Telegram reply
    to that trade's entry-signal message in every destination it has a stored
    message_id for (reply_map, from send_entry_signal). Uses plain sendMessage —
    forwardMessage can't set reply-to — so premium emoji fall back to plain glyphs
    on these specific messages. A destination missing from reply_map (e.g. entry
    capture failed, or reply_map is empty/old-format) just gets a normal post.

    locked_text: when given AND share_free is False (signal was locked at
    entry), Free gets this redacted variant instead of nothing — same idea as
    send_entry_signal's locked_text, for follow-ups like Trailing SL where a
    generic "no real numbers" version still makes sense to show."""
    reply_map = reply_map or {}
    ids = {}
    channels = [("1", TELEGRAM_CHANNEL_ID), ("2", os.getenv("TELEGRAM_CHANNEL_ID_2",""))]
    for key, cid in channels:
        if not cid: continue
        if channel_paused.get(key): continue
        if key == "2" and not include_ch2: continue
        mid = _send_plain_reply(cid, text, reply_to=reply_map.get(f"ch{key}"), reply_markup=reply_markup)
        if mid: ids[f"ch{key}"] = mid
    if tier_routed:
        for cid in _channels_by_tier("vip"):
            mid = _send_plain_reply(cid, text, reply_to=reply_map.get(f"vip:{cid}"), reply_markup=reply_markup)
            if mid: ids[f"vip:{cid}"] = mid
        if share_free:
            for cid in _channels_by_tier("free"):
                mid = _send_plain_reply(cid, text, reply_to=reply_map.get(f"free:{cid}"), reply_markup=reply_markup)
                if mid: ids[f"free:{cid}"] = mid
        elif locked_text:
            for cid in _channels_by_tier("free"):
                mid = _send_plain_reply(cid, locked_text, reply_to=reply_map.get(f"free:{cid}"), reply_markup=reply_markup)
                if mid: ids[f"free:{cid}"] = mid
    return ids

def _send_sl_and_log(text: str, reply_map: dict, sig_id: str, result: str, **kwargs) -> dict:
    """Same as send_lifecycle_reply, but also records this signal's Free-channel
    SL-hit message_id + final result (SL/BE) so /clearslfree can later find and
    delete exactly this signal's messages — only if result is a real SL, never
    for BE. See _track_free_sl/_finalize_free_sl for the actual bookkeeping."""
    ids = send_lifecycle_reply(text, reply_map, **kwargs)
    for k, v in (ids or {}).items():
        if k.startswith("free:"):
            _track_free_sl(sig_id, k.split(":", 1)[1], "sl_mid", v)
    _finalize_free_sl(sig_id, result)
    return ids

def send_to_tier_channels(text: str, share_free: bool):
    """Sends to every registered VIP channel always, and to FREE channels only
    if share_free is True (the daily quota decision made once per signal).
    Uses the true-forwardMessage relay so custom_emoji survives; falls back
    to a direct send only if the forward relay itself fails."""
    text = _apply_premium_emojis(text)
    for cid in _channels_by_tier("vip"):
        if _send_via_true_forward(text, cid, "vip"):
            continue
        try:
            r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
            rj = r.json()
            if not rj.get("ok"):
                print(f"  [TIER CHANNEL] vip {cid} rejected: {rj.get('description')}")
        except Exception as e: print(f"  [TIER CHANNEL] vip {cid}: {e}")
    if share_free:
        for cid in _channels_by_tier("free"):
            if _send_via_true_forward(text, cid, "free"):
                continue
            try:
                r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": cid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
                rj = r.json()
                if not rj.get("ok"):
                    print(f"  [TIER CHANNEL] free {cid} rejected: {rj.get('description')}")
                else:
                    _ents = rj.get("result", {}).get("entities", [])
                    _ce = [e for e in _ents if e.get("type") == "custom_emoji"]
                    print(f"  [TIER CHANNEL DEBUG] free {cid}: {len(_ce)} custom_emoji entities echoed back of {len(_ents)} total entities")
            except Exception as e: print(f"  [TIER CHANNEL] free {cid}: {e}")

def _all_channel_ids() -> list:
    """Every destination CLEXER posts signals to — legacy channels (skipping
    any that are currently paused via /pausechannel) + all VIP/Free tier channels."""
    ids = []
    if TELEGRAM_CHANNEL_ID and not channel_paused.get("1"): ids.append(("legacy1", TELEGRAM_CHANNEL_ID))
    _ch2 = os.getenv("TELEGRAM_CHANNEL_ID_2","")
    if _ch2 and not channel_paused.get("2"): ids.append(("legacy2", _ch2))
    for cid in _channels_by_tier("vip"): ids.append(("vip", cid))
    for cid in _channels_by_tier("free"): ids.append(("free", cid))
    return ids

# ─── Daily TP1/TP2/SL tracker — drives the streak promo, SL reassurance, and
# end-of-day recap. Trades are bucketed by the IST calendar day they were
# OPENED on (their entry_date), not by whichever day their TP1/TP2/SL happens
# to fire on — so a trade opened on the 14th that closes at 12:02 AM on the
# 15th still counts toward the 14th's recap, exactly like every other trade
# from that day. Once a day's recap has been sent, any later-arriving result
# for a trade from that day is simply not recapped again (there's no way to
# retroactively edit an already-sent Telegram message). ────────────────────
_daily_buckets: dict = {}   # entry_date str -> {"date","tp1","tp2","sl","free_tp1","tp1_promo_sent","trades"}
_daily_summary_last_sent_date = ""

def _save_daily_buckets():
    """Persists _daily_buckets both to local disk AND the central store — local
    disk alone was the original bug: on Railway, the app filesystem is NOT
    guaranteed to survive a redeploy unless DATA_DIR is on a mounted volume, so
    a disk-only save can silently reset to empty on every push exactly like the
    in-memory version did. Pushing to the central store (same one save_state()
    and the slot-stats system use) makes this survive redeploys regardless of
    whether local disk does. Cheap to call on every _track_daily_result()
    update since the payload is tiny."""
    _blob = {"buckets": _daily_buckets, "last_sent": _daily_summary_last_sent_date}
    try:
        with open(os.path.join(DATA_DIR, "daily_buckets.json"), "w") as f:
            json.dump(_blob, f)
    except Exception as e:
        print(f"[DAILY BUCKETS] local save error: {e}")
    # Active-server gate — see save_settings()'s comment.
    if CLEXER_API_URL and is_active_server():
        try:
            _kv_push("daily_buckets", _blob)
        except Exception as e:
            print(f"[DAILY BUCKETS] central push error: {e}")

def _load_daily_buckets():
    global _daily_buckets, _daily_summary_last_sent_date
    try:
        d = None
        path = os.path.join(DATA_DIR, "daily_buckets.json")
        if CLEXER_API_URL:
            r = _central_get("/kv/daily_buckets")
            if r is not None and r.ok:
                d = _kv_pick_newer(path, r.json(), "DAILY BUCKETS")
        if d is None and os.path.exists(path):
            with open(path) as f:
                d = json.load(f)
        if d is not None:
            _daily_buckets = d.get("buckets", {})
            _daily_summary_last_sent_date = d.get("last_sent", "")
            print(f"[DAILY BUCKETS] Restored {len(_daily_buckets)} day(s)")
    except Exception as e:
        print(f"[DAILY BUCKETS] load error: {e}")

def _ist_date_str(epoch_seconds=None) -> str:
    """IST calendar-day string for a given epoch timestamp, or today if none given."""
    if epoch_seconds is None:
        return now_ist().strftime("%Y-%m-%d")
    try:
        return (datetime.fromtimestamp(float(epoch_seconds), timezone.utc) + IST).strftime("%Y-%m-%d")
    except Exception:
        return now_ist().strftime("%Y-%m-%d")

def _get_daily_bucket(date_str: str) -> dict:
    if date_str not in _daily_buckets:
        _daily_buckets[date_str] = {"date": date_str, "tp1": 0, "tp2": 0, "sl": 0,
                                     "free_tp1": 0, "tp1_promo_sent": False, "trades": []}
        for _old_date in sorted(_daily_buckets.keys())[:-7]:   # bound memory — keep last 7 days
            del _daily_buckets[_old_date]
    return _daily_buckets[date_str]

def _send_tp1_streak_promo(symbol: str, detail: dict):
    """After the 3rd TP1 of the day that was actually shown in the Free channel
    closes, post a VIP-conversion promo with that real trade's details —
    plain emoji (none needed), native buttons (no forward trick required
    since there's no custom emoji to preserve here)."""
    _uname = _get_bot_username()
    btns = []
    if _uname:
        btns.append({"text": "🤖 Open Bot", "url": f"https://t.me/{_uname}", "style": "primary"})
    if ADMIN_CHAT_ID:
        btns.append({"text": "💬 Contact Admin", "url": f"tg://user?id={ADMIN_CHAT_ID}", "style": "primary"})
    mkp = {"inline_keyboard": [btns]} if btns else None
    coin = symbol.replace("-USDT", "").replace("USDT", "")
    tag = detail.get("tag", "?"); side = detail.get("side", "?")
    tp1 = detail.get("tp1", 0); sl_be = detail.get("sl_be", 0); tp2 = detail.get("tp2", 0)
    arrow = "🟩" if side == "BUY" else "🟥"
    text = (
        f"💰 <b>TP1 HIT — #{coin}USDT!</b> 🎉  |  <b>{tag}</b>\n"
        f"{arrow} {side}\n"
        f"✅ TP1: <b>{tp1:,.4g}</b>\n"
        f"🛡 SL moved to BE: <b>{sl_be:,.4g}</b>\n"
        f"🚀 Riding TP2: <b>{tp2:,.4g}</b>...\n"
        "Another TAKE PROFIT closed successfully. Congratulations to everyone who followed the setup! 🔥\n\n"
        "This is just a glimpse of what our community receives.\n\n"
        "👑 Crypto Clexer VIP members get:\n"
        "• High-quality trade setups\n"
        "• Entry, Targets & Stop-Loss\n"
        "• Real-time trade updates\n"
        "• Risk management guidance\n"
        "• Priority market analysis\n\n"
        "If you're serious about improving your trading and want access to our premium content, DM me now to learn how to join the VIP community.\n\n"
        "📩 Limited VIP access available."
    )
    for cid in _channels_by_tier("free"):
        try:
            payload = {"chat_id": cid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
            if mkp: payload["reply_markup"] = mkp
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json=payload, timeout=10)
        except Exception as e: print(f"  [TP1 PROMO] free {cid}: {e}")

def _free_and_vip_channel_ids() -> list:
    """Free + VIP tier channels only — excludes the Signal (legacy) channel.
    Used for messages that should reach both tiers but not the Signal channel."""
    ids = []
    for cid in _channels_by_tier("vip"): ids.append(("vip", cid))
    for cid in _channels_by_tier("free"): ids.append(("free", cid))
    return ids

def _sl_reassurance_channels(tier_routed: bool, share_free: bool) -> list:
    """Which channels should get the SL reassurance post, mirroring exactly
    where this trade's entry was shown. Signal-only entries get none."""
    if not tier_routed:
        return []
    if share_free:
        return _free_and_vip_channel_ids()
    return [("vip", cid) for cid in _channels_by_tier("vip")]

def _send_sl_reassurance(symbol: str, tag: str, side: str, entry_price, channels: list, reply_map: dict = None, sig_id: str = ""):
    """Sent every real SL loss (not breakeven) — only to the tiers that actually
    received this trade's entry (Signal-only entries get nothing here; the
    Signal channel keeps its own separate SL message, unchanged).
    No buttons — buttons are TP-only, per admin's instruction.
    reply_map (from send_entry_signal): replies to the trade's entry post in each
    channel it has a stored message_id for, so this threads with the signal."""
    if not channels:
        return
    reply_map = reply_map or {}
    coin = symbol.replace("-USDT", "").replace("USDT", "")
    try:
        entry_str = f"{float(entry_price):,.4g}"
    except Exception:
        entry_str = str(entry_price)
    _sl_line1 = _smallcaps_title("Not every trade is a winner, and that's part of professional trading.")
    _sl_line2 = _smallcaps_title("Losses are controlled through proper risk management.")
    _sl_line3 = _smallcaps_title("We stay disciplined, protect our capital, and move on to the next opportunity.")
    _sl_line4 = _smallcaps_title("The goal isn't to win every trade—it's to stay consistently profitable over time.")
    _sl_line5 = _smallcaps_title("Crypto Clexer focuses on strategy, discipline, and long-term results.")
    _sid_line = f"\n🪪 {sig_id}" if sig_id else ""
    text = _apply_premium_emojis(
        f"🚨 <b>SL HIT — #{coin}USDT</b> 🚨  |  <b>{tag}</b>\n"
        f"❌ Loss on {side} @ <code>{entry_str}</code>\n\n"
        f"<blockquote>{_sl_line1}\n\n"
        f"✅ {_sl_line2}\n"
        f"📊 {_sl_line3}\n\n"
        f"{_sl_line4}\n\n"
        f"💎 {_sl_line5}</blockquote>"
        f"{_sid_line}"
    )
    for _tag, cid in channels:
        _send_plain_reply(cid, text, reply_to=reply_map.get(f"{_tag}:{cid}"))

def _send_tp2_congrats():
    """Sent every TP2 hit — plain emoji, direct send (no forward needed),
    with both Open Bot + Contact Admin buttons (TP-only, per instruction)."""
    _uname = _get_bot_username()
    btns = []
    if _uname: btns.append({"text": "🤖 Open Bot", "url": f"https://t.me/{_uname}", "style": "primary"})
    if ADMIN_CHAT_ID: btns.append({"text": "💬 Contact Admin", "url": f"tg://user?id={ADMIN_CHAT_ID}", "style": "primary"})
    mkp = {"inline_keyboard": [btns]} if btns else None
    text = (
        "🎯 TP2 HIT! ✅🔥\n\n"
        "Another target achieved successfully! Congratulations to everyone who stayed patient and trusted the setup.\n\n"
        "📈 TP1 ✅\n"
        "🎯 TP2 ✅\n\n"
        "This is the level of precision we aim to deliver consistently through disciplined analysis and risk management."
    )
    for cid in _channels_by_tier("free"):
        try:
            payload = {"chat_id": cid, "text": text}
            if mkp: payload["reply_markup"] = mkp
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json=payload, timeout=10)
        except Exception as e: print(f"  [TP2 CONGRATS] free {cid}: {e}")

def _notify_free_late(symbol: str, trade: dict, result: str):
    """If this trade was VIP-only at entry (daily free quota was already used
    up, so Free never saw it), and it just hit TP1/TP2, post a VIP-conversion
    pitch to Free-tier viewers — plain emoji, native buttons, quoting exactly
    when the original VIP-only signal went out."""
    if not trade.get("tier_routed", True):
        return  # Signal-only entry — never went to VIP either, nothing to "catch up" on
    if trade.get("share_free", True):
        return  # already shared to Free at entry — no catch-up needed
    free_chans = _channels_by_tier("free")
    if not free_chans:
        return
    entry_ts = trade.get("entry_time_str", "")
    coin = symbol.replace("-USDT", "").replace("USDT", "")
    _uname = _get_bot_username()
    btns = []
    if _uname: btns.append({"text": "🤖 Open Bot", "url": f"https://t.me/{_uname}", "style": "primary"})
    if _uname: btns.append({"text": "👑 Get VIP", "url": f"https://t.me/{_uname}?start=vip", "style": "primary"})
    mkp = {"inline_keyboard": [btns]} if btns else None
    if result == "TP1":
        text = (
            "🚨 <b>VIP SIGNAL UPDATE</b>\n\n"
            f"#{coin}USDT 🎯 <b>TP1 HIT</b> ✅\n\n"
            f"<blockquote>This signal was shared exclusively in \"Crypto Clexer VIP\" AT {entry_ts}\n"
            "Congratulations to all our VIP members who secured profits. 🔥\n\n"
            "Want to receive these signals?\n"
            "📩 DM now for VIP access.</blockquote>"
        )
    else:
        text = (
            "🏆 <b>VIP RESULT</b>\n\n"
            f"#{coin}USDT 🚀 <b>TP2 HIT</b> ✅\n\n"
            "<blockquote>VIP-exclusive signal closed successfully.\n\n"
            "✅ TP1 Achieved\n"
            "✅ TP2 Achieved\n\n"
            f"This setup was shared with our VIP members AT {entry_ts}\n"
            "If you're seeing this in the free channel, imagine having the trade before the move.\n\n"
            "💎 Crypto Clexer VIP\n"
            "📩 DM now for VIP access.</blockquote>"
        )
    # Thread this as a genuine reply to the locked/redacted entry post that
    # went out to Free at signal time (same reply_map + _send_plain_reply
    # mechanism send_lifecycle_reply uses for TP1/TP2/SL) — so this promo
    # visibly quotes the exact signal it's talking about, instead of landing
    # as an unrelated standalone post. Falls back to a normal post if no
    # entry message_id was captured for a given channel (e.g. old trade,
    # pre-locked-signal feature, or the capture failed).
    _reply_map = trade.get("reply_map", {})
    for cid in free_chans:
        try:
            _send_plain_reply(cid, text, reply_to=_reply_map.get(f"free:{cid}"), reply_markup=mkp)
        except Exception as e: print(f"  [FREE CATCHUP] {cid}: {e}")

def _build_recap_text(trades: list, date_str: str) -> str:
    tp2_list  = [t for t in trades if t["result"] == "TP2"]
    tp1_list  = [t for t in trades if t["result"] == "TP1"]
    sl_list   = [t for t in trades if t["result"] == "SL"]
    to_list   = [t for t in trades if t["result"] == "TIMEOUT"]
    lines = [f"📊 <b>Daily Recap — {date_str}</b>\n"]
    if tp2_list:
        lines.append("🏆 <b>TP2 Hit:</b>")
        lines += [f"✅ {t['symbol']} — {t['time']}" for t in tp2_list]
        lines.append("")
    if tp1_list:
        lines.append("💰 <b>TP1 Hit:</b>")
        lines += [f"🎯 {t['symbol']} — {t['time']}" for t in tp1_list]
        lines.append("")
    if sl_list:
        lines.append("🛑 <b>SL Hit:</b>")
        lines += [f"❌ {t['symbol']} — {t['time']}" for t in sl_list]
        lines.append("")
    if to_list:
        lines.append("⏰ <b>Timeout:</b>")
        lines += [f"➖ {t['symbol']} — {t['time']}" +
                  (f" ({t['pnl']:+.2f}%)" if t.get("pnl") is not None else "")
                  for t in to_list]
    return "\n".join(lines)

def _send_daily_summary(tracker: dict):
    """End-of-day recap for one specific day's tracker snapshot — every TP1/TP2/SL
    that actually triggered on that calendar day and was tier-routed, with premium
    emoji via forward. VIP gets every tier-routed trade (VIP-only + VIP+Free);
    Free only gets the subset that was also shown in Free — so the two recaps
    are no longer identical, and Signal-only trades never appear in either.
    Only counts trades whose TP1/TP2/SL actually fired on this tracker's own
    date — a trade opened the day before but not yet closed is simply absent
    from the trades list (never added until it actually triggers), and a trade
    that triggers just after midnight belongs to the NEXT day's tracker/recap,
    never this one."""
    date_str = tracker.get("date", "")
    all_trades = [t for t in tracker.get("trades", []) if t.get("tier_routed")]
    if not all_trades:
        return
    vip_text = _apply_premium_emojis(_build_recap_text(all_trades, date_str))
    for cid in _channels_by_tier("vip"):
        _mid = _send_via_true_forward(vip_text, cid, "daily-summary-vip")
        if not _mid:
            try:
                r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": cid, "text": vip_text, "parse_mode": "HTML"}, timeout=10)
                _mid = r.json().get("result", {}).get("message_id")
            except Exception as e: print(f"  [DAILY SUMMARY] vip {cid}: {e}")
        if isinstance(_mid, int):
            _pin_message(cid, _mid)
    free_trades = [t for t in all_trades if t.get("free_shown")]
    if free_trades:
        free_text = _apply_premium_emojis(_build_recap_text(free_trades, date_str))
        for cid in _channels_by_tier("free"):
            _mid = _send_via_true_forward(free_text, cid, "daily-summary-free")
            if not _mid:
                try:
                    r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                        json={"chat_id": cid, "text": free_text, "parse_mode": "HTML"}, timeout=10)
                    _mid = r.json().get("result", {}).get("message_id")
                except Exception as e: print(f"  [DAILY SUMMARY] free {cid}: {e}")
            if isinstance(_mid, int):
                _pin_message(cid, _mid)

def _track_daily_result(symbol: str, result: str, tier_routed: bool = False, free_shown: bool = False,
                         tp1_detail: dict = None, entry_date: str = None, sig_id: str = None, pnl: float = None):
    """Call this at every genuine TP1/TP2/SL close (result: 'TP1'/'TP2'/'SL').
    Drives the 3rd-Free-TP1-of-the-day promo and feeds the end-of-day recap.
    entry_date: the IST calendar day this TRADE was opened on (not today) —
    the result is credited to that day's bucket, so a trade opened on the
    14th that closes just after midnight on the 15th still recaps under the
    14th. Defaults to today if not given (e.g. BTC call sites not yet passing it).
    tier_routed: True if this trade was shown in VIP (and maybe Free) — only
    tier_routed trades are eligible for the recap at all (Signal-only never appears).
    free_shown: True if also visible in the Free channel — builds Free's own
    (shorter) recap and gates the 3rd-TP1 streak promo.
    tp1_detail: {'tag','side','tp1','sl_be','tp2'} for the promo's message.
    sig_id: same trade's signal id — when the trade later runs to TP2, its
    earlier TP1 recap line (same sig_id) is dropped so the recap only ever
    shows the trade's final/best result, not both TP1 and TP2 for one trade."""
    date_str = entry_date or _ist_date_str()
    bucket = _get_daily_bucket(date_str)
    key = result.lower()
    bucket[key] = bucket.get(key, 0) + 1
    if tier_routed:
        if result == "TP2" and sig_id:
            bucket["trades"] = [tr for tr in bucket["trades"]
                                 if not (tr.get("sig_id") == sig_id and tr.get("result") == "TP1")]
        bucket["trades"].append({
            "symbol": symbol, "result": result,
            "time": now_ist().strftime("%I:%M %p IST"),
            "free_shown": free_shown, "tier_routed": True, "sig_id": sig_id,
            "pnl": pnl,
        })
    if result == "TP1" and free_shown:
        bucket["free_tp1"] = bucket.get("free_tp1", 0) + 1
        if bucket["free_tp1"] == 3 and not bucket["tp1_promo_sent"]:
            bucket["tp1_promo_sent"] = True
            _send_tp1_streak_promo(symbol, tp1_detail or {})
    # TP2 congrats broadcast disabled — admin asked to stop sending it.
    _save_daily_buckets()

def _daily_summary_loop():
    """Background thread — fires the recap for the day that just ENDED, shortly
    after midnight IST, reading that day's bucket from _daily_buckets (trades
    are grouped by the day they were OPENED, so this only ever grabs trades
    that actually belong to the outgoing day). Checked every 60s for up to 10
    minutes after midnight, to give any TP1/TP2/SL that fires right at the
    rollover boundary time to land in its bucket before the recap is sent.
    Whatever hasn't landed by then is simply not included — a trade opened on
    day D that resolves after D's recap has already gone out is not recapped
    again on any later day."""
    global _daily_summary_last_sent_date
    while True:
        try:
            now = datetime.now(timezone.utc) + IST
            if now.hour == 0 and now.minute < 10:
                yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                if _daily_summary_last_sent_date != yesterday_str:
                    bucket = _daily_buckets.get(yesterday_str)
                    if bucket and bucket.get("trades"):
                        _send_daily_summary(bucket)
                        _daily_summary_last_sent_date = yesterday_str  # only lock once actually sent
                        _save_daily_buckets()
        except Exception as e:
            print(f"  [DAILY SUMMARY LOOP] {e}")
        time.sleep(60)

ACTIVE_PROFILE = "mine"   # "mine" or "coadmin" — which scan-settings snapshot is currently live
_SETTINGS_PROFILES = {"mine": {}, "coadmin": {}}  # each holds a snapshot of every setting co-admin can touch

def _snapshot_scan_settings() -> dict:
    return {
        "scan_model": SCAN_MODEL, "use_aerolink": USE_AEROLINK,
        "aicfg_grid": {k: {t: dict(v) for t, v in tiers.items()} for k, tiers in AICFG_GRID.items()},
        "zone_entry_enabled": ZONE_ENTRY_ENABLED,
        "tp1_close_pct": ct.TP1_CLOSE_PCT,
        "scan1_auto": SCAN1_AUTO_ENABLED, "scan2_auto": SCAN2_AUTO_ENABLED,
        "test_scan": TEST_SCAN_ENABLED, "btc_analysis": btc_analysis_enabled,
        "scan1_schedule": list(SCAN1_SCHEDULE), "scan2_schedule": list(SCAN2_SCHEDULE),
        "scan1_test_schedule": list(SCAN1_TEST_SCHEDULE), "scan2_test_schedule": list(SCAN2_TEST_SCHEDULE),
        "btc_ct_enabled": ct.BTC_CT_ENABLED, "scan1_ct_enabled": ct.SCAN1_CT_ENABLED,
        "scan2_ct_enabled": ct.SCAN2_CT_ENABLED,
        "demo1_ct_enabled": ct.DEMO1_CT_ENABLED, "demo2_ct_enabled": ct.DEMO2_CT_ENABLED,
    }

def _apply_aicfg_grid(saved: dict):
    """Merges a saved aicfg_grid dict into AICFG_GRID IN PLACE (no `global`
    needed — only nested values are mutated, the dict itself is never rebound)."""
    if not saved:
        return
    for kind in AICFG_GRID:
        for tier in AICFG_GRID[kind]:
            cell = (saved.get(kind) or {}).get(tier)
            if cell:
                AICFG_GRID[kind][tier]["model"] = cell.get("model", AICFG_GRID[kind][tier]["model"])
                AICFG_GRID[kind][tier]["aerolink"] = cell.get("aerolink", AICFG_GRID[kind][tier]["aerolink"])

def _apply_scan_settings(d: dict):
    global SCAN_MODEL, USE_AEROLINK, ZONE_ENTRY_ENABLED, SCAN1_AUTO_ENABLED, SCAN2_AUTO_ENABLED
    global TEST_SCAN_ENABLED, btc_analysis_enabled, SCAN1_SCHEDULE, SCAN2_SCHEDULE, SCAN1_TEST_SCHEDULE, SCAN2_TEST_SCHEDULE
    if not d:
        return  # nothing snapshotted yet for this profile — leave current values as-is
    SCAN_MODEL = d.get("scan_model", SCAN_MODEL); USE_AEROLINK = d.get("use_aerolink", USE_AEROLINK)
    _apply_aicfg_grid(d.get("aicfg_grid"))
    ZONE_ENTRY_ENABLED = d.get("zone_entry_enabled", ZONE_ENTRY_ENABLED)
    ct.TP1_CLOSE_PCT = d.get("tp1_close_pct", ct.TP1_CLOSE_PCT)
    SCAN1_AUTO_ENABLED = d.get("scan1_auto", SCAN1_AUTO_ENABLED); SCAN2_AUTO_ENABLED = d.get("scan2_auto", SCAN2_AUTO_ENABLED)
    TEST_SCAN_ENABLED = d.get("test_scan", TEST_SCAN_ENABLED); btc_analysis_enabled = d.get("btc_analysis", btc_analysis_enabled)
    SCAN1_SCHEDULE = d.get("scan1_schedule", SCAN1_SCHEDULE); SCAN2_SCHEDULE = d.get("scan2_schedule", SCAN2_SCHEDULE)
    SCAN1_TEST_SCHEDULE = d.get("scan1_test_schedule", SCAN1_TEST_SCHEDULE)
    SCAN2_TEST_SCHEDULE = d.get("scan2_test_schedule", SCAN2_TEST_SCHEDULE)
    ct.BTC_CT_ENABLED = d.get("btc_ct_enabled", ct.BTC_CT_ENABLED); ct.SCAN1_CT_ENABLED = d.get("scan1_ct_enabled", ct.SCAN1_CT_ENABLED)
    ct.SCAN2_CT_ENABLED = d.get("scan2_ct_enabled", ct.SCAN2_CT_ENABLED)
    ct.DEMO1_CT_ENABLED = d.get("demo1_ct_enabled", ct.DEMO1_CT_ENABLED); ct.DEMO2_CT_ENABLED = d.get("demo2_ct_enabled", ct.DEMO2_CT_ENABLED)
CONTACT_ADMIN_ENABLED  = True   # shows/hides the "Contact Admin" button for users — toggled via /adminlinks
SIGNAL_CHANNEL_ENABLED = True   # shows/hides the "Signal Channel" button for users — toggled via /adminlinks
SIGNAL_CHANNEL_LINK    = ""     # admin-provided channel link — set/removed via /adminlinks
last_update_id        = 0
last_force_scan_time  = 0
last_signal_scan_time = 0
last_price_check_time = 0
last_tick_time        = 0
latest_news_context: list = []
trade_lock = threading.Lock()

DATA_DIR           = os.getenv("DATA_DIR", ".")
CLEXER_API_URL     = os.getenv("CLEXER_API_URL", "").rstrip("/")
PUSH_STATE_SECRET  = os.getenv("PUSH_STATE_SECRET", "")

def _central_get(path: str, timeout: int = 8, retries: int = 3, delay: float = 2.5):
    """GET from CLEXER_API_URL with a couple of retries — several startup pulls
    firing in quick succession can hit a freshly-started Postgres before it's
    fully ready (transient 503), which would otherwise silently make a server
    start blank instead of restoring its real shared data."""
    if not CLEXER_API_URL:
        return None
    hdrs = {"X-Push-Secret": PUSH_STATE_SECRET} if PUSH_STATE_SECRET else {}
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(f"{CLEXER_API_URL}{path}", headers=hdrs, timeout=timeout)
            if r.ok:
                return r
            last_err = f"HTTP {r.status_code} — {r.text[:150]}"
            if r.status_code < 500:
                return r  # 4xx won't fix itself with a retry (e.g. bad secret)
        except Exception as e:
            last_err = str(e)
        if attempt < retries - 1:
            time.sleep(delay)
    print(f"[CENTRAL] {path} failed after {retries} attempts: {last_err}")
    return None

def _kv_pick_newer(local_path: str, kv_body: dict, log_tag: str):
    """Compare a /kv/{key} response's updated_at against local_path's mtime —
    return the central data dict only if it's actually newer (or local doesn't
    exist yet); otherwise return None so the caller falls through to the local
    file. Prevents a stale central pull from silently clobbering a local change
    made after the last /syncup."""
    if not kv_body or not kv_body.get("found"):
        print(f"[{log_tag}] Central store reachable but no data found yet")
        return None
    local_mtime = os.path.getmtime(local_path) if os.path.exists(local_path) else 0
    central_ts = 0
    ts_str = kv_body.get("updated_at")
    if ts_str:
        try:
            central_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        except Exception:
            central_ts = 0
    if central_ts >= local_mtime or not os.path.exists(local_path):
        print(f"[{log_tag}] Loaded from central store (central:{central_ts:.0f} local:{local_mtime:.0f})")
        return kv_body["data"]
    print(f"[{log_tag}] Local file is newer than central ({local_mtime:.0f} > {central_ts:.0f}) — using local")
    return None

# ── Multi-server active/standby switch ─────────────────────────────────────────
# Each Railway deployment (main, co-server1, co-server2, ...) sets its own unique
# SERVER_NAME. Only ONE name is ever the "active" one at a time — stored centrally
# via api.py/kv_store so every server agrees on it regardless of which is running.
# Standby servers keep polling/analyzing normally but never place real copytrade
# orders — flip which one is active with /server <name> from Telegram.
SERVER_NAME = os.getenv("SERVER_NAME", "main")
_active_server_cache = {"name": None, "checked_at": 0.0}

def get_active_server_name() -> str:
    """Which server name is currently flagged active. Refreshes from the central
    store at most once every 20s; if no central store is configured, this server
    is always considered active (preserves single-server behavior)."""
    if not CLEXER_API_URL:
        return SERVER_NAME
    now = time.time()
    if now - _active_server_cache["checked_at"] < 20 and _active_server_cache["name"]:
        return _active_server_cache["name"]
    try:
        hdrs = {"X-Push-Secret": PUSH_STATE_SECRET} if PUSH_STATE_SECRET else {}
        r = requests.get(f"{CLEXER_API_URL}/kv/active_server", headers=hdrs, timeout=8)
        if r.ok:
            body = r.json()
            name = (body.get("data") or {}).get("name") if body.get("found") else None
            if name:
                _active_server_cache["name"] = name
                _active_server_cache["checked_at"] = now
                return name
        else:
            print(f"[SERVER] active-check HTTP {r.status_code} — {r.text[:150]}")
    except Exception as e:
        print(f"[SERVER] active-check error: {e}")
    # Unreachable/never-set — fall back to whatever we last knew. If nothing was
    # ever known, "main" is the safe grandfather default (the original, only-ever
    # server before multi-server existed) — any OTHER named server (co1, co2, ...)
    # must NEVER default to assuming itself active, or two servers could both
    # decide independently that they're the one allowed to poll Telegram / trade.
    return _active_server_cache["name"] or "main"

def is_active_server() -> bool:
    return get_active_server_name() == SERVER_NAME

def _kv_push(key: str, data) -> bool:
    """Push any JSON-able blob to the shared store under `key`. Used by /syncup
    for the pieces that don't already have their own dedicated push path.
    Returns True only if the server actually accepted it (2xx) — a wrong/missing
    PUSH_STATE_SECRET returns 403, which must NOT be reported as success."""
    if not CLEXER_API_URL:
        return False
    hdrs = {"X-Push-Secret": PUSH_STATE_SECRET} if PUSH_STATE_SECRET else {}
    r = requests.post(f"{CLEXER_API_URL}/kv/{key}", json=data, headers=hdrs, timeout=15)
    if not r.ok:
        raise Exception(f"HTTP {r.status_code} — {r.text[:150]}")
    return True

def set_active_server(name: str):
    _active_server_cache["name"] = name
    _active_server_cache["checked_at"] = time.time()
    if CLEXER_API_URL:
        try:
            hdrs = {"X-Push-Secret": PUSH_STATE_SECRET} if PUSH_STATE_SECRET else {}
            requests.post(f"{CLEXER_API_URL}/kv/active_server", json={"name": name}, headers=hdrs, timeout=8)
        except Exception as e:
            print(f"[SERVER] set-active error: {e}")
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
        d = None
        r = _central_get("/kv/registered_users")
        if r is not None and r.ok:
            d = _kv_pick_newer(USER_DB_FILE, r.json(), "USERS")
        if d is None and os.path.exists(USER_DB_FILE):
            with open(USER_DB_FILE, "r") as f:
                d = json.load(f)
        if d is not None:
            if isinstance(d, list):
                registered_users = set(int(x) for x in d)  # legacy format — just a list of ids
            else:
                registered_users = set(int(x) for x in d.get("users", []))
                user_usernames   = {str(k): v for k, v in d.get("usernames", {}).items()}
                blocked_users    = set(int(x) for x in d.get("blocked", []))
    except Exception as e:
        print(f"[USERS] Load error: {e}"); registered_users = set()

def save_users():
    _blob = {
        "users": list(registered_users),
        "usernames": user_usernames,
        "blocked": list(blocked_users),
    }
    try:
        with open(USER_DB_FILE, "w") as f:
            json.dump(_blob, f)
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
    is_new = chat_id not in registered_users
    if username and user_usernames.get(str(chat_id)) != username:
        user_usernames[str(chat_id)] = username; changed = True
    if chat_id in blocked_users:
        blocked_users.discard(chat_id); changed = True  # they messaged us — clearly not blocked
    if is_new:
        registered_users.add(chat_id); changed = True
    if changed:
        save_users()
        if is_new:
            # A brand-new user must never depend on someone remembering to run
            # /syncup — push immediately so a server switch can never silently
            # lose them, even if nothing else gets manually synced that day.
            _kv_push("registered_users", {
                "users": list(registered_users),
                "usernames": user_usernames,
                "blocked": list(blocked_users),
            })

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

# ─── /chat — free-form AI chat session (Gemini free tier), open to every user.
# _chat_sessions: cid str -> {"last": epoch, "history": [{"role","parts":[{"text":...}]}]}
# A session starts with /chat, then every non-command message from that user
# routes to the AI until 5 minutes pass with no message, at which point the
# sweep loop auto-closes it and notifies the user. ─────────────────────────
_chat_sessions: dict = {}
_CHAT_TIMEOUT_SEC = 300
_CHAT_HISTORY_MAX_TURNS = 12   # user+model pairs kept per session, to bound token usage
_CHAT_IMAGE_HINTS = ("generate an image","generate image","draw","create an image","create a picture",
                     "make an image","make a picture","picture","pic","photo","pfp","image","img",
                     "wallpaper","artwork","drawing","paint","illustrate","sketch")

def _gemini_headers():
    return {"Content-Type": "application/json"}

def _chat_is_image_request(text: str) -> bool:
    t = text.lower()
    return any(h in t for h in _CHAT_IMAGE_HINTS)

_CHAT_TEXT_MODEL = "gemini-3.5-flash"  # newest/smartest model with free quota on this account — only 5 RPM / 20 RPD though (vs 3.1-flash-lite's 500 RPD)

# Shared by every /chat text engine (Gemini + Claude) so switching engines via
# /model doesn't change tone/formatting rules — only which model answers.
_CHAT_SYSTEM_PROMPT = (
    "You are a helpful, friendly assistant inside a Telegram bot, chatting in an ongoing "
    "conversation. Read the actual message, not just the topic of earlier messages — "
    "each reply must directly answer or respond to what THIS specific message says.\n\n"
    "CONVERSATION AWARENESS — critical:\n"
    "- If the user's message is a complaint, venting, a reaction to your own last reply "
    "(e.g. \"that reply was bad\", \"you didn't understand\", \"why so robotic\"), an opinion, "
    "a short reaction, or small talk — do NOT treat it as a new factual question. Respond "
    "naturally and briefly like a person would: acknowledge it, ask what they'd actually "
    "like, or adjust — never generate an unrelated new table/topic in response to a complaint.\n"
    "- Match the user's own language and tone. If they write in Hindi/Hinglish, reply in "
    "Hindi/Hinglish naturally — don't force English structure onto a casual message.\n"
    "- Most replies in a real conversation are short and plain. Do not manufacture structure "
    "(tables, multi-point breakdowns) where the message doesn't call for it.\n\n"
    "FORMATTING:\n"
    "- Never use Markdown syntax (no **bold**, no ### headers, no - or * bullets). "
    "Telegram does not render Markdown here, so raw asterisks/hashes show up as literal "
    "garbage characters.\n"
    "- Use Telegram HTML tags only: <b>bold</b> and <i>italic</i>. No other tags.\n"
    "- Only reach for the 'Table Summary' format when the user is genuinely asking for a "
    "comparison, multi-step breakdown, or list of options with more than one fact per item — "
    "not for every reply. Format: one short header line (with emoji), a blank line, an "
    "aligned table in a <pre></pre> block, optionally one italic note line. Example:\n\n"
    "📍 <b>Cuttack to Jaipur — ~1,700 km</b>\n\n"
    "<pre>Mode      Distance    Time\n"
    "✈️ Air     ~1,700 km   5–9 hrs\n"
    "🚂 Train   ~1,730 km   28–36 hrs\n"
    "🚗 Road    ~1,700 km   30–35 hrs</pre>\n\n"
    "<i>No direct flights — connects via Delhi/Mumbai/Kolkata</i>\n\n"
    "- For anything else — direct questions, opinions, casual chat, complaints, single facts "
    "— just reply in plain text/HTML, no table, no forced structure.\n"
    "- Pick emoji that fit the topic naturally; don't add emoji-heavy tables to plain chat.\n"
    "- Only add the \"educational only, not financial advice\" disclaimer when you actually "
    "gave trading/investment advice or a prediction — never on unrelated questions (tech, "
    "AI models, general knowledge, etc.), and never more than once every few messages in a row."
)

def _chat_call_gemini_text(history: list) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{_CHAT_TEXT_MODEL}:generateContent?key={GEMINI_API_KEY}"
    body = {
        "contents": history,
        "systemInstruction": {"parts": [{"text": _CHAT_SYSTEM_PROMPT}]},
    }
    r = requests.post(url, headers=_gemini_headers(), json=body, timeout=30)
    if not r.ok:
        raise Exception(f"{r.status_code} {r.reason} — {r.text[:500]}")
    d = r.json()
    parts = d.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    return "".join(p.get("text","") for p in parts).strip() or "…"

def _chat_call_claude_text(history: list, model_id: str) -> str:
    """Same conversation history format as Gemini (role 'user'/'model', one
    text part each) — translated to Claude's 'user'/'assistant' shape so
    /model can swap engines without touching how history is stored."""
    messages = [{"role": "assistant" if h["role"] == "model" else "user",
                 "content": h["parts"][0]["text"]} for h in history]
    client = _claude_client("chat")
    resp = client.messages.create(
        model=model_id,
        max_tokens=2000,
        system=_CHAT_SYSTEM_PROMPT,
        thinking={"type": "adaptive"},
        output_config={"effort": "medium"},
        messages=messages,
    )
    return _claude_text(resp) or "…"

def _chat_generate_image(prompt: str):
    """Free, no-API-key image generation via Pollinations.ai (image.pollinations.ai) —
    every Gemini image model on this account turned out to be billing-gated, so this
    swaps to a genuinely free provider instead. GET request, image bytes come back
    directly in the response body (no JSON wrapper). Returns (text, image_bytes_or_None)."""
    import urllib.parse
    url = "https://image.pollinations.ai/prompt/" + urllib.parse.quote(prompt, safe="")
    r = requests.get(url, params={"width": 1024, "height": 1024, "nologo": "true"}, timeout=60)
    if not r.ok or not r.content:
        raise Exception(f"{r.status_code} {r.reason}")
    return "", r.content

def _handle_chat_message(cid, text: str):
    sess = _chat_sessions.get(str(cid))
    if not sess:
        return
    sess["last"] = time.time()
    _is_image = _chat_is_image_request(text)
    if not _is_image and CHAT_MODEL == "google" and not GEMINI_API_KEY:
        send_reply(cid, "⚠️ Chat AI isn't configured yet — admin needs to set GEMINI_API_KEY.")
        return
    try:
        if _is_image:
            send_reply(cid, "🎨 Generating image…")
            reply_text, img_bytes = _chat_generate_image(text)
            if img_bytes:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                    data={"chat_id": cid, "caption": reply_text[:1024] if reply_text else ""},
                    files={"photo": ("image.png", img_bytes, "image/png")}, timeout=30)
            else:
                send_reply(cid, reply_text or "⚠️ Couldn't generate that image, try rephrasing.")
            sess["history"].append({"role": "user", "parts": [{"text": text}]})
            sess["history"].append({"role": "model", "parts": [{"text": reply_text or "[sent an image]"}]})
        else:
            sess["history"].append({"role": "user", "parts": [{"text": text}]})
            if CHAT_MODEL in _CHAT_MODEL_IDS:
                reply_text = _chat_call_claude_text(sess["history"], _CHAT_MODEL_IDS[CHAT_MODEL])
            else:
                reply_text = _chat_call_gemini_text(sess["history"])
            sess["history"].append({"role": "model", "parts": [{"text": reply_text}]})
            send_reply(cid, reply_text)
        # Trim history to bound token usage
        max_msgs = _CHAT_HISTORY_MAX_TURNS * 2
        if len(sess["history"]) > max_msgs:
            sess["history"] = sess["history"][-max_msgs:]
    except Exception as e:
        print(f"  [CHAT] {cid}: {e}")
        if "429" in str(e):
            send_reply(cid, "⚠️ Chat AI hit today's free-tier limit (this model only allows a small number of replies per day). Try again later, or ask admin to switch to a higher-quota model.")
        else:
            send_reply(cid, "⚠️ Chat AI had an error — try again.")

def _chat_session_sweep_loop():
    """Background thread — closes any chat session idle for 5+ minutes and notifies the user."""
    while True:
        try:
            now = time.time()
            for cid_str in list(_chat_sessions.keys()):
                if now - _chat_sessions[cid_str]["last"] > _CHAT_TIMEOUT_SEC:
                    del _chat_sessions[cid_str]
                    try:
                        send_reply(int(cid_str), "💬 Chat session closed (5 min of inactivity).")
                    except Exception as e:
                        print(f"  [CHAT SWEEP] notify {cid_str}: {e}")
        except Exception as e:
            print(f"  [CHAT SWEEP] {e}")
        time.sleep(20)
_last_help_msg: dict = {}  # cid → message_id of last /help message (for dedup/cleanup)
_tp_state: dict = {}       # cid → {"target": "scan1"/"scan2"/"demo", "digits": [], "times": [(h,m),...], "msg_id": int}
_pending_confirm: dict = {}  # cid → {"action": str, "label": str, "back_cb": str} — awaiting Yes/Cancel on a destructive action
_np_state: dict = {}       # cid → {"target": "setsize"/"setleverage"/"setrisk", "digits": str, "back_cb": str}
_NP_CONFIG = {
    "setsize":     {"label": "Margin Per Trade",       "unit": "USDT", "cmd": "/setsize",     "decimals": True},
    "setleverage": {"label": "Leverage",                "unit": "x",    "cmd": "/setleverage", "decimals": False},
    "setrisk":     {"label": "Auto-Risk (Max Loss)",    "unit": "USDT", "cmd": "/setrisk",     "decimals": True},
    "tp1size":     {"label": "TP1 Close %",             "unit": "%",    "cmd": "/tp1size",     "decimals": False},
    "freelimit":   {"label": "Free Channel Share %", "unit": "%", "cmd": "/freelimit", "decimals": False},
    "wrscan1":     {"label": "Scan1 Win Rate Target",  "unit": "%", "cmd": "/wrscan1", "decimals": False},
    "wrscan2":     {"label": "Scan2 Win Rate Target",  "unit": "%", "cmd": "/wrscan2", "decimals": False},
    "wrts1":       {"label": "TS1 Win Rate Target",    "unit": "%", "cmd": "/wrts1",   "decimals": False},
    "wrts2":       {"label": "TS2 Win Rate Target",    "unit": "%", "cmd": "/wrts2",   "decimals": False},
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

def _log_api_usage(call_type: str, model: str, input_tokens: int, output_tokens: int, gateway: str = "Direct"):
    """Log every Claude API call with token count, cost, and gateway (Direct/Aerolink) to CSV."""
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
    headers = ["date","time","call_type","model","gateway","input_tokens","output_tokens","cost_usd"]
    row = [date_str, time_str, call_type, model, gateway, input_tokens, output_tokens, f"{cost:.6f}"]
    _pull_csv_central("api_cost_log_csv", API_COST_LOG)
    write_header = not os.path.exists(API_COST_LOG)
    try:
        if not write_header:
            # Migrate old rows (written before the gateway column existed) in place.
            with open(API_COST_LOG, "r", newline="", encoding="utf-8") as rf:
                first_line = rf.readline()
            if first_line and "gateway" not in first_line:
                with open(API_COST_LOG, "r", newline="", encoding="utf-8") as rf:
                    old_rows = list(csv.DictReader(rf))
                with open(API_COST_LOG, "w", newline="", encoding="utf-8") as wf:
                    dw = csv.DictWriter(wf, fieldnames=headers)
                    dw.writeheader()
                    for r in old_rows:
                        dw.writerow({h: r.get(h, "") for h in headers})
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

# Specific scheduled slots that always run on Direct + Opus 4.8 and post to
# VIP/Free ("special"); every other auto-scheduled slot ("regular") still forces
# Opus 4.8 but routes through Aerolink instead, and stays Signal-channel only.
# Manual (non-scheduled) triggers like typing /scan1 keep using the admin's
# normal /aiconfig setting untouched. Set right before the auto-trigger fires
# and cleared right after that scan cycle finishes (see _scan_run_mode below).
_SCAN_SPECIAL = {
    "scan1": {(3,2), (5,23), (9,2), (16,15), (12,2), (23,23), (7,23)},
    "scan2": {(2,23), (11,27), (12,3), (13,7), (5,23), (5,28), (6,23), (9,27), (10,7), (7,7)},
    # test1 (TS1) and test2 (TS2) start out as copies of the old shared "test"
    # set — now fully independent, each can be promoted/demoted on its own.
    "test1": {(3,24), (8,24), (15,24), (17,10), (0,0), (0,9), (0,27), (1,9), (2,24), (4,3), (8,9), (12,24), (16,8), (18,27), (21,27), (22,27)},
    "test2": {(3,24), (8,24), (15,24), (17,10), (0,0), (0,9), (0,27), (1,9), (2,24), (4,3), (8,9), (12,24), (16,8), (18,27), (21,27), (22,27)},
}
# Newly-added special times that aren't verified yet — they still post to
# VIP/Free like any special time, but copytrade must NEVER auto-execute real
# orders on them until admin has watched enough live results to trust them.
# Move a time out of here (once proven) to let copytrade start using it.
_SCAN_SPECIAL_NO_COPY = {
    "scan1": {(9,2), (16,15)},
    "scan2": {(2,23), (5,23), (5,28), (9,27), (10,7), (11,27), (13,7)},
    "test1": {(0,0), (0,9), (1,9), (2,24), (4,3), (8,9), (12,24), (16,8), (18,27), (21,27), (22,27)},
    "test2": {(0,0), (0,9), (1,9), (2,24), (4,3), (8,9), (12,24), (16,8), (18,27), (21,27), (22,27)},
}
_scan_run_mode = {"scan1": None, "scan2": None, "test1": None, "test2": None}  # None | "special" | "regular"
_scan_trigger_hm = {"scan1": None, "scan2": None, "test1": None, "test2": None}  # exact (hour,min) that triggered this run — used to check _SCAN_SPECIAL_NO_COPY

# ─── Auto promote/demote special times by live win rate ────────────────────
# Per-slot (kind, hour:minute) outcome tracker that automatically:
#   1. Promotes a REGULAR (non-special) time to special+verified once it has
#      proven itself: win% >= threshold AND at least 4 wins banked.
#   2. Demotes a VERIFIED special time to unverified (no-copy) if its win%
#      drops below threshold — protects real copytrade money automatically.
#   3. Re-promotes an UNVERIFIED special time back to verified once win% is
#      back above threshold AND it has strung together >=2 wins in a row
#      since its last real SL (a "clean streak", not just an overall average).
# Thresholds: 41% for scan1/scan2, 35% for demo1/demo2. TS1 and TS2 now run
# fully independent schedules and win-rate tracking — a promotion/demotion on
# one never affects the other (see _SLOT_SCHEDULE_KIND below).
# A win = TP2, BE (SL after TP1 already hit — that trade already banked TP1,
# so it's a win not a loss), or a positive-P/L timeout. A loss = a real SL
# (never hit TP1), LOSS, or a negative-P/L timeout. TP1 alone is never a
# terminal state in this bot (the runner keeps riding to TP2/BE/timeout), so
# it's not tracked as its own event — only these 4 terminal outcomes are.
_SLOT_EVAL_THRESHOLD = {"scan1": 55, "scan2": 55, "demo1": 50, "demo2": 50}
_SLOT_MIN_WINS_FOR_NEW_PROMOTION = 4
_SLOT_MIN_STREAK_FOR_REVERIFY = 2
# demo1/demo2 each map to their own independent schedule kind (test1/test2).
_SLOT_SCHEDULE_KIND = {"scan1": "scan1", "scan2": "scan2", "demo1": "test1", "demo2": "test2"}
_SLOT_STATE_FILE = os.path.join(DATA_DIR, "slot_auto_state.json")
_slot_stats: dict = {}  # "kind|H.M" -> {"tp": int, "sl": int, "streak": int}

def _slot_key(kind: str, hm: tuple) -> str:
    return f"{kind}|{hm[0]}.{hm[1]:02d}"

# ─── Auto-blacklist & relocate a time that's proven itself bad ──────────────
# Applies to EVERY slot regardless of trust level (verified/unverified/normal
# testing). Trigger: losses >= 3 AND losses >= 3x wins (i.e. sl >= max(3, 3*tp))
# — covers 1/3, 1/4, 1/5... (any 1-win slot once it racks up 3+ losses), 0/3,
# 0/4, 0/5... (zero wins, 3+ losses), and exact 1:3 multiples like 2/6, 3/9.
# Explicitly excludes 0/2 and similar too-early cases — needs at least 3
# losses banked before it's allowed to fire at all. Once triggered, the slot
# is permanently retired at that exact clock time (never tested again, even
# after a redeploy) and the search hops forward for a fresh nearby minute to
# test instead, starting completely clean (0/0, no inherited history, no
# inherited trust). Each kind (scan1/scan2/demo1/demo2) keeps its own
# independent blacklist and relocation set — never shared across kinds, same
# as everything else in this system.
_KIND_GRID_MINUTES = {"scan1": (2, 23), "scan2": (7, 27), "demo1": (9, 27), "demo2": (9, 27)}
_SLOT_BLACKLIST: dict = {"scan1": set(), "scan2": set(), "demo1": set(), "demo2": set()}
_SLOT_RELOCATED: dict = {"scan1": set(), "scan2": set(), "demo1": set(), "demo2": set()}

def _slot_hits_1_3(tp: int, sl: int) -> bool:
    return sl >= 3 and sl >= tp * 3

def _kind_active_times(kind: str) -> set:
    """Every (h,m) currently occupied for this kind — special (verified or
    not), the fixed regular grid, and anything already relocated here from a
    previous hop. Used to know what's "taken" when looking for a hop target."""
    sched_kind = _SLOT_SCHEDULE_KIND[kind]
    ma, mb = _KIND_GRID_MINUTES[kind]
    special = _SCAN_SPECIAL.get(sched_kind, set())
    return special | _regular_grid(ma, mb, special) | _SLOT_RELOCATED.get(kind, set())

def _find_hop_target(kind: str, hm: tuple):
    """Finds the next free, never-blacklisted minute in the SAME hour for this
    kind — tries +4 repeatedly first; if that whole hour is exhausted, falls
    back to ONE +2 step from the original failed minute and resumes +4
    stepping from there (the +2 step is just a one-time unstick move, not a
    new permanent step size — a slot that lands via it and later fails again
    still hops by +4 like normal). Never rolls into the next hour. Returns
    None if nothing is free anywhere in the hour."""
    h, m = hm
    occupied = _kind_active_times(kind)
    blacklist = _SLOT_BLACKLIST.get(kind, set())

    def _free(cand_m):
        return 0 <= cand_m <= 59 and (h, cand_m) not in occupied and (h, cand_m) not in blacklist

    cand_m = m
    while cand_m + 4 <= 59:
        cand_m += 4
        if _free(cand_m):
            return (h, cand_m)

    cand_m = m + 2
    if cand_m > 59:
        return None
    if _free(cand_m):
        return (h, cand_m)
    while cand_m + 4 <= 59:
        cand_m += 4
        if _free(cand_m):
            return (h, cand_m)
    return None

def _check_slot_blacklist(kind: str, hm: tuple) -> bool:
    """Call right after a slot's stats update, before the normal promote/
    demote evaluation. If this slot just crossed the losing-ratio bar (see
    _slot_hits_1_3), retires it permanently (blacklisted, removed from
    special/unverified if it was there) and relocates to a fresh minute if
    one's free. Returns True if it fired, so the caller skips the normal
    promote/demote check for this (now-retired) time this cycle."""
    key = _slot_key(kind, hm)
    st = _slot_stats.get(key)
    if not st or not _slot_hits_1_3(st.get("tp", 0), st.get("sl", 0)):
        return False
    if hm in _SLOT_BLACKLIST.get(kind, set()):
        return False  # already retired earlier — nothing new to do
    sched_kind = _SLOT_SCHEDULE_KIND[kind]
    _SLOT_BLACKLIST.setdefault(kind, set()).add(hm)
    _SCAN_SPECIAL.get(sched_kind, set()).discard(hm)
    _SCAN_SPECIAL_NO_COPY.get(sched_kind, set()).discard(hm)
    hm_str = f"{hm[0]}:{hm[1]:02d}"
    target = _find_hop_target(kind, hm)
    if target:
        _SLOT_RELOCATED.setdefault(kind, set()).add(target)
        t_str = f"{target[0]}:{target[1]:02d}"
        send_admin(f"🚫 <b>Auto-blacklisted</b> {kind} {hm_str}\n\n"
                   f"Hit a losing ratio ({st['tp']}tp/{st['sl']}sl) — retired permanently.\n"
                   f"Now testing fresh at <b>{t_str}</b> instead (0/0, unverified).", pin=True)
    else:
        send_admin(f"🚫 <b>Auto-blacklisted</b> {kind} {hm_str}\n\n"
                   f"Hit a losing ratio ({st['tp']}tp/{st['sl']}sl) — retired permanently.\n"
                   f"No free minute left in that hour — not replaced.", pin=True)
    _rebuild_schedules()
    _save_slot_state()
    return True

def _load_slot_state():
    """Restores _slot_stats AND any runtime-promoted/demoted _SCAN_SPECIAL /
    _SCAN_SPECIAL_NO_COPY changes — checks the central store first (survives
    redeploys even if local disk doesn't), falls back to local disk."""
    global _slot_stats
    try:
        d = None
        if CLEXER_API_URL:
            r = _central_get("/kv/slot_auto_state")
            if r is not None and r.ok:
                d = _kv_pick_newer(_SLOT_STATE_FILE, r.json(), "SLOT AUTO")
        if d is None and os.path.exists(_SLOT_STATE_FILE):
            with open(_SLOT_STATE_FILE) as f:
                d = json.load(f)
        if d is None:
            return
        _slot_stats = d.get("stats", {})
        # FULL REPLACE, not update() — _save_slot_state() always writes the
        # complete current set for every kind, so the saved list is the
        # authoritative state. update() would only ever ADD times back in
        # (from the hardcoded module-level defaults on a fresh restart) and
        # could never express a REMOVAL — e.g. a reverify that discards a
        # time from no_copy would silently revert to locked/unverified on
        # every redeploy, since the hardcoded default still had it.
        for kind, times in d.get("special", {}).items():
            _SCAN_SPECIAL[kind] = set(tuple(hm) for hm in times)
        for kind, times in d.get("no_copy", {}).items():
            _SCAN_SPECIAL_NO_COPY[kind] = set(tuple(hm) for hm in times)
        for kind, times in d.get("blacklist", {}).items():
            _SLOT_BLACKLIST[kind] = set(tuple(hm) for hm in times)
        for kind, times in d.get("relocated", {}).items():
            _SLOT_RELOCATED[kind] = set(tuple(hm) for hm in times)
        print(f"[SLOT AUTO] Loaded {len(_slot_stats)} tracked slots")
    except Exception as e:
        print(f"[SLOT AUTO] load error: {e}")

def _save_slot_state():
    d = {
        "stats": _slot_stats,
        "special": {k: sorted(list(v)) for k, v in _SCAN_SPECIAL.items()},
        "no_copy": {k: sorted(list(v)) for k, v in _SCAN_SPECIAL_NO_COPY.items()},
        "blacklist": {k: sorted(list(v)) for k, v in _SLOT_BLACKLIST.items()},
        "relocated": {k: sorted(list(v)) for k, v in _SLOT_RELOCATED.items()},
    }
    try:
        with open(_SLOT_STATE_FILE, "w") as f:
            json.dump(d, f)
    except Exception as e:
        print(f"[SLOT AUTO] local save error: {e}")
    # Active-server gate — see save_settings()'s comment. This one matters most:
    # _SCAN_SPECIAL / _SCAN_SPECIAL_NO_COPY directly control whether a slot
    # auto-executes real copytrade orders, so a stale push from an abandoned
    # server could silently re-verify or de-verify a slot behind the admin's back.
    if CLEXER_API_URL and is_active_server():
        try:
            _kv_push("slot_auto_state", d)
        except Exception as e:
            print(f"[SLOT AUTO] central push error: {e}")

def _rebuild_schedules():
    global SCAN1_SCHEDULE, SCAN2_SCHEDULE, SCAN1_TEST_SCHEDULE, SCAN2_TEST_SCHEDULE
    # Each kind's real schedule = special times + its fixed regular grid +
    # anything relocated onto a fresh minute after a 1:3 blacklist — minus
    # whatever's currently blacklisted (a fixed-grid time doesn't stop being
    # generated by _regular_grid just because it got blacklisted, so it has
    # to be explicitly subtracted here or it'd keep firing forever).
    SCAN1_SCHEDULE = sorted((_SCAN_SPECIAL["scan1"] | _regular_grid(2, 23, _SCAN_SPECIAL["scan1"])
                              | _SLOT_RELOCATED["scan1"]) - _SLOT_BLACKLIST["scan1"])
    SCAN2_SCHEDULE = sorted((_SCAN_SPECIAL["scan2"] | _regular_grid(7, 27, _SCAN_SPECIAL["scan2"])
                              | _SLOT_RELOCATED["scan2"]) - _SLOT_BLACKLIST["scan2"])
    SCAN1_TEST_SCHEDULE = sorted((_SCAN_SPECIAL["test1"] | _regular_grid(9, 27, _SCAN_SPECIAL["test1"])
                                   | _SLOT_RELOCATED["demo1"]) - _SLOT_BLACKLIST["demo1"])
    SCAN2_TEST_SCHEDULE = sorted((_SCAN_SPECIAL["test2"] | _regular_grid(9, 27, _SCAN_SPECIAL["test2"])
                                   | _SLOT_RELOCATED["demo2"]) - _SLOT_BLACKLIST["demo2"])

def _evaluate_slot(kind: str, hm: tuple):
    key = _slot_key(kind, hm)
    st = _slot_stats.get(key)
    if not st:
        return
    total = st["tp"] + st["sl"]
    if total == 0:
        return
    if kind not in _SLOT_EVAL_THRESHOLD:
        return  # stale legacy key (e.g. "test" from before the demo1/demo2 split) — nothing to evaluate
    win_pct = st["tp"] / total * 100
    threshold = _SLOT_EVAL_THRESHOLD[kind]
    sched_kind = _SLOT_SCHEDULE_KIND[kind]   # demo1->test1, demo2->test2 — each independent
    is_special = hm in _SCAN_SPECIAL.get(sched_kind, set())
    is_unverified = hm in _SCAN_SPECIAL_NO_COPY.get(sched_kind, set())
    changed = False
    hm_str = f"{hm[0]}:{hm[1]:02d}"

    if not is_special:
        if win_pct >= threshold and st["tp"] >= _SLOT_MIN_WINS_FOR_NEW_PROMOTION:
            _SCAN_SPECIAL.setdefault(sched_kind, set()).add(hm)
            changed = True
            send_admin(f"⭐ <b>Auto-promoted</b> {kind} {hm_str} → SPECIAL + VERIFIED\n\n"
                       f"{win_pct:.1f}% win rate ({st['tp']}tp/{st['sl']}sl) — copytrade now enabled here.", pin=True)
    elif is_special and not is_unverified:
        if win_pct < threshold:
            _SCAN_SPECIAL_NO_COPY.setdefault(sched_kind, set()).add(hm)
            changed = True
            send_admin(f"⚠️ <b>Auto-demoted</b> {kind} {hm_str} → UNVERIFIED\n\n"
                       f"Win rate dropped to {win_pct:.1f}% ({st['tp']}tp/{st['sl']}sl) — copytrade paused here until it recovers.", pin=True)
    elif is_unverified:
        if win_pct >= threshold and st.get("streak", 0) >= _SLOT_MIN_STREAK_FOR_REVERIFY:
            _SCAN_SPECIAL_NO_COPY[sched_kind].discard(hm)
            changed = True
            send_admin(f"✅ <b>Auto-reverified</b> {kind} {hm_str} → VERIFIED\n\n"
                       f"{win_pct:.1f}% win rate, {st['streak']} clean wins in a row — copytrade resumed here.", pin=True)

    if changed:
        _rebuild_schedules()
        _save_slot_state()

def _slot_track(kind: str, hm: tuple, is_win: bool):
    """Call once per trade, at its terminal outcome only (TP2 / BE / real-SL /
    LOSS / timeout) — never at TP1 (not terminal here, the runner keeps going)."""
    key = _slot_key(kind, hm)
    st = _slot_stats.setdefault(key, {"tp": 0, "sl": 0, "streak": 0})
    if is_win:
        st["tp"] += 1
        st["streak"] += 1
    else:
        st["sl"] += 1
        st["streak"] = 0
    _save_slot_state()
    if not _check_slot_blacklist(kind, hm):
        _evaluate_slot(kind, hm)

def _ist_hm_from_epoch(epoch):
    if not epoch:
        return None
    try:
        dt = datetime.fromtimestamp(epoch, timezone.utc) + IST
        return (dt.hour, dt.minute)
    except Exception:
        return None

def _status_trade_cat(kind: str, created_at) -> str:
    """Classifies a scan/demo trade as 'verified' (special + copy-enabled),
    'unverified' (special but auto-demoted), or 'nonspecial' — shared by
    /status and /trade to gate which viewer tier can see it, and by admin's
    view to tag which category a trade belongs to."""
    _hm = _ist_hm_from_epoch(created_at)
    if not _hm:
        return "nonspecial"
    _sched_kind = _SLOT_SCHEDULE_KIND.get(kind, kind)
    if _hm in _SCAN_SPECIAL_NO_COPY.get(_sched_kind, set()):
        return "unverified"
    if _hm in _SCAN_SPECIAL.get(_sched_kind, set()):
        return "verified"
    return "nonspecial"

_CAT_TAG = {"verified": "⭐", "unverified": "⚠️", "nonspecial": "➖"}

def _trade_reveal(cat: str, share_free: bool, tier_routed: bool, viewer_tier: str, full_view: bool):
    """Decides whether a scan/demo trade should be fully revealed, shown as
    a locked VIP tag, or hidden entirely for a given viewer — the same rule
    /status and /trade both apply. Returns (reveal: bool, show_locked_tag: bool)."""
    if full_view:
        return True, False
    if viewer_tier == "vip":
        return (cat == "verified"), False
    # free / unregistered — tier_routed required too, so a stray share_free=True
    # on a never-VIP-routed (Signal-only) trade can never leak it to Free.
    if tier_routed and share_free:
        return True, False
    return False, tier_routed  # locked VIP tag only if it was ever routed to VIP

_load_slot_state()

# Self-heal on startup: a promote/demote/reverify only ever fires live, right
# when a trade closes at that exact slot — if a redeploy happened between an
# evaluation and the next trade at that same time, the loaded state could be
# stale relative to what the numbers actually say (this is what silently
# reverted this morning's Scan1 9:02 reverify back to locked). Re-running the
# check for every tracked slot here corrects any such staleness immediately
# on every restart, instead of waiting for the next trade at that exact slot.
for _sk, _st in list(_slot_stats.items()):
    try:
        _k, _hm_str = _sk.split("|", 1)
        _h, _m = _hm_str.split(".")
        _evaluate_slot(_k, (int(_h), int(_m)))
    except Exception as _e:
        print(f"[SLOT AUTO] startup re-evaluate error for {_sk}: {_e}")

# Self-heal #2: a slot sitting in _SCAN_SPECIAL (verified) with ZERO recorded
# trade history has no track record to justify auto-executing real copytrade
# orders on it — _evaluate_slot's win-rate gate only ever fires after a trade
# closes, so a slot that was seeded directly into _SCAN_SPECIAL (e.g. the
# original test1/test2 list) rather than earning its way in via real results
# can sit there permanently "verified" with 0/0 shown next to it. Demote any
# such slot to unverified here — it stays that way until it banks a real
# TP/SL and earns verification the normal way.
_SLOT_SCHED_TO_KIND = {"scan1": "scan1", "scan2": "scan2", "test1": "demo1", "test2": "demo2"}
_untested_demoted = False
for _sched_kind, _hm_set in _SCAN_SPECIAL.items():
    _stat_kind = _SLOT_SCHED_TO_KIND.get(_sched_kind)
    if not _stat_kind:
        continue
    for _hm in list(_hm_set):
        if _hm in _SCAN_SPECIAL_NO_COPY.get(_sched_kind, set()):
            continue  # already unverified
        _st2 = _slot_stats.get(_slot_key(_stat_kind, _hm))
        if not _st2 or (_st2.get("tp", 0) + _st2.get("sl", 0)) == 0:
            _SCAN_SPECIAL_NO_COPY.setdefault(_sched_kind, set()).add(_hm)
            _untested_demoted = True
            print(f"[SLOT AUTO] {_stat_kind} {_hm[0]}:{_hm[1]:02d} has no trade history — marked unverified until it earns its first result")
if _untested_demoted:
    _save_slot_state()

_load_daily_buckets()
_load_free_tracker()

def _ai_sched_kind(kind: str = "btc", scan_ver: int = None):
    """Maps a scan kind (+ scan_ver for TS1/TS2) to its AICFG_GRID row key
    (scan1/scan2/test1/test2), or None for btc/chat (not part of the grid —
    those use SCAN_MODEL/USE_AEROLINK directly, always "verified")."""
    if kind in ("btc", "chat"):
        return None
    return f"test{scan_ver}" if kind == "test" else kind

def _ai_category(kind: str = "btc", scan_ver: int = None) -> str:
    """Maps a scan kind (+ scan_ver for TS1/TS2) to its verified/unverified/
    nonspecial classification for that run — this is the column /aiconfig's
    grid is keyed by. BTC has no schedule-slot concept and is always verified."""
    sched_kind = _ai_sched_kind(kind, scan_ver)
    if sched_kind is None:
        return "verified"
    if _scan_run_mode.get(sched_kind) != "special":
        return "nonspecial"
    _hm = _scan_trigger_hm.get(sched_kind)
    if _hm and _hm in _SCAN_SPECIAL_NO_COPY.get(sched_kind, set()):
        return "unverified"
    return "verified"

def _ai_model(kind: str = "btc", scan_ver: int = None) -> str:
    """Which Claude model to use — driven by (scan type x classification),
    the full AICFG_GRID set via /aiconfig. BTC always uses SCAN_MODEL."""
    sched_kind = _ai_sched_kind(kind, scan_ver)
    if sched_kind is None:
        return SCAN_MODEL
    return AICFG_GRID[sched_kind][_ai_category(kind, scan_ver)]["model"]

def _ai_aerolink(kind: str = "btc", scan_ver: int = None) -> bool:
    """Which gateway to use — same (scan type x classification) grid as _ai_model()."""
    sched_kind = _ai_sched_kind(kind, scan_ver)
    if sched_kind is None:
        return USE_AEROLINK
    return AICFG_GRID[sched_kind][_ai_category(kind, scan_ver)]["aerolink"]

def _gw_model_tag(kind: str = "btc", scan_ver: int = None) -> str:
    """Gateway+model tag for signal headers: A4.8/D4.8 (Aerolink/Direct + Opus 4.8)
    or AF/DF (Aerolink/Direct + Fable 5)."""
    gw = "A" if _ai_aerolink(kind, scan_ver) else "D"
    mdl = "F" if _ai_model(kind, scan_ver) == "claude-fable-5" else "4.8"
    return f"{gw}{mdl}"

def _aerolink_configured_keys() -> list:
    """All 10 possible Aerolink key slots, filtered down to whichever are
    actually non-empty in Railway right now — single source of truth used
    everywhere key rotation/counting/skipping happens."""
    return [k for k in (AEROLINK_API_KEY, AEROLINK_API_KEY_2, AEROLINK_API_KEY_3, AEROLINK_API_KEY_4,
                         AEROLINK_API_KEY_5, AEROLINK_API_KEY_6, AEROLINK_API_KEY_7, AEROLINK_API_KEY_8,
                         AEROLINK_API_KEY_9, AEROLINK_API_KEY_10) if k]

def _claude_client(kind: str = "btc", attempt: int = 0, scan_ver: int = None):
    """Returns an Anthropic client for the given scan type (btc/scan1/scan2/test).
    When that type's gateway is Aerolink, uses ONLY the Aerolink key slots +
    AEROLINK_BASE_URL — the real ANTHROPIC_API_KEY is never touched or sent to the gateway.
    Up to 10 Aerolink key slots (AEROLINK_API_KEY..._10) are supported — on a retry
    (attempt >= 1), rotates to the next CONFIGURED slot in order, skipping any empty
    ones, so a failure on one key doesn't fail the whole call as long as another slot
    has a key in it. Slot 1 is required; slots 2-10 are optional and can be left empty
    until keys are added later."""
    if _ai_aerolink(kind, scan_ver) and AEROLINK_API_KEY:
        _keys = _aerolink_configured_keys()
        key = _keys[attempt % len(_keys)] if _keys else AEROLINK_API_KEY
        return anthropic.Anthropic(api_key=key, base_url=AEROLINK_BASE_URL)
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

def _pick_aerolink_key(attempt: int, bad_keys: set) -> str:
    """Same rotation as _claude_client, but skips any key already marked bad
    earlier in the CURRENT scan cycle (see _claude_client_skip) — a key that
    just failed on coin #1 gets skipped on coin #2/#3 instead of being
    retried from scratch every time. Falls back to the full list if every
    key has somehow already been marked bad, rather than crashing."""
    _all = _aerolink_configured_keys()
    _keys = [k for k in _all if k not in bad_keys] or _all
    return _keys[attempt % len(_keys)] if _keys else AEROLINK_API_KEY

def _claude_client_skip(kind: str, attempt: int, bad_keys: set, scan_ver: int = None):
    """Like _claude_client, but for callers doing their own multi-coin retry
    loop with a shared bad_keys set across the whole scan cycle. Returns
    (client, key_used) — key_used is "" for the Direct gateway (nothing to
    mark bad), or the actual Aerolink key string so the caller can add it to
    bad_keys if this attempt fails."""
    if _ai_aerolink(kind, scan_ver) and AEROLINK_API_KEY:
        key = _pick_aerolink_key(attempt, bad_keys)
        return anthropic.Anthropic(api_key=key, base_url=AEROLINK_BASE_URL), key
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY), ""

def _aerolink_gw_debug_tag(using_aero: bool, attempt: int, bad_keys: set = None) -> str:
    """Debug label for scan logs — 'direct', or 'aerolink-keyN' showing which
    of the up-to-10 configured Aerolink key slots this attempt will actually
    rotate to (same slot-skipping logic as _claude_client/_claude_client_skip)."""
    if not using_aero:
        return "direct"
    _all = _aerolink_configured_keys()
    _keys = [k for k in _all if k not in (bad_keys or set())] or _all
    if not _keys:
        return "aerolink-key1"
    idx = _all.index(_keys[attempt % len(_keys)]) + 1
    return f"aerolink-key{idx}"

def _claude_retry_budget(using_aero: bool) -> int:
    """How many attempts a single coin's Claude call gets. On Aerolink, this
    scales to the number of CONFIGURED key slots (up to 10) so a coin only
    gets abandoned once every real key has actually been tried, not just the
    first 3 — previously a coin failed after 3 tries even with a healthy
    key4-10 sitting completely untried. Direct gateway has no key rotation
    to matter for, so it keeps the original 3-attempt floor. Never fewer than
    3 either way, matching prior behavior when only 1-2 keys are set."""
    if not using_aero:
        return 3
    return max(3, len(_aerolink_configured_keys()))

_CSV_HEADERS = ["type","coin","direction","signal_time","entry_price","sl_price","tp1_price","tp2_price",
                 "entry_trigger_time","tp1_hit_time","tp2_hit_time","sl_hit_time","timeout_time","result","notes"]

def _pull_csv_central(key: str, path: str) -> bool:
    """On a fresh server with no local CSV yet, restore it from the shared
    store so a new co-server starts with main's full history instead of empty.
    Never overwrites a CSV that already exists locally."""
    if os.path.exists(path) or not CLEXER_API_URL:
        return False
    r = _central_get(f"/kv/{key}", timeout=15)
    if r is not None and r.ok:
        body = r.json()
        if body.get("found") and body["data"].get("csv"):
            with open(path, "w", encoding="utf-8", newline="") as f:
                f.write(body["data"]["csv"])
            print(f"  [LOG] Restored {key} from central store")
            return True
    return False

def _ensure_csv():
    if not os.path.exists(TRADE_LOG_CSV):
        if _pull_csv_central("trade_history_csv", TRADE_LOG_CSV):
            return
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
        "demo1_trades": demo_scan1_trades,
        "demo2_trades": demo_scan2_trades,
        "stats":        trade_stats,
        "history":      signal_history,
        "outcomes":     trade_outcomes,
        "scan_history": scan_history,
        "demo_history": demo_history,
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[STATE] Save error: {e}")
        return
    # Push to the central store on every save, not just during a manual
    # /syncup — the mini app's /trades/active reads from there, so without
    # this it only ever reflected whatever state existed at the last manual
    # sync, making trades (including these newly-added demo ones) look stale
    # or missing entirely most of the time.
    if CLEXER_API_URL:
        try:
            requests.post(f"{CLEXER_API_URL}/push_state", json=state,
                headers=({"X-Push-Secret": PUSH_STATE_SECRET} if PUSH_STATE_SECRET else {}), timeout=8)
        except Exception as e:
            print(f"[STATE] Central push error: {e}")

def save_active_trade():
    save_state()

def load_active_trade():
    global active_trade, scan1_trades, scan2_trades, trade_stats, signal_history, trade_outcomes, scan_history
    global demo_scan1_trades, demo_scan2_trades, demo_history
    d = None
    path = STATE_FILE if os.path.exists(STATE_FILE) else ACTIVE_TRADE_FILE
    _local_mtime = os.path.getmtime(path) if os.path.exists(path) else 0
    r = _central_get("/push_state")
    if r is not None and r.ok:
        body = r.json()
        _central_state = body.get("state") if isinstance(body, dict) and "state" in body else body
        _central_ts_str = body.get("updated_at") if isinstance(body, dict) else None
        if _central_state:
            _central_ts = 0
            if _central_ts_str:
                try:
                    _central_ts = datetime.fromisoformat(_central_ts_str.replace("Z", "+00:00")).timestamp()
                except Exception:
                    _central_ts = 0
            if _central_ts >= _local_mtime or not os.path.exists(path):
                d = _central_state
                print(f"[STATE] Loaded from central store (newer than local — central:{_central_ts:.0f} local:{_local_mtime:.0f})")
            else:
                print(f"[STATE] Local file is newer than central ({_local_mtime:.0f} > {_central_ts:.0f}) — using local")
        else:
            print("[STATE] Central store reachable but empty (no /syncup run yet?)")
    try:
        if d is None and os.path.exists(path):
            d = json.load(open(path))
        if d is not None:
            trade_stats.update(d.get("stats", {}))
            signal_history[:] = d.get("history", [])
            trade_outcomes[:]  = d.get("outcomes", [])
            scan_history[:]    = d.get("scan_history", [])
            demo_history[:]    = d.get("demo_history", [])
            t = d.get("trade", {})
            if t.get("signal"):
                active_trade = t
                print(f"[STATE] Restored BTC trade: {t['signal']} @ {t['entry']:,.0f} "
                      f"entry_hit:{t.get('entry_hit')} tp1_hit:{t.get('tp1_hit')}")
            scan1_trades[:] = [x for x in d.get("scan1_trades", []) if x.get("signal")]
            scan2_trades[:] = [x for x in d.get("scan2_trades", []) if x.get("signal")]
            demo_scan1_trades[:] = [x for x in d.get("demo1_trades", []) if x.get("signal")]
            demo_scan2_trades[:] = [x for x in d.get("demo2_trades", []) if x.get("signal")]
            if scan1_trades: print(f"[STATE] Restored scan1: {[t['symbol'] for t in scan1_trades]}")
            if scan2_trades: print(f"[STATE] Restored scan2: {[t['symbol'] for t in scan2_trades]}")
            if demo_scan1_trades: print(f"[STATE] Restored demo1: {[t['symbol'] for t in demo_scan1_trades]}")
            if demo_scan2_trades: print(f"[STATE] Restored demo2: {[t['symbol'] for t in demo_scan2_trades]}")
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

def set_trade(s: dict, share_free: bool = True):
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
            "share_free": share_free, "entry_time_str": (datetime.now(timezone.utc)+IST).strftime("%d.%m.%y %H:%M"),
            "is_d48": _gw_model_tag("btc") == "D4.8",  # channel-2 only gets D4.8 (Direct+Opus4.8) signals
            "sig_id": s.get("sig_id") or _gen_signal_id(),
            "reply_map": s.get("reply_map") or {},
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
            msg = _claude_client(attempt=attempt).messages.create(
                model=SCAN_MODEL, max_tokens=1200,
                messages=[{"role": "user", "content": content}])
            _log_api_usage("btc_analysis", SCAN_MODEL,
                           msg.usage.input_tokens, msg.usage.output_tokens,
                           gateway="Aerolink" if _ai_aerolink("btc") else "Direct")
            raw = _claude_text(msg)
            if raw: break
            time.sleep(2)
        except Exception as e:
            print(f"  [CLAUDE ERROR] attempt {attempt+1}: {e}")
            if "image" in str(e).lower() and attempt == 0:
                print("  [CLAUDE] Retrying text-only...")
                content_text = [c for c in content if c["type"] == "text"]
                try:
                    msg = _claude_client(attempt=1).messages.create(
                        model=SCAN_MODEL, max_tokens=1200,
                        messages=[{"role": "user", "content": content_text}])
                    _log_api_usage("btc_analysis_textonly", SCAN_MODEL,
                                   msg.usage.input_tokens, msg.usage.output_tokens,
                                   gateway="Aerolink" if _ai_aerolink("btc") else "Direct")
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
                f"{ist_str()}")
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
                f"<i>/resetsl to lower bar.</i>")
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
                       msg.usage.input_tokens, msg.usage.output_tokens,
                       gateway="Aerolink" if _ai_aerolink("btc") else "Direct")
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
                f"<i>{_html.escape(sig.get('reasoning','')[:120])}</i>")
    if s == "HOLD":
        return (f"<b>[{label}]</b> 🔒 HOLD\n"
                f"4H: {sig.get('structure_4h','?')} | Conf: {sig.get('confidence','?')}\n"
                f"<i>{_html.escape(sig.get('reasoning','')[:120])}</i>")
    e = "🟢" if s == "BUY" else "🔴"
    entry = float(sig.get("entry", 0)); sl = float(sig.get("sl", 0)); tp1 = float(sig.get("tp1", 0)); tp2 = float(sig.get("tp2", 0))
    return (f"<b>[{label}]</b> {e} <b>{s}</b>\n"
            f"Entry: <b>{entry:,.0f}</b> ({sig.get('entry_type','?')})\n"
            f"SL: {sl:,.0f} | TP1: {tp1:,.0f} | TP2: {tp2:,.0f}\n"
            f"R:R: {sig.get('rr','?')} | Conf: {sig.get('confidence','?')}\n"
            f"4H: {sig.get('structure_4h','?')}\n"
            f"<i>{_html.escape(sig.get('reasoning','')[:150])}</i>")

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
        f"Source:   <b>{get_current_source()}</b>")

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
    global channel_paused, SEND_CHARTS, CHART_TFS, SEND_NEWS, SIGNAL_SCAN_INTERVAL, BTC_PROMPT_MODE, btc_analysis_enabled, SCAN1_AUTO_ENABLED, SCAN2_AUTO_ENABLED, TEST_SCAN_ENABLED, SCAN_MODEL, USE_AEROLINK, CONTACT_ADMIN_ENABLED, SIGNAL_CHANNEL_ENABLED, SIGNAL_CHANNEL_LINK, ZONE_ENTRY_ENABLED, CO_ADMIN_CHAT_ID, CO_ADMIN_ENABLED, ACTIVE_PROFILE, _SETTINGS_PROFILES, CHANNELS, FREE_SIGNAL_DAILY_LIMIT, TRAIL_SL_BTC, TRAIL_SL_SCAN1, TRAIL_SL_SCAN2, TRAIL_SL_DEMO1, TRAIL_SL_DEMO2, WEEKEND_SLEEP_ENABLED, VIP_MONTHLY_PRICE, CHAT_MODEL, STATS_VISIBLE_TO_USERS
    try:
        d = None
        # Central store first (shared across every server pointed at the same
        # CLEXER_API_URL) — falls back to the local file if unreachable/unset.
        r = _central_get("/kv/bot_settings")
        if r is not None and r.ok:
            d = _kv_pick_newer(_SETTINGS_FILE, r.json(), "SETTINGS")
        if d is None and os.path.exists(_SETTINGS_FILE):
            d = json.load(open(_SETTINGS_FILE))
        if d is not None:
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
            _apply_aicfg_grid(d.get("aicfg_grid"))
            _SLOT_EVAL_THRESHOLD.update(d.get("slot_eval_threshold", {}))
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
            TRAIL_SL_DEMO1 = d.get("trail_sl_demo1", False)
            TRAIL_SL_DEMO2 = d.get("trail_sl_demo2", False)
            WEEKEND_SLEEP_ENABLED = d.get("weekend_sleep_enabled", True)
            VIP_MONTHLY_PRICE = d.get("vip_monthly_price", VIP_MONTHLY_PRICE)
            CHAT_MODEL = d.get("chat_model", CHAT_MODEL)
            STATS_VISIBLE_TO_USERS = d.get("stats_visible_to_users", STATS_VISIBLE_TO_USERS)
            CONTACT_ADMIN_ENABLED  = d.get("contact_admin_enabled",  True)
            SIGNAL_CHANNEL_ENABLED = d.get("signal_channel_enabled", True)
            SIGNAL_CHANNEL_LINK    = d.get("signal_channel_link",    "")
            ct.BTC_CT_ENABLED   = d.get("btc_ct_enabled",   True)
            ct.SCAN1_CT_ENABLED = d.get("scan1_ct_enabled", True)
            ct.SCAN2_CT_ENABLED = d.get("scan2_ct_enabled", True)
            ct.DEMO1_CT_ENABLED = d.get("demo1_ct_enabled", False)
            ct.DEMO2_CT_ENABLED = d.get("demo2_ct_enabled", False)
            print(f"[SETTINGS] Loaded — charts:{SEND_CHARTS} news:{SEND_NEWS} "
                  f"interval:{SIGNAL_SCAN_INTERVAL//3600}h "
                  f"btcmode:{BTC_PROMPT_MODE} "
                  f"model:{SCAN_MODEL} "
                  f"ch_paused:{channel_paused}")
    except Exception as e:
        print(f"[SETTINGS] Load error: {e}")

def save_settings():
    _settings_blob = {
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
            "aicfg_grid": {k: {t: dict(v) for t, v in tiers.items()} for k, tiers in AICFG_GRID.items()},
            "slot_eval_threshold": dict(_SLOT_EVAL_THRESHOLD),
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
            "trail_sl_demo1": TRAIL_SL_DEMO1,
            "trail_sl_demo2": TRAIL_SL_DEMO2,
            "weekend_sleep_enabled": WEEKEND_SLEEP_ENABLED,
            "vip_monthly_price": VIP_MONTHLY_PRICE,
            "chat_model": CHAT_MODEL,
            "stats_visible_to_users": STATS_VISIBLE_TO_USERS,
            "contact_admin_enabled":  CONTACT_ADMIN_ENABLED,
            "signal_channel_enabled": SIGNAL_CHANNEL_ENABLED,
            "signal_channel_link":    SIGNAL_CHANNEL_LINK,
            "btc_ct_enabled":   ct.BTC_CT_ENABLED,
            "scan1_ct_enabled": ct.SCAN1_CT_ENABLED,
            "scan2_ct_enabled": ct.SCAN2_CT_ENABLED,
            "demo1_ct_enabled": ct.DEMO1_CT_ENABLED,
            "demo2_ct_enabled": ct.DEMO2_CT_ENABLED,
    }
    try:
        json.dump(_settings_blob, open(_SETTINGS_FILE, "w"), indent=2)
    except Exception as e:
        print(f"[SETTINGS] Local save error: {e}")
    # Previously only pushed to the central store during a manual /syncup —
    # same bug class as the daily-recap/slot-stats issue: a redeploy between
    # a settings change and the next manual sync could silently revert VIP/Free
    # channel IDs, model toggles, copytrade flags, etc. back to whatever the
    # central store last had (or defaults, if it never had anything).
    #
    # Gated to the ACTIVE server only — a standby server (e.g. an old/abandoned
    # deployment from the multi-server rotation that's still running) keeps
    # scanning and calling save_settings() with its own stale in-memory values.
    # _kv_pick_newer() trusts whichever push has the latest timestamp, not
    # whichever is actually correct — so an abandoned server pushing unchanged
    # defaults (e.g. demo2's win-rate threshold reverting from an admin-set 35%
    # back to the hardcoded 50%) can silently clobber a real, newer change made
    # on the active server. Only the active server's writes are trustworthy.
    if CLEXER_API_URL and is_active_server():
        try:
            _kv_push("bot_settings", _settings_blob)
        except Exception as e:
            print(f"[SETTINGS] Central push error: {e}")

channel_paused = {"1": False, "2": False}  # per-channel pause state

# Premium (Telegram Premium) animated emoji IDs — rendered via <tg-emoji emoji-id="…">
# HTML tag so they coexist with existing parse_mode="HTML" formatting. Falls back to
# the plain emoji glyph automatically for non-Premium viewers.
PREMIUM_EMOJI_MAP = {
    "🟢": "5215685881989442149", "🔴": "4926956800005112527",
    "🟩": "5262747715552438702", "🟥": "5809816842713174497",  # BUY/SELL-only direction icon on signal cards (distinct from the generic 🟢/🔴 used for toggles/checks elsewhere)
    "🛑": "5366040905927113475", "🎯": "5461009483314517035",
    "🏆": "5188344996356448758", "✅": "6120713655366455614",
    "❌": "6120660741369369103", "🚫": "5240241223632954241",
    "🚨": "5395695537687123235", "🚀": "6221996895535896347",
    "💰": "6224365445445590974", "🤖": "5197252827247841976",
    "📊": "5231200819986047254", "📡": "6174682466356303760",
    "⏰": "5213349767672769194", "🕐": "5363857580777029543",
    "🕦": "5933544413740403607", "🛡": "6070930852647278292",
    "📌": "5193159135004211919", "💬": "5330237710655306682",
    "✨": "5325547803936572038", "🎉": "5208895581644140071",
    "🔺": "5980787993139481991", "🗂": "5332586662629227075",
    "▶️": "5264919878082509254", "👏": "5357052372600250759",
    "😭": "5339386257283764734", "💀": "5379930048478330552",
    "📣": "5215668805199473901", "🧠": "6120687391641440754",
    "⚠️": "5213181173026533794", "👑": "6120766436219555441",
    "👋": "5258029071207505708", "🏷️": "6016997440777883054",
    "🏷": "6016997440777883054",
    # VIP tier-label star (e.g. "⭐ VIP") — distinct from the Stars-payment ⭐
    # override (PAYMENT_STAR_EMOJI_ID below), which wins on payment screens
    # since per-call overrides take precedence over this global map.
    "⭐️": "5314546133538715992", "⭐": "5314546133538715992",
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
    "💎": "6122857771760094969", "🔎": "5017088445353296841",
    "📈": "6224129999633388168", "📉": "6222274114200015993",
    "🎆": "5064672027248427816", "🪪": "5890864241388293875",
    "🔒": "5296369303661067030",
}
PREMIUM_EMOJIS_ENABLED = True

_SMALLCAPS_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyz",
    "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ"
)

def _smallcaps_title(text: str) -> str:
    """'Likely breaks' -> 'Lɪᴋᴇʟʏ Bʀᴇᴀᴋꜱ' — first letter of each word stays a
    normal capital, the rest render in small-caps unicode glyphs. Acronyms
    (AI, BTC, EMA20, ...) and slash-commands (/go, /help, ...) are left
    untouched instead of getting mangled — a command has to stay literal
    text for Telegram to keep it tappable/copyable."""
    words = text.split(" ")
    out = []
    for w in words:
        if not w:
            out.append(w); continue
        if w.startswith("/"):
            out.append(w)  # slash-command — leave as-is
            continue
        letters = [c for c in w if c.isalpha()]
        if letters and len(letters) > 1 and all(c.isupper() for c in letters):
            out.append(w)  # acronym — leave as-is
        else:
            out.append(w[0].upper() + w[1:].lower().translate(_SMALLCAPS_MAP))
    return " ".join(out)

GLOBAL_SMALLCAPS_ENABLED = True   # every outbound message body + button label rendered in smallcaps, per admin request

_HTML_TAG_RE = re.compile(r'(<[^>]+>)')

def _smallcaps_body(text: str) -> str:
    """Runs _smallcaps_title() over a full HTML message body instead of a
    short title — splits on HTML tags so tag syntax/attributes are never
    touched, and skips the contents of <code>/<pre> blocks entirely so
    prices, tickers, and other exact values stay unmangled."""
    if not text:
        return text
    parts = _HTML_TAG_RE.split(text)
    out = []
    skip = 0
    for part in parts:
        if part.startswith("<") and part.endswith(">"):
            inner = part.strip("<>").lstrip("/").split()[0].lower() if part.strip("<>").lstrip("/") else ""
            if inner in ("code", "pre"):
                skip = max(0, skip - 1) if part.startswith("</") else skip + 1
            out.append(part)
        else:
            out.append(part if skip > 0 else _smallcaps_title(part))
    return "".join(out)

def _apply_premium_emojis(text: str, overrides: dict = None) -> str:
    """Wraps known emoji glyphs in <tg-emoji> so Premium users see the animated
    version; everyone else still sees the plain glyph (Telegram's own fallback).
    `overrides` lets a specific caller swap the emoji ID for one or more
    glyphs (e.g. a distinct ⭐ for Stars-payment screens) without touching the
    global PREMIUM_EMOJI_MAP used everywhere else — mapping a glyph to None
    excludes it from custom-emoji wrapping entirely (kept as the plain,
    predictably-narrow glyph), e.g. for fixed-width ASCII-box layouts where
    a wider custom-emoji sticker would throw off manual padding. Also
    applies the bot-wide smallcaps text style (GLOBAL_SMALLCAPS_ENABLED) as
    the last step, after emoji glyphs are wrapped, so the "BingX" glyph-swap
    match above always runs against unmangled text first."""
    if not text:
        return text
    if PREMIUM_EMOJIS_ENABLED:
        emap = {**PREMIUM_EMOJI_MAP, **overrides} if overrides else PREMIUM_EMOJI_MAP
        for glyph, emoji_id in emap.items():
            if emoji_id is not None and glyph in text:
                text = text.replace(glyph, f'<tg-emoji emoji-id="{emoji_id}">{glyph}</tg-emoji>')
        if "BingX" in text:
            text = text.replace("BingX", '<tg-emoji emoji-id="5289756243731162671">🔀</tg-emoji> BingX')
    if GLOBAL_SMALLCAPS_ENABLED:
        text = _smallcaps_body(text)
    return text

# Distinct custom emoji ID for ⭐ specifically on Telegram-Stars payment
# screens/buttons (VIP, signal unlock, add funds) — separate from the ⭐ used
# elsewhere in the bot (PREMIUM_EMOJI_MAP), per admin request.
PAYMENT_STAR_EMOJI_ID = "5190768311394130762"
_PAYMENT_STAR_OVERRIDE = {"⭐️": PAYMENT_STAR_EMOJI_ID, "⭐": PAYMENT_STAR_EMOJI_ID}

def _star_button(text: str, callback_data: str = None, url: str = None) -> dict:
    """Builds an inline-keyboard button whose leading ⭐ renders as the
    dedicated payment-Stars custom emoji (icon_custom_emoji_id) instead of
    the plain glyph or the bot's other global ⭐ mapping."""
    label = text.replace("⭐", "", 1).strip() if text.startswith("⭐") else text
    btn = {"text": label, "icon_custom_emoji_id": PAYMENT_STAR_EMOJI_ID}
    if callback_data is not None:
        btn["callback_data"] = callback_data
    if url is not None:
        btn["url"] = url
    return btn

# Distinct custom emoji ID for 💰 specifically on the VIP-buying screen's
# flat-price button — separate from the 💰 used elsewhere in the bot.
VIP_MONEYBAG_EMOJI_ID = "5458675903028535170"

def _vip_moneybag_button(text: str, callback_data: str = None, url: str = None) -> dict:
    """Same pattern as _star_button but for the 💰 icon on VIP-buy buttons."""
    label = text.replace("💰", "", 1).strip() if text.startswith("💰") else text
    btn = {"text": label, "icon_custom_emoji_id": VIP_MONEYBAG_EMOJI_ID}
    if callback_data is not None:
        btn["callback_data"] = callback_data
    if url is not None:
        btn["url"] = url
    return btn

_STYLE_SUCCESS_HINTS = ("Turn ON", "🟢", "Yes, confirm", "Adopt", "💾 Save", "✅")
_STYLE_DANGER_HINTS  = ("Turn OFF", "🔴", "Cancel", "Remove", "Reset", "❌ Close", "🗑", "🚫", "❌")
# Exact buttons that should never get a color — plain settings/nav entries, not ON/OFF actions
_STYLE_NONE_LABELS = (
    "Set Custom SL", "Set Custom TP1", "Set Custom TP2", "TP1 Close %", "Trailing SL",
    "My Copy Trade", "Trade Control", "Copy Admin", "TV & Advanced", "Broadcast & Channels",
    "Contact/Channel Settings", "Active BTC + all scan trades", "Current BTC price",
    "London / NY / Sleep session", "Last 5 signals", "Other Actions",
    "Open Dashboard", "Free Channel", "VIP Channel",
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
            if GLOBAL_SMALLCAPS_ENABLED and "text" in btn:
                btn["text"] = _smallcaps_title(btn["text"])
    return markup

def send_telegram(text, include_ch2=True, with_bot_button=False):
    success = False
    text = _apply_premium_emojis(text)
    channels = [("1", TELEGRAM_CHANNEL_ID), ("2", os.getenv("TELEGRAM_CHANNEL_ID_2",""))]
    for key, cid in channels:
        if not cid: continue
        if channel_paused.get(key): continue
        if key == "2" and not include_ch2: continue
        if _send_via_true_forward(text, cid, f"legacy-ch{key}", with_bot_button=with_bot_button):
            success = True; continue
        try:
            r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": text,
                      "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
            r.raise_for_status(); success = True
        except Exception as e: print(f"  [TG ERROR] {cid}: {e}")
    return success

def send_admin(text, pin: bool = False, emoji_overrides: dict = None):
    """Send message to admin DM only (not channel). pin=True also pins it
    there — used for things the admin needs to keep visible/handy, like a
    special-time promote/demote notice."""
    if not ADMIN_CHAT_ID: return
    text = _apply_premium_emojis(text, overrides=emoji_overrides)
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
        rj = r.json()
        if not rj.get("ok"):
            print(f"  [ADMIN MSG ERROR] Telegram rejected: {rj.get('description')}")
        elif pin:
            _mid = rj.get("result", {}).get("message_id")
            if _mid: _pin_message(ADMIN_CHAT_ID, _mid)
    except Exception as e: print(f"  [ADMIN MSG ERROR] {e}")

def _run_agentrouter_cli(prompt: str, model: str = "claude-opus-4-8", timeout: int = 90) -> str:
    """TEST ONLY — re-verifying whether AgentRouter's malformed/inconsistent
    responses from Railway (confirmed broken in an earlier test) still
    reproduce. AgentRouter only accepts the real Claude Code CLI as a client,
    so this shells out to the actual installed binary. Requires Node.js +
    the CLI npm package (see railpack.json). Returns raw stdout, or an error
    string starting with '⚠️'."""
    if not AGENTROUTER_AUTH_TOKEN:
        return "⚠️ AGENTROUTER_AUTH_TOKEN not set."
    env = os.environ.copy()
    env["ANTHROPIC_BASE_URL"] = AGENTROUTER_BASE_URL
    env["ANTHROPIC_AUTH_TOKEN"] = AGENTROUTER_AUTH_TOKEN
    env["ANTHROPIC_MODEL"] = model
    env["CLAUDE_CODE_USE_AUTH_TOKEN"] = "true"
    try:
        r = subprocess.run(["claude", "-p", prompt], env=env, capture_output=True,
                            text=True, timeout=timeout)
        out = (r.stdout or "").strip()
        err = (r.stderr or "").strip()
        if not out and not err:
            return "⚠️ Empty response from CLI."
        parts = []
        if out: parts.append(out)
        if err: parts.append(f"[stderr]\n{err[:1500]}")
        parts.append(f"[exit_code={r.returncode}]")
        return "\n\n".join(parts)
    except FileNotFoundError:
        return "⚠️ `claude` CLI not found — Node.js/CLI install may not have completed on this deploy."
    except subprocess.TimeoutExpired:
        return f"⚠️ Timed out after {timeout}s."
    except Exception as e:
        return f"⚠️ {e}"

def _test_agentrouter(cid):
    """Admin-only /testar — pure connectivity re-test, no scan logic, no
    tracking, no side effects. Just: does AgentRouter respond correctly to
    the real Claude Code CLI from THIS Railway deployment right now?"""
    send_reply(cid, "🧪 <b>AgentRouter Re-Test</b>\n\n⏳ Calling AgentRouter CLI...")
    t0 = time.time()
    output = _run_agentrouter_cli("Reply with exactly: TEST OK")
    elapsed = time.time() - t0
    send_reply(cid,
        f"🧪 <b>Result</b> ({elapsed:.1f}s)\n\n<pre>{_html.escape(output[:3500])}</pre>")

_reply_capture: dict = {}  # cid → {"texts": [], "cat_id": str} when capturing for inline menu

def send_reply(chat_id, text, reply_markup=None, emoji_overrides=None):
    cid_str = str(chat_id)
    if cid_str in _reply_capture:
        # Captured (not actually sent yet) — store raw text. The eventual
        # _help_edit_or_send() call that delivers it applies premium-emoji
        # processing exactly once; pre-applying here too caused double-wrapping
        # (e.g. "🔀 🔀 BingX"). emoji_overrides must be stashed too — a
        # captured command (e.g. tapping the /status button from the help
        # menu, instead of typing it) previously lost any per-call override
        # entirely, since only the final _help_edit_or_send() call actually
        # applies premium-emoji processing and it had no way to know about it.
        _reply_capture[cid_str]["texts"].append(text)
        if reply_markup:
            _reply_capture[cid_str]["markup"] = reply_markup
        if emoji_overrides:
            _reply_capture[cid_str].setdefault("emoji_overrides", {}).update(emoji_overrides)
        return
    text = _apply_premium_emojis(text, overrides=emoji_overrides)
    reply_markup = _style_keyboard(reply_markup)
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
        _payload = {"chat_id": int(cid),
                  "text": "⏰ <b>Your VIP expired</b>\n\nRenew within 24 hours or you'll be removed from "
                          "VIP and the VIP channel.",
                  "parse_mode": "HTML"}
        if _mkp: _payload["reply_markup"] = _mkp
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json=_payload, timeout=10)
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
        _payload = {"chat_id": int(cid), "text": "⏰ <b>Your VIP has expired</b>\n\nYou've been removed from the VIP channel. Contact admin to renew.",
                  "parse_mode": "HTML"}
        if _mkp: _payload["reply_markup"] = _mkp
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json=_payload, timeout=10)
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

def _all_broadcast_channel_targets() -> list:
    """Every channel/group the bot can broadcast to — legacy channels + every
    VIP/Free tier channel. Returns [(id, label), ...]."""
    out = []
    if TELEGRAM_CHANNEL_ID: out.append((TELEGRAM_CHANNEL_ID, "📡 Signal Channel 1"))
    _ch2 = os.getenv("TELEGRAM_CHANNEL_ID_2", "")
    if _ch2: out.append((_ch2, "📡 Signal Channel 2"))
    for c in CHANNELS:
        if c.get("id"):
            out.append((c["id"], c.get("label") or (("⭐ VIP" if c.get("tier")=="vip" else "🆓 Free") + f" · {c['id']}")))
    return out

def do_broadcast(admin_chat_id, text, file_id=None, file_type=None, mode="all", channel_targets=None):
    """channel_targets: optional explicit list of channel/group chat_ids to use
    instead of the legacy-only default — set by the new multi-select picker."""
    if mode == "users":
        targets = [u for u in registered_users if u not in blocked_users]
    elif mode == "channels":
        targets = channel_targets if channel_targets is not None else [cid for cid, _ in _all_broadcast_channel_targets()]
    else:
        _chan = channel_targets if channel_targets is not None else [cid for cid, _ in _all_broadcast_channel_targets()]
        targets = [u for u in registered_users if u not in blocked_users] + _chan
    ok = 0; fail = 0
    for cid in targets:
        if send_to_user(cid, text, file_id, file_type): ok += 1
        else: fail += 1
        time.sleep(0.05)
    send_reply(admin_chat_id, f"<b>Broadcast Done</b>\n{ok} delivered | {fail} failed")

# --- MESSAGE FORMATS ----------------------------------------------------------
def fmt_signal(s):
    e   = "🟩" if s["signal"]=="BUY" else "🟥"
    ci  = {"HIGH":"🔥 HIGH","MEDIUM":"⚡ MED","LOW":"🌀 LOW"}.get(s.get("confidence",""),"")
    wk = s.get("weekly_trend",""); s4h = s.get("structure_4h","")
    ez = s.get("entry_zone","");   rs  = s.get("reasoning","")
    entry_lines = [f"🎯 {_smallcaps_title('Entry')}: <code>{s['entry']:,.0f}</code>"]
    if s.get("entry_type")=="PULLBACK" and s.get("entry_note"):
        entry_lines.append(f"📍 {_html.escape(s['entry_note'])}")
    levels = [f"🛑 SL: <code>{s['sl']:,.0f}</code>", f"💰 TP1: <code>{s['tp1']:,.0f}</code>", f"🏆 TP2: <code>{s['tp2']:,.0f}</code>",
              f"⚖️ R:R: {s.get('rr','-')}"]
    context = []
    if wk:  context.append(f"🌐 {_smallcaps_title('Weekly')}: {_html.escape(wk)}")
    if s4h: context.append(f"📊 4ʜ: {_html.escape(s4h)}")
    if ez:  context.append(f"📍 {_smallcaps_title('Zone')}: {_html.escape(ez)}")
    sections = [entry_lines, levels]
    if context: sections.append(context)
    return _scan_box(
        f"{SYMBOL} Signal",
        f"{e} {s['signal']} - {SYMBOL}  {ci}  {_gw_model_tag('btc')}",
        sections,
        tag=s.get("sig_id",""),
    )

def _send_btc_entry_signal(signal: dict, share_free: bool) -> dict:
    """Gens a sig_id, saves the signal snapshot (for the Free-channel unlock
    flow), builds both the real and locked-Free-channel variants, and sends.
    All 3 BTC entry call sites were byte-identical — consolidated here."""
    signal["sig_id"] = _gen_signal_id()
    _save_sig_snapshot(signal["sig_id"], SYMBOL, signal["signal"], signal["entry"], signal["sl"], signal["tp1"], signal["tp2"], "btc")
    _ids = send_entry_signal(fmt_signal(signal), include_ch2=False, tier_routed=True,
        share_free=share_free, locked_text=_locked_signal_text(SYMBOL.replace("USDT",""), f"BTC {_gw_model_tag('btc')}", signal["sig_id"]), sig_id=signal["sig_id"])
    for k, v in (_ids or {}).items():
        if k.startswith("free:"): _track_free_sl(signal["sig_id"], k.split(":", 1)[1], "entry_mid", v)
    return _ids

def fmt_update(status, price=None):
    t = active_trade; entry = t.get("entry") or 0
    _hdr = lambda emj, title: f"{emj} #{SYMBOL}"
    _sid = t.get("sig_id","")
    msgs = {
        "SL_HIT": _scan_box(
            "SL Hit", _hdr("🚨", "SL Hit"),
            [[f"❌ {_smallcaps_title('Loss taken on')} {t.get('signal','?')} @ <code>{t.get('entry',0):,.0f}</code>"],
             [f"⛔ {_smallcaps_title('Do not open any trade now')}",
              f"🔍 {_smallcaps_title('Waiting for next valid setup')}..."]],
            tag=_sid,
        ),
        "TP1_HIT": _scan_box(
            f"TP1 Hit — {ct.TP1_CLOSE_PCT}% Closed", _hdr("💰", "TP1 Hit"),
            [[f"✅ {_smallcaps_title(f'{ct.TP1_CLOSE_PCT}% position closed at')} <code>{t.get('tp1',0):,.0f}</code>",
              f"🛡️ {_smallcaps_title('SL moved to breakeven')}: <code>{entry:,.0f}</code>",
              f"🚀 {_smallcaps_title(f'Remaining {100-ct.TP1_CLOSE_PCT}% riding to TP2')}: <code>{t.get('tp2',0):,.0f}</code>"],
             [f"⚠️ {_smallcaps_title('Do not close manually — bot is managing the rest')}"]],
            tag=_sid,
        ),
        "TP2_HIT": _scan_box(
            "TP2 Hit — Trade Closed", _hdr("🏆", "TP2 Hit"),
            [[f"✅ {_smallcaps_title('Full profit taken on')} {t.get('signal','?')} @ <code>{t.get('tp2',0):,.0f}</code>"],
             [f"🔍 {_smallcaps_title('Waiting for next valid setup')}..."]],
            tag=_sid,
        ),
        "STOP_HUNT": _scan_box(
            "Stop Hunt Detected", _hdr("🎣", "Stop Hunt"),
            [[f"{_smallcaps_title('Price spiked below SL and closed back above')}.",
              f"✅ {_smallcaps_title('Still in')} {t.get('signal','?')} {_smallcaps_title('trade — position held')}."],
             [f"⚠️ {_smallcaps_title('No action needed — bot is managing this')}"]],
            tag=_sid,
        ),
        "SETUP_INVALID": _scan_box(
            "Trade Cancelled — Setup Invalid", _hdr("⚠️", "Setup Invalid"),
            [[f"{_smallcaps_title('Price closed past SL before entry was hit. No position was opened')}."],
             [f"⛔ {_smallcaps_title('Do not open any trade now')}",
              f"🔍 {_smallcaps_title('Waiting for next valid setup')}..."]],
            tag=_sid,
        ),
        "ENTRY_MISSED": _scan_box(
            "Trade Cancelled — Entry Missed", _hdr("😔", "Entry Missed"),
            [[f"{_smallcaps_title('Price moved past entry zone')} <code>{entry:,.0f}</code> {_smallcaps_title('without filling. No position was opened')}."],
             [f"⛔ {_smallcaps_title('Do not chase — do not open a trade now')}",
              f"🔍 {_smallcaps_title('Waiting for next valid setup')}..."]],
            tag=_sid,
        ),
        "STRUCTURE_FLIP": _scan_box(
            "Trade Closed — Structure Flipped", _hdr("🔄", "Structure Flipped"),
            [[f"{_smallcaps_title('Market structure changed — current')} {t.get('signal','?')} {_smallcaps_title('trade closed')}."],
             [f"⛔ {_smallcaps_title('Wait for the next signal from CLEXER')}",
              f"🔍 {_smallcaps_title('Analysing new direction')}..."]],
            tag=_sid,
        ),
        "WAITING_ENTRY": _scan_box(
            "Waiting Pullback", _hdr("⏳", "Waiting Pullback"),
            [[f"🎯 {_smallcaps_title('Entry')}: <code>{entry:,.0f}</code>", f"🛑 SL: <code>{t.get('sl',0):,.0f}</code>",
              f"🎯 TP1: <code>{t.get('tp1',0):,.0f}</code>", f"🎯 TP2: <code>{t.get('tp2',0):,.0f}</code>"]
             + ([f"📊 {_smallcaps_title('Current')}: <code>{price:,.0f}</code> ({abs((price or 0)-entry):,.0f} pts away)"] if price else [])],
            tag=_sid,
        ),
    }
    return msgs.get(status, _scan_box("Trade Update", f"✅ #{SYMBOL}", [[f"{_smallcaps_title('Trade running')}"]], tag=_sid))

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
                ct.on_entry_hit(entry, sl, tp1, tp2)
                send_lifecycle_reply(
                    f"🚀 <b>ENTRY TRIGGERED!</b>  🕐 {ist_str()}\n\n"
                    f"{'🟩' if sig=='BUY' else '🟥'} <b>{sig} — {SYMBOL}</b>\n\n"
                    f"🎯 Entry:  <b>{entry:,.0f}</b>  |  📊 Price: <b>{price:,.2f}</b>\n"
                    f"🛡️ SL:     <b>{sl:,.0f}</b>  ({abs(price-sl):.0f} pts)\n"
                    f"💰 TP1:   <b>{tp1:,.0f}</b>\n"
                    f"🏆 TP2:   <b>{tp2:,.0f}</b>\n\n"
                    f"⚠️ <b>Trade is now LIVE — SL and TP active</b>",
                    active_trade.get("reply_map"), include_ch2=False)
            return False

        _apply_trail_sl_btc(price)
        sl = active_trade["sl"]

        # TP2 — use candle high/low to catch spike
        tp2_hit = (sig=="BUY" and check_high >= tp2) or (sig=="SELL" and check_low <= tp2)
        if tp2_hit:
            trade_stats["total_tp2"] += 1; trade_stats["consecutive_sl"] = 0
            _delete_trail_sl_messages(active_trade)
            log_trade_outcome("TP2_HIT", f"closed at {tp2:,.0f}")
            _tp2_msg = (f"🏆 <b>TP2 HIT!</b> 🎊💵  🕐 {ist_str()}\n\n"
                f"{'🟩' if sig=='BUY' else '🟥'} {sig} {SYMBOL}\n"
                f"🎯 Entry: {entry:,.0f} ✅ TP2: <b>{tp2:,.0f}</b>")
            send_lifecycle_reply(_tp2_msg, active_trade.get("reply_map"), include_ch2=True,
                tier_routed=True, share_free=active_trade.get("share_free", True), reply_markup=_tp_buttons())
            _track_daily_result(SYMBOL, "TP2", tier_routed=True, free_shown=active_trade.get("share_free", True), entry_date=_ist_date_str(active_trade.get("entry_time")), sig_id=active_trade.get("sig_id",""))
            _notify_free_late(SYMBOL, active_trade, "TP2")
            _close_sig_snapshot(active_trade.get("sig_id",""), "TP2")
            ct.on_tp2(entry, tp2); reset_trade(); return True

        # TP1 — use candle high/low
        if not t["tp1_hit"]:
            tp1_hit = (sig=="BUY" and check_high >= tp1) or (sig=="SELL" and check_low <= tp1)
            if tp1_hit:
                active_trade["tp1_hit"] = True; active_trade["sl"] = entry
                trade_stats["total_tp1"] += 1; trade_stats["consecutive_sl"] = 0
                _delete_trail_sl_messages(active_trade)
                save_active_trade()
                ct.on_tp1(entry, tp1)
                _tp1_msg = (f"💰 <b>TP1 HIT!</b> 🎉  🕐 {ist_str()}\n\n"
                    f"{'🟩' if sig=='BUY' else '🟥'} {sig} {SYMBOL}\n"
                    f"✅ TP1: <b>{tp1:,.0f}</b>\n🛡️ SL moved to BE: <b>{entry:,.0f}</b>\n"
                    f"🚀 Riding TP2: <b>{tp2:,.0f}</b>...")
                send_lifecycle_reply(_tp1_msg, active_trade.get("reply_map"), include_ch2=True,
                    tier_routed=True, share_free=active_trade.get("share_free", True), reply_markup=_tp_buttons())
                _track_daily_result(SYMBOL, "TP1", tier_routed=True, free_shown=active_trade.get("share_free", True),
                    tp1_detail={"tag": "BTC", "side": sig, "tp1": tp1, "sl_be": entry, "tp2": tp2},
                    entry_date=_ist_date_str(active_trade.get("entry_time")), sig_id=active_trade.get("sig_id",""))
                _notify_free_late(SYMBOL, active_trade, "TP1")

        # SL — use candle low/high to catch wick SL hits
        sl_margin = 80
        sl_hit = (sig=="BUY"  and check_low  < sl - sl_margin) or \
                 (sig=="SELL" and check_high > sl + sl_margin)
        if sl_hit:
            trade_stats["total_sl"] += 1; trade_stats["consecutive_sl"] += 1
            n = trade_stats["consecutive_sl"]
            _delete_trail_sl_messages(active_trade)
            log_trade_outcome("SL_HIT", f"{n} in a row, low:{check_low:,.0f} sl:{sl:,.0f}")
            # Suppress ch2 if SL hit within 10 min of entry (stop hunt / quick SL)
            _entry_ts = t.get("entry_time", 0)
            _sl_in_ch2 = (time.time() - _entry_ts) > 600 and active_trade.get("is_d48", False)
            if n >= 3:
                trade_stats["cooldown_scans"] = 2
                _sl_msg = (
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {sig} @ {entry:,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 2 scans...")
                _send_sl_and_log(_sl_msg, active_trade.get("reply_map"), active_trade.get("sig_id",""), "BE" if active_trade.get("tp1_hit", False) else "SL", include_ch2=False)
            elif n == 2:
                trade_stats["cooldown_scans"] = 1
                _sl_msg = (
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {sig} @ {entry:,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 1 scan...")
                _send_sl_and_log(_sl_msg, active_trade.get("reply_map"), active_trade.get("sig_id",""), "BE" if active_trade.get("tp1_hit", False) else "SL", include_ch2=False)
            else:
                _sl_msg = fmt_update("SL_HIT")
                _send_sl_and_log(_sl_msg, active_trade.get("reply_map"), active_trade.get("sig_id",""), "BE" if active_trade.get("tp1_hit", False) else "SL", include_ch2=False)
            if not active_trade.get("tp1_hit", False):
                _track_daily_result(SYMBOL, "SL", tier_routed=True, free_shown=active_trade.get("share_free", True), entry_date=_ist_date_str(active_trade.get("entry_time")))  # breakeven exit after TP1 isn't a real loss
                _send_sl_reassurance(SYMBOL, "BTC", sig, entry,
                    _sl_reassurance_channels(True, active_trade.get("share_free", True)), active_trade.get("reply_map"), active_trade.get("sig_id",""))
            _close_sig_snapshot(active_trade.get("sig_id",""), "BE" if active_trade.get("tp1_hit", False) else "SL")
            ct.on_sl(entry, sl, tp1_hit=active_trade.get("tp1_hit", False)); reset_trade(); return True
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

def _log_demo_history(t: dict, result: str, close_price: float, dver: int):
    """Append a closed TS1/TS2 (demo) trade to demo_history (max 30) — same
    shape as _log_scan_history, keyed by 'dver' instead of 'ver' since demo
    trades aren't scan1/scan2 signals."""
    demo_history.append({
        "time":        ist_str(),
        "symbol":      t.get("symbol", "?"),
        "signal":      t.get("signal", "?"),
        "entry":       t.get("entry", 0),
        "sl":          t.get("sl", 0),
        "tp1":         t.get("tp1", 0),
        "tp2":         t.get("tp2", 0),
        "result":      result,          # TP1 / TP2 / SL / BE / TIMEOUT(...)
        "close_price": close_price,
        "tp1_hit":     t.get("tp1_hit", False),
        "dver":        dver,
    })
    if len(demo_history) > 30: demo_history.pop(0)
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

def _force_close_scan_trade(ver: int, symbol: str, result: str) -> str:
    """Admin /forceclose — manually closes a Scan1/Scan2 trade the bot lost
    track of (e.g. a redeploy landed mid-trade) with the REAL result, running
    the exact same close path the live tick handler uses: channel
    announcement, daily-recap tracking, win-rate slot tracking, copytrade
    notification, sig snapshot close. result: tp1/tp2/sl/be."""
    result = result.lower()
    if result not in ("tp1", "tp2", "sl", "be"):
        return "Result must be one of: tp1, tp2, sl, be."
    lst = _scan_list(ver)
    t = next((x for x in lst if x.get("symbol", "").upper().startswith(symbol.upper())), None)
    if not t:
        return f"No open {'Scan1' if ver == 1 else 'Scan2'} trade found matching '{symbol}'."
    sym = t["symbol"]; sig = t["signal"]; entry = t["entry"]; tp1 = t["tp1"]; tp2 = t["tp2"]
    price = get_bingx_price(sym) or entry

    if result == "tp2":
        trade_stats["scan_tp2"] += 1; trade_stats["scan_tp1"] += (0 if t["tp1_hit"] else 1)
        trade_stats[f"scan{ver}_tp2"] += 1; trade_stats[f"scan{ver}_tp1"] += (0 if t["tp1_hit"] else 1)
        _delete_trail_sl_messages(t)
        _log_scan_history(t, "TP2", price)
        send_lifecycle_reply(fmt_scan_update("TP2_HIT", price, t), t.get("reply_map"), include_ch2=True,
            tier_routed=bool(t.get("tier_routed")), share_free=t.get("share_free", True), reply_markup=_tp_buttons())
        ct.on_scan_tp2(sym)
        log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
            "tp2_hit_time": _ist_str_now(), "result": "TP2",
            "entry_price": entry, "sl_price": t.get("sl", 0), "tp2_price": tp2})
        _track_daily_result(sym, "TP2", tier_routed=bool(t.get("tier_routed")), free_shown=t.get("share_free", True),
            entry_date=_ist_date_str(t.get("created_at")), sig_id=t.get("sig_id", ""))
        _notify_free_late(sym, t, "TP2")
        _slot_hm = _ist_hm_from_epoch(t.get("created_at"))
        if _slot_hm: _slot_track(f"scan{ver}", _slot_hm, True)
        _close_sig_snapshot(t.get("sig_id", ""), "TP2")
        _remove_scan_trade(ver, sym)
        return f"✅ {sym} force-closed as TP2 @ {price:,.4g} — announced, recorded, removed."

    if result == "tp1":
        if t["tp1_hit"]:
            return f"{sym} already shows tp1_hit=True — nothing to do."
        t["tp1_hit"] = True; t["sl"] = entry
        _delete_trail_sl_messages(t)
        trade_stats["scan_tp1"] += 1; trade_stats[f"scan{ver}_tp1"] += 1
        send_lifecycle_reply(fmt_scan_update("TP1_HIT", price, t), t.get("reply_map"), include_ch2=True,
            tier_routed=bool(t.get("tier_routed")), share_free=t.get("share_free", True), reply_markup=_tp_buttons())
        ct.on_scan_tp1(sym)
        log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
            "tp1_hit_time": _ist_str_now(), "result": "TP1_partial",
            "entry_price": entry, "sl_price": entry, "tp1_price": tp1})
        _free_shown = bool(t.get("tier_routed")) and t.get("share_free", True)
        _track_daily_result(sym, "TP1", tier_routed=bool(t.get("tier_routed")), free_shown=_free_shown,
            tp1_detail={"tag": f"S{ver}", "side": sig, "tp1": tp1, "sl_be": entry, "tp2": tp2},
            entry_date=_ist_date_str(t.get("created_at")), sig_id=t.get("sig_id", ""))
        _notify_free_late(sym, t, "TP1")
        save_state()
        return f"✅ {sym} force-marked TP1 hit @ {price:,.4g} — SL moved to BE, trade stays open for TP2."

    # sl / be
    close_result = "BE" if t["tp1_hit"] else "SL"
    trade_stats["scan_sl"] += 1; trade_stats[f"scan{ver}_sl"] += 1
    _delete_trail_sl_messages(t)
    _log_scan_history(t, close_result, price)
    _send_sl_and_log(fmt_scan_update("SL_HIT", price, t), t.get("reply_map"), t.get("sig_id", ""), close_result, include_ch2=False,
        tier_routed=(close_result == "BE" and bool(t.get("tier_routed"))), share_free=t.get("share_free", True))
    ct.on_scan_sl(sym)
    log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
        "sl_hit_time": _ist_str_now(), "result": close_result,
        "entry_price": entry, "sl_price": t.get("sl", 0)})
    if close_result == "SL":
        _track_daily_result(sym, "SL", tier_routed=bool(t.get("tier_routed")), free_shown=bool(t.get("tier_routed")) and t.get("share_free", True), entry_date=_ist_date_str(t.get("created_at")))
        _send_sl_reassurance(sym, f"S{ver}", sig, entry,
            _sl_reassurance_channels(bool(t.get("tier_routed")), t.get("share_free", True)), t.get("reply_map"), t.get("sig_id", ""))
    _slot_hm = _ist_hm_from_epoch(t.get("created_at"))
    if _slot_hm: _slot_track(f"scan{ver}", _slot_hm, close_result == "BE")
    _close_sig_snapshot(t.get("sig_id", ""), close_result)
    _remove_scan_trade(ver, sym)
    return f"✅ {sym} force-closed as {close_result} @ {price:,.4g} — announced, recorded, removed."

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

_SCAN_DIV    = "┈" * 26

def _gen_signal_id() -> str:
    """Unique per-trade ID shown on every lifecycle message (signal, TP1, TP2, SL,
    timeout) so the same trade can be found/grepped across the whole chat history."""
    return "#CLEX" + "".join(random.choices(_string.ascii_uppercase + _string.digits, k=6))

# --- Signal snapshots — durable lookup for the Free-channel unlock flow -------
# By the time a Free-channel user taps "Unlock", the live trade may already
# have closed and been removed from scan1_trades/scan2_trades/demo lists — this
# is the only durable source for "what were the real numbers" at unlock time.
_SIG_SNAPSHOTS_FILE = os.path.join(DATA_DIR, "sig_snapshots.json")
_sig_snapshots: dict = {}   # sig_id -> {symbol, direction, entry, sl, tp1, tp2, type}

def _save_sig_snapshot(sig_id: str, symbol: str, direction: str, entry, sl, tp1, tp2, kind: str):
    _sig_snapshots[sig_id] = {"symbol": symbol, "direction": direction, "entry": entry,
                               "sl": sl, "tp1": tp1, "tp2": tp2, "type": kind, "created_at": time.time()}
    # Bound memory/storage — keep the most recent 500 (unlocks only matter for
    # signals still fresh enough to be worth revealing).
    if len(_sig_snapshots) > 500:
        for _old in sorted(_sig_snapshots, key=lambda k: _sig_snapshots[k].get("created_at", 0))[:len(_sig_snapshots) - 500]:
            del _sig_snapshots[_old]
    try:
        with open(_SIG_SNAPSHOTS_FILE, "w") as f:
            json.dump(_sig_snapshots, f)
    except Exception as e:
        print(f"[SIG SNAPSHOTS] local save error: {e}")
    if CLEXER_API_URL:
        try:
            _kv_push("sig_snapshots", _sig_snapshots)
        except Exception as e:
            print(f"[SIG SNAPSHOTS] central push error: {e}")

def _close_sig_snapshot(sig_id: str, result: str):
    """Marks a snapshot as closed once its trade hits a terminal outcome — the
    Free-channel unlock flow checks this so a user is never asked to pay to
    unlock a signal that has already finished (win or lose); they're told to
    pick a different one instead."""
    if not sig_id or sig_id not in _sig_snapshots:
        return
    _sig_snapshots[sig_id]["result"] = result
    try:
        with open(_SIG_SNAPSHOTS_FILE, "w") as f:
            json.dump(_sig_snapshots, f)
    except Exception as e:
        print(f"[SIG SNAPSHOTS] local save error: {e}")
    if CLEXER_API_URL:
        try:
            _kv_push("sig_snapshots", _sig_snapshots)
        except Exception as e:
            print(f"[SIG SNAPSHOTS] central push error: {e}")

def _load_sig_snapshots():
    global _sig_snapshots
    try:
        d = None
        if CLEXER_API_URL:
            r = _central_get("/kv/sig_snapshots")
            if r is not None and r.ok:
                d = _kv_pick_newer(_SIG_SNAPSHOTS_FILE, r.json(), "SIG SNAPSHOTS")
        if d is None and os.path.exists(_SIG_SNAPSHOTS_FILE):
            with open(_SIG_SNAPSHOTS_FILE) as f:
                d = json.load(f)
        if d is not None:
            _sig_snapshots = d
            print(f"[SIG SNAPSHOTS] Loaded {len(_sig_snapshots)} snapshot(s)")
    except Exception as e:
        print(f"[SIG SNAPSHOTS] load error: {e}")

# --- Free-channel SL message log — lets admin bulk-delete only the messages --
# belonging to signals that hit a REAL SL loss (never BE/breakeven, which isn't
# a loss and is left alone). Grouped per sig_id so clearing one signal removes
# exactly its own entry + trailing-SL + SL-hit messages from Free — not every
# trailing-SL message site-wide, and never anything from a BE-outcome trade.
_FREE_SL_LOG_FILE = os.path.join(DATA_DIR, "free_sl_log.json")
_free_sl_log: dict = {}   # sig_id -> {"cid": str, "entry_mid": int|None, "trailing_mid": int|None, "sl_mid": int|None, "result": "SL"|"BE"|None}

def _save_free_sl_log():
    try:
        with open(_FREE_SL_LOG_FILE, "w") as f:
            json.dump(_free_sl_log, f)
    except Exception as e:
        print(f"[FREE SL LOG] save error: {e}")
    if CLEXER_API_URL:
        try:
            _kv_push("free_sl_log", _free_sl_log)
        except Exception as e:
            print(f"[FREE SL LOG] central push error: {e}")

def _track_free_sl(sig_id: str, cid: str, field: str, message_id: int):
    """Records one Free-channel message_id (entry/trailing/sl) under its
    signal's sig_id. Only actually queued for deletion later if that signal's
    result turns out to be a real SL (see _finalize_free_sl)."""
    if not sig_id or not message_id:
        return
    entry = _free_sl_log.setdefault(sig_id, {"cid": cid, "entry_mid": None, "trailing_mid": None, "sl_mid": None, "result": None})
    entry["cid"] = cid
    entry[field] = message_id
    if len(_free_sl_log) > 500:
        for _old in list(_free_sl_log)[:len(_free_sl_log) - 500]:
            del _free_sl_log[_old]
    _save_free_sl_log()

def _finalize_free_sl(sig_id: str, result: str):
    """Marks a signal's final outcome (SL or BE) once it closes. Called even
    if the signal was never tracked via _track_free_sl (e.g. no Free message
    ever went out for it) — safe no-op in that case."""
    if not sig_id or sig_id not in _free_sl_log:
        return
    _free_sl_log[sig_id]["result"] = result
    _save_free_sl_log()

def _load_free_sl_log():
    global _free_sl_log
    try:
        d = None
        if CLEXER_API_URL:
            r = _central_get("/kv/free_sl_log")
            if r is not None and r.ok:
                d = _kv_pick_newer(_FREE_SL_LOG_FILE, r.json(), "FREE SL LOG")
        if d is None and os.path.exists(_FREE_SL_LOG_FILE):
            with open(_FREE_SL_LOG_FILE) as f:
                d = json.load(f)
        if d is not None:
            _free_sl_log = d
            print(f"[FREE SL LOG] Loaded {len(_free_sl_log)} tracked signal(s)")
    except Exception as e:
        print(f"[FREE SL LOG] load error: {e}")

def _pending_free_sl_count() -> int:
    """How many signals are actually eligible to be cleared right now — real
    SL result only, BE and still-open ones don't count."""
    return sum(1 for v in _free_sl_log.values() if v.get("result") == "SL")

def _clear_free_sl_messages() -> tuple:
    """Deletes the entry + trailing-SL + SL-hit messages for every signal
    whose final result was a real SL (never BE) from Free, then drops those
    signals from the log. BE and still-open signals are left untouched.
    Returns (deleted_count, failed_count)."""
    global _free_sl_log
    ok = 0; fail = 0
    _remaining = {}
    for sig_id, entry in _free_sl_log.items():
        if entry.get("result") != "SL":
            _remaining[sig_id] = entry   # not a real loss (or still open) — keep, don't touch
            continue
        cid = entry.get("cid")
        for field in ("entry_mid", "trailing_mid", "sl_mid"):
            mid = entry.get(field)
            if not mid:
                continue
            try:
                r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage",
                    json={"chat_id": cid, "message_id": mid}, timeout=10)
                if r.json().get("ok"):
                    ok += 1
                else:
                    fail += 1
            except Exception:
                fail += 1
    _free_sl_log = _remaining
    _save_free_sl_log()
    return ok, fail

def _locked_signal_text(coin: str, tag_label: str, sig_id: str) -> str:
    """Redacted Free-channel entry-signal variant — direction/entry/SL/TP
    replaced with lock placeholders. Same _scan_box template every other
    lifecycle message uses, so it looks native rather than bolted-on."""
    return _scan_box(
        "VIP Signal", f"📣 #{coin}-USDT  |  {tag_label}",
        [[f"🔒 {_smallcaps_title('Direction')}: Locked",
          f"🔒 {_smallcaps_title('Entry')}: Locked",
          "🔒 SL: Locked", "🔒 TP1: Locked", "🔒 TP2: Locked"],
         [f"🔓 {_smallcaps_title('Tap Unlock below to reveal full details in your DM')}"]],
        tag=sig_id,
    )

_load_sig_snapshots()
_load_free_sl_log()

def _deploy_status_box(tv_status: str, source_status: str, charts_on: bool, news_on: bool, paused: bool) -> str:
    """Renders the admin-only startup status message. No box-drawing borders —
    Telegram only guarantees monospace alignment inside <pre>/<code>, and
    <pre>/<code> don't allow nested entities (premium <tg-emoji>), so any
    bordered box built outside <pre> drifts out of alignment depending on
    device/font (confirmed live) while one built inside <pre> loses the
    premium icons and the tappable /go link. A plain list sidesteps the
    conflict entirely — premium icons and /go both work, nothing to misalign."""
    status_line = "⚠️ <b>PAUSED</b>" if paused else "🟢 <b>SCANNING</b>"
    action_line = "▶️ Send /go to start scanning." if paused else "▶️ Scanning live."
    return (
        f"👑 <b>CLEXER V17.8.5 Deployed</b>\n\n"
        f"📡 TV Feed: {tv_status}\n"
        f"🔄 Source: {source_status}\n"
        f"📈 Charts: {'ON' if charts_on else 'OFF (default)'}\n"
        f"📰 News Feed: {'ON' if news_on else 'OFF'}\n\n"
        f"{status_line}\n"
        f"{action_line}\n\n"
    )

def _scan_box(title: str, header: str, sections: list, tag: str = "") -> str:
    """Shared decorative box template for every Scan1/Scan2/Demo lifecycle
    message (entry, TP1, TP2, SL, BE, timeout, etc.) — sections is a list of
    line-lists, each rendered as its own ┃-prefixed block separated by a
    ┈┈┈ divider, so callers just pass their own content and get consistent
    styling for free. tag (if given) is the trade's signal ID, shown just
    above the footer so the same trade can be found across every message."""
    out = [f"✦ {_smallcaps_title(title)} ✦", "",
           f"┃ {header}"]
    for sec in sections:
        out.append(_SCAN_DIV)
        out += [f"┃ {l}" for l in sec]
    out.append(_SCAN_DIV)
    if tag:
        out.append(f"┃ 🪪 {tag}")
    return "\n".join(out)

def fmt_scan_signal(t: dict) -> str:
    sym  = t["symbol"]; sig = t["signal"]
    entry = t["entry"]; sl = t["sl"]; tp1 = t["tp1"]; tp2 = t["tp2"]
    et   = t.get("entry_type","MARKET")
    ver  = t.get("ver", 1)
    sl_pct = abs(entry - sl) / entry * 100 if entry else 0
    coin = sym.replace("-USDT","").replace("USDT","")

    _gw_tag = _gw_model_tag("scan1" if ver == 1 else "scan2")
    if et == "ZONE" and t.get("zone_lo") and t.get("zone_hi"):
        zone_lo, zone_hi = t["zone_lo"], t["zone_hi"]
        dir_lbl = "📉 Short Entry Zone" if sig == "SELL" else "📈 Long Entry Zone"
        sig_id = t.get("sig_id") or f"#ID{int(t.get('created_at', time.time()))}"
        return (
            f"📩 <b>#{coin}USDT</b>  S{ver} {_gw_tag} | Mid-Term\n\n"
            f"{dir_lbl}: <b>{min(zone_lo,zone_hi):,.4g} - {max(zone_lo,zone_hi):,.4g}</b>\n\n"
            f"⏳ Signal Details:\n"
            f"Target 1: <b>{tp1:,.4g}</b>\n"
            f"Target 2: <b>{tp2:,.4g}</b>\n\n"
            f"🔺 Stop-Loss: <b>{sl:,.4g}</b>\n"
            f"💡 After reaching the first target you can put the rest of the position to breakeven.\n\n"
            f"🔎 Signal ID: <i>{sig_id}</i>\n\n"
        )

    arrow = "🟢 LONG" if sig == "BUY" else "🔴 SHORT"
    return _scan_box(
        "Scan Signal",
        f"📣 #{coin}-USDT  |  S{ver} {_gw_tag}",
        [
            [f"{arrow} — {_smallcaps_title('Market Entry')}"],
            [f"🎯 {_smallcaps_title('Entry')}: <code>{entry:,.4g}</code>",
             f"🛑 SL: <code>{sl:,.4g}</code>  ({sl_pct:.1f}%)",
             f"💰 TP1: <code>{tp1:,.4g}</code>",
             f"🏆 TP2: <code>{tp2:,.4g}</code>"],
        ],
        tag=t.get("sig_id",""),
    )

def fmt_scan_update(status: str, price: float = 0, t: dict = None) -> str:
    if t is None: t = scan_active_trade
    coin = t.get('symbol','?')
    sym  = f"#{coin}"; sig = t.get("signal","?")
    ver_lbl = f"S{t.get('ver', 1)}"
    entry = t.get("entry") or 0; tp1 = t.get("tp1",0); tp2 = t.get("tp2",0)
    _hdr = lambda title_emoji, title: f"{title_emoji} #{coin}  |  {ver_lbl}  🕐 {_smallcaps_title(ist_str())}"
    _hdr_notime = lambda title_emoji, title: f"{title_emoji} #{coin}  |  {ver_lbl}"
    _sid = t.get("sig_id","")
    msgs = {
        "ENTRY_HIT": _scan_box(
            "Entry Triggered", _hdr("🚀", "Entry Triggered"),
            [[f"{'🟩' if sig=='BUY' else '🟥'} {sig}",
              f"🎯 {_smallcaps_title('Entry')}: <code>{entry:,.4g}</code>  |  📊 {_smallcaps_title('Price')}: <code>{price:,.4g}</code>",
              f"🛑 SL: <code>{t.get('sl',0):,.4g}</code>", f"💰 TP1: <code>{tp1:,.4g}</code>", f"🏆 TP2: <code>{tp2:,.4g}</code>"],
             [f"⚠️ {_smallcaps_title('Trade is now live')}"]],
            tag=_sid,
        ),
        "TP1_HIT": _scan_box(
            "TP1 Hit", _hdr_notime("💰", "TP1 Hit"),
            [[f"{'🟩' if sig=='BUY' else '🟥'} {sig}", f"✅ TP1: <code>{tp1:,.4g}</code>",
              f"🛡️ {_smallcaps_title('SL moved to BE')}: <code>{entry:,.4g}</code>",
              f"🚀 {_smallcaps_title('Riding TP2')}: <code>{tp2:,.4g}</code>..."]],
            tag=_sid,
        ),
        "TP2_HIT": _scan_box(
            "TP2 Hit", _hdr_notime("🏆", "TP2 Hit"),
            [[f"{'🟩' if sig=='BUY' else '🟥'} {sig}",
              f"✅ {_smallcaps_title('Full profit')} @ TP2: <code>{tp2:,.4g}</code>"]],
            tag=_sid,
        ),
        "SL_HIT": (
            _scan_box(
                "BE Exit", _hdr_notime("🛡️", "BE Exit"),
                [[f"{'🟩' if sig=='BUY' else '🟥'} {sig}",
                  f"✅ {_smallcaps_title('TP1 already hit — closed at entry')} <code>{entry:,.4g}</code>",
                  f"📊 {_smallcaps_title('Result')}: {_smallcaps_title('Breakeven (no loss)')}"]],
                tag=_sid,
            ) if t.get("tp1_hit") else
            _scan_box(
                "SL Hit", _hdr_notime("🚨", "SL Hit"),
                [[f"❌ {_smallcaps_title('Loss on')} {sig} @ <code>{entry:,.4g}</code>"],
                 [f"⛔ {_smallcaps_title('Do not open any trade now')}"]],
                tag=_sid,
            )
        ),
        "ENTRY_MISSED": _scan_box(
            "Entry Missed", _hdr("😔", "Entry Missed"),
            [[f"{_smallcaps_title('Price bypassed entry zone')} <code>{entry:,.4g}</code> {_smallcaps_title('without filling')}."],
             [f"⛔ {_smallcaps_title('Do not chase')}"]],
            tag=_sid,
        ),
        "TIMEOUT": _scan_box(
            "Timeout", _hdr("⏰", "Timeout"),
            [[f"{'🟩' if sig=='BUY' else '🟥'} {sig} {_smallcaps_title('still running after 12 hours — force-closed')}.",
              f"📊 {_smallcaps_title('Result')}: {t.get('_timeout_pnl', '?')}"]],
            tag=_sid,
        ),
        "WAITING_ENTRY": _scan_box(
            "Waiting Entry", _hdr("⏳", "Waiting Entry"),
            [[f"🎯 {_smallcaps_title('Entry')}: <code>{entry:,.4g}</code>", f"🛑 SL: <code>{t.get('sl',0):,.4g}</code>",
              f"💰 TP1: <code>{tp1:,.4g}</code>", f"🏆 TP2: <code>{tp2:,.4g}</code>"]
             + ([f"📊 {_smallcaps_title('Current')}: <code>{price:,.4g}</code> ({abs(price-entry)/entry*100:.2f}% away)"] if price else [])],
            tag=_sid,
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
        elapsed_min = (now_ist().replace(tzinfo=None) - start_dt).total_seconds() / 60
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
            _delete_trail_sl_messages(t)
            _log_scan_history(t, f"TIMEOUT({pnl:+.2f}%)", price)
            send_lifecycle_reply(fmt_scan_update("TIMEOUT", price, t), t.get("reply_map"), include_ch2=False,
                tier_routed=bool(t.get("tier_routed")), share_free=t.get("share_free", True))
            ct.on_scan_sl(sym)
            log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
                "timeout_time": _ist_str_now(), "result": f"TIMEOUT({pnl:+.2f}%)",
                "entry_price": entry, "sl_price": t.get("sl",0)})
            _track_daily_result(sym, "TIMEOUT", tier_routed=bool(t.get("tier_routed")),
                free_shown=bool(t.get("tier_routed")) and t.get("share_free", True),
                entry_date=_ist_date_str(t.get("created_at")), pnl=pnl)
            _slot_hm = _ist_hm_from_epoch(t.get("created_at"))
            if _slot_hm: _slot_track(f"scan{ver}", _slot_hm, pnl >= 0)
            _close_sig_snapshot(t.get("sig_id",""), f"TIMEOUT({pnl:+.2f}%)")
            _remove_scan_trade(ver, sym); return True

        price = get_bingx_price(sym)
        if price <= 0: return False
        # Too soon after entry — a 1m candle can still span/precede the actual
        # entry moment, so its high/low would misattribute a pre-entry wick as a
        # post-entry SL/TP hit (this caused a real false SL within the same
        # minute as entry). Use live price only until at least 1 minute has
        # passed, matching BTC's own tick check.
        mins_since_entry = (time.time() - _created_at) / 60 if _created_at else 999
        if mins_since_entry >= 1:
            df1m = bingx_klines(sym, "1m", 3)
            if df1m is not None and len(df1m) > 0:
                check_high = max(price, float(df1m["high"].max()))
                check_low  = min(price, float(df1m["low"].min()))
            else:
                check_high = price; check_low = price
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
            send_lifecycle_reply(fmt_scan_update("ENTRY_HIT", price, t), t.get("reply_map"), include_ch2=False)

        tp2_hit = (sig == "BUY" and check_high >= tp2) or (sig == "SELL" and check_low <= tp2)
        if tp2_hit:
            trade_stats["scan_tp2"] += 1; trade_stats["scan_tp1"] += (0 if t["tp1_hit"] else 1)
            trade_stats[f"scan{ver}_tp2"] += 1; trade_stats[f"scan{ver}_tp1"] += (0 if t["tp1_hit"] else 1)
            _delete_trail_sl_messages(t)
            _log_scan_history(t, "TP2", price)
            _tp2_msg = fmt_scan_update("TP2_HIT", price, t)
            send_lifecycle_reply(_tp2_msg, t.get("reply_map"), include_ch2=True,
                tier_routed=bool(t.get("tier_routed")), share_free=t.get("share_free", True), reply_markup=_tp_buttons())
            ct.on_scan_tp2(sym)
            log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
                "tp2_hit_time": _ist_str_now(), "result": "TP2",
                "entry_price": entry, "sl_price": t.get("sl",0), "tp2_price": tp2})
            _track_daily_result(sym, "TP2", tier_routed=bool(t.get("tier_routed")), free_shown=t.get("share_free", True),
                entry_date=_ist_date_str(t.get("created_at")), sig_id=t.get("sig_id",""))
            _notify_free_late(sym, t, "TP2")
            _slot_hm = _ist_hm_from_epoch(t.get("created_at"))
            if _slot_hm: _slot_track(f"scan{ver}", _slot_hm, True)
            _close_sig_snapshot(t.get("sig_id",""), "TP2")
            _remove_scan_trade(ver, sym); return True

        if not t["tp1_hit"]:
            # Use current mark price only (not wick) — prevents false triggers from brief spikes
            tp1_hit = (sig == "BUY" and price >= tp1) or (sig == "SELL" and price <= tp1)
            if tp1_hit:
                t["tp1_hit"] = True
                t["sl"] = entry
                sl = entry
                _delete_trail_sl_messages(t)
                trade_stats["scan_tp1"] += 1
                trade_stats[f"scan{ver}_tp1"] += 1
                _tp1_msg = fmt_scan_update("TP1_HIT", price, t)
                send_lifecycle_reply(_tp1_msg, t.get("reply_map"), include_ch2=True,
                    tier_routed=bool(t.get("tier_routed")), share_free=t.get("share_free", True), reply_markup=_tp_buttons())
                ct.on_scan_tp1(sym)
                log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
                    "tp1_hit_time": _ist_str_now(), "result": "TP1_partial",
                    "entry_price": entry, "sl_price": entry, "tp1_price": tp1})
                _free_shown = bool(t.get("tier_routed")) and t.get("share_free", True)
                _track_daily_result(sym, "TP1", tier_routed=bool(t.get("tier_routed")), free_shown=_free_shown,
                    tp1_detail={"tag": f"S{ver}", "side": sig, "tp1": tp1, "sl_be": entry, "tp2": tp2},
                    entry_date=_ist_date_str(t.get("created_at")), sig_id=t.get("sig_id",""))
                _notify_free_late(sym, t, "TP1")
                save_state()  # persist tp1_hit + breakeven SL immediately — a restart before final close must not revert to the pre-TP1 SL

        sl_margin = sl * 0.002
        sl_hit = (sig == "BUY"  and check_low  < sl - sl_margin) or \
                 (sig == "SELL" and check_high > sl + sl_margin)
        if sl_hit:
            trade_stats["scan_sl"] += 1
            trade_stats[f"scan{ver}_sl"] += 1
            result = "BE" if t["tp1_hit"] else "SL"
            _delete_trail_sl_messages(t)
            _log_scan_history(t, result, price)
            _sl_msg = fmt_scan_update("SL_HIT", price, t)
            _send_sl_and_log(_sl_msg, t.get("reply_map"), t.get("sig_id",""), result, include_ch2=False,
                tier_routed=(result == "BE" and bool(t.get("tier_routed"))), share_free=t.get("share_free", True))
            ct.on_scan_sl(sym)
            log_trade_event({"type": f"scan{ver}", "coin": sym, "direction": sig,
                "sl_hit_time": _ist_str_now(), "result": result,
                "entry_price": entry, "sl_price": t.get("sl",0)})
            if result == "SL":
                _track_daily_result(sym, "SL", tier_routed=bool(t.get("tier_routed")), free_shown=bool(t.get("tier_routed")) and t.get("share_free", True), entry_date=_ist_date_str(t.get("created_at")))
                _send_sl_reassurance(sym, f"S{ver}", sig, entry,
                    _sl_reassurance_channels(t.get("tier_routed", False), t.get("share_free", True)), t.get("reply_map"), t.get("sig_id",""))
            _slot_hm = _ist_hm_from_epoch(t.get("created_at"))
            if _slot_hm: _slot_track(f"scan{ver}", _slot_hm, result == "BE")
            _close_sig_snapshot(t.get("sig_id",""), result)
            _remove_scan_trade(ver, sym); return True

    except Exception as e:
        print(f"  [SCAN{ver} {sym} TICK ERROR] {e}")
    return False

def run_scan_tick_check() -> bool:
    any_closed = False
    for t in list(scan1_trades): any_closed |= _tick_one(1, t)
    for t in list(scan2_trades): any_closed |= _tick_one(2, t)
    return any_closed

def _ghost_confirm_close(symbol: str, reason: str = ""):
    """Backup confirmation — copytrade's own monitor_sl_tp() sometimes detects, from a
    copy user's actual BingX order history, that a position for `symbol` already closed
    (e.g. an SL fill) before our own live price-monitor got to it. copytrade.py's
    _detect_close_reason() already knows the REAL result and exit price (e.g.
    "SL hit @ 0.6338") — use that directly via _force_close_scan_trade/_force_close_demo_trade
    instead of a live re-check, since a live re-check (re-testing the SL/TP condition
    against CURRENT price) silently finds nothing and does nothing if price has since
    moved away from that level — leaving the bot's own state (and the channel/portfolio)
    stuck showing an already-closed trade as open forever, with no recovery.
    Falls back to the old live-tick-check behavior if the reason can't be parsed."""
    import re as _gre
    _m = _gre.search(r"(SL|TP1/BE|TP2) hit @ ([\d.]+)", reason)
    result = {"SL": "sl", "TP1/BE": "tp1", "TP2": "tp2"}.get(_m.group(1)) if _m else None
    try:
        if active_trade.get("signal") and SYMBOL == symbol:
            run_tick_check(); return
        if result:
            for ver in (1, 2):
                if any(t.get("symbol") == symbol for t in _scan_list(ver)):
                    print(f"  [GHOST CONFIRM] {symbol} scan{ver} -> {result} ({reason})")
                    send_admin(_force_close_scan_trade(ver, symbol, result)); return
            for dver, lst in ((1, demo_scan1_trades), (2, demo_scan2_trades)):
                if any(t.get("symbol") == symbol for t in lst):
                    print(f"  [GHOST CONFIRM] {symbol} demo{dver} -> {result} ({reason})")
                    send_admin(_force_close_demo_trade(dver, symbol, result)); return
            return
        # Reason didn't parse — fall back to a live re-check (old behavior)
        for ver, lst in ((1, scan1_trades), (2, scan2_trades)):
            for t in list(lst):
                if t.get("symbol") == symbol:
                    _tick_one(ver, t); return
    except Exception as e:
        print(f"  [GHOST CONFIRM] {symbol}: {e}")

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
        _ch2_ok = active_trade.get("is_d48", False)
        _rmap = active_trade.get("reply_map")
        if detect_entry_missed(price):
            trade_stats["missed_entries"] += 1
            log_trade_outcome("ENTRY_MISSED", f"price bypassed entry {active_trade['entry']:,.0f}")
            ct.on_cancel_limits()
            send_lifecycle_reply(fmt_update("ENTRY_MISSED"), _rmap, include_ch2=False); reset_trade(); return True
        if not active_trade["entry_hit"] and detect_entry_invalidated(price, df_4h):
            log_trade_outcome("SETUP_INVALID", "4H closed past SL before entry")
            ct.on_cancel_limits()
            send_lifecycle_reply(fmt_update("SETUP_INVALID"), _rmap, include_ch2=False); reset_trade(); return True
        status = check_price_status(price, high_1h, low_1h, df_5m)
        print(f"  [1H] {active_trade['signal']} | {status}")
        if status == "TP2_HIT":
            trade_stats["total_tp2"] += 1; trade_stats["consecutive_sl"] = 0
            _delete_trail_sl_messages(active_trade)
            log_trade_outcome("TP2_HIT", "hit during 1H check")
            ct.on_tp2(active_trade.get("entry",0), active_trade.get("tp2",0))
            _tp2_msg = fmt_update("TP2_HIT")
            send_lifecycle_reply(_tp2_msg, _rmap, include_ch2=True, tier_routed=True, share_free=active_trade.get("share_free", True), reply_markup=_tp_buttons())
            _track_daily_result(SYMBOL, "TP2", tier_routed=True, free_shown=active_trade.get("share_free", True), entry_date=_ist_date_str(active_trade.get("entry_time")), sig_id=active_trade.get("sig_id","")); _notify_free_late(SYMBOL, active_trade, "TP2")
            _close_sig_snapshot(active_trade.get("sig_id",""), "TP2")
            reset_trade(); return True
        elif status == "SL_HIT":
            trade_stats["total_sl"] += 1; trade_stats["consecutive_sl"] += 1
            n = trade_stats["consecutive_sl"]
            _delete_trail_sl_messages(active_trade)
            log_trade_outcome("SL_HIT", f"{n} in a row during 1H check")
            if n >= 3:
                trade_stats["cooldown_scans"] = 2
                _sl_msg = (
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {active_trade.get('signal','?')} @ {active_trade.get('entry',0):,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 2 scans...")
                _send_sl_and_log(_sl_msg, _rmap, active_trade.get("sig_id",""), "BE" if active_trade.get("tp1_hit", False) else "SL", include_ch2=False)
            elif n == 2:
                trade_stats["cooldown_scans"] = 1
                _sl_msg = (
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {active_trade.get('signal','?')} @ {active_trade.get('entry',0):,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 1 scan...")
                _send_sl_and_log(_sl_msg, _rmap, active_trade.get("sig_id",""), "BE" if active_trade.get("tp1_hit", False) else "SL", include_ch2=False)
            else:
                _sl_msg = fmt_update("SL_HIT")
                _send_sl_and_log(_sl_msg, _rmap, active_trade.get("sig_id",""), "BE" if active_trade.get("tp1_hit", False) else "SL", include_ch2=False)
            if not active_trade.get("tp1_hit", False):
                _track_daily_result(SYMBOL, "SL", tier_routed=True, free_shown=active_trade.get("share_free", True), entry_date=_ist_date_str(active_trade.get("entry_time")))  # breakeven exit after TP1 isn't a real loss
                _send_sl_reassurance(SYMBOL, "BTC", active_trade.get("signal","?"), active_trade.get("entry",0),
                    _sl_reassurance_channels(True, active_trade.get("share_free", True)), active_trade.get("reply_map"), active_trade.get("sig_id",""))
            _close_sig_snapshot(active_trade.get("sig_id",""), "BE" if active_trade.get("tp1_hit", False) else "SL")
            ct.on_sl(active_trade.get("entry",0), active_trade.get("sl",0), tp1_hit=active_trade.get("tp1_hit", False)); reset_trade(); return True
        elif status == "TP1_HIT" and not active_trade["tp1_hit"]:
            active_trade["tp1_hit"] = True; active_trade["sl"] = active_trade["entry"]
            trade_stats["total_tp1"] += 1; trade_stats["consecutive_sl"] = 0
            _delete_trail_sl_messages(active_trade)
            save_active_trade()
            ct.on_tp1(active_trade["entry"], active_trade.get("tp1",0))
            _tp1_msg = fmt_update("TP1_HIT")
            send_lifecycle_reply(_tp1_msg, _rmap, include_ch2=True, tier_routed=True, share_free=active_trade.get("share_free", True), reply_markup=_tp_buttons())
            _track_daily_result(SYMBOL, "TP1", tier_routed=True, free_shown=active_trade.get("share_free", True),
                tp1_detail={"tag": "BTC", "side": active_trade.get("signal","?"),
                    "tp1": active_trade.get("tp1",0), "sl_be": active_trade.get("entry",0),
                    "tp2": active_trade.get("tp2",0)},
                entry_date=_ist_date_str(active_trade.get("entry_time")), sig_id=active_trade.get("sig_id",""))
            _notify_free_late(SYMBOL, active_trade, "TP1")
        elif status in ("STOP_HUNT",):      send_lifecycle_reply(fmt_update("STOP_HUNT"), _rmap, include_ch2=False)
        elif status in ("ENTRY_MISSED","SETUP_INVALID"):
            log_trade_outcome(status, ""); ct.on_cancel_limits()
            send_lifecycle_reply(fmt_update(status), _rmap, include_ch2=False); reset_trade(); return True
        elif status == "WAITING_ENTRY":
            active_trade["scan_count"] += 1; send_lifecycle_reply(fmt_update("WAITING_ENTRY", price), _rmap, include_ch2=False)
        elif status == "RUNNING":
            active_trade["scan_count"] += 1  # trade running, no message needed
    except Exception as e: print(f"  [1H ERROR] {e}")
    return False

# --- NEWS (free Binance liquidation feed — "Trending Insights" style cards) ---
# Whale Alert has no permanent free tier (7-day trial only, confirmed), so this
# uses Binance Futures' PUBLIC liquidation WebSocket instead — genuinely free,
# no API key, no rate limit, no signup. A big LONG liquidation = forced selling
# (bearish pressure); a big SHORT liquidation = forced buying (bullish pressure).
_last_liq_post_time = 0

def _handle_liquidation_msg(raw: str):
    global _last_liq_post_time, latest_news_context
    if not SEND_NEWS:
        return
    try:
        d = json.loads(raw).get("o", {})
        sym   = d.get("s", "?")
        side  = d.get("S", "?")          # side of the liquidation order itself
        price = float(d.get("ap", 0) or d.get("p", 0) or 0)
        qty   = float(d.get("q", 0) or 0)
        usd   = price * qty
        if usd < LIQUIDATION_MIN_USD:
            return
        now = time.time()
        if now - _last_liq_post_time < LIQUIDATION_POST_COOLDOWN:
            return
        # SELL-side liquidation order = a LONG position got force-closed (bearish);
        # BUY-side liquidation order = a SHORT position got force-closed (bullish).
        if side == "SELL":
            impact, emoji, closed = "BEARISH", "🔴", "LONG"
        else:
            impact, emoji, closed = "BULLISH", "🟢", "SHORT"
        title = f"{usd/1e6:,.2f}M {sym} {closed} liquidated"
        msg_text = (
            f"<b>TRENDING INSIGHT</b>\n\n{emoji} <b>{impact}</b>\n"
            f"<b>{title}</b>\n\n"
            f"💥 {closed} position force-closed\n"
            f"💰 Size: <code>{qty:,.4g} {sym.replace('USDT','')}</code> (${usd:,.0f})\n"
            f"💵 Price: <code>{price:,.4g}</code>\n\n"
            f"<i>{ist_str()}</i>"
        )
        send_telegram(msg_text)
        send_to_tier_channels(msg_text, share_free=True)
        _last_liq_post_time = now
        latest_news_context = ([f"• {impact}: {title}"] + latest_news_context)[:3]
    except Exception as e:
        print(f"  [LIQUIDATION] parse/post error: {e}")

def _liquidation_ws_loop():
    """Runs forever in a background thread — reconnects automatically on drop.
    Binance's !forceOrder@arr stream is public/unauthenticated and covers every
    liquidation across the whole futures market, not just one symbol."""
    if not HAS_WEBSOCKET:
        print("  [LIQUIDATION] websocket-client not installed — feed disabled"); return
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    while True:
        try:
            ws = _ws_client.create_connection(url, timeout=30)
            print("  [LIQUIDATION] connected")
            while True:
                msg = ws.recv()
                if msg:
                    _handle_liquidation_msg(msg)
        except Exception as e:
            print(f"  [LIQUIDATION] connection error: {e} — reconnecting in 10s")
        time.sleep(10)

# --- CRYPTO PAY (@CryptoBot) — invoices + payment event poll loop -------------
CRYPTO_PAY_BASE_URL = "https://pay.crypt.bot/api"

def _cryptopay_create_invoice(amount_usd: float, payload_dict: dict, description: str = "") -> str:
    """Creates a CryptoBot invoice, returns its pay_url or None on failure.
    payload_dict is echoed back verbatim (as a JSON string) in the webhook
    once paid — that's how api.py's /cryptopay/webhook knows what the payment
    was for and which user, without us needing a separate pending-invoice table."""
    if not CRYPTO_PAY_API_TOKEN:
        print("  [CRYPTOPAY] CRYPTO_PAY_API_TOKEN not set"); return None
    try:
        r = requests.post(f"{CRYPTO_PAY_BASE_URL}/createInvoice",
            headers={"Crypto-Pay-API-Token": CRYPTO_PAY_API_TOKEN},
            json={"asset": "USDT", "amount": f"{amount_usd:.2f}",
                  "description": description or "CLEXER payment",
                  "payload": json.dumps(payload_dict)},
            timeout=15)
        rj = r.json()
        if rj.get("ok"):
            return rj["result"]["pay_url"]
        print(f"  [CRYPTOPAY] createInvoice failed: {rj}")
    except Exception as e:
        print(f"  [CRYPTOPAY] createInvoice error: {e}")
    return None

def _grant_vip(cid: str, days: int = 30):
    """Grants VIP for `days` days from today — mirrors /setvip's exact field
    shape (copytrade.py's handle(), '/setvip <cid> <start> <end>' branch):
    DD.MM.YYYY date strings, tier='vip', vip_grace_notified_at reset to 0.
    Shared by both payment gateways (CryptoBot and Telegram Stars) since the
    grant itself doesn't care how the user paid."""
    u = ct._db.get(str(cid)) or ct._default_user(cid)
    start = now_ist(); end = start + timedelta(days=days)
    u["tier"] = "vip"
    u["vip_start"] = start.strftime("%d.%m.%Y")
    u["vip_end"] = end.strftime("%d.%m.%Y")
    u["vip_grace_notified_at"] = 0
    ct._set(cid, u)

# Tracks each unpaid Stars invoice's own message_id, keyed by (cid, type, sig_id) —
# so once successful_payment arrives, the invoice card itself can be deleted
# instead of sitting in the chat forever after payment. sig_id is "" for
# vip/topup payloads (only one field-set that can collide on cid+type there).
_pending_star_invoices: dict = {}

def _stars_send_invoice(chat_id, title: str, description: str, payload_dict: dict, amount_usd: float) -> bool:
    """Sends a Telegram Stars (XTR) invoice — Telegram renders its own native
    pay button right in the chat, no external checkout page and no provider
    token needed. payload_dict is echoed back verbatim as the invoice_payload
    on the resulting successful_payment update (see command_listener), the
    same role CryptoBot's invoice payload plays for _poll_payment_events."""
    stars = max(1, round(amount_usd * STARS_PER_USD))
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendInvoice",
            json={"chat_id": chat_id, "title": title[:32], "description": description[:255],
                  "payload": json.dumps(payload_dict), "provider_token": "", "currency": "XTR",
                  "prices": [{"label": title[:32], "amount": stars}]}, timeout=15)
        rj = r.json()
        if rj.get("ok"):
            _mid = rj.get("result", {}).get("message_id")
            if _mid:
                _key = (str(chat_id), payload_dict.get("type"), payload_dict.get("sig_id", ""))
                _pending_star_invoices[_key] = _mid
            return True
        print(f"  [STARS] sendInvoice failed: {rj}")
    except Exception as e:
        print(f"  [STARS] sendInvoice error: {e}")
    return False

def _poll_payment_events():
    """Runs forever — every 30s, applies any unprocessed CryptoBot payment
    events (wallet topup / VIP purchase) via ct._get/_set. Safe against races:
    this is the one long-running process that owns ct._db in memory, and the
    webhook (api.py, a separate stateless process) only ever INSERTs rows into
    payment_events — never touches the user DB directly. See api.py's
    /cryptopay/webhook and the payment_events table for the other half."""
    if not CLEXER_API_URL:
        print("  [PAYMENT EVENTS] CLEXER_API_URL not set — poll loop disabled"); return
    while True:
        try:
            time.sleep(30)
            hdrs = {"X-Push-Secret": PUSH_STATE_SECRET} if PUSH_STATE_SECRET else {}
            r = requests.get(f"{CLEXER_API_URL}/payment_events", params={"processed": "false"}, headers=hdrs, timeout=10)
            if not r.ok:
                continue
            for ev in r.json().get("events", []):
                try:
                    cid = ev["cid"]; etype = ev["event_type"]; amount = float(ev["amount"])
                    if etype == "topup":
                        u = ct._db.get(str(cid)) or ct._default_user(cid)
                        u["wallet_balance"] = round(u.get("wallet_balance", 0) + amount, 2)
                        ct._set(cid, u)
                        send_to_user(cid, f"💰 <b>Wallet credited</b>: +${amount:,.2f}\n\nNew balance: <b>${u['wallet_balance']:,.2f}</b>")
                    elif etype == "vip":
                        _grant_vip(cid, days=30)
                        send_to_user(cid, f"🎉 <b>VIP Activated!</b>\n\nPaid: ${amount:,.2f} · 30 days\n\nTap ⭐ VIP Channel in /help to get access.")
                    requests.post(f"{CLEXER_API_URL}/payment_events/{ev['id']}/ack", headers=hdrs, timeout=10)
                except Exception as e:
                    print(f"  [PAYMENT EVENTS] apply error for event {ev.get('id')}: {e}")
        except Exception as e:
            print(f"  [PAYMENT EVENTS] poll error: {e}")

def check_news(force=False):
    """Kept for /latestnews compatibility — the liquidation feed posts live as
    events happen (see _liquidation_ws_loop), so there's nothing to "fetch on
    demand" the way the old RSS/Whale-Alert polling model worked. Just reports
    feed status instead."""
    if not force:
        return
    if not HAS_WEBSOCKET:
        send_admin("⚠️ websocket-client not installed — liquidation feed can't run."); return
    send_admin("ℹ️ Liquidation feed runs continuously in the background — nothing to fetch on demand. "
               f"Posts big liquidations (≥ ${LIQUIDATION_MIN_USD:,.0f}) live as they happen when News is ON.")

# --- /tvstatus ----------------------------------------------------------------
def cmd_tvstatus(chat_id):
    if not TV_BRIDGE_URL:
        send_reply(chat_id, "<b>TV Status</b>\n\nTV_BRIDGE_URL not set.\nRunning on <b>Binance</b>."); return
    send_reply(chat_id, f"Checking...\n<code>{TV_BRIDGE_URL}</code>")
    now = time.time(); health = tv_ping()
    if not health:
        ls = tv_bridge_state.get("last_seen",0)
        since = f"{int((now-ls)//60)}m ago" if ls else "never"
        send_reply(chat_id, f"<b>TV Status</b>\n\n🔴 Bridge OFFLINE\nLast seen: {since}\n\nUsing: <b>Binance</b>"); return
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
        f"Uptime: <b>{uptime_str}</b>\n\n{ist_str()}")

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
/st - Special times, verified/unverified + win rate
/nt - Non-special (regular grid) times win rate
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

FRIEND_COMMANDS = {"/start","/help","/status","/price","/trade","/history","/stats","/session","/chat","/endchat"}

# False = scan uses BingX candles + matplotlib (default, no TV bridge needed)
# True  = scan uses TV bridge candles + TV screenshots (old behaviour)
SCAN_USE_TV = False

ADMIN_COMMANDS  = {"/go","/signal","/pause","/resume","/resetsl","/setinterval",
    "/close","/sltobe","/setsl","/settp1","/settp2","/tvstatus",
    "/broadcast","/users","/allusers","/user","/kick","/pauseuser",
    "/images","/setimages","/news","/latestnews",
    "/pausechannel","/resumechannel","/channels","/btcmode",
    "/scan","/scan1","/scan2","/scantoggle","/model","/gateway","/stop","/pause","/coin","/ctclose","/closetrade","/closescan","/scancopy","/readindicators","/checktvdata","/tvstudies","/calcstudies","/scantv",
    "/compare","/charts","/chartson","/chartsoff","/force_reload","/miniapp","/ctstatus","/ctretry","/btcanalysis","/demo","/synccheck","/forceclose","/fc","/report","/tradelog","/alt","/alt2","/altdemo","/altdemo2","/adminlinks","/userstats","/aiconfig","/entrystyle","/coadmin","/tp1size","/freelimit","/winrate","/wrscan1","/wrscan2","/wrts1","/wrts2","/channelmgmt","/trailsl","/syncup","/server","/testreply","/testar","/st","/nt","/list","/un","/ws","/clearslfree","/resetspins","/setvipprice","/chatmodel","/statsaccess"}

# ---- Date-range navigation (year -> monthly/weekly -> month -> week) for /tradelog and /report ----
_MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
_WEEK_RANGES = [(1,7),(8,14),(15,21),(22,31)]

def _dnav_label(report_type):
    return "Trade History" if report_type == "tradelog" else "API Cost Report"

def _dnav_years_mkp(report_type):
    cur_year = now_ist().year
    rows = [[{"text": str(y), "callback_data": f"dnav:{report_type}:year:{y}"}] for y in range(2026, cur_year + 1)]
    rows.append([{"text": "◀️  Back", "callback_data": "settings_sub:data"}])
    return {"inline_keyboard": rows}

def _dnav_period_mkp(report_type, year):
    return {"inline_keyboard": [
        [{"text": "📅 Monthly", "callback_data": f"dnav:{report_type}:period:{year}:monthly"},
         {"text": "📆 Weekly",  "callback_data": f"dnav:{report_type}:period:{year}:weekly"}],
        [{"text": "◀️  Back", "callback_data": f"dnav:{report_type}:years"}],
    ]}

def _dnav_months_mkp(report_type, year, period):
    rows, row = [], []
    for i, mn in enumerate(_MONTH_NAMES, start=1):
        row.append({"text": mn, "callback_data": f"dnav:{report_type}:month:{year}:{period}:{i}"})
        if len(row) == 4:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([{"text": "◀️  Back", "callback_data": f"dnav:{report_type}:year:{year}"}])
    return {"inline_keyboard": rows}

def _dnav_weeks_mkp(report_type, year, month):
    row = [{"text": f"Week{i} [{lo}-{hi}]", "callback_data": f"dnav:{report_type}:week:{year}:{month}:{i}"}
           for i, (lo, hi) in enumerate(_WEEK_RANGES, start=1)]
    return {"inline_keyboard": [row, [{"text": "◀️  Back", "callback_data": f"dnav:{report_type}:period:{year}:weekly"}]]}

def _dnav_row_date(report_type, row):
    if report_type == "report":
        try:
            return datetime.strptime(row.get("date", ""), "%Y-%m-%d").date()
        except Exception:
            return None
    for col in ("signal_time", "entry_trigger_time", "tp1_hit_time", "tp2_hit_time", "sl_hit_time", "timeout_time"):
        v = row.get(col)
        if v:
            try:
                return datetime.strptime(v.replace(" IST", "").strip(), "%Y-%m-%d %H:%M").date()
            except Exception:
                continue
    return None

def _dnav_send_file(chat_id, report_type, year, month, week=None, message_id=None):
    csv_path = TRADE_LOG_CSV if report_type == "tradelog" else API_COST_LOG
    if not os.path.exists(csv_path):
        send_reply(chat_id, "📂 No data file yet."); return
    import csv as _csv, io
    with open(csv_path, "r") as f:
        reader = _csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    if week:
        lo, hi = _WEEK_RANGES[week - 1]
        filtered = [r for r in rows if (d := _dnav_row_date(report_type, r)) and d.year == year and d.month == month and lo <= d.day <= hi]
        period_label = f"{year} {_MONTH_NAMES[month-1]} Week{week} [{lo}-{hi}]"
        fname_part = f"{year}_{_MONTH_NAMES[month-1]}_Week{week}"
    else:
        filtered = [r for r in rows if (d := _dnav_row_date(report_type, r)) and d.year == year and d.month == month]
        period_label = f"{year} {_MONTH_NAMES[month-1]}"
        fname_part = f"{year}_{_MONTH_NAMES[month-1]}"

    if not filtered:
        send_reply(chat_id, f"📂 No data for {period_label} yet.")
        return

    buf = io.StringIO()
    writer = _csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(filtered)
    content = buf.getvalue().encode("utf-8")
    base_name = "trade_history" if report_type == "tradelog" else "api_cost_log"
    fname = f"{base_name}_{fname_part}.csv"
    send_reply(chat_id, f"📂 <b>{_dnav_label(report_type)} — {period_label}</b>\n\n{len(filtered)} row(s) found.")
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
        data={"chat_id": chat_id, "caption": f"{period_label} — {len(filtered)} rows"},
        files={"document": (fname, content, "text/csv")}, timeout=30)

def handle_command(text, chat_id, message=None, sender_id=None):
    global SIGNAL_SCAN_INTERVAL, SEND_CHARTS, CHART_TFS, SEND_NEWS, last_force_scan_time, broadcast_pending, BTC_PROMPT_MODE, btc_analysis_enabled, ALT_SCAN_MINUTE, ALT_SCAN2_MINUTE, _auto_scan1_last_hour, _auto_scan2_last_hour, SCAN1_SCHEDULE, SCAN2_SCHEDULE, SCAN1_AUTO_ENABLED, SCAN2_AUTO_ENABLED, TEST_SCAN_ENABLED, SCAN_MODEL, USE_AEROLINK, SCAN1_TEST_SCHEDULE, SCAN2_TEST_SCHEDULE, CONTACT_ADMIN_ENABLED, SIGNAL_CHANNEL_ENABLED, SIGNAL_CHANNEL_LINK, FREE_SIGNAL_DAILY_LIMIT, CHANNELS, VIP_MONTHLY_PRICE, CHAT_MODEL, STATS_VISIBLE_TO_USERS
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

    if cmd in ("/forceclose", "/fc") and is_admin:
        if len(parts) < 4:
            send_reply(chat_id,
                "<b>Force Close a Stuck Trade</b>\n\n"
                "For when a redeploy made the bot lose track of a trade that already "
                "closed on BingX — runs the exact same close path as if the bot had "
                "caught it live (channel announcement, recap, win-rate tracking).\n\n"
                "Usage: <code>/fc s1|s2|t1|t2 SYMBOL tp1|tp2|sl|be</code>\n"
                "Example: <code>/fc s2 home tp2</code>")
            return
        _fc_kind = parts[1].lower(); _fc_symbol = parts[2]; _fc_result = parts[3]
        _fc_map = {
            "scan1": (1, "scan"), "s1": (1, "scan"),
            "scan2": (2, "scan"), "s2": (2, "scan"),
            "ts1": (1, "demo"), "t1": (1, "demo"),
            "ts2": (2, "demo"), "t2": (2, "demo"),
        }
        if _fc_kind not in _fc_map:
            send_reply(chat_id, "First arg must be s1, s2, t1, or t2 (or scan1/scan2/ts1/ts2)."); return
        _fc_ver, _fc_type = _fc_map[_fc_kind]
        _fc_result_text = (_force_close_scan_trade(_fc_ver, _fc_symbol, _fc_result) if _fc_type == "scan"
            else _force_close_demo_trade(_fc_ver, _fc_symbol, _fc_result))
        send_reply(chat_id, _fc_result_text)

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
                    elif cb.startswith("reset_scan_ghost_"):
                        parts3 = cb.split("_"); sym = parts3[4] if len(parts3) > 4 else "?"
                        row.append({"text": f"🔄 Reset {sym.replace('-USDT','')}", "callback_data": f"sync_reset_scan_ghost:{uid}:{sym}"})
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
        # Deep-link payload from a locked Free-channel signal's Unlock button
        # (t.me/bot?start=unlock_XXXXXX) — "#" is stripped from sig_id when the
        # button URL is built (Telegram deep-link params only allow [A-Za-z0-9_-]),
        # so it must be re-added here to match the "#CLEXxxxxxx" keys in _sig_snapshots.
        if cmd == "/start" and len(parts) > 1 and parts[1].startswith("unlock_"):
            send_unlock_screen(chat_id, str(_hm_uid), "#" + parts[1][len("unlock_"):])
            return
        if cmd == "/start" and len(parts) > 1 and parts[1] == "vip":
            send_vip_offer_screen(chat_id, str(_hm_uid))
            return
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
                ""); return
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
                results = ct.on_sl(sig.get("entry",0), sig.get("sl",0), tp1_hit=active_trade.get("tp1_hit", False))
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
                    "")
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
            f"Use ▶️ Resume to restart.", reply_markup=_ctrl_btns)

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
            f"❌ News blocked", reply_markup=_ctrl_btns)

    elif cmd == "/btcanalysis":
        arg = parts[1].lower() if len(parts) > 1 else ""
        if arg in ("on", "off"):
            btc_analysis_enabled = (arg == "on")
            save_settings()
        _btca_mkp = {"inline_keyboard": [[
            {"text": "🟢 Enable Analysis",  "callback_data": "btca_on"},
            {"text": "🔴 Disable Analysis", "callback_data": "btca_off"},
        ], [
            {"text": "◀️  Back", "callback_data": "settings_sub:btcsettings"},
        ]]}
        if btc_analysis_enabled:
            _btca_text = "📡 <b>BTC Analysis</b>  ✅ ON\n\n<blockquote>Scheduled scans active.\n\n</blockquote>"
        else:
            _btca_text = "📡 <b>BTC Analysis</b>  ⏸ OFF\n\n<blockquote>Scheduled scans paused.\n\n</blockquote>"
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
        # Per-viewer trade visibility: admin/co-admin sees everything, tagged
        # by verified/unverified/non-special so they can tell which slots are
        # copy-trade-safe; VIP sees only verified (special + copy-enabled)
        # signals; Free sees only trades it actually got (share_free=True),
        # plus a locked "VIP-exclusive" tag (no numbers) for anything that
        # was routed to VIP only — never a fully-hidden trade Free never
        # heard of at all, and never a full reveal of a VIP-only trade.
        _user_ct = ct._get(str(chat_id))
        _tier_val = (_user_ct or {}).get("tier", "free")
        _full_status_view = is_admin or is_co_admin(_check_id)
        def _status_line(label, sig, sym, entry, sl, tp1, entry_hit, tp1_hit, share_free, tier_routed, kind, created_at, extra=""):
            _cat = _status_trade_cat(kind, created_at)
            _dir = "🟢" if sig == "BUY" else "🔴"
            _reveal, _locked = _trade_reveal(_cat, share_free, tier_routed, _tier_val, _full_status_view)
            _prefix = f"{_CAT_TAG.get(_cat,'➖')} " if (_reveal and _full_status_view) else ""
            if _reveal:
                return (f"\n\n<b>{label}:</b> {_prefix}{_dir} {sig} {sym}\n"
                        f"Entry:{entry:,.4g} {'✅' if entry_hit else '⏳'}  "
                        f"SL:{sl:,.4g}  TP1:{tp1:,.4g} {'✔️' if tp1_hit else ''}{extra}")
            if _locked:
                return f"\n\n<b>{label}:</b> 🔒 VIP-exclusive signal — upgrade to view"
            return ""
        scan_lines = ""
        for _ver, _lst in [(1, scan1_trades), (2, scan2_trades)]:
            for sc in _lst:
                scan_lines += _status_line(f"Scan{_ver}", sc['signal'], sc['symbol'], sc['entry'], sc['sl'], sc['tp1'],
                    sc.get('entry_hit'), sc.get('tp1_hit'), sc.get('share_free', True), sc.get('tier_routed', True),
                    f"scan{_ver}", sc.get('created_at'))
        for _dlst in (demo_scan1_trades, demo_scan2_trades):
            for dc in _dlst:
                _cp = get_bingx_price(dc.get("symbol","")) if dc.get("symbol") else 0
                _pnl = (_cp - dc["entry"]) / dc["entry"] * 100 * (1 if dc["signal"]=="BUY" else -1) if _cp and dc.get("entry") else 0
                _dver = dc.get('scan_ver', 1)
                scan_lines += _status_line(f"TS{_dver}", dc['signal'], dc.get('symbol','?'), dc.get('entry',0), dc.get('sl',0), dc.get('tp1',0),
                    True, dc.get('tp1_hit'), dc.get('share_free', True), dc.get('tier_routed', True),
                    f"demo{_dver}", dc.get('created_at'), extra=f"  P/L:{_pnl:+.2f}%")
        _next_btc_scan, _, _ = _next_schedule_times()
        _next_scan1 = _next_special_time("scan1")
        _next_scan2 = _next_special_time("scan2")
        _next_test1 = _next_special_time("test1")
        _next_test2 = _next_special_time("test2")
        _next_btc_line = f"⏰ Next BTC scan:   <b>{_next_btc_scan} IST</b>\n" if btc_analysis_enabled else "⏰ Next BTC scan:   <b>OFF</b>\n"
        _next_s1_line  = f"⏰ Next Scan1:      <b>{_next_scan1}</b>\n" if (not bot_paused.is_set() and SCAN1_AUTO_ENABLED) else "⏰ Next Scan1:      <b>OFF</b>\n"
        _next_s2_line  = f"⏰ Next Scan2:      <b>{_next_scan2}</b>\n" if (not bot_paused.is_set() and SCAN2_AUTO_ENABLED) else "⏰ Next Scan2:      <b>OFF</b>\n"
        _next_ts1_line = f"⏰ Next TS1:        <b>{_next_test1}</b>\n" if (not bot_paused.is_set() and TEST_SCAN_ENABLED) else "⏰ Next TS1:        <b>OFF</b>\n"
        _next_ts2_line = f"⏰ Next TS2:        <b>{_next_test2}</b>\n" if (not bot_paused.is_set() and TEST_SCAN_ENABLED) else "⏰ Next TS2:        <b>OFF</b>\n"
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
        # (_user_ct / _tier_val already computed above for the trade-visibility filter)
        _copy_flag = "✅ ON" if (_user_ct and _user_ct.get("copy_on")) else "❌ OFF"
        _tier_tag = ("⭐ VIP" + (f" (until {_user_ct['vip_end']})" if _user_ct and _user_ct.get("vip_end") else "")) if _tier_val == "vip" else "🆓 FREE"
        _users_summary = _build_users_summary()
        send_reply(chat_id,
            f"<b>CLEXER V17.8.5</b>  |  {ist_str()}\n\n"
            f"🤖 Bot:        <b>{st}</b>\n"
            + (
                f"📡 BTC Scan:   <b>{_btc_flag}</b>  ({_btcmode_lbl})\n"
                f"🔍 Alt Scan:   {_alt_flag}\n"
                f"🧠 BTC Model:  <b>{_model_lbl}</b>\n"
                f"🔌 BTC Gateway:<b>{_gateway_lbl}</b>\n"
                f"<i>(Scan1/Scan2/TS1/TS2 each have their own via /aiconfig)</i>\n"
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
            + (
                f"\n{_next_btc_line}"
                f"{_next_s1_line}"
                f"{_next_s2_line}"
                f"{_next_ts1_line}"
                f"{_next_ts2_line}"
                if is_admin else ""
            )
            + f"\n📊 Session: {get_session()} | Conf: {required_confidence()} | SL streak: {trade_stats['consecutive_sl']}\n"
            + (_users_summary if is_admin else "")
            + (f"📡 Source: {src} | TV: {tv_status}\n" if is_admin else "")
            + (f"{cd}" if cd else "")
            + f"\n<b>BTC Trade:</b>\n{ti}"
            + scan_lines,
            emoji_overrides={"🟢": "5262747715552438702", "🔴": "5809816842713174497", "✔️": "5206607081334906820"})

    elif cmd == "/price":
        try:
            tk = get_ticker()
            send_reply(chat_id, f"<b>BTCUSDT</b>\n\nPrice: <b>{tk['price']:,.2f}</b>\n"
                f"24h: {tk['change']:+.2f}% | Vol: ${tk['volume']/1e6:.1f}M\n"
                f"H:{tk['high24']:,.2f}  L:{tk['low24']:,.2f}\n"
                f"Source: {tk.get('source',get_current_source())}\n{ist_str()}")
        except Exception as e: send_reply(chat_id, f"Error: {e}")

    elif cmd == "/chat":
        _chat_sessions[str(chat_id)] = {"last": time.time(), "history": []}
        if CHAT_MODEL == "google" and not GEMINI_API_KEY:
            send_reply(chat_id, "⚠️ Chat AI isn't configured yet — admin needs to set GEMINI_API_KEY.")
        else:
            _is_admin_own_chat = ADMIN_CHAT_ID and str(chat_id) == str(ADMIN_CHAT_ID)
            _reply_note = "" if _is_admin_own_chat else ("\n\n↪️ In a group: <b>reply directly to one of my messages</b> to get a response.\n"
                "In a private chat: <b>forward a message here</b> instead — plain typed messages won't trigger a response.")
            send_reply(chat_id,
                "💬 <b>Chat Session Started</b>\n\n"
                "Ask me anything about crypto, trading, market analysis, or general questions.\n\n"
                "🎨 Need an image? Just describe what you want.\n\n"
                f"⏳ Session will automatically close after 5 minutes of inactivity, or end it anytime with /endchat.{_reply_note}")

    elif cmd == "/endchat":
        if _chat_sessions.pop(str(chat_id), None) is not None:
            send_reply(chat_id, "💬 <b>Chat Session Ended</b>\n\nType /chat anytime to start a new one.")
        else:
            send_reply(chat_id, "⚠️ You don't have an active chat session. Type /chat to start one.")

    elif cmd == "/chatmodel" and is_admin:
        _model_labels = {"google": "🟢 Google (Gemini)", "sonnet": "🔵 Sonnet 5", "opus": "🟠 Opus 4.8"}
        if len(parts) < 2:
            send_reply(chat_id,
                f"<b>/chat AI Engine</b>\n\nCurrent: <b>{_model_labels[CHAT_MODEL]}</b>\n\n"
                f"Usage: /chatmodel g|s|o\n"
                f"g = Google (Gemini, free) — default\n"
                f"s = Sonnet 5 (Claude, highest Sonnet)\n"
                f"o = Opus 4.8 (Claude, highest Opus)\n\n"
                f"This is a global switch — every user's /chat uses whichever engine you pick here; "
                f"users can't change it themselves.\n\n"
                f"<i>Note: /model is already taken (scan-analysis model picker), so this lives at /chatmodel instead.</i>")
        else:
            _arg = parts[1].lower()
            _new = {"g": "google", "s": "sonnet", "o": "opus"}.get(_arg)
            if not _new:
                send_reply(chat_id, "Usage: /chatmodel g|s|o")
            else:
                CHAT_MODEL = _new; save_settings()
                send_reply(chat_id, f"<b>/chat engine → {_model_labels[CHAT_MODEL]}</b> ✅")

    elif cmd == "/testar" and is_admin and chat_id > 0:
        # chat_id > 0 = DM only, never fires in a channel/group
        threading.Thread(target=_test_agentrouter, args=(chat_id,), daemon=True).start()

    elif cmd == "/testreply" and is_admin:
        _test_entry = _scan_box(
            "Reply Test — Entry", "🧪 #TESTCOIN-USDT  |  Reply Threading Check",
            [[f"🎯 Entry: 100.00", f"🛑 SL: 95.00", f"💰 TP1: 110.00", f"🏆 TP2: 120.00"]],
            tag="#CLEXTEST01",
        )
        _mid = _send_plain_reply(chat_id, _test_entry)
        if not _mid:
            send_reply(chat_id, "❌ <b>Test FAILED at step 1</b> — couldn't send/forward the entry message at all. Check ADMIN_CHAT_ID and bot permissions.")
            return
        time.sleep(1)
        _test_reply = _scan_box(
            "Reply Test — TP1", "🧪 #TESTCOIN-USDT  |  This should reply to the message above ⬆️",
            [["✅ If this message shows a quoted preview linking to the entry above, reply-threading is WORKING."]],
            tag="#CLEXTEST01",
        )
        _mid2 = _send_plain_reply(chat_id, _test_reply, reply_to=_mid)
        if _mid2:
            send_reply(chat_id, "✅ Test sent — scroll up: does the 2nd message show <b>\"Reply to CRYPTO CLEXER\"</b> quoting the 1st message? If yes, reply-threading works correctly.")
        else:
            send_reply(chat_id, "❌ <b>Test FAILED at step 2</b> — entry sent OK, but the reply message failed to send. Check logs for [PLAIN REPLY] errors.")

    elif cmd == "/st":
        _st_labels = {"scan1": "SCAN1", "scan2": "SCAN2", "demo1": "DEMO TS1", "demo2": "DEMO TS2"}
        _st_blocks = []
        for _kind in ("scan1", "scan2", "demo1", "demo2"):
            _sched_kind = _SLOT_SCHEDULE_KIND[_kind]   # demo1->test1, demo2->test2 — each independent
            _times = sorted(_SCAN_SPECIAL.get(_sched_kind, set()))
            if not _times:
                continue
            _rows = []
            for _hm in _times:
                _unverified = _hm in _SCAN_SPECIAL_NO_COPY.get(_sched_kind, set())
                _key = _slot_key(_kind, _hm)
                _stat = _slot_stats.get(_key)
                _hm_str = f"{_hm[0]}:{_hm[1]:02d}"
                if _stat and (_stat["tp"] + _stat["sl"]) > 0:
                    _total = _stat["tp"] + _stat["sl"]
                    _wr = f"{_stat['tp'] / _total * 100:.0f}%"
                    _cnt = f"{_stat['tp']}/{_stat['sl']}"
                    _streak = str(_stat.get("streak", 0))
                else:
                    _wr = "—%"; _cnt = "0/0"; _streak = "0"
                _icon = "🔒" if _unverified else "✅"
                # One shared <pre> per KIND block (not per row) — a separate <pre>
                # on every single line makes Telegram render each row as its own
                # standalone copyable code card instead of a compact table. <pre>
                # (vs <code>) genuinely preserves whitespace exactly, like a
                # terminal — <code> silently collapses repeated spaces, breaking
                # padding-based column alignment even when the underlying text is
                # correct. The icon has to live INSIDE the shared <pre> now (can't
                # sit outside per-row anymore since the block spans many lines) —
                # excluded from premium-emoji wrapping via emoji_overrides below,
                # since a <tg-emoji> tag nested inside <pre> is invalid.
                # Kept short (no "streak" word, tight spacing) — a wider row wraps
                # on phone screens in monospace, dropping the tail onto its own
                # line and creating a big visual gap between rows.
                _rows.append(f"{_icon} {_hm_str:<6} {_wr:<4} {_cnt:<4} s{_streak}")
            _st_blocks.append(f"<b>{_st_labels[_kind]}</b> ({_SLOT_EVAL_THRESHOLD[_kind]}%)\n<pre>" + "\n".join(_rows) + "</pre>")
        if not _st_blocks:
            send_reply(chat_id, "No special times configured."); return
        send_reply(chat_id, "⭐ <b>Special Times</b>\n\n" + "\n\n".join(_st_blocks) +
            "", emoji_overrides={"✅": None, "🔒": None})

    elif cmd == "/nt":
        # Non-special (regular grid) times — same table shape as /st, but for
        # everything /st doesn't cover. Only shows slots with actual tracked
        # data (tp+sl > 0) since the full regular grid is dozens of untested
        # slots per kind and dumping all of them would just be noise.
        _nt_labels = {"scan1": "SCAN1", "scan2": "SCAN2", "demo1": "DEMO TS1", "demo2": "DEMO TS2"}
        _nt_grid_minutes = {"scan1": (2, 23), "scan2": (7, 27), "demo1": (9, 27), "demo2": (9, 27)}
        _nt_blocks = []
        for _kind in ("scan1", "scan2", "demo1", "demo2"):
            _sched_kind = _SLOT_SCHEDULE_KIND[_kind]
            _ma, _mb = _nt_grid_minutes[_kind]
            # Include times relocated here after a 1:3 blacklist elsewhere (minus
            # any that have since been promoted to special, shown in /st instead),
            # and exclude anything currently blacklisted (retired for good).
            _relocated_now = _SLOT_RELOCATED.get(_kind, set()) - _SCAN_SPECIAL.get(_sched_kind, set())
            _regular = (_regular_grid(_ma, _mb, _SCAN_SPECIAL.get(_sched_kind, set())) | _relocated_now) \
                - _SLOT_BLACKLIST.get(_kind, set())
            _rows = []
            for _hm in sorted(_regular):
                _key = _slot_key(_kind, _hm)
                _stat = _slot_stats.get(_key)
                if not _stat or (_stat["tp"] + _stat["sl"]) == 0:
                    continue
                _total = _stat["tp"] + _stat["sl"]
                _wr = f"{_stat['tp'] / _total * 100:.0f}%"
                _cnt = f"{_stat['tp']}/{_stat['sl']}"
                _streak = str(_stat.get("streak", 0))
                _hm_str = f"{_hm[0]}:{_hm[1]:02d}"
                _rows.append(f"{_hm_str:<6} {_wr:<4} {_cnt:<4} s{_streak}")
            if _rows:
                # One shared <pre> per KIND block, not one per row — a separate
                # <pre> on every line makes Telegram render each row as its own
                # standalone copyable code card instead of a compact table.
                _nt_blocks.append(f"<b>{_nt_labels[_kind]}</b> ({_SLOT_EVAL_THRESHOLD[_kind]}%)\n<pre>" + "\n".join(_rows) + "</pre>")
        if not _nt_blocks:
            send_reply(chat_id, "No non-special times have tracked data yet."); return
        send_reply(chat_id, "📊 <b>Non-Special Times</b>\n\n" + "\n\n".join(_nt_blocks) +
            "")

    elif cmd == "/list":
        _bl_labels = {"scan1": "SCAN1", "scan2": "SCAN2", "demo1": "DEMO TS1", "demo2": "DEMO TS2"}
        _bl_blocks = []
        for _kind in ("scan1", "scan2", "demo1", "demo2"):
            _bl_times = sorted(_SLOT_BLACKLIST.get(_kind, set()))
            if not _bl_times:
                continue
            _bl_rows = [f"<code>{_bh}:{_bm:02d}</code>" for _bh, _bm in _bl_times]
            _bl_blocks.append(f"<b>{_bl_labels[_kind]}</b> ({len(_bl_times)})\n" + "\n".join(_bl_rows))
        if not _bl_blocks:
            send_reply(chat_id, "No blacklisted times — nothing has hit a 1:3 ratio yet."); return
        send_reply(chat_id, "🚫 <b>Blacklisted Times</b>\n\n" + "\n\n".join(_bl_blocks) +
            "\n\n<i>Use /un s1|s2|t1|t2 H.MM to clear one, e.g. /un s1 3.09</i>")

    elif cmd == "/un":
        if len(parts) < 3:
            send_reply(chat_id, "Usage: /un s1|s2|t1|t2 H.MM\nExample: /un s1 3.09"); return
        _un_map = {"s1": "scan1", "s2": "scan2", "t1": "demo1", "t2": "demo2"}
        _un_kind = _un_map.get(parts[1].lower())
        if not _un_kind:
            send_reply(chat_id, "First arg must be s1, s2, t1, or t2."); return
        try:
            _un_h, _un_m = parts[2].split(".")
            _un_hm = (int(_un_h), int(_un_m))
        except Exception:
            send_reply(chat_id, "Time must be H.MM, e.g. 3.09"); return
        if _un_hm not in _SLOT_BLACKLIST.get(_un_kind, set()):
            send_reply(chat_id, f"{_un_kind} {_un_hm[0]}:{_un_hm[1]:02d} isn't blacklisted."); return
        _SLOT_BLACKLIST[_un_kind].discard(_un_hm)
        _slot_stats.pop(_slot_key(_un_kind, _un_hm), None)  # reset to fresh 0/0
        _rebuild_schedules()
        _save_slot_state()
        send_reply(chat_id, f"✅ {_un_kind} {_un_hm[0]}:{_un_hm[1]:02d} un-blacklisted — reset to 0/0, scanning resumes there.")

    elif cmd == "/trade":
        parts_out = []
        # Same viewer-tier filtering as /status — admin/co-admin sees
        # everything (tagged by category), VIP sees only verified trades,
        # Free sees trades it actually got plus a locked VIP tag for the
        # rest. BTC is intentionally unfiltered, shown to everyone.
        _trade_user_ct = ct._get(str(chat_id))
        _trade_tier_val = (_trade_user_ct or {}).get("tier", "free")
        _trade_full_view = is_admin or is_co_admin(_check_id)
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
                _kind = f"scan{_ver}"
                _cat = _status_trade_cat(_kind, sc.get('created_at'))
                _reveal, _locked = _trade_reveal(_cat, sc.get('share_free', True), sc.get('tier_routed', True),
                                                  _trade_tier_val, _trade_full_view)
                if not _reveal:
                    if _locked:
                        parts_out.append(f"<b>Scan{_ver} Trade</b>\n\n🔒 VIP-exclusive signal — upgrade to view")
                    continue
                try:
                    sp = get_bingx_price(sc["symbol"])
                    spl = f"Current: <b>{sp:,.4g}</b>\n" if sp else ""
                except: spl = ""
                # Check tp1_hit from bot state OR from any copy user's state
                _tp1_hit = sc.get('tp1_hit') or ct.is_scan_tp1_hit(sc["symbol"])
                _sl_label = f"<b>{sc['sl']:,.4g}</b>" + (" ← BE" if _tp1_hit else "")
                _cat_tag = f"{_CAT_TAG.get(_cat,'➖')} " if _trade_full_view else ""
                parts_out.append(
                    f"<b>Scan{_ver} Trade</b>\n\n{_cat_tag}{sc['signal']} - {sc['symbol']}\n{spl}"
                    f"Entry: <b>{sc['entry']:,.4g}</b> {'✅' if sc.get('entry_hit') else '⏳ pending'}\n"
                    f"SL:    {_sl_label}\n"
                    f"TP1:   <b>{sc['tp1']:,.4g}</b> {'✅ HIT' if _tp1_hit else '⏳ pending'}\n"
                    f"TP2:   <b>{sc['tp2']:,.4g}</b>\nType:  {sc.get('entry_type','MARKET')}"
                )
        # Demo trades
        for _dlst in (demo_scan1_trades, demo_scan2_trades):
            for dc in _dlst:
                _dver = dc.get('scan_ver', 1)
                _kind = f"demo{_dver}"
                _cat = _status_trade_cat(_kind, dc.get('created_at'))
                _reveal, _locked = _trade_reveal(_cat, dc.get('share_free', True), dc.get('tier_routed', True),
                                                  _trade_tier_val, _trade_full_view)
                if not _reveal:
                    if _locked:
                        parts_out.append(f"<b>TS{_dver} ALT SIGNAL</b>\n\n🔒 VIP-exclusive signal — upgrade to view")
                    continue
                try: _dcp = get_bingx_price(dc.get("symbol","")); _dcpl = f"Current: <b>{_dcp:,.4g}</b>\n" if _dcp else ""
                except: _dcp = 0; _dcpl = ""
                _dpnl = (_dcp - dc["entry"]) / dc["entry"] * 100 * (1 if dc["signal"]=="BUY" else -1) if _dcp and dc.get("entry") else 0
                _cat_tag = f"{_CAT_TAG.get(_cat,'➖')} " if _trade_full_view else ""
                parts_out.append(
                    f"<b>TS{_dver} ALT SIGNAL</b>\n\n{_cat_tag}{dc['signal']} - {dc.get('symbol','?')}\n{_dcpl}"
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
            {"text": "🔍 Scan2", "callback_data": "history_scan2"}],
            [{"text": "🧪 TS1", "callback_data": "history_ts1"},
             {"text": "🧪 TS2", "callback_data": "history_ts2"}],
        ]
        if is_admin:
            _hist_btns_rows.append([{"text": "🗑 Reset History", "callback_data": "reset_signal_history"}])
        _hist_btns = {"inline_keyboard": _hist_btns_rows}
        # Last 5 only ever shows wins (TP1/TP2/BE) — SL and any TIMEOUT result
        # (win or loss) are excluded entirely, per admin request.
        def _is_shown_result(res: str) -> bool:
            return not (res.startswith("SL") or res.startswith("TIMEOUT"))
        def _fmt_hist_entry(s: dict) -> list:
            res = s.get("result", "?")
            em = "🏆" if res == "TP2" else "💰"
            price = s.get("tp2") if res == "TP2" else s.get("tp1", 0)
            label = "TP1" if res == "BE" else res
            try:
                _dt = datetime.strptime(s.get("time", ""), "%d %b %Y  %I:%M %p IST")
                _tstr = f"{_dt.day} {_dt.strftime('%b')} • {_dt.strftime('%I:%M %p')}"
            except Exception:
                _tstr = s.get("time", "")
            return [f"{em} {s['symbol']} • {s['signal']}",
                    f"🎯 {label} Hit • {price:,.4g}",
                    f"🕒 {_tstr}"]
        if sub in ("scan1", "scan2"):
            ver = sub[-1]
            _sh = [s for s in scan_history if str(s.get("ver","1")) == ver and _is_shown_result(s.get("result","?"))]
            if not _sh:
                send_reply(chat_id, f"📜 <b>Scan{ver} History</b>\n\nNo wins yet.", reply_markup=_hist_btns); return
            _entries = list(reversed(_sh[-5:]))
            lines = [f"📜 <b>Scan{ver} History</b>", ""]
            for i, s in enumerate(_entries):
                lines += _fmt_hist_entry(s)
                if i < len(_entries) - 1: lines.append("──────────────")
            send_reply(chat_id, "\n".join(lines), reply_markup=_hist_btns)
        elif sub in ("ts1", "ts2"):
            dver = sub[-1]
            _dh = [s for s in demo_history if str(s.get("dver",1)) == dver and _is_shown_result(s.get("result","?"))]
            if not _dh:
                send_reply(chat_id, f"📜 <b>TS{dver} History</b>\n\nNo wins yet.", reply_markup=_hist_btns); return
            _entries = list(reversed(_dh[-5:]))
            lines = [f"📜 <b>TS{dver} History</b>", ""]
            for i, s in enumerate(_entries):
                lines += _fmt_hist_entry(s)
                if i < len(_entries) - 1: lines.append("──────────────")
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
        if not STATS_VISIBLE_TO_USERS and not is_admin and not is_co_admin(_check_id):
            send_reply(chat_id, "⚠️ Win rate & trade statistics are currently disabled by admin."); return
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
                "<b>Mini App Control</b>", reply_markup=_mini_btns)
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
                send_reply(chat_id, f"🔧 Mini App {'⏸ PAUSED' if on else '▶️ RESUMED (state synced)'}", reply_markup=_mini_btns)
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
            reset_trade(); send_telegram(f"<b>Trade Closed</b>\n{info}")
            send_reply(chat_id, f"Closed: {info}"); force_scan.set()

    elif cmd == "/sltobe":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        else:
            old = active_trade["sl"]; active_trade["sl"] = active_trade["entry"]
            ct.on_sl_to_be(active_trade["entry"])
            send_telegram(f"<b>SL -> BE</b>  {old:,.0f} -> <b>{active_trade['entry']:,.0f}</b>")
            send_reply(chat_id, f"SL -> {active_trade['entry']:,.0f}")

    elif cmd == "/setsl":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /setsl 61500")
        else:
            try:
                v = float(parts[1].replace(",","")); old = active_trade["sl"]
                active_trade["sl"] = v
                ct.on_update_sl(v)
                send_telegram(f"<b>SL</b>  {old:,.0f} -> <b>{v:,.0f}</b>")
                send_reply(chat_id, f"SL = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /setsl 61500")

    elif cmd == "/settp1":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp1 63000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp1"] = v
                send_telegram(f"<b>TP1 -> {v:,.0f}</b>")
                send_reply(chat_id, f"TP1 = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /settp1 63000")

    elif cmd == "/settp2":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp2 65000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp2"] = v
                send_telegram(f"<b>TP2 -> {v:,.0f}</b>")
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
                                   msg.usage.input_tokens, msg.usage.output_tokens,
                                   gateway="Aerolink" if _ai_aerolink("btc") else "Direct")
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
                                   msg.usage.input_tokens, msg.usage.output_tokens,
                                   gateway="Aerolink" if _ai_aerolink("btc") else "Direct")
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
                f"<b>V9 Current</b> — CRITICAL header, silent scan, Rule 8 hard block", reply_markup=_bmode_btns)
        elif parts[1].lower() == "on":
            BTC_PROMPT_MODE = "V7"; save_settings()
            send_reply(chat_id,
                f"<b>BTC Mode → 🔵 V7 CLASSIC</b> ✅\n\n"
                f"Narrated scan | min 2 pause candles | no Rule 8", reply_markup=_bmode_btns)
        elif parts[1].lower() == "off":
            BTC_PROMPT_MODE = "V9"; save_settings()
            send_reply(chat_id,
                f"<b>BTC Mode → 🟠 V9 CURRENT</b> ✅\n\n"
                f"CRITICAL header | silent scan | Rule 8 hard block", reply_markup=_bmode_btns)
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
                f"TFs: <b>{', '.join(CHART_TFS).upper()}</b>",
                reply_markup=_img_btns)
        elif parts[1].lower()=="on":
            SEND_CHARTS = True; save_settings()
            send_reply(chat_id, f"✅ <b>Charts ON</b>\nTFs: {', '.join(CHART_TFS).upper()}", reply_markup=_img_btns)
        elif parts[1].lower()=="off":
            SEND_CHARTS = False; save_settings()
            send_reply(chat_id, "❌ <b>Charts OFF</b>", reply_markup=_img_btns)
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
                f"<b>Crypto News</b>\n\nStatus: <b>{'✅ ON' if SEND_NEWS else '❌ OFF'}</b>",
                reply_markup=_news_btns)
        elif parts[1].lower()=="on":
            SEND_NEWS = True; save_settings()
            _ws_note = "" if HAS_WEBSOCKET else "\n\n⚠️ websocket-client not installed on this server — feed can't run."
            send_reply(chat_id, f"✅ <b>News ON</b> — Liquidation feed (Trending Insights){_ws_note}", reply_markup=_news_btns)
        elif parts[1].lower()=="off":
            SEND_NEWS = False; save_settings()
            send_reply(chat_id, "❌ <b>News OFF</b>", reply_markup=_news_btns)
        else: send_reply(chat_id, "Usage: /news on|off", reply_markup=_news_btns)

    elif cmd == "/ws":
        global WEEKEND_SLEEP_ENABLED
        _ws_btns = {"inline_keyboard": [[
            {"text": "🟢  ON",  "callback_data": "weekendsleep_on"},
            {"text": "🔴  OFF", "callback_data": "weekendsleep_off"}]]}
        if len(parts) < 2:
            send_reply(chat_id,
                f"<b>Weekend Sleep</b>\n\nStatus: <b>{'✅ ON (bot pauses Fri 10PM → Sun 11PM IST)' if WEEKEND_SLEEP_ENABLED else '❌ OFF (bot runs straight through the weekend)'}</b>",
                reply_markup=_ws_btns)
        elif parts[1].lower() == "on":
            WEEKEND_SLEEP_ENABLED = True; save_settings()
            send_reply(chat_id, "✅ <b>Weekend Sleep ON</b> — bot will pause Fri 10PM → Sun 11PM IST as usual.", reply_markup=_ws_btns)
        elif parts[1].lower() == "off":
            WEEKEND_SLEEP_ENABLED = False; save_settings()
            send_reply(chat_id, "❌ <b>Weekend Sleep OFF</b> — bot will now run straight through the weekend, no Fri-Sun pause.", reply_markup=_ws_btns)
        else: send_reply(chat_id, "Usage: /ws on|off", reply_markup=_ws_btns)

    elif cmd == "/statsaccess" and is_admin:
        _sa_btns = {"inline_keyboard": [[
            {"text": "🟢  ON",  "callback_data": "statsaccess_on"},
            {"text": "🔴  OFF", "callback_data": "statsaccess_off"}]]}
        if len(parts) < 2:
            send_reply(chat_id,
                f"<b>Win Rate & Trade Stats — User Access</b>\n\nStatus: <b>{'✅ ON (users can see /stats)' if STATS_VISIBLE_TO_USERS else '❌ OFF (hidden from users)'}</b>\n\n"
                f"Admin and co-admin always keep access regardless of this setting.",
                reply_markup=_sa_btns)
        elif parts[1].lower() == "on":
            STATS_VISIBLE_TO_USERS = True; save_settings()
            send_reply(chat_id, "✅ <b>Win Rate & Trade Stats → ON</b> — users can now see /stats again.", reply_markup=_sa_btns)
        elif parts[1].lower() == "off":
            STATS_VISIBLE_TO_USERS = False; save_settings()
            send_reply(chat_id, "❌ <b>Win Rate & Trade Stats → OFF</b> — hidden from regular users (admin/co-admin unaffected).", reply_markup=_sa_btns)
        else: send_reply(chat_id, "Usage: /statsaccess on|off", reply_markup=_sa_btns)

    elif cmd == "/vip":
        send_vip_offer_screen(chat_id, str(_check_id))

    elif cmd == "/addfunds":
        send_addfunds_screen(chat_id)

    elif cmd == "/clearslfree":
        _n = _pending_free_sl_count()
        if not _n:
            send_reply(chat_id, "📭 No real-SL signals in Free channel(s) to clear right now (BE outcomes are never touched)."); return
        _ask_confirm(chat_id, _check_id, "clear_free_sl",
            f"Delete {_n} signal(s) that hit a real SL from Free channel(s)? For each one this removes its entry signal, its trailing-SL notice, and its SL-hit message — never BE/breakeven trades, and never other signals' trailing-SL messages.",
            "help_main")

    elif cmd == "/resetspins":
        _n = sum(1 for u in ct._db.values() if u.get("vip_spin_amount") or u.get("vip_spin_month"))
        if not _n:
            send_reply(chat_id, "📭 No users currently have a locked VIP spin price."); return
        _ask_confirm(chat_id, _check_id, "reset_all_spins",
            f"Reset the locked spin price for {_n} user(s)? Everyone will be able to spin again immediately, instead of waiting for next month.",
            "help_main")

    elif cmd == "/setvipprice":
        if len(parts) < 2:
            send_reply(chat_id, f"<b>VIP Monthly Price</b>\n\nCurrent: <b>${VIP_MONTHLY_PRICE:.2f}/month</b>\n\nUsage: <code>/setvipprice 15</code>"); return
        try:
            _new_price = float(parts[1])
            if _new_price <= 0: raise ValueError
        except ValueError:
            send_reply(chat_id, "⚠️ Enter a valid number greater than 0, e.g. <code>/setvipprice 20</code>"); return
        VIP_MONTHLY_PRICE = _new_price
        save_settings()
        send_reply(chat_id, f"✅ <b>VIP price set to ${VIP_MONTHLY_PRICE:.2f}/month</b>")

    elif cmd == "/latestnews":
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

    elif cmd == "/winrate" and is_scanadmin:
        send_winrate_screen(chat_id)

    elif cmd in ("/wrscan1", "/wrscan2", "/wrts1", "/wrts2") and is_scanadmin:
        _wr_kind = {"/wrscan1": "scan1", "/wrscan2": "scan2", "/wrts1": "demo1", "/wrts2": "demo2"}[cmd]
        _wr_label = {"scan1": "Scan1", "scan2": "Scan2", "demo1": "TS1", "demo2": "TS2"}[_wr_kind]
        if len(parts) < 2:
            send_reply(chat_id,
                f"<b>{_wr_label} Win Rate Target</b>\n\nCurrent: <b>{_SLOT_EVAL_THRESHOLD[_wr_kind]}%</b>\n\n"
                f"Use the tap keypad or type a number 1–99.")
            return
        try:
            n = int(parts[1])
            if n < 1 or n > 99:
                send_reply(chat_id, "Win rate target must be 1–99."); return
            _SLOT_EVAL_THRESHOLD[_wr_kind] = n
            save_settings()
            send_reply(chat_id, f"<b>{_wr_label} Win Rate Target Set</b>\n\n{n}% required to promote/stay verified.")
        except ValueError:
            send_reply(chat_id, "Please enter a valid whole number.")

    elif cmd == "/freelimit" and is_admin:
        if len(parts) < 2:
            send_reply(chat_id,
                f"<b>Free Channel Share %</b>\n\nCurrent: <b>{FREE_SIGNAL_DAILY_LIMIT}%</b> of each day's "
                f"verified/special signals also shown in Free (window 06:00–19:00 IST)\n\n"
                f"Use the tap keypad or type a number 0–100.")
            return
        try:
            n = int(parts[1])
            if n < 0 or n > 100:
                send_reply(chat_id, "Share % must be 0–100."); return
            FREE_SIGNAL_DAILY_LIMIT = n
            save_settings()
            send_reply(chat_id, f"<b>Free Channel Share % Set</b>\n\n{n}% of verified signals now shared to Free, 06:00–19:00 IST.")
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
                ""); return
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
                f"Admin closed all positions.")
            send_reply(chat_id, f"✅ All positions closed.\nBTC trade reset + Scan1 ({scan1_count}) + Scan2 ({scan2_count}) trades cleared.")
        else:
            results = ct.close_coin_all(coin)
            # If it's BTC and we have an active BTC trade, also reset it
            if coin in ("BTC","BTCUSDT","BTC-USDT") and active_trade["signal"]:
                log_trade_outcome("MANUAL_CLOSE", f"admin /closetrade {coin}")
                reset_trade()
            reply = f"<b>Close {coin.upper()}-USDT</b>\n\n" + "\n".join(results)
            send_reply(chat_id, reply + "")

    elif cmd == "/closescan" and is_scanadmin:
        s1 = len(scan1_trades); s2 = len(scan2_trades)
        _syms = {t["symbol"] for t in scan1_trades + scan2_trades if t.get("symbol")}
        for _sym in _syms:
            ct.close_coin_all(_sym)
        scan1_trades.clear(); scan2_trades.clear(); save_state()
        send_reply(chat_id,
            f"✅ <b>Scan trades cleared</b>\n\n"
            f"Scan1: {s1} removed\nScan2: {s2} removed\nClosed on BingX: {', '.join(_syms) if _syms else 'none'}")

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
                f"Current times:\n<code>{_sched_str}</code>", reply_markup=_alt_btns); return
        # /alt loop 2  → every hour at :02
        if parts[1].lower() == "loop" and len(parts) > 2:
            try: new_min = int(parts[2]); assert 0 <= new_min <= 59
            except: send_reply(chat_id, "❌ Usage: /alt loop 02"); return
            SCAN1_SCHEDULE = sorted(set((h, new_min) for h in range(24)))
            _clear_own_triggers(_scan1_triggered_today, 1)
            send_reply(chat_id, f"✅ <b>Scan1 → Loop Mode</b>\n\nRuns every hour at <b>:{new_min:02d}</b>", reply_markup=_alt_btns); return
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
            _clear_own_triggers(_scan1_triggered_today, 1)
            _times = "\n".join(f"• {h}:{m:02d} IST" for h,m in SCAN1_SCHEDULE)
            _rej_note = f"\n\n⚠️ Ignored invalid: <code>{' '.join(rejected)}</code>" if rejected else ""
            send_reply(chat_id, f"✅ <b>Scan1 → Manual Times</b>\n\n{_times}{_rej_note}", reply_markup=_alt_btns); return
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
                f"Current times:\n<code>{_sched2_str}</code>", reply_markup=_alt2_btns); return
        if parts[1].lower() == "loop" and len(parts) > 2:
            try: new_min = int(parts[2]); assert 0 <= new_min <= 59
            except: send_reply(chat_id, "❌ Usage: /alt2 loop 24"); return
            SCAN2_SCHEDULE = sorted(set((h, new_min) for h in range(24)))
            _clear_own_triggers(_scan1_triggered_today, 2)
            send_reply(chat_id, f"✅ <b>Scan2 → Loop Mode</b>\n\nRuns every hour at <b>:{new_min:02d}</b>", reply_markup=_alt2_btns); return
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
            _clear_own_triggers(_scan1_triggered_today, 2)
            _times = "\n".join(f"• {h}:{m:02d} IST" for h,m in SCAN2_SCHEDULE)
            _rej_note = f"\n\n⚠️ Ignored invalid: <code>{' '.join(rejected)}</code>" if rejected else ""
            send_reply(chat_id, f"✅ <b>Scan2 → Manual Times</b>\n\n{_times}{_rej_note}", reply_markup=_alt2_btns); return
        send_reply(chat_id, "❌ Tap a button below 👇", reply_markup=_alt2_btns); return

    elif cmd == "/altdemo" and is_scanadmin:
        _altd_btns = {"inline_keyboard": [[
            {"text": "📋  Manual Times", "callback_data": "alt_manual:3"},
        ], [
            {"text": "🔢  Tap to Pick Times", "callback_data": "tp_start:demo1"},
        ]]}
        _sched_str = "  ".join(f"{h}:{m:02d}" for h,m in SCAN1_TEST_SCHEDULE)
        if len(parts) < 2:
            send_reply(chat_id,
                f"⏰ <b>TS1 Schedule</b>\n\n"
                f"Current times:\n<code>{_sched_str}</code>", reply_markup=_altd_btns); return
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
            _clear_own_triggers(_test_triggered_today, 1)
            _times = "\n".join(f"• {h}:{m:02d} IST" for h,m in SCAN1_TEST_SCHEDULE)
            _rej_note = f"\n\n⚠️ Ignored invalid: <code>{' '.join(rejected)}</code>" if rejected else ""
            send_reply(chat_id, f"✅ <b>TS1 → Manual Times</b>\n\n{_times}{_rej_note}", reply_markup=_altd_btns); return
        send_reply(chat_id, "❌ Tap a button below 👇", reply_markup=_altd_btns); return

    elif cmd == "/altdemo2" and is_scanadmin:
        _altd2_btns = {"inline_keyboard": [[
            {"text": "📋  Manual Times", "callback_data": "alt_manual:4"},
        ], [
            {"text": "🔢  Tap to Pick Times", "callback_data": "tp_start:demo2"},
        ]]}
        _sched2_str = "  ".join(f"{h}:{m:02d}" for h,m in SCAN2_TEST_SCHEDULE)
        if len(parts) < 2:
            send_reply(chat_id,
                f"⏰ <b>TS2 Schedule</b>\n\n"
                f"Current times:\n<code>{_sched2_str}</code>", reply_markup=_altd2_btns); return
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
            SCAN2_TEST_SCHEDULE = sorted(set(new_slots))
            _clear_own_triggers(_test_triggered_today, 2)
            _times = "\n".join(f"• {h}:{m:02d} IST" for h,m in SCAN2_TEST_SCHEDULE)
            _rej_note = f"\n\n⚠️ Ignored invalid: <code>{' '.join(rejected)}</code>" if rejected else ""
            send_reply(chat_id, f"✅ <b>TS2 → Manual Times</b>\n\n{_times}{_rej_note}", reply_markup=_altd2_btns); return
        send_reply(chat_id, "❌ Tap a button below 👇", reply_markup=_altd2_btns); return

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
            _help_edit_or_send(chat_id,
                "\n".join(lines) + f"\n\nTotal rows: {len(rows)}\n\n📅 <b>Select a year:</b>",
                _dnav_years_mkp("tradelog"), rotate=False)
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
            _help_edit_or_send(chat_id,
                "\n".join(lines) + "\n\n📅 <b>Select a year:</b>",
                _dnav_years_mkp("report"), rotate=False)
        except Exception as e:
            send_reply(chat_id, f"❌ Error: {e}")
        return

    elif cmd == "/test" and is_scanadmin:
        global _test_scan1_last_hour, _test_scan2_last_hour
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
                    lines.append(f"{'🟩' if sig=='BUY' else '🟥'} {sym}  Entry:{entry:,.4g}  SL:{sl:,.4g}  TP1:{tp1h}  P/L:{pnl:+.2f}%")
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
                f"🧪 Demo Trade  —  <b>{_ts}</b>", reply_markup=_mkp)
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
                        f"TV mode is ON. Start TV bridge or run /scantv off to use BingX mode.")
                    return

                # ── Check slot availability (scan1=6 slots, scan2=6 slots) ──────
                # Special-time signals (the verified, tier-routed ones) are NEVER
                # blocked by this cap, even if all 6 slots are occupied by regular-
                # grid/testing trades — a blocked special time is a real missed
                # opportunity, so it always gets to place, going over the 6-slot pool.
                _max_slots = 6
                my_list = _scan_list(scan_ver)
                _kind = "scan1" if scan_ver == 1 else "scan2"
                _is_special_now = _scan_run_mode.get(_kind) == "special"
                if len(my_list) >= _max_slots and not _is_special_now:
                    send_reply(cid,
                        f"🚫 <b>Scan{scan_ver} slots full ({_max_slots}/{_max_slots})</b>\n\n" +
                        "\n".join(f"  {'🟢' if x['signal']=='BUY' else '🔴'} {x['symbol']}" for x in my_list) +
                        f"\n\nWaiting for a trade to close before scanning again.")
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
                _aero_bad_keys = set()  # Aerolink keys that failed anywhere THIS scan cycle — once a
                # key fails on one coin it's skipped on every later coin too, instead of being
                # retried from scratch each time (a no-credit key isn't going to fix itself
                # between coin #1 and coin #3). Resets fresh on the next scan cycle.

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
                    # candidate["price"] is a snapshot from the ticker fetch at the very
                    # START of this scan cycle — fine for coin #1, but #2/#3 only get
                    # tried after #1's full Claude analysis (real wall-clock minutes),
                    # by which point that snapshot can be badly stale. Refetch live
                    # price right as this candidate's own turn starts, so the integrity
                    # check, the AI prompt, and any SL/TP math all use a current price —
                    # the entry itself still gets refetched again right before actually
                    # placing the trade, this just fixes everything upstream of that.
                    _fresh_cp = get_bingx_price(chosen_sym)
                    if _fresh_cp:
                        cp = _fresh_cp
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
                    # Wrapped in a function (not built inline once) so it can be
                    # rebuilt with a FRESH price on every retry attempt below —
                    # previously the price was baked in once before the retry loop,
                    # so a coin needing 2-3+ retries (10s sleep each) fed the AI an
                    # increasingly stale "current price" the whole time.
                    def _build_analysis_prompt(_price):
                        if BTC_PROMPT_MODE == "V7":
                            _prompt = f"""{smc}
BTC: ${btc_price:,.0f} | Session: {get_session()} | Current price: {_price:,.6g}

You are CLEXER. Analyze {chosen_sym} for MARKET entry at current price {_price:,.6g}.
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
Entry: {_price:,.6g}
Entry_Type: MARKET
SL: [number only]
TP1: [number only]
TP2: [number only]
R:R: [number only]
Confidence: HIGH / MED / LOW
Reasoning: [one line]"""
                            return _prompt, 200
                        _prompt = f"""{smc}
BTC: ${btc_price:,.0f} | Session: {get_session()} | Current price: {_price:,.6g}

You are CLEXER. Analyze {chosen_sym}. Decide: is this coin ready for MARKET entry RIGHT NOW?
If not → WAIT. Do not force. Another coin will be tried. Go directly to output.

RULES:
1. 4H trend: HH+HL=BULLISH, LH+LL=BEARISH, unclear=WAIT
2. 1H: must agree with 4H or be neutral. Opposite=WAIT
3. 5M NOW: higher lows forming=BUY ready, lower highs forming=SELL ready, choppy/mixed=WAIT
4. Entry = {_price:,.6g} (MARKET, fills now)
5. SL = lowest low of last 3-5 x 5M candles (BUY) or highest high (SELL). Min 1.5%, Max 4%. +0.3% buffer.
6. TP1 = entry ± sl_dist×1.5. TP2 = entry ± sl_dist×3
7. Confidence: HIGH=all 3 TFs agree clearly. MED=4H+1H agree, 5M forming. LOW=only 4H clear.
8. HARD BLOCK→WAIT: last 4H candle <-6%, price fell >10% in 2 candles, 4H/1H opposite, 5M choppy.

OUTPUT ONLY (no steps, no working, replace bracketed values):
Signal: BUY / SELL / WAIT
Entry: {_price:,.6g}
Entry_Type: MARKET
SL: [number only]
TP1: [number only]
TP2: [number only]
R:R: [number only]
Confidence: HIGH / MED / LOW
Reasoning: [one line]"""
                        return _prompt, 200

                    analysis_prompt, _max_tokens = _build_analysis_prompt(cp)

                    def _build_content(_prompt):
                        _c = []
                        if scan_screenshots:
                            for tf in ["4H","1H","5"]:
                                img_b64 = scan_screenshots.get(tf)
                                if not img_b64: continue
                                _c.append({"type":"text","text":f"=== {chosen_sym} {tf} CHART ==="})
                                _c.append({"type":"image","source":{"type":"base64","media_type":"image/png","data":img_b64}})
                        _c.append({"type":"text","text":_prompt})
                        return _c
                    content = _build_content(analysis_prompt)

                    analysis = ""
                    _claude_ok = False
                    _last_claude_err = ""
                    _kind = f"scan{scan_ver}"
                    _using_aero = _ai_aerolink(_kind)
                    _retry_budget = _claude_retry_budget(_using_aero)
                    for _attempt in range(_retry_budget):
                        try:
                            if _attempt > 0:
                                # Refresh price on every retry — a coin needing
                                # multiple attempts (10s sleep between each) would
                                # otherwise keep feeding the AI the price from
                                # whenever the FIRST attempt started, no matter
                                # how stale that's become by attempt 3, 4, 8...
                                _fresh_retry_px = get_bingx_price(chosen_sym)
                                if _fresh_retry_px:
                                    cp = _fresh_retry_px
                                    analysis_prompt, _max_tokens = _build_analysis_prompt(cp)
                                    content = _build_content(analysis_prompt)
                            _gw_dbg = _aerolink_gw_debug_tag(_using_aero, _attempt, _aero_bad_keys)
                            print(f"  [SCAN] attempt {_attempt+1}/{_retry_budget} using gateway={_gw_dbg} model={_ai_model(_kind)}")
                            _client, _used_key = _claude_client_skip(_kind, _attempt, _aero_bad_keys)
                            r2 = _client.messages.create(
                                model=_ai_model(_kind), max_tokens=_max_tokens,
                                messages=[{"role":"user","content":content}])
                            _log_api_usage(f"scan{scan_ver}_{chosen_sym}", _ai_model(_kind),
                                           r2.usage.input_tokens, r2.usage.output_tokens,
                                           gateway="Aerolink" if _using_aero else "Direct")
                            analysis = _claude_text(r2)
                            _claude_ok = True
                            break
                        except Exception as _ce:
                            _last_claude_err = str(_ce)
                            print(f"  [SCAN] Claude attempt {_attempt+1} FAIL (gateway={_gw_dbg}): {_last_claude_err}")
                            if _using_aero and _used_key:
                                _aero_bad_keys.add(_used_key)  # skip this key for the rest of this scan cycle
                            if _attempt < _retry_budget - 1:
                                time.sleep(10)
                    if not _claude_ok:
                        print(f"  [SCAN] {chosen_sym}: Claude failed {_retry_budget} times — skipping coin")
                        api_fail_count += 1
                        skip_log.append(f"🔴 {chosen_sym}: Claude API call failed {_retry_budget}x — NOT analyzed (last error: {_last_claude_err[:120]})")
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
                        f"<pre>{_html.escape(analysis[:900])}</pre>")

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
                    # `cp` was captured when this candidate was first picked, BEFORE
                    # the chart-fetch + Claude analysis that can take several minutes
                    # (screenshots, retries across up to 3 candidates) — refetch right
                    # here so the recorded entry (and SL/TP1/TP2 offsets from it) match
                    # where price actually is now, not where it was minutes ago. MARKET
                    # orders fill at the real current price on BingX regardless, so a
                    # stale `cp` here was only ever hurting our OWN entry/SL/TP accuracy,
                    # not the real fill.
                    scan_entry = get_bingx_price(chosen_sym) or cp
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
                        _kind = "scan1" if scan_ver == 1 else "scan2"
                        _tier_routed = _scan_run_mode.get(_kind) == "special"
                        # Only verified/special-time (tier_routed) signals ever compete
                        # for the Free-channel share — a regular-grid (Signal-only, never
                        # posted to VIP/Free) trade must never consume quota or be marked
                        # share_free=True, since /status's free-viewer reveal check trusts
                        # this flag on its own (see _trade_reveal) and would otherwise leak
                        # a trade that was never actually shown anywhere near Free.
                        _share_free = _free_quota_available() if _tier_routed else False
                        if _share_free: _consume_free_quota()
                        _effective_share_free = _share_free
                        slot_data["share_free"] = _effective_share_free
                        slot_data["tier_routed"] = _tier_routed
                        slot_data["is_d48"] = _gw_model_tag(_kind) == "D4.8"  # channel-2 only gets D4.8 (Direct+Opus4.8) signals
                        slot_data["sig_id"] = _gen_signal_id()
                        slot_data["entry_time_str"] = (datetime.now(timezone.utc)+IST).strftime("%d.%m.%y %H:%M")
                        _save_sig_snapshot(slot_data["sig_id"], chosen_sym, scan_signal_val, scan_entry, scan_sl, scan_tp1, scan_tp2, _kind)
                        # Regular-grid (non-special) auto-runs still never reach VIP/Free at
                        # all (tier_routed=False) — legacy channel only, unchanged.
                        slot_data["reply_map"] = send_entry_signal(fmt_scan_signal(slot_data),
                            include_ch2=False, tier_routed=_tier_routed, share_free=_effective_share_free,
                            locked_text=_locked_signal_text(chosen_sym.replace("-USDT","").replace("USDT",""), f"S{scan_ver} {_gw_model_tag(_kind)}", slot_data["sig_id"]),
                            sig_id=slot_data["sig_id"])
                        for _k, _v in (slot_data["reply_map"] or {}).items():
                            if _k.startswith("free:"): _track_free_sl(slot_data["sig_id"], _k.split(":", 1)[1], "entry_mid", _v)
                        log_trade_event({"type": f"scan{scan_ver}", "coin": chosen_sym,
                            "direction": scan_signal_val, "signal_time": _ist_str_now(),
                            "entry_price": scan_entry, "sl_price": scan_sl,
                            "tp1_price": scan_tp1, "tp2_price": scan_tp2,
                            "entry_trigger_time": _ist_str_now(), "result": "open"})
                        sd["ver"] = scan_ver
                        # Copy trade only mirrors signals that were actually shown in
                        # VIP/Free — regular-grid (Signal-only) auto-runs place no orders.
                        # A special time can ALSO be marked "unverified" (freshly added,
                        # not yet proven) — those still post to VIP/Free but copytrade
                        # must never auto-execute real orders on them until admin moves
                        # them out of _SCAN_SPECIAL_NO_COPY.
                        _trigger_hm = _scan_trigger_hm.get(_kind)
                        _is_unverified = _tier_routed and _trigger_hm in _SCAN_SPECIAL_NO_COPY.get(_kind, set())
                        if _tier_routed and not _is_unverified:
                            ct_results = ct.on_scan_signal(sd, chosen_sym, cp, _effective_share_free)
                            send_reply(cid, f"📋 <b>Copy Trade ({chosen_sym}):</b>\n"+"\n".join(ct_results[:5]))
                        elif _is_unverified:
                            send_reply(cid, f"📋 <b>{chosen_sym}:</b> Unverified special time ({_trigger_hm[0]}:{_trigger_hm[1]:02d}) — posted to VIP/Free, but no copy trade orders placed until this slot is verified.")
                        else:
                            send_reply(cid, f"📋 <b>{chosen_sym}:</b> Signal-only slot (not VIP/Free) — no copy trade orders placed.")
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
                            f"Next auto-scan runs at :{ALT_SCAN_MINUTE:02d} IST.")
                    else:
                        send_reply(cid,
                            f"⏸ <b>No signal found</b>  {ist_str()}\n\n"
                            f"Tried {len(tried)} coin(s): <b>{tried_str}</b>\n\n"
                            f"None had clear 4H+1H+5M alignment for MARKET entry right now.\n"
                            f"Next auto-scan runs at :{ALT_SCAN_MINUTE:02d} IST.")
                    # Special-time slots matter for tracking — let VIP know this specific
                    # slot didn't fire, and clearly say why (gateway/API error vs Claude
                    # genuinely finding no clean setup) instead of silently skipping it.
                    if _is_special_now:
                        _trig_hm = _scan_trigger_hm.get(_kind)
                        _trig_str = f"{_trig_hm[0]}:{_trig_hm[1]:02d}" if _trig_hm else "?"
                        _label = f"S{scan_ver}"
                        _gw = _gw_model_tag(_kind)
                        if api_fail_count > 0 and api_fail_count >= len(tried):
                            _no_sig_msg = _scan_box(
                                f"{_label} No Signal", f"⏸ {_label} {_gw}  |  {_trig_str} IST",
                                [[f"🔴 {_smallcaps_title(f'{_gw} Error')}",
                                  f"{_smallcaps_title('Gateway/API failed — no chart was analyzed')}."]],
                            )
                        else:
                            _no_sig_msg = _scan_box(
                                f"{_label} No Signal", f"⏸ {_label} {_gw}  |  {_trig_str} IST",
                                [[f"🔍 {_smallcaps_title('No Clear Trade Found')}",
                                  f"{_smallcaps_title('Claude analyzed but no clean setup at this slot')}."]],
                            )
                        send_to_tier_channels(_no_sig_msg, False)  # VIP only

            except Exception as e:
                send_reply(cid, f"❌ Scan error: {e}")
                import traceback as _tb2; print(_tb2.format_exc())
            finally:
                # /scan1 and /scan2 both run here in their own background thread —
                # the run-mode override must stay set for this whole run, so it can
                # only be safely cleared once this thread is actually done, not
                # right after handle_command() returns (that only kicks off this thread).
                _scan_run_mode["scan1" if scan_ver == 1 else "scan2"] = None
        threading.Thread(target=lambda: _do_scan(cid=chat_id, scan_ver=ver), daemon=True).start()

    elif cmd == "/syncup" and is_admin:
        if not CLEXER_API_URL:
            send_reply(chat_id, "❌ CLEXER_API_URL isn't set on this server — nothing to sync to."); return
        send_reply(chat_id, "🔄 Pushing this server's current users, settings, and trade state to the shared store...")
        _results = []
        try:
            ct._save()
            ct.push_to_central()
            _results.append(f"✅ Copy-trade users ({len(ct._db)})")
        except Exception as e:
            _results.append(f"❌ Copy-trade users: {e}")
        try:
            save_settings()
            _kv_push("bot_settings", json.load(open(_SETTINGS_FILE)))
            _results.append("✅ Bot settings")
        except Exception as e:
            _results.append(f"❌ Bot settings: {e}")
        try:
            save_state()
            _r = requests.post(f"{CLEXER_API_URL}/push_state", json=json.load(open(STATE_FILE)),
                headers=({"X-Push-Secret": PUSH_STATE_SECRET} if PUSH_STATE_SECRET else {}), timeout=15)
            if not _r.ok:
                raise Exception(f"HTTP {_r.status_code} — {_r.text[:150]}")
            _results.append("✅ Trade state")
        except Exception as e:
            _results.append(f"❌ Trade state: {e}")
        try:
            save_users()
            _kv_push("registered_users", json.load(open(USER_DB_FILE)))
            _results.append(f"✅ Registered users ({len(registered_users)})")
        except Exception as e:
            _results.append(f"❌ Registered users: {e}")
        try:
            if os.path.exists(ct._SIGNAL_FILE):
                _kv_push("ct_last_signal", json.load(open(ct._SIGNAL_FILE)))
                _results.append("✅ Last copy-trade signal")
            else:
                _results.append("⏭️ Last copy-trade signal (none yet)")
        except Exception as e:
            _results.append(f"❌ Last copy-trade signal: {e}")
        try:
            if os.path.exists(TRADE_LOG_CSV):
                _kv_push("trade_history_csv", {"csv": open(TRADE_LOG_CSV, encoding="utf-8").read()})
                _results.append("✅ Trade history CSV")
            else:
                _results.append("⏭️ Trade history CSV (none yet)")
        except Exception as e:
            _results.append(f"❌ Trade history CSV: {e}")
        try:
            if os.path.exists(API_COST_LOG):
                _kv_push("api_cost_log_csv", {"csv": open(API_COST_LOG, encoding="utf-8").read()})
                _results.append("✅ API cost log CSV")
            else:
                _results.append("⏭️ API cost log CSV (none yet)")
        except Exception as e:
            _results.append(f"❌ API cost log CSV: {e}")
        send_reply(chat_id,
            f"<b>Sync-up complete</b>\n\n<blockquote>" + "\n".join(_results) + "</blockquote>\n\n"
            f"Any co-server pointed at the same CLEXER_API_URL will now see this data on its next load.")
        return

    elif cmd == "/server" and is_admin:
        _arg = parts[1].strip() if len(parts) > 1 else ""
        _active_now = get_active_server_name()
        if _arg:
            set_active_server(_arg)
            _active_now = _arg
            send_reply(chat_id,
                f"🖥️ <b>Active Server Switched</b>\n\n"
                f"<blockquote>Now active: <b>{_active_now}</b>\n"
                f"This server (<b>{SERVER_NAME}</b>) is {'🟢 ACTIVE — placing real orders' if _active_now==SERVER_NAME else '⏸️ STANDBY — no copytrade orders will be placed'}.</blockquote>")
            return
        _status = "🟢 ACTIVE — placing real orders" if is_active_server() else "⏸️ STANDBY — no copytrade orders will be placed"
        _mkp = {"inline_keyboard": [
            [{"text": f"✅ Make \"{SERVER_NAME}\" active", "callback_data": f"srvset:{SERVER_NAME}"}],
        ]}
        send_reply(chat_id,
            f"🖥️ <b>Server Status</b>\n\n"
            f"<blockquote>This server's name: <b>{SERVER_NAME}</b>\n"
            f"Currently active server: <b>{_active_now}</b>\n"
            f"Status here: {_status}</blockquote>\n\n"
            f"Switch with <code>/server &lt;name&gt;</code> (e.g. <code>/server co1</code>) — "
            f"run it from whichever server you're switching TO.", reply_markup=_mkp)
        return

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
            f"Used for all scan/BTC/coin analysis calls.", reply_markup=_mkp)

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
            f"Your real Anthropic key is never sent to Aerolink — the two keys stay fully separate.", reply_markup=_mkp)

    elif cmd == "/coin" and is_scanadmin:
        if len(parts) < 2:
            send_reply(chat_id,
                "🪙 <b>Coin Lookup</b>\n\n"
                "Just type a coin's name — e.g. <code>eth</code>, <code>sol</code>, <code>avax</code> — "
                "and the bot finds it on BingX and analyzes it for you.\n\n"
                "🔎 If more than one coin shares that name (e.g. two different Broccoli tokens), "
                "the bot shows you all the matches so you can tell it exactly which one you want.\n\n"
                ""); return
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
                        f"Try: /coin ETH  /coin SOL  /coin BNB"); return

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
                    send_reply(cid, "\n".join(lines) + ""); return

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

                # Ask Claude for structured analysis (JSON, not freeform prose)
                resp = _claude_client().messages.create(
                    model=SCAN_MODEL, max_tokens=700,
                    system="Respond with RAW JSON ONLY. No markdown, no code fences, no text before or after.",
                    messages=[{"role": "user", "content":
                        f"Analyze {sym} for a short-term futures trade:\n"
                        f"Current Price: ${price:,.6g}\n"
                        f"24h Change: {change:+.2f}%\n"
                        f"24h High: ${high24:,.6g}  |  24h Low: ${low24:,.6g}\n"
                        f"24h Volume: ${vol:,.0f}\n"
                        f"BTC: ${get_ticker()['price']:,.0f} ({get_session()} session)\n\n"
                        f'Return this exact JSON shape:\n'
                        f'{{"bias":"LONG|SHORT|WAIT","entry_zone":"e.g. 1791-1798",'
                        f'"sl":"e.g. 1846","tp1":"e.g. 1778","tp2":"e.g. 1773.45",'
                        f'"confidence":"HIGH|MEDIUM|LOW","reasoning":["point 1","point 2","point 3"],'
                        f'"practical_note":"1-2 sentences, the actual trade plan in plain words",'
                        f'"btc_watch":["if BTC does X, then...","if BTC does Y, then..."]}}\n\n'
                        f"Be practical and concise. No fluff. 3-4 reasoning points max."}])
                _log_api_usage(f"coin_{sym}", SCAN_MODEL,
                               resp.usage.input_tokens, resp.usage.output_tokens,
                               gateway="Aerolink" if _ai_aerolink("btc") else "Direct")
                import json as _cjson, re as _cre
                _raw = _claude_text(resp)
                _m = _cre.search(r'\{.*\}', _raw, _cre.DOTALL)
                a = _cjson.loads(_m.group()) if _m else {}
                bias  = str(a.get("bias","WAIT")).upper()
                conf  = str(a.get("confidence","LOW")).upper()
                arrow = "🟢" if change >= 0 else "🔴"
                bias_emoji = "🟢" if bias == "LONG" else ("🔴" if bias == "SHORT" else "🟡")
                reasoning = a.get("reasoning") or []
                btc_watch = a.get("btc_watch") or []
                _reason_lines = "\n".join(f"• {_smallcaps_title(str(r))}" for r in reasoning) or f"• {_smallcaps_title('No clear structure yet')}"
                _btc_lines = "\n".join(f"• {_smallcaps_title(str(b))}" for b in btc_watch)
                coin_disp = sym.replace("-", "/")
                _BORDER = "࿇═════════════════════════════════࿇"
                _DIV    = "━━━━━━━━━━━━━━━━━━━━"
                text_out = (
                    f"{_BORDER}\n"
                    f"✦ {_smallcaps_title('Coin Analysis')} ✦\n"
                    f"{_BORDER}\n\n"
                    f"{arrow} {coin_disp}\n"
                    f"📅 {ist_str()}\n\n"
                    f"{_DIV}\n\n"
                    f"💰 {_smallcaps_title('Price')}: ${price:,.6g} ({change:+.2f}%)\n"
                    f"📈 24ʜ {_smallcaps_title('High')}: ${high24:,.6g}\n"
                    f"📉 24ʜ {_smallcaps_title('Low')}: ${low24:,.6g}\n"
                    f"📦 {_smallcaps_title('Volume')}: ${vol/1e6:.1f}M\n\n"
                    f"{_DIV}\n\n"
                    f"🧠 {_smallcaps_title('AI Analysis')}\n\n"
                    f"📍 {_smallcaps_title('Bias')}: {bias_emoji} {bias}\n\n"
                    f"🎯 {_smallcaps_title('Entry Zone')}:\n{a.get('entry_zone','—')}\n\n"
                    f"🛑 {_smallcaps_title('Stop Loss')}:\n{a.get('sl','—')}\n\n"
                    f"🎯 {_smallcaps_title('Targets')}:\n"
                    f"• TP1: {a.get('tp1','—')}\n"
                    f"• TP2: {a.get('tp2','—')}\n\n"
                    f"📊 {_smallcaps_title('Confidence')}:\n{conf}\n\n"
                    f"{_DIV}\n\n"
                    f"📖 <blockquote>{_smallcaps_title('Reason')}\n\n{_reason_lines}</blockquote>\n\n"
                    f"⚠️ <blockquote>{_smallcaps_title('Practical Note')}\n\n{_smallcaps_title(str(a.get('practical_note','Size small — low-conviction setup.')))}</blockquote>\n\n"
                    + (f"📌 {_smallcaps_title('Keep an Eye on BTC')}:\n{_btc_lines}\n\n" if _btc_lines else "")
                    + f"{_DIV}"
                )
                send_reply(cid, text_out)
            except Exception as e:
                send_reply(cid, f"❌ Error: {e}")
                import traceback; traceback.print_exc()
        threading.Thread(target=_do_coin, daemon=True).start()

    else:
        send_reply(chat_id, f"Unknown: {cmd}\n/help")

_bc_picker_state: dict = {}  # chat_id str -> {"text","file_id","file_type","mode","selected": set()}

def handle_broadcast_message(chat_id, message):
    text = message.get("text") or message.get("caption") or ""
    photo = message.get("photo"); doc = message.get("document")
    file_id = None; file_type = None
    if photo:   file_id = photo[-1]["file_id"]; file_type = "photo"
    elif doc:   file_id = doc["file_id"];       file_type = "document"
    if not text and not file_id: send_reply(chat_id, "Empty. /cancel to abort."); return
    mode = broadcast_pending.get(chat_id, {}).get("mode", "all")
    del broadcast_pending[chat_id]
    if mode == "users":
        _mode_label = {"users": "registered users", "channels": "channels", "all": "users + channels"}[mode]
        send_reply(chat_id, f"📢 Broadcasting to {_mode_label}...")
        threading.Thread(target=do_broadcast, args=(chat_id, text, file_id, file_type, mode), daemon=True).start()
        return
    all_targets = _all_broadcast_channel_targets()
    _bc_picker_state[str(chat_id)] = {
        "text": text, "file_id": file_id, "file_type": file_type, "mode": mode,
        "selected": {cid for cid, _ in all_targets},  # pre-select all by default
    }
    _send_broadcast_picker(chat_id)

def _send_broadcast_picker(chat_id, message_id=None):
    st = _bc_picker_state.get(str(chat_id))
    if not st: return
    all_targets = _all_broadcast_channel_targets()
    if not all_targets:
        send_reply(chat_id, "⚠️ No channels are set up yet — add one via /channelmgmt or /adminlinks first.")
        _bc_picker_state.pop(str(chat_id), None)
        return
    rows = []
    for cid, label in all_targets:
        mark = "✅" if cid in st["selected"] else "⬜"
        rows.append([{"text": f"{mark} {label}", "callback_data": f"bctgl:{cid}"}])
    rows.append([
        {"text": "◀️ Previous", "callback_data": "bcprev"},
        {"text": "🚫 Back",     "callback_data": "bcback"},
        {"text": "✅ Send",     "callback_data": "bcsend"},
    ])
    n = len(st["selected"])
    text = f"📢 <b>Choose channels/groups</b>\n\n{n} of {len(all_targets)} selected. Tap to toggle."
    _help_edit_or_send(chat_id, text, {"inline_keyboard": rows}, message_id=message_id)

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
        ("/aiconfig", "🧠", "AI Model & Gateway", "Set model + gateway for Scan1/Scan2/TS1/TS2, each split by Verified/Unverified/Nonspecial."),
        ("/entrystyle", "🎯", "Scan Entry Style", "Choose Market (instant) or Zone (limit order at a price range) entries for Scan1/Scan2."),
    ]),
    "schedule": ("⏰ Schedule Editor", [
        ("/alt",     "⏰", "Scan1 Times",       "Edit the exact hour:minute slots Scan1 fires at."),
        ("/alt2",    "⏰", "Scan2 Times",       "Edit the exact hour:minute slots Scan2 fires at."),
        ("/altdemo", "⏰", "TS1 Times",   "Edit the exact hour:minute slots TS1 (demo scan1) fires at."),
        ("/altdemo2","⏰", "TS2 Times",   "Edit the exact hour:minute slots TS2 (demo scan2) fires at."),
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
        ("/forceclose", "🛠", "Force Close Stuck Trade", "Manually close a Scan1/Scan2/TS1/TS2 trade the bot lost track of, with the real TP1/TP2/SL/BE result."),
        ("/syncup",    "☁️", "Push to Central Store", "Force-pushes this server's current users, settings, trade state, and CSVs to the shared multi-server store."),
        ("/server",    "🖥️", "Server Status / Switch", "Shows which server is currently active, or switch which one is (/server &lt;name&gt;)."),
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
        ("/ws", "😴", "Weekend Sleep", "Turn off to let the bot run straight through Fri-Sun instead of auto-pausing."),
        ("/clearslfree", "🗑", "Clear Free SL Messages", "Bulk-delete every logged SL/BE-hit message from the Free channel(s)."),
        ("/resetspins", "🎰", "Reset All VIP Spins", "Clear every user's locked VIP spin price so everyone can spin again immediately."),
        ("/setvipprice", "💰", "Set VIP Price", "Change the flat VIP monthly price (currently used for the full-price button on /vip)."),
        ("/statsaccess", "🏆", "Win Rate Access", "Turn /stats (win rate & trade statistics) on or off for regular users."),
        ("/winrate", "🎯", "Win Rate Targets", "Set the promote/demote win-rate target independently for Scan1, Scan2, TS1, and TS2."),
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
        ("/latestnews",  "📰", "News Feed Status",  "Check whether the live liquidation feed is running."),
    ]),
    "channels": ("📡 Channel Control", [
        ("/channels",      "📡", "Channel Status",    "Show the current status of all connected signal channels."),
        ("/pausechannel",  "⏸", "Pause a Channel",   "Stop signals from being sent to a specific channel."),
        ("/resumechannel", "▶️", "Resume a Channel",  "Re-enable signals for a specific channel."),
        ("/channelmgmt",   "⭐", "VIP / Free Channels","Add/remove any number of VIP or Free channels and set the free daily signal limit."),
    ]),
}

# ─── Tap-to-pick time keypad (digit entry for Scan1/Scan2/Demo schedules) ─────
_TP_LABELS  = {"scan1": "Scan1", "scan2": "Scan2", "demo": "Demo/Test", "demo1": "TS1", "demo2": "TS2"}
_TP_APPLYCMD = {"scan1": "/alt manual", "scan2": "/alt2 manual", "demo": "/altdemo manual",
                 "demo1": "/altdemo manual", "demo2": "/altdemo2 manual"}
_TP_BACKCAT  = {"scan1": "scan", "scan2": "scan", "demo": "scan", "demo1": "scan", "demo2": "scan"}

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
    _help_edit_or_send(chat_id, result_text, {"inline_keyboard": merged}, message_id=msg_id,
        emoji_overrides=captured.get("emoji_overrides"))

VIP_MONTHLY_PRICE = 15.0
VIP_SPIN_MIN, VIP_SPIN_MAX = 11.0, 16.0
VIP_STAR_DISCOUNT_MAX = 0.10   # star lucky draw: up to 10% off the $-in-stars price, never more

def _play_spin_animation(chat_id, message_id=None):
    """Plays Telegram's own native slot-machine animation (sendDice with
    emoji='🎰') — a real client-side spin lasting ~2.5s, not a fake text
    countdown. Optionally blanks out the buttons on the current screen first
    so the user can't double-tap Spin while it's playing. Runs in a background
    thread (see callers) so this blocking sleep never stalls the main update
    loop for other users."""
    if message_id:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageReplyMarkup",
                json={"chat_id": chat_id, "message_id": message_id, "reply_markup": {"inline_keyboard": []}}, timeout=10)
        except Exception:
            pass
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDice",
            json={"chat_id": chat_id, "emoji": "🎰"}, timeout=10)
    except Exception as e:
        print(f"  [SPIN ANIM] {e}")
    time.sleep(2.5)   # matches roughly how long the native animation takes to settle

def send_vip_offer_screen(chat_id, cid, message_id=None):
    """VIP purchase screen with two independent lucky draws — a $ spin
    ($11-16) and a ⭐ Stars spin (up to 10% off the $-in-stars price) — each
    one spin per calendar month, locked once rolled, plus the flat
    $15/month button and Contact Admin. Payment creates a CryptoBot/Stars
    invoice — actual VIP grant happens later via _poll_payment_events() /
    the successful_payment handler once payment is confirmed, never
    synchronously on the button tap."""
    u = ct._get(str(cid))
    if u.get("tier") == "vip":
        _until = f" until <b>{u['vip_end']}</b>" if u.get("vip_end") else ""
        _help_edit_or_send(chat_id,
            f"👑 <b>{_smallcaps_title('You Are Already VIP')}</b>\n\n<blockquote>Your VIP is active{_until} — no need to buy another one.</blockquote>",
            {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": "help_main"}]]},
            message_id, rotate=False)
        return
    cur_month = now_ist().strftime("%Y-%m")
    has_spin = u.get("vip_spin_month") == cur_month and u.get("vip_spin_amount")
    has_star_spin = u.get("vip_star_spin_month") == cur_month and u.get("vip_star_spin_amount")
    star_base = round(VIP_MONTHLY_PRICE * STARS_PER_USD)
    star_min = round(star_base * (1 - VIP_STAR_DISCOUNT_MAX))

    rows = []
    if has_spin:
        _amt = u["vip_spin_amount"]
        rows.append([{"text": f"💳 Pay ${_amt:.2f} (your $ spin price)", "callback_data": f"vip_pay:{_amt:.2f}"}])
    else:
        rows.append([{"text": _smallcaps_title("🎰 Lucky Draw Spin ($)"), "callback_data": "vip_spin"}])
    if has_star_spin:
        _samt = u["vip_star_spin_amount"]
        rows.append([_star_button(f"⭐ Pay {_samt:,} Stars (your spin price)", callback_data=f"vip_paystarsflat:{_samt}")])
    else:
        rows.append([_star_button(_smallcaps_title("⭐ Lucky Draw Spin"), callback_data="vip_starspin")])
    rows.append([_vip_moneybag_button(f"💰 ${VIP_MONTHLY_PRICE:.0f}/month", callback_data=f"vip_pay:{VIP_MONTHLY_PRICE:.2f}")])
    _last_row = [{"text": "◀️  Back", "callback_data": "help_main"}]
    if ADMIN_CHAT_ID:
        _last_row.append({"text": "💬 Contact Admin", "url": f"tg://user?id={ADMIN_CHAT_ID}"})
    rows.append(_last_row)

    _dollar_line = (f"You got <b>${u['vip_spin_amount']:.2f}</b> on the $ draw — pay that above to unlock VIP for 1 month."
                     if has_spin else
                     f"🎰 <b>$ draw:</b> random VIP price between <b>${VIP_SPIN_MIN:.0f}</b> and <b>${VIP_SPIN_MAX:.0f}</b> — could beat the ${VIP_MONTHLY_PRICE:.0f} full price.")
    _star_line = (f"You got <b>⭐{u['vip_star_spin_amount']:,}</b> on the ⭐ draw — pay that above to unlock VIP for 1 month."
                  if has_star_spin else
                  f"🎰 <b>⭐ draw:</b> random Stars price between <b>⭐{star_min:,}</b> and <b>⭐{star_base:,}</b> — up to 10% off.")
    text = (f"👑 <b>{_smallcaps_title('Get VIP')}</b>\n\n<blockquote>"
            f"Two lucky draws, one spin each per month — spin for a discounted $ price, a discounted ⭐ Stars price, or skip straight to the flat ${VIP_MONTHLY_PRICE:.0f}/month. Whatever you land on stays locked in until paid or the month resets.\n\n"
            f"{_dollar_line}\n\n{_star_line}</blockquote>")
    markup = {"inline_keyboard": rows}
    # rotate=False — plain/no-color buttons on this screen, per admin request.
    _help_edit_or_send(chat_id, text, markup, message_id, rotate=False, emoji_overrides=_PAYMENT_STAR_OVERRIDE)

# --- Free-channel signal unlock (wallet-funded, one spin per signal) ----------
SIG_SPIN_MIN, SIG_SPIN_MAX = 0.01, 0.05

def _reveal_signal_text(snap: dict, sig_id: str) -> str:
    arrow = "🟢 LONG" if snap["direction"] in ("BUY", "LONG") else "🔴 SHORT"
    return _scan_box(
        f"{snap['symbol']} Unlocked", f"🔓 #{snap['symbol']}",
        [[arrow],
         [f"🎯 {_smallcaps_title('Entry')}: <code>{snap['entry']:,.4g}</code>",
          f"🛑 SL: <code>{snap['sl']:,.4g}</code>",
          f"💰 TP1: <code>{snap['tp1']:,.4g}</code>",
          f"🏆 TP2: <code>{snap['tp2']:,.4g}</code>"]],
        tag=sig_id,
    )

def send_unlock_screen(chat_id, cid, sig_id: str, message_id=None):
    """DM screen shown after tapping a locked Free-channel signal's Unlock
    button. One spin per signal (locked forever once rolled, per admin's
    rule) — wallet payment is a synchronous same-process deduction; Stars
    payment goes through Telegram's own invoice instead (these amounts are
    below CryptoBot's $1 minimum, so crypto isn't offered here)."""
    snap = _sig_snapshots.get(sig_id)
    if not snap:
        send_reply(chat_id, "⚠️ This signal is no longer available to unlock (too old, or already closed out).")
        return
    u = ct._get(str(cid))
    if sig_id in u.get("unlocked_sigs", []):
        send_reply(chat_id, _reveal_signal_text(snap, sig_id))
        return
    if snap.get("result"):
        # Trade already hit its terminal outcome (TP/SL/BE/timeout/etc) — no
        # point charging for a signal that's already over, tell them to try
        # another. Only reveal the result if it was an actual TP hit (a win) —
        # never mention a loss/BE/timeout outcome, so a losing/neutral trade
        # doesn't get advertised as such to someone who never paid to see it.
        if snap["result"].startswith("TP"):
            send_reply(chat_id, f"🏆 <b>This signal already closed — {snap['result']} Hit!</b>\n\n"
                                 f"Try unlocking a different signal instead.")
        else:
            send_reply(chat_id, f"⏰ <b>This signal already closed.</b>\n\n"
                                 f"Try unlocking a different signal instead.")
        return
    spins = u.get("sig_spins", {})
    rows = []
    if sig_id in spins:
        _amt = spins[sig_id]
        _bal = u.get("wallet_balance", 0)
        rows.append([{"text": f"💳 ${_amt:.2f} from wallet", "callback_data": f"sig_pay:{sig_id}"}])
        _wallet_line = f"Your $ unlock price: <b>${_amt:.2f}</b> (locked in for this signal) — wallet balance: <b>${_bal:.2f}</b>."
    else:
        rows.append([{"text": _smallcaps_title("🎰 Spin To See Price ($)"), "callback_data": f"sig_spin:{sig_id}"}])
        _wallet_line = f"Spin once to see your $ unlock price (${SIG_SPIN_MIN:.2f}-${SIG_SPIN_MAX:.2f}) — locked in for this signal, no re-rolling."
    rows.append([_star_button("⭐ Unlock for 1 Star", callback_data=f"sig_unlockstar:{sig_id}")])
    rows.append([{"text": "💰 Add Funds", "callback_data": "addfunds_menu"}])
    rows.append([{"text": "🏠 Main Menu", "callback_data": "help_main"}])
    text = (f"🔒 <b>{_smallcaps_title('Signal Locked')}</b>\n\n<blockquote>{_wallet_line}\n\n"
            f"Or skip the spin entirely — unlock instantly for a flat <b>⭐ 1 Star</b>, no spinning needed.</blockquote>")
    markup = {"inline_keyboard": rows}
    # rotate=False — plain/no-color buttons on this screen, per admin request.
    _help_edit_or_send(chat_id, text, markup, message_id, rotate=False, emoji_overrides=_PAYMENT_STAR_OVERRIDE)

def send_addfunds_screen(chat_id, message_id=None):
    rows = [[{"text": "$1", "callback_data": "addfunds:1"}, {"text": "$5", "callback_data": "addfunds:5"}, {"text": "$10", "callback_data": "addfunds:10"}],
            [{"text": "◀️  Back", "callback_data": "help_main"}]]
    text = f"💰 <b>{_smallcaps_title('Add Funds')}</b>\n\n<blockquote>Top up your wallet — used to unlock Free-channel signals. Pay with crypto or Telegram Stars.</blockquote>"
    markup = {"inline_keyboard": rows}
    # rotate=False — plain/no-color buttons on this screen, per admin request.
    _help_edit_or_send(chat_id, text, markup, message_id, rotate=False)

def _help_edit_or_send(chat_id, text, markup, message_id=None, rotate=True, emoji_overrides=None):
    base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    text = _apply_premium_emojis(text, overrides=emoji_overrides)
    markup = _style_keyboard(markup, rotate=rotate)
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
               "reply_markup": markup or {"inline_keyboard": []}, "disable_web_page_preview": True}
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
    _extra = ""
    if st["target"] == "freelimit":
        _verified_n = sum(len(_SCAN_SPECIAL.get(k, set())) - len(_SCAN_SPECIAL_NO_COPY.get(k, set()))
                           for k in ("scan1", "scan2", "test1", "test2"))
        _tr = _free_signal_tracker
        _today_n = _tr.get("total", 0); _shared_n = _tr.get("shared", 0)
        _actual_pct = round(_shared_n / _today_n * 100) if _today_n else 0
        _extra = (f"📊 Verified times (all slots): <b>{_verified_n}</b>\n"
                   f"🔧 Current setting: <b>{FREE_SIGNAL_DAILY_LIMIT}%</b>\n"
                   f"📅 Today: {_shared_n}/{_today_n} shared ({_actual_pct}%)\n\n")
    elif st["target"] == "tp1size":
        _extra = f"🔧 Current: <b>{ct.TP1_CLOSE_PCT}{cfg['unit']}</b>\n\n"
    elif st["target"] in ("wrscan1", "wrscan2", "wrts1", "wrts2"):
        _wr_kind = {"wrscan1": "scan1", "wrscan2": "scan2", "wrts1": "demo1", "wrts2": "demo2"}[st["target"]]
        _extra = f"🔧 Current: <b>{_SLOT_EVAL_THRESHOLD[_wr_kind]}{cfg['unit']}</b>\n\n"
    text = (
        f"🔢 <b>Set {cfg['label']}</b>\n\n"
        f"{_extra}"
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
    text = f"⚠️ <b>Are you sure?</b>\n\n<blockquote>{label}\n\n<i>This cannot be undone.</i></blockquote>"
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
    elif action_id == "clear_free_sl":
        _ok, _fail = _clear_free_sl_messages()
        result_text = f"✅ <b>Deleted {_ok} message(s)</b> (entry + trailing-SL + SL-hit) for real-SL signals from Free channel(s). BE trades untouched." + (f"\n⚠️ {_fail} failed to delete (already gone or too old)." if _fail else "")
    elif action_id == "reset_all_spins":
        _n = 0
        for _uid, _u in list(ct._db.items()):
            if _u.get("vip_spin_amount") or _u.get("vip_spin_month"):
                _u.pop("vip_spin_amount", None); _u.pop("vip_spin_month", None)
                ct._set(_uid, _u); _n += 1
        result_text = f"✅ <b>Reset the VIP spin lock for {_n} user(s).</b>\n\nEveryone can spin again right away."
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
                send_telegram(f"<b>SL -&gt; BE</b>  {symbol} -&gt; <b>{active_trade['entry']:,.4f}</b>")
                result_text = f"✅ <b>{symbol} SL moved to breakeven</b> ({active_trade['entry']:,.4f})"
            else:
                result_text = f"⚠️ {symbol} trade no longer open."
        else:
            lst = scan1_trades if kind == "scan1" else scan2_trades
            if 0 <= idx < len(lst) and lst[idx].get("symbol") == symbol:
                lst[idx]["sl"] = lst[idx]["entry"]
                ct.scan_sl_to_be(symbol, lst[idx]["entry"]); save_state()
                send_telegram(f"<b>SL -&gt; BE</b>  {symbol} -&gt; <b>{lst[idx]['entry']:,.4f}</b>")
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
        f"<blockquote>Controls whether users see the <b>Contact Admin</b> and <b>Signal Channel</b>\n"
        f"buttons on their main /help menu.\n\n"
        f"{channel_line}</blockquote>")
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
        [{"text": "◀️  Back", "callback_data": "copyadmin_sub:directory"}],
    ]
    _help_edit_or_send(chat_id,
        "📊 <b>User Stats</b>\n\n<blockquote>Tap a category to see the users with DM links.</blockquote>",
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

_AICFG_KIND_LABELS = {"scan1": "🔍 Scan1", "scan2": "🔍 Scan2", "test1": "🧪 TS1", "test2": "🧪 TS2"}
_AICFG_TIER_LABELS = {"verified": "⭐ Verified", "unverified": "⚠️ Unverified", "nonspecial": "➖ Nonspecial"}

def send_winrate_screen(chat_id, message_id=None):
    _wr_rows = [
        ("scan1", "🔍 Scan1", "wrscan1"), ("scan2", "🔍 Scan2", "wrscan2"),
        ("demo1", "🧪 TS1", "wrts1"), ("demo2", "🧪 TS2", "wrts2"),
    ]
    rows = [[{"text": f"{label}: {_SLOT_EVAL_THRESHOLD[kind]}%", "callback_data": f"winrate_open:{np_key}"}]
            for kind, label, np_key in _wr_rows]
    rows.append([{"text": "◀️  Back", "callback_data": "settings_sub:extras"}])
    _help_edit_or_send(chat_id,
        "<b>🎯 Win Rate Targets</b>\n\n"
        "<blockquote>The % win rate a time slot needs to hit (with at least 4 wins) to auto-promote to "
        "verified/VIP-routed, or to stay verified before auto-demoting to unverified.\n"
        "Tap a type below to change its target.</blockquote>",
        {"inline_keyboard": rows}, message_id=message_id)

def send_aiconfig_screen(chat_id, message_id=None):
    """Top level — pick which scan type's grid to edit. BTC gets its own row
    too, but skips the tier level (always verified, no unverified/nonspecial
    split) — tapping it jumps straight to the model+gateway combo screen."""
    _btc_gw  = "Aerolink" if USE_AEROLINK else "Direct"
    _btc_mdl = "Opus 4.8" if SCAN_MODEL == "claude-opus-4-8" else "Fable 5"
    rows = [[{"text": f"₿ BTC: {_btc_gw} · {_btc_mdl}", "callback_data": "aicfg_open2:btc:verified"}]]
    rows += [[{"text": label, "callback_data": f"aicfg_open:{kind}"}] for kind, label in _AICFG_KIND_LABELS.items()]
    rows.append([{"text": "◀️  Back", "callback_data": "scan_sub:system"}])
    _help_edit_or_send(chat_id,
        "<b>🧠 AI Model & Gateway — By Scan Type & Trade Type</b>\n\n"
        "<blockquote>Each scan type (Scan1, Scan2, TS1, TS2) picks its own model + gateway "
        "independently PER classification (⭐ Verified / ⚠️ Unverified / ➖ Nonspecial) — 12 slots total.\n"
        "BTC has one combo (always verified, no split) — tap it to change.\n"
        "Tap a type below.</blockquote>",
        {"inline_keyboard": rows}, message_id=message_id)

def send_aiconfig_kind_screen(chat_id, kind, message_id=None):
    """Second level — pick which of the 3 classifications to edit for this scan type."""
    klabel = _AICFG_KIND_LABELS.get(kind, kind)
    rows = []
    for tier, tlabel in _AICFG_TIER_LABELS.items():
        cfg = AICFG_GRID[kind][tier]
        gw  = "Aerolink" if cfg["aerolink"] else "Direct"
        mdl = "Opus 4.8" if cfg["model"] == "claude-opus-4-8" else "Fable 5"
        rows.append([{"text": f"{tlabel}: {gw} · {mdl}", "callback_data": f"aicfg_open2:{kind}:{tier}"}])
    rows.append([{"text": "◀️  Back", "callback_data": "aicfg_open"}])
    _help_edit_or_send(chat_id, f"<b>{klabel} — AI Model &amp; Gateway</b>\n\n<blockquote>Tap a trade type to change its combo:</blockquote>",
        {"inline_keyboard": rows}, message_id=message_id)

def send_aiconfig_type_screen(chat_id, kind, tier, message_id=None):
    """Third level — pick the actual model+gateway combo. BTC has no grid
    cell (uses SCAN_MODEL/USE_AEROLINK directly, "tier" is ignored for it)."""
    if kind == "btc":
        klabel, tlabel = "₿ BTC", ""
        cur_model, cur_aero = SCAN_MODEL, USE_AEROLINK
        _back_cb = "aicfg_open"
    else:
        klabel = _AICFG_KIND_LABELS.get(kind, kind); tlabel = f" · {_AICFG_TIER_LABELS.get(tier, tier)}"
        cfg = AICFG_GRID[kind][tier]
        cur_model, cur_aero = cfg["model"], cfg["aerolink"]
        _back_cb = f"aicfg_open:{kind}"
    def mark(m, a): return "✅ " if (cur_model == m and cur_aero == a) else ""
    rows = [
        [{"text": f"{mark('claude-opus-4-8', False)}Direct · Opus 4.8",    "callback_data": f"aicfg_set:{kind}:{tier}:direct:opus"}],
        [{"text": f"{mark('claude-fable-5', False)}Direct · Fable 5",     "callback_data": f"aicfg_set:{kind}:{tier}:direct:fable"}],
        [{"text": f"{mark('claude-opus-4-8', True)}Aerolink · Opus 4.8",   "callback_data": f"aicfg_set:{kind}:{tier}:aerolink:opus"}],
        [{"text": f"{mark('claude-fable-5', True)}Aerolink · Fable 5",    "callback_data": f"aicfg_set:{kind}:{tier}:aerolink:fable"}],
        [{"text": "◀️  Back", "callback_data": _back_cb}],
    ]
    _help_edit_or_send(chat_id, f"<b>{klabel}{tlabel} — AI Model &amp; Gateway</b>\n\n<blockquote>Choose a combo:</blockquote>",
        {"inline_keyboard": rows}, message_id=message_id)

def send_entrystyle_screen(chat_id, message_id=None):
    _is_market = not ZONE_ENTRY_ENABLED
    rows = [
        [{"text": f"{'✅ ' if _is_market else ''}📍 Market Entry", "callback_data": "entrystyle:market"}],
        [{"text": f"{'✅ ' if not _is_market else ''}📩 Zone Entry",  "callback_data": "entrystyle:zone"}],
        [{"text": "◀️  Back", "callback_data": "scan_sub:system"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>🎯 Scan Entry Style</b>\n\n"
        "<blockquote><b>Market Entry</b> — places the trade instantly at the current price.\n\n"
        "<b>Zone Entry</b> — shows a price range (like a signal-channel style zone) and "
        "places a single LIMIT order at the zone's midpoint for every copy user. "
        "The order only fills if price actually trades back into that zone — if it never "
        "does, the position stays unfilled on BingX (this applies to Scan1/Scan2 only).</blockquote>",
        {"inline_keyboard": rows}, message_id=message_id)

def send_channel_picker_screen(chat_id, message_id=None):
    rows = [
        [{"text": "🆓 Free Channel", "callback_data": "chanpick:free"}],
        [{"text": "⭐ VIP Channel",  "callback_data": "chanpick:vip"}],
        [{"text": "◀️  Back", "callback_data": "help_main"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>📡 Signal Channels</b>\n\n<blockquote>Choose which one you want to join:</blockquote>",
        {"inline_keyboard": rows}, message_id=message_id)

def send_channel_picker_result(chat_id, tier, message_id=None):
    if tier == "free":
        chans = [c for c in CHANNELS if c.get("tier") == "free" and c.get("id")]
        if not chans:
            text = "🆓 <b>Free Channel</b>\n\n<blockquote>No free channel is set up yet — check back later.</blockquote>"
            rows = [[{"text": "◀️  Back", "callback_data": "chanpick_open"}]]
        else:
            text = ("🆓 <b>Free Channel</b>\n\n<blockquote>👋 Welcome! Glad to have you here.\n\n"
                     "You'll get profitable signals per day, shared with everyone — tap below to join:</blockquote>")
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
                "<blockquote>You're VIP — tap below to request to join. Your request is approved automatically.</blockquote>")
            rows = [[{"text": c.get("label") or "Join", "url": c["link"]}] for c in vip_chans]
            rows.append([{"text": "◀️  Back", "callback_data": "chanpick_open"}])
        else:
            text = (
                "⭐ <b>VIP Channel</b>\n\n"
                "<blockquote>Get every signal, no limits — BTC, Scan1, and Scan2, the moment they fire.\n\n"
                "VIP access is activated by the admin. Tap below to request it.</blockquote>")
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
    rows.append([{"text": f"🔢 Free Share: {FREE_SIGNAL_DAILY_LIMIT}%", "callback_data": "freelimit_open"}])
    rows.append([{"text": "◀️  Back", "callback_data": "broadcast_sub:channels"}])
    _help_edit_or_send(chat_id,
        "<b>📡 Channels — VIP / Free</b>\n\n"
        "<blockquote>Add as many VIP or Free channels as you want. VIP channels get every signal. "
        "Free channels only get up to your daily limit, between 06:00–19:00 IST — "
        "free-tier bot users copy exactly the same signals the free channels got.</blockquote>",
        {"inline_keyboard": rows}, message_id=message_id)

def send_trailsl_screen(chat_id, message_id=None):
    _btc_flag   = "✅ ON" if TRAIL_SL_BTC   else "❌ OFF"
    _scan1_flag = "✅ ON" if TRAIL_SL_SCAN1 else "❌ OFF"
    _scan2_flag = "✅ ON" if TRAIL_SL_SCAN2 else "❌ OFF"
    _demo1_flag = "✅ ON" if TRAIL_SL_DEMO1 else "❌ OFF"
    _demo2_flag = "✅ ON" if TRAIL_SL_DEMO2 else "❌ OFF"
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
        [{"text": f"🧪 Demo1  {_demo1_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "trailsl_demo1_on"},
         {"text": "🔴 Turn OFF", "callback_data": "trailsl_demo1_off"}],
        [{"text": f"🧪 Demo2  {_demo2_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "trailsl_demo2_on"},
         {"text": "🔴 Turn OFF", "callback_data": "trailsl_demo2_off"}],
        [{"text": "◀️  Back", "callback_data": "tradecontrol_sub:levels"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>🛡️ Trailing SL</b>\n\n"
        "<blockquote>Once price reaches the halfway point to TP1, SL automatically moves to the halfway "
        "point between the original SL and entry — locking in more capital before TP1 even hits.\n\n"
        "Example: Entry 10, TP1 18, SL 6 → at price 14, SL moves to 8.\n\n"
        "Turn on independently for BTC, Scan1, Scan2, Demo1, and Demo2.</blockquote>",
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
        [{"text": "◀️  Back", "callback_data": "copyadmin_sub:coadmin"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>🤝 Co-Admin</b>\n\n"
        "<blockquote>Gives one trusted person control of Scan Control + Trade Control — force scans, "
        "BTC/Scan1/Scan2 on-off, AI model &amp; gateway per type, entry style, TP1%, schedules, "
        "SL/TP/close on any trade, and the Trade History CSV. They still can't see/manage users, "
        "see billing, reset anything, broadcast, or touch this Co-Admin screen. Their contact "
        "shows next to Contact Admin while ON.\n\n"
        "<b>🔀 Switch Settings</b> swaps between two remembered configs — yours and the "
        "co-admin's — of everything above (model, gateway, entry style, TP1%, schedules, "
        "on/off toggles). Switching saves your current setup before loading the other one, "
        "so nothing is lost either way.</blockquote>",
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
    _help_edit_or_send(chat_id, "<b>👤 Choose Co-Admin</b>\n\n<blockquote>Tap the user to grant Trade History CSV access:</blockquote>",
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
            tier_tag = "  ⭐" if _u_ct.get("tier", "free") == "vip" and _u_ct.get("connected") else ("  🆓" if _u_ct.get("connected") else "")
        label = (f"@{uname}" if uname else f"ID {uid}") + tier_tag
        rows.append([{"text": label, "callback_data": f"vip_pick:{uid}"}])
    rows.append([{"text": "◀️  Back", "callback_data": "help_cat:copyadmin"}])
    _help_edit_or_send(chat_id,
        "⭐ <b>Promote to VIP</b>\n\n<blockquote>Choose any registered user — they don't need to have connected BingX yet:</blockquote>",
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
            tier_tag = "  ⭐" if _u_ct.get("tier", "free") == "vip" and _u_ct.get("connected") else ("  🆓" if _u_ct.get("connected") else "")
        label = (f"@{uname}" if uname else f"ID {uid}") + tier_tag
        rows.append([{"text": label, "callback_data": f"free_set:{uid}"}])
    rows.append([{"text": "◀️  Back", "callback_data": "help_cat:copyadmin"}])
    _help_edit_or_send(chat_id,
        "🆓 <b>Demote to Free</b>\n\n<blockquote>Choose any registered user:</blockquote>",
        {"inline_keyboard": rows}, message_id=message_id)

def send_ctpause_screen(chat_id, message_id=None):
    _btc_flag   = "✅ ON" if ct.BTC_CT_ENABLED   else "❌ OFF"
    _scan1_flag = "✅ ON" if ct.SCAN1_CT_ENABLED else "❌ OFF"
    _scan2_flag = "✅ ON" if ct.SCAN2_CT_ENABLED else "❌ OFF"
    _demo1_flag = "✅ ON" if ct.DEMO1_CT_ENABLED else "❌ OFF"
    _demo2_flag = "✅ ON" if ct.DEMO2_CT_ENABLED else "❌ OFF"
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
        [{"text": f"🧪 Demo1 Copy Trade  {_demo1_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "ctdemo1_on"},
         {"text": "🔴 Turn OFF", "callback_data": "ctdemo1_off"}],
        [{"text": f"🧪 Demo2 Copy Trade  {_demo2_flag}", "callback_data": "noop"}],
        [{"text": "🟢 Turn ON",  "callback_data": "ctdemo2_on"},
         {"text": "🔴 Turn OFF", "callback_data": "ctdemo2_off"}],
        [{"text": "◀️  Back", "callback_data": "scan_sub:toggles"}],
    ]
    _help_edit_or_send(chat_id,
        "<b>📋 Copy Trade — By Type</b>\n\n"
        "<blockquote>Turn auto-copy on or off separately for BTC, Scan1, Scan2, Demo1 and Demo2 signals.\n"
        "OFF for a type means no user's account copies those trades — analysis/signals still post as normal.\n"
        "Demo1/Demo2 are OFF by default — turning them ON places REAL orders on users' accounts for demo signals too.</blockquote>",
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
        f"<blockquote>All scans, monitoring and alerts active.\n\n"
        f"🧠 BTC Model:  <b>{_go_model_lbl}</b>\n"
        f"🔌 BTC Gateway: <b>{_go_gateway_lbl}</b>\n\n"
        f"{_go_btc_line}"
        f"{_go_s1_line}"
        f"{_go_s2_line}"
        f"</blockquote>")
    if message_id:
        _help_edit_or_send(chat_id, text, _ctrl_btns, message_id=message_id)
    else:
        send_reply(chat_id, text, reply_markup=_ctrl_btns)

def send_help_menu(chat_id, is_admin, message_id=None, uname=None, cid=None):
    _sees_scanadmin_cats = is_admin or is_co_admin(cid if cid is not None else chat_id)
    _u_ct = ct._get(str(cid)) if cid is not None else None
    _is_vip_user = bool(_u_ct and _u_ct.get("tier") == "vip")
    rows = []
    # CLEXER_API_URL (not MINI_APP_URL — that's a separate, older env var only
    # used for the chart-screenshot feature and was found pointing at a dead
    # Railway domain, causing this button to 404) is the confirmed-live host
    # that actually serves /app.
    _miniapp_base = CLEXER_API_URL
    if _miniapp_base:
        if int(chat_id) > 0:
            # Cache-busting query param — Telegram's Menu Button web app can get stuck
            # serving a stale cached copy indefinitely on some clients even with
            # server no-cache headers. An inline web_app button with a fresh URL
            # every time it's rendered forces Telegram to treat it as a new resource.
            # Top of the menu — this is the flagship surface, not one option among many.
            rows.append([{"text": "📱 Open Dashboard", "web_app": {"url": f"{_miniapp_base}/app?v={int(time.time())}"}}])
        else:
            # web_app inline buttons are only allowed in private chats — Telegram
            # rejects the WHOLE message if one is sent in a group (this is exactly
            # what silently broke /help in groups). A plain t.me deep link works
            # everywhere and still opens the mini app when tapped.
            _mini_uname = _get_bot_username()
            if _mini_uname:
                rows.append([{"text": "📱 Open Dashboard", "url": f"https://t.me/{_mini_uname}?startapp=menu"}])
    if not _is_vip_user:
        rows.append([{"text": "👑 Upgrade to VIP", "callback_data": "vip_menu"}])
    # "Status & Info" and "My Copy Trade" are the two rooms every non-admin user
    # sees — paired into one row instead of stacking every category full-width.
    _monitor_label = _HELP_CATS["monitor"][0]
    _copyuser_label = _HELP_CATS["copyuser"][0]
    rows.append([{"text": _monitor_label, "callback_data": "help_cat:monitor"},
                 {"text": _copyuser_label, "callback_data": "help_cat:copyuser"}])
    for cat_id, (label, admin_only, _) in _HELP_CATS.items():
        if cat_id in ("monitor", "copyuser"):
            continue  # already placed above
        if admin_only and not is_admin:
            if cat_id in ("scan", "tradecontrol") and _sees_scanadmin_cats:
                pass  # co-admin can see these two rooms
            else:
                continue
        rows.append([{"text": label, "callback_data": f"help_cat:{cat_id}"}])
    rows.append([{"text": "🆓 Free Channel", "callback_data": "chanpick:free"},
                 {"text": "⭐ VIP Channel",  "callback_data": "chanpick:vip"}])
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
    if is_admin:
        rows.append([{"text": "🔗 Contact/Channel Settings", "callback_data": "adminlinks_open"}])
    markup = {"inline_keyboard": rows}
    _is_co = is_co_admin(cid) if cid is not None else False
    role = "👑 Admin" if is_admin else ("🤝 Co-Admin" if _is_co else "👤 User")
    _greeting = f"👋 Welcome back, <b>{uname}</b>!\n\n" if uname else ""
    _pnl_line = ""
    _tier_line = ""
    if cid is not None:
        if _u_ct and _u_ct.get("connected"):
            _h = _u_ct.get("history", {})
            _pnl = _h.get("total_pnl", 0.0)
            _pnl_s = f"+${_pnl:.2f} 🟢" if _pnl > 0 else (f"-${abs(_pnl):.2f} 🔴" if _pnl < 0 else "$0.00")
            _pnl_line = f"💰 Your Copy Trade P&L: <b>{_pnl_s}</b>\n\n"
        if _u_ct:
            _tier_val = _u_ct.get("tier", "free")
            _tag = ("⭐ VIP" + (f" (until {_u_ct['vip_end']})" if _u_ct.get("vip_end") else "")) if _tier_val == "vip" else "🆓 FREE"
            _tier_line = f"🏷 Your Tier: <b>{_tag}</b>\n\n"
    text = (
        f"✨ <b>Welcome to CLEXER</b>  {role}\n\n"
        f"<blockquote>{_greeting}{_tier_line}{_pnl_line}"
        "Tap a button to get started 👇</blockquote>"
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
                   "reply_markup": _style_keyboard(markup, rotate=True) or {"inline_keyboard": []}, "disable_web_page_preview": True}
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
    elif back_cb == "winrate_open":
        send_winrate_screen(chat_id, message_id=msg_id)
    elif back_cb == "aicfg_open":
        send_aiconfig_screen(chat_id, message_id=msg_id)
    elif back_cb.startswith("aicfg_open:"):
        send_aiconfig_kind_screen(chat_id, back_cb.split(":", 1)[1], message_id=msg_id)
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
        text = f"<b>{label}</b>\n\n<blockquote>Pick a section 👇</blockquote>"
        _help_edit_or_send(chat_id, text, markup, message_id)
        return

    rows = []
    for cmd, emoji, desc in cmds:
        if cmd == "/stats" and not STATS_VISIBLE_TO_USERS and not is_admin and not is_co_admin(chat_id):
            continue
        rows.append([{"text": f"{emoji}  {desc}", "callback_data": f"help_cmd:{cmd}"}])
    rows.append([{"text": "◀️  Back to Menu", "callback_data": "help_main"}])
    markup = {"inline_keyboard": rows}
    text = f"<b>{label}</b>\n\n<blockquote>Tap any command to run it instantly 👇</blockquote>"
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
    text = f"<b>{label}</b>\n\n<blockquote>" + "\n\n".join(desc_lines) + "</blockquote>"
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
    text = f"<b>{label}</b>\n\n<blockquote>" + "\n\n".join(desc_lines) + "</blockquote>"
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
    text = f"<b>{label}</b>\n\n<blockquote>" + "\n\n".join(desc_lines) + "</blockquote>"
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
    text = f"<b>{label}</b>\n\n<blockquote>" + "\n\n".join(desc_lines) + "</blockquote>"
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
                params={"offset": last_update_id+1, "timeout": 20, "allowed_updates": ["message","callback_query","chat_join_request","pre_checkout_query"]}, timeout=25)
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
                                send_telegram(f"<b>{st['symbol']} {st['action'].upper()} -&gt; {price:,.6f}</b>")
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
                                             "/trailsl": send_trailsl_screen, "/winrate": send_winrate_screen}
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
                            _help_edit_or_send(cb_chat_id, result_text, {"inline_keyboard": merged_rows}, message_id=cb_msg_id,
                                emoji_overrides=captured.get("emoji_overrides"))

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
                            _help_edit_or_send(cb_chat_id, result_text, cap_mkp, message_id=cb_msg_id,
                                emoji_overrides=captured.get("emoji_overrides"))
                        else:
                            _help_edit_or_send(cb_chat_id, result_text, {"inline_keyboard": [[{"text": "◀️  Back", "callback_data": _nc_back_cb}]]}, message_id=cb_msg_id,
                                emoji_overrides=captured.get("emoji_overrides"))
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
                            [{"text": f"{'✅' if '5m' in CHART_TFS else '⚡'}  5M",          "callback_data": "setimg:5m"}],
                            [{"text": "◀️  Back", "callback_data": "settings_sub:charts"}]]}
                        _help_edit_or_send(cb_chat_id,
                            f"<b>Chart Timeframes</b>\n\nActive: <b>{', '.join(CHART_TFS).upper() or 'none'}</b>\n\n<i>Tap to toggle ✅ = active</i>",
                            _tf_btns2, message_id=cb_msg_id)

                    # ── News ON/OFF ───────────────────────────────────────────
                    elif cb_data in ("news_on", "news_off"):
                        _toggle_cmd(f"/news {'on' if cb_data=='news_on' else 'off'}", cb_chat_id, cb_cid, cb_msg_id, "settings")

                    # ── Weekend Sleep ON/OFF ────────────────────────────────────
                    elif cb_data in ("weekendsleep_on", "weekendsleep_off"):
                        _toggle_cmd(f"/ws {'on' if cb_data=='weekendsleep_on' else 'off'}", cb_chat_id, cb_cid, cb_msg_id, "settings")

                    # ── Win Rate & Trade Stats — user access ON/OFF ─────────────
                    elif cb_data in ("statsaccess_on", "statsaccess_off") and cb_is_admin:
                        _toggle_cmd(f"/statsaccess {'on' if cb_data=='statsaccess_on' else 'off'}", cb_chat_id, cb_cid, cb_msg_id, "settings")

                    # ── VIP lucky-draw purchase ───────────────────────────────
                    elif cb_data == "vip_spin":
                        def _do_vip_spin(_chat_id=cb_chat_id, _cid=cb_cid, _msg_id=cb_msg_id):
                            _play_spin_animation(_chat_id, _msg_id)
                            _u = ct._get(str(_cid))
                            _cur_month = now_ist().strftime("%Y-%m")
                            # Admin is exempt from the one-spin-per-month lock — unlimited
                            # retries, for testing the spin without waiting for next month.
                            _is_admin_cid = ADMIN_CHAT_ID and str(_cid) == str(ADMIN_CHAT_ID)
                            if _is_admin_cid or _u.get("vip_spin_month") != _cur_month:
                                _u = ct._db.get(str(_cid)) or ct._default_user(_cid)
                                _u["vip_spin_amount"] = round(random.uniform(VIP_SPIN_MIN, VIP_SPIN_MAX), 2)
                                _u["vip_spin_month"] = _cur_month
                                ct._set(_cid, _u)
                            send_vip_offer_screen(_chat_id, _cid)
                        threading.Thread(target=_do_vip_spin, daemon=True).start()
                    elif cb_data.startswith("vip_pay:"):
                        _amount = float(cb_data.split(":", 1)[1])
                        _help_edit_or_send(cb_chat_id,
                            f"👑 <b>VIP — ${_amount:.2f}</b>\n\n<blockquote>Choose how you'd like to pay.</blockquote>",
                            {"inline_keyboard": [[{"text": "💳 Crypto", "callback_data": f"vip_paycrypto:{_amount:.2f}"},
                                                  _star_button("⭐ Stars", callback_data=f"vip_paystars:{_amount:.2f}")],
                                                 [{"text": "◀️  Back", "callback_data": "vip_menu"}]]},
                            message_id=cb_msg_id, rotate=False, emoji_overrides=_PAYMENT_STAR_OVERRIDE)
                    elif cb_data.startswith("vip_paycrypto:"):
                        _amount = float(cb_data.split(":", 1)[1])
                        _pay_url = _cryptopay_create_invoice(_amount, {"type": "vip", "cid": str(cb_cid)}, description="CLEXER VIP — 30 days")
                        if _pay_url:
                            _help_edit_or_send(cb_chat_id,
                                f"👑 <b>VIP — ${_amount:.2f}</b>\n\n<blockquote>Tap below to pay. VIP activates automatically within ~30s of payment confirming — no need to message anyone.</blockquote>",
                                {"inline_keyboard": [[{"text": f"💳 Pay ${_amount:.2f}", "url": _pay_url, "style": "primary"}],
                                                      [{"text": "◀️  Back", "callback_data": f"vip_pay:{_amount:.2f}"}]]},
                                message_id=cb_msg_id)
                        else:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                json={"callback_query_id": cb["id"], "text": "⚠️ Couldn't create the payment link — try again shortly.", "show_alert": True}, timeout=5)
                    elif cb_data.startswith("vip_paystars:"):
                        _amount = float(cb_data.split(":", 1)[1])
                        _ok = _stars_send_invoice(cb_chat_id, "CLEXER VIP — 30 days",
                            f"VIP membership, 30 days (${_amount:.2f})", {"type": "vip", "cid": str(cb_cid)}, _amount)
                        if not _ok:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                json={"callback_query_id": cb["id"], "text": "⚠️ Couldn't create the Stars invoice — try again shortly.", "show_alert": True}, timeout=5)
                    elif cb_data == "vip_menu":
                        send_vip_offer_screen(cb_chat_id, cb_cid, message_id=cb_msg_id)
                    elif cb_data == "vip_starspin":
                        def _do_vip_starspin(_chat_id=cb_chat_id, _cid=cb_cid, _msg_id=cb_msg_id):
                            _play_spin_animation(_chat_id, _msg_id)
                            _u = ct._get(str(_cid))
                            _cur_month = now_ist().strftime("%Y-%m")
                            _is_admin_cid = ADMIN_CHAT_ID and str(_cid) == str(ADMIN_CHAT_ID)
                            if _is_admin_cid or _u.get("vip_star_spin_month") != _cur_month:
                                _u = ct._db.get(str(_cid)) or ct._default_user(_cid)
                                _star_base = round(VIP_MONTHLY_PRICE * STARS_PER_USD)
                                _star_min = round(_star_base * (1 - VIP_STAR_DISCOUNT_MAX))
                                _u["vip_star_spin_amount"] = random.randint(_star_min, _star_base)
                                _u["vip_star_spin_month"] = _cur_month
                                ct._set(_cid, _u)
                            send_vip_offer_screen(_chat_id, _cid)
                        threading.Thread(target=_do_vip_starspin, daemon=True).start()
                    elif cb_data.startswith("vip_paystarsflat:"):
                        _stars = int(cb_data.split(":", 1)[1])
                        _ok = _stars_send_invoice(cb_chat_id, "CLEXER VIP — 30 days",
                            f"VIP membership, 30 days (⭐{_stars:,})", {"type": "vip", "cid": str(cb_cid)}, _stars / STARS_PER_USD)
                        if not _ok:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                json={"callback_query_id": cb["id"], "text": "⚠️ Couldn't create the Stars invoice — try again shortly.", "show_alert": True}, timeout=5)

                    # ── Free-channel signal unlock ────────────────────────────
                    elif cb_data.startswith("sig_spin:"):
                        def _do_sig_spin(_sig_id=cb_data.split(":", 1)[1], _chat_id=cb_chat_id, _cid=cb_cid, _msg_id=cb_msg_id):
                            _play_spin_animation(_chat_id, _msg_id)
                            _u = ct._db.get(str(_cid)) or ct._default_user(_cid)
                            _spins = _u.setdefault("sig_spins", {})
                            # Admin is exempt from the one-spin-per-signal lock — unlimited retries.
                            _is_admin_cid = ADMIN_CHAT_ID and str(_cid) == str(ADMIN_CHAT_ID)
                            if _is_admin_cid or _sig_id not in _spins:
                                _spins[_sig_id] = round(random.uniform(SIG_SPIN_MIN, SIG_SPIN_MAX), 2)
                                ct._set(_cid, _u)
                            send_unlock_screen(_chat_id, _cid, _sig_id)
                        threading.Thread(target=_do_sig_spin, daemon=True).start()
                    elif cb_data.startswith("sig_pay:"):
                        _sig_id = cb_data.split(":", 1)[1]
                        _u = ct._db.get(str(cb_cid)) or ct._default_user(cb_cid)
                        _amt = _u.get("sig_spins", {}).get(_sig_id)
                        _bal = _u.get("wallet_balance", 0)
                        if _sig_snapshots.get(_sig_id, {}).get("result"):
                            # Closed between spin and pay — don't charge for a dead signal.
                            send_unlock_screen(cb_chat_id, cb_cid, _sig_id, message_id=cb_msg_id)
                        elif _amt is None:
                            try:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": "⚠️ Spin first.", "show_alert": True}, timeout=5)
                            except Exception as e:
                                print(f"  [SIG PAY] answerCallbackQuery error: {e}")
                            send_unlock_screen(cb_chat_id, cb_cid, _sig_id, message_id=cb_msg_id)
                        elif _bal < _amt:
                            # Belt-and-suspenders: the popup alert is the primary feedback,
                            # but also redraw the screen with the shortfall spelled out in
                            # the message body itself — if the alert silently fails to reach
                            # the user (expired callback, network hiccup), they still see why
                            # nothing happened instead of the button just going quiet.
                            try:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": f"⚠️ Not enough balance (${_bal:.2f}) — tap Add Funds first.", "show_alert": True}, timeout=5)
                            except Exception as e:
                                print(f"  [SIG PAY] answerCallbackQuery error: {e}")
                            _short = round(_amt - _bal, 2)
                            _help_edit_or_send(cb_chat_id,
                                f"🔒 <b>Signal Locked</b>\n\n<blockquote>Your unlock price: <b>${_amt:.2f}</b> (locked in for this signal)\n\n"
                                f"Wallet balance: <b>${_bal:.2f}</b> — short by <b>${_short:.2f}</b>. Tap Add Funds to top up, or unlock instantly for a flat ⭐ 1 Star instead.</blockquote>",
                                {"inline_keyboard": [[{"text": f"💳 Pay ${_amt:.2f} from wallet", "callback_data": f"sig_pay:{_sig_id}"}],
                                                      [_star_button("⭐ Unlock for 1 Star", callback_data=f"sig_unlockstar:{_sig_id}")],
                                                      [{"text": "💰 Add Funds", "callback_data": "addfunds_menu"}],
                                                      [{"text": "🏠 Main Menu", "callback_data": "help_main"}]]},
                                message_id=cb_msg_id, rotate=False, emoji_overrides=_PAYMENT_STAR_OVERRIDE)
                        else:
                            # Same-process wallet debit — no external payment involved here,
                            # so this applies synchronously (safe: ct._set is race-free
                            # within this one long-running process).
                            _u["wallet_balance"] = round(_bal - _amt, 2)
                            _u.setdefault("unlocked_sigs", []).append(_sig_id)
                            ct._set(cb_cid, _u)
                            send_unlock_screen(cb_chat_id, cb_cid, _sig_id, message_id=cb_msg_id)
                    elif cb_data.startswith("sig_paystars:"):
                        _sig_id = cb_data.split(":", 1)[1]
                        _u = ct._db.get(str(cb_cid)) or ct._default_user(cb_cid)
                        _amt = _u.get("sig_spins", {}).get(_sig_id)
                        if _sig_snapshots.get(_sig_id, {}).get("result"):
                            # Closed between spin and pay — don't charge for a dead signal.
                            send_unlock_screen(cb_chat_id, cb_cid, _sig_id, message_id=cb_msg_id)
                        elif _amt is None:
                            try:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": "⚠️ Spin first.", "show_alert": True}, timeout=5)
                            except Exception as e:
                                print(f"  [SIG PAY] answerCallbackQuery error: {e}")
                            send_unlock_screen(cb_chat_id, cb_cid, _sig_id, message_id=cb_msg_id)
                        else:
                            _ok = _stars_send_invoice(cb_chat_id, "CLEXER Signal Unlock",
                                f"Unlock signal {_sig_id} (${_amt:.2f})",
                                {"type": "sig_unlock", "cid": str(cb_cid), "sig_id": _sig_id}, _amt)
                            if not _ok:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": "⚠️ Couldn't create the Stars invoice — try again shortly.", "show_alert": True}, timeout=5)
                    elif cb_data.startswith("sig_unlockstar:"):
                        _sig_id = cb_data.split(":", 1)[1]
                        _u = ct._db.get(str(cb_cid)) or ct._default_user(cb_cid)
                        if _sig_id in _u.get("unlocked_sigs", []):
                            send_unlock_screen(cb_chat_id, cb_cid, _sig_id, message_id=cb_msg_id)
                        elif _sig_snapshots.get(_sig_id, {}).get("result"):
                            # Closed between locking and paying — don't charge for a dead signal.
                            send_unlock_screen(cb_chat_id, cb_cid, _sig_id, message_id=cb_msg_id)
                        else:
                            # Flat 1 Star, no spin required — separate from the $ wallet-spin path.
                            _ok = _stars_send_invoice(cb_chat_id, "CLEXER Signal Unlock",
                                f"Unlock signal {_sig_id} (⭐1)",
                                {"type": "sig_unlock", "cid": str(cb_cid), "sig_id": _sig_id}, 1 / STARS_PER_USD)
                            if not _ok:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                    json={"callback_query_id": cb["id"], "text": "⚠️ Couldn't create the Stars invoice — try again shortly.", "show_alert": True}, timeout=5)
                    elif cb_data == "addfunds_menu":
                        send_addfunds_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data.startswith("addfunds:"):
                        _amt = float(cb_data.split(":", 1)[1])
                        _help_edit_or_send(cb_chat_id,
                            f"💰 <b>Add ${_amt:.2f}</b>\n\n<blockquote>Choose how you'd like to pay.</blockquote>",
                            {"inline_keyboard": [[{"text": "💳 Crypto", "callback_data": f"addfundscrypto:{_amt:.2f}"},
                                                  _star_button("⭐ Stars", callback_data=f"addfundsstars:{_amt:.2f}")],
                                                 [{"text": "◀️  Back", "callback_data": "addfunds_menu"}]]},
                            message_id=cb_msg_id, rotate=False, emoji_overrides=_PAYMENT_STAR_OVERRIDE)
                    elif cb_data.startswith("addfundscrypto:"):
                        _amt = float(cb_data.split(":", 1)[1])
                        _pay_url = _cryptopay_create_invoice(_amt, {"type": "topup", "cid": str(cb_cid)}, description="CLEXER Wallet Top-Up")
                        if _pay_url:
                            _help_edit_or_send(cb_chat_id,
                                f"💰 <b>Add ${_amt:.2f}</b>\n\n<blockquote>Tap below to pay. Your wallet credits automatically within ~30s of payment confirming.</blockquote>",
                                {"inline_keyboard": [[{"text": f"💳 Pay ${_amt:.2f}", "url": _pay_url, "style": "primary"}],
                                                      [{"text": "◀️  Back", "callback_data": f"addfunds:{_amt:.2f}"}]]},
                                message_id=cb_msg_id)
                        else:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                json={"callback_query_id": cb["id"], "text": "⚠️ Couldn't create the payment link — try again shortly.", "show_alert": True}, timeout=5)
                    elif cb_data.startswith("addfundsstars:"):
                        _amt = float(cb_data.split(":", 1)[1])
                        _ok = _stars_send_invoice(cb_chat_id, "CLEXER Wallet Top-Up",
                            f"Wallet top-up (${_amt:.2f})", {"type": "topup", "cid": str(cb_cid), "usd": _amt}, _amt)
                        if not _ok:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                json={"callback_query_id": cb["id"], "text": "⚠️ Couldn't create the Stars invoice — try again shortly.", "show_alert": True}, timeout=5)

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
                    elif cb_data == "ctdemo1_on" and cb_is_scanadmin:
                        ct.set_demo1_ct(True); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "ctdemo1_off" and cb_is_scanadmin:
                        ct.set_demo1_ct(False); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "ctdemo2_on" and cb_is_scanadmin:
                        ct.set_demo2_ct(True); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "ctdemo2_off" and cb_is_scanadmin:
                        ct.set_demo2_ct(False); save_settings(); send_ctpause_screen(cb_chat_id, message_id=cb_msg_id)

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
                        ], [
                            {"text": "◀️  Back", "callback_data": "settings_sub:btcsettings"},
                        ]]}
                        if btc_analysis_enabled:
                            _btca_text = "📡 <b>BTC Analysis</b>  ✅ ON\n\n<blockquote>Scheduled scans active.\n\n</blockquote>"
                        else:
                            _btca_text = "📡 <b>BTC Analysis</b>  ⏸ OFF\n\n<blockquote>Scheduled scans paused.\n\n</blockquote>"
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
                            json={"chat_id": cb_chat_id, "message_id": cb_msg_id,
                                  "text": _apply_premium_emojis(_btca_text), "parse_mode": "HTML",
                                  "reply_markup": _style_keyboard(_btca_mkp)}, timeout=10)
                    elif cb_data in ("history_btc", "history_scan1", "history_scan2", "history_ts1", "history_ts2"):
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
                    elif cb_data.startswith("aicfg_open2:") and cb_is_scanadmin:
                        _, _ao_kind, _ao_tier = cb_data.split(":", 2)
                        send_aiconfig_type_screen(cb_chat_id, _ao_kind, _ao_tier, message_id=cb_msg_id)
                    elif cb_data.startswith("aicfg_open:") and cb_is_scanadmin:
                        send_aiconfig_kind_screen(cb_chat_id, cb_data.split(":", 1)[1], message_id=cb_msg_id)
                    elif cb_data.startswith("aicfg_set:") and cb_is_scanadmin:
                        _, _kind, _tier, _gw, _mdl = cb_data.split(":", 4)
                        _model_val = "claude-opus-4-8" if _mdl == "opus" else "claude-fable-5"
                        _aero_val = (_gw == "aerolink")
                        if _kind == "btc":
                            global SCAN_MODEL, USE_AEROLINK
                            SCAN_MODEL = _model_val; USE_AEROLINK = _aero_val
                        else:
                            AICFG_GRID[_kind][_tier]["model"] = _model_val
                            AICFG_GRID[_kind][_tier]["aerolink"] = _aero_val
                        save_settings()
                        send_aiconfig_type_screen(cb_chat_id, _kind, _tier, message_id=cb_msg_id)
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
                    elif cb_data == "trailsl_demo1_on" and cb_is_scanadmin:
                        global TRAIL_SL_DEMO1
                        TRAIL_SL_DEMO1 = True; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "trailsl_demo1_off" and cb_is_scanadmin:
                        TRAIL_SL_DEMO1 = False; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "trailsl_demo2_on" and cb_is_scanadmin:
                        global TRAIL_SL_DEMO2
                        TRAIL_SL_DEMO2 = True; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data == "trailsl_demo2_off" and cb_is_scanadmin:
                        TRAIL_SL_DEMO2 = False; save_settings(); send_trailsl_screen(cb_chat_id, message_id=cb_msg_id)
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
                    elif cb_data == "winrate_open" and cb_is_scanadmin:
                        send_winrate_screen(cb_chat_id, message_id=cb_msg_id)
                    elif cb_data.startswith("winrate_open:") and cb_is_scanadmin:
                        _np_state[str(cb_cid)] = {"target": cb_data.split(":", 1)[1], "digits": "", "back_cb": "winrate_open"}
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
                        broadcast_pending[cb_chat_id] = {"step": "waiting_message", "mode": _mode, "msg_id": cb_msg_id}
                        _mode_lbl = {"users": "Users Only", "channels": "Channels Only", "all": "Both"}[_mode]
                        _help_edit_or_send(cb_chat_id,
                            f"📢 <b>Broadcast — {_mode_lbl}</b>\n\nSend message now (text/image/PDF).\n\n<i>/cancel to abort</i>",
                            None, message_id=cb_msg_id)

                    elif cb_data.startswith("bctgl:") and cb_is_admin:
                        _tgl_id = cb_data.split(":", 1)[1]
                        st = _bc_picker_state.get(str(cb_chat_id))
                        if st:
                            if _tgl_id in st["selected"]: st["selected"].discard(_tgl_id)
                            else: st["selected"].add(_tgl_id)
                            _send_broadcast_picker(cb_chat_id, message_id=cb_msg_id)

                    elif cb_data == "bcprev" and cb_is_admin:
                        st = _bc_picker_state.pop(str(cb_chat_id), None)
                        _mode = st["mode"] if st else "all"
                        broadcast_pending[cb_chat_id] = {"step": "waiting_message", "mode": _mode, "msg_id": cb_msg_id}
                        _mode_lbl = {"users": "Users Only", "channels": "Channels Only", "all": "Both"}[_mode]
                        _help_edit_or_send(cb_chat_id,
                            f"📢 <b>Broadcast — {_mode_lbl}</b>\n\nSend message now (text/image/PDF).\n\n<i>/cancel to abort</i>",
                            None, message_id=cb_msg_id)

                    elif cb_data == "bcback" and cb_is_admin:
                        _bc_picker_state.pop(str(cb_chat_id), None)
                        broadcast_pending.pop(cb_chat_id, None)
                        _bc_btns = {"inline_keyboard": [[
                            {"text": "👥 Users Only",    "callback_data": "broadcast_mode:users"},
                            {"text": "📢 Channels Only", "callback_data": "broadcast_mode:channels"},
                        ], [
                            {"text": "🌍 Both (Users + Channels)", "callback_data": "broadcast_mode:all"},
                        ]]}
                        _help_edit_or_send(cb_chat_id, "📢 <b>Broadcast Mode</b>\n\nWho should receive this message?",
                            _bc_btns, message_id=cb_msg_id)

                    elif cb_data == "bcsend" and cb_is_admin:
                        st = _bc_picker_state.pop(str(cb_chat_id), None)
                        if not st:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                json={"callback_query_id": cb["id"], "text": "⚠️ Nothing to send — start over.", "show_alert": True}, timeout=5)
                        else:
                            _sel = list(st["selected"])
                            _mode_label = {"users": "registered users", "channels": "channels", "all": "users + channels"}[st["mode"]]
                            _help_edit_or_send(cb_chat_id, f"📢 Broadcasting to {_mode_label} ({len(_sel)} channels)...", None, message_id=cb_msg_id)
                            threading.Thread(target=do_broadcast,
                                args=(cb_chat_id, st["text"], st["file_id"], st["file_type"], st["mode"], _sel), daemon=True).start()
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
                        send_reply(cb_chat_id, ct.reset_ghost_state(uid, "btc"))
                    elif cb_data.startswith("sync_reset_scan_ghost:"):
                        _, uid, sym = cb_data.split(":", 2)
                        send_reply(cb_chat_id, ct.reset_ghost_state(uid, "scan", sym))
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
                    elif cb_data.startswith("srvset:") and cb_is_admin:
                        handle_command(f"/server {cb_data.split(':',1)[1]}", cb_chat_id, {}, sender_id=cb_cid)
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
                            f"Used for all scan/BTC/coin analysis calls.")
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
                            f"Your real Anthropic key is never sent to Aerolink — the two keys stay fully separate.")
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
                        _cmd = {"1": "/alt", "2": "/alt2", "3": "/altdemo", "4": "/altdemo2"}[_ver]
                        _lbl = {"1": "Scan1", "2": "Scan2", "3": "TS1", "4": "TS2"}[_ver]
                        _alt_back_cb, _ = _find_back_target(_cmd)
                        if _mode == "alt_loop":
                            pending_input[cb_cid] = {"cmd": _cmd, "step": "loop", "msg_id": None, "cat_id": _alt_back_cb}
                            send_reply(cb_chat_id,
                                f"🔁 <b>Loop Mode — {_lbl}</b>\n\n"
                                f"Type the minute <b>(0–59)</b>:\n"
                                f"Bot will run every hour at that minute.\n\n"
                                f"<i>Example: type <code>2</code> → runs at 1:02, 2:02, 3:02, 4:02...</i>")
                        else:
                            pending_input[cb_cid] = {"cmd": _cmd, "step": "manual", "msg_id": None, "cat_id": _alt_back_cb}
                            send_reply(cb_chat_id,
                                f"📋 <b>Manual Times — {_lbl}</b>\n\n"
                                f"Type your specific times separated by spaces:\n\n"
                                f"<i>Example: <code>2.02 2.23 14.25 15.26 15.46</code></i>")

                    elif cb_data.startswith("dnav:") and (cb_is_admin or is_co_admin(cb_cid)):
                        _dp = cb_data.split(":")
                        _rtype, _action = _dp[1], _dp[2]
                        if _rtype == "report" and not cb_is_admin:
                            pass
                        elif _action == "years":
                            _help_edit_or_send(cb_chat_id, f"📅 <b>{_dnav_label(_rtype)} — Select Year</b>",
                                _dnav_years_mkp(_rtype), message_id=cb_msg_id, rotate=False)
                        elif _action == "year":
                            _year = int(_dp[3])
                            _help_edit_or_send(cb_chat_id, f"📅 <b>{_year}</b>\n\nChoose how to filter:",
                                _dnav_period_mkp(_rtype, _year), message_id=cb_msg_id, rotate=False)
                        elif _action == "period":
                            _year = int(_dp[3]); _period = _dp[4]
                            _help_edit_or_send(cb_chat_id, f"📅 <b>{_year} — {_period.title()}</b>\n\nChoose a month:",
                                _dnav_months_mkp(_rtype, _year, _period), message_id=cb_msg_id, rotate=False)
                        elif _action == "month":
                            _year = int(_dp[3]); _period = _dp[4]; _month = int(_dp[5])
                            if _period == "monthly":
                                _dnav_send_file(cb_chat_id, _rtype, _year, _month, message_id=cb_msg_id)
                            else:
                                _help_edit_or_send(cb_chat_id, f"📆 <b>{_MONTH_NAMES[_month-1]} {_year}</b>\n\nChoose a week:",
                                    _dnav_weeks_mkp(_rtype, _year, _month), message_id=cb_msg_id, rotate=False)
                        elif _action == "week":
                            _year = int(_dp[3]); _month = int(_dp[4]); _week = int(_dp[5])
                            _dnav_send_file(cb_chat_id, _rtype, _year, _month, week=_week, message_id=cb_msg_id)
                    continue

                # Telegram Stars checkout — must be answered within 10s of the user
                # tapping Pay, before the successful_payment message ever arrives.
                # We always approve; the invoice was only ever created for known
                # amounts/types by our own code, so there's nothing to validate.
                pcq = upd.get("pre_checkout_query")
                if pcq:
                    try:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerPreCheckoutQuery",
                            json={"pre_checkout_query_id": pcq["id"], "ok": True}, timeout=10)
                    except Exception as e:
                        print(f"  [STARS] answerPreCheckoutQuery error: {e}")
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
                            _jr_payload = {"chat_id": _jr_user_id,
                                      "text": "⭐ This channel is VIP-only. Contact admin to activate VIP first."}
                            if _mkp: _jr_payload["reply_markup"] = _mkp
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                json=_jr_payload, timeout=10)
                            print(f"  [VIP CHANNEL] declined @{_jr_uname} ({_jr_user_id}) — not VIP")
                    except Exception as e:
                        print(f"  [VIP CHANNEL] join request error: {e}")
                    continue

                msg = upd.get("message",{}); text = msg.get("text","") or ""
                cid = msg.get("chat",{}).get("id"); uname = msg.get("from",{}).get("username","?")
                sender_uid = msg.get("from",{}).get("id")
                if not cid: continue

                # Telegram Stars payment completed — arrives as a normal message
                # carrying successful_payment, no webhook/poll loop needed (unlike
                # CryptoBot, Stars payments settle inside Telegram itself). Mirrors
                # _poll_payment_events' vip/topup handling for the crypto gateway.
                sp = msg.get("successful_payment")
                if sp:
                    try:
                        _sp_payload = json.loads(sp.get("invoice_payload") or "{}")
                    except Exception:
                        _sp_payload = {}
                    _sp_stars = sp.get("total_amount", 0)
                    _sp_type  = _sp_payload.get("type")
                    _sp_cid   = _sp_payload.get("cid") or str(cid)
                    if _sp_type == "vip":
                        _grant_vip(_sp_cid, days=30)
                        send_to_user(_sp_cid, f"🎉 <b>VIP Activated!</b>\n\nPaid: ⭐{_sp_stars:,} Stars · 30 days\n\nTap ⭐ VIP Channel in /help to get access.")
                    elif _sp_type == "topup":
                        _u = ct._db.get(str(_sp_cid)) or ct._default_user(_sp_cid)
                        _sp_usd = float(_sp_payload.get("usd", _sp_stars / STARS_PER_USD))
                        _u["wallet_balance"] = round(_u.get("wallet_balance", 0) + _sp_usd, 2)
                        ct._set(_sp_cid, _u)
                        send_to_user(_sp_cid, f"💰 <b>Wallet credited</b>: +${_sp_usd:,.2f} (⭐{_sp_stars:,} Stars)\n\nNew balance: <b>${_u['wallet_balance']:,.2f}</b>")
                    elif _sp_type == "sig_unlock":
                        _sp_sig_id = _sp_payload.get("sig_id")
                        _u = ct._db.get(str(_sp_cid)) or ct._default_user(_sp_cid)
                        _u.setdefault("unlocked_sigs", []).append(_sp_sig_id)
                        ct._set(_sp_cid, _u)
                        _sp_snap = _sig_snapshots.get(_sp_sig_id)
                        if _sp_snap:
                            send_to_user(_sp_cid, _reveal_signal_text(_sp_snap, _sp_sig_id))
                        else:
                            send_to_user(_sp_cid, f"✅ <b>Unlocked</b> (⭐{_sp_stars:,} Stars paid) — but this signal's snapshot expired, contact admin if it doesn't show up.")
                    # Delete the invoice card itself now that it's paid — Telegram
                    # leaves it sitting in the chat forever otherwise, since the
                    # successful_payment update is a separate new message, not an
                    # edit of the invoice. Best-effort: a delete failure (e.g. the
                    # 48h window, or the key was never captured) is non-fatal.
                    _inv_key = (str(cid), _sp_type, _sp_payload.get("sig_id", ""))
                    _inv_mid = _pending_star_invoices.pop(_inv_key, None)
                    if _inv_mid:
                        try:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage",
                                json={"chat_id": cid, "message_id": _inv_mid}, timeout=10)
                        except Exception as e:
                            print(f"  [STARS] delete invoice msg error: {e}")
                    print(f"  [STARS] payment applied: type={_sp_type} cid={_sp_cid} stars={_sp_stars}")
                    continue

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
                                _help_edit_or_send(cid, result_text, _back_mkp, message_id=_pi_msg_id,
                                    emoji_overrides=captured.get("emoji_overrides"))
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
                                    send_telegram(f"<b>{ppst['symbol']} {ppst['action'].upper()} -&gt; {price:,.6f}</b>")
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
                            _help_edit_or_send(cid, result_text, final_mkp, message_id=_pi_msg_id,
                                emoji_overrides=captured.get("emoji_overrides"))
                        else:
                            send_reply(cid, result_text, reply_markup=final_mkp)
                    continue
                if text.startswith("/"):
                    handle_command(text, cid, msg, sender_id=sender_uid)
                elif str(cid) in _chat_sessions:
                    # Regular users must actually forward a message (in a private
                    # chat) or reply directly to one of the bot's own messages (in
                    # a group — forwarding into a group you're already chatting in
                    # doesn't make sense, and typing the full @username every time
                    # is annoying) to get a /chat answer — plain typed messages
                    # don't trigger a reply on their own. forward_origin is the
                    # current Bot API field; forward_from / forward_from_chat /
                    # forward_date are the pre-7.0 fields, kept as a fallback for
                    # older forwarded-message shapes. Admin's own private chat is exempt.
                    _is_admin_chat = ADMIN_CHAT_ID and str(cid) == str(ADMIN_CHAT_ID)
                    _is_forward = bool(msg.get("forward_origin") or msg.get("forward_from")
                                        or msg.get("forward_from_chat") or msg.get("forward_date"))
                    _reply_to = msg.get("reply_to_message") or {}
                    _is_reply_to_bot = bool(_reply_to.get("from", {}).get("is_bot"))
                    if _is_admin_chat or _is_forward or _is_reply_to_bot:
                        _handle_chat_message(cid, text)
        except Exception as e:
            print(f"  [CMD] {e}")
            # Previously silent to the admin — a crash here (e.g. mid-callback,
            # like a button inside a help submenu) just looked like the bot not
            # responding at all, with the real reason only visible in server
            # logs. Surface it in DM instead, best-effort (never let the error
            # notification itself take the loop down).
            try:
                if ADMIN_CHAT_ID:
                    send_reply(ADMIN_CHAT_ID, f"⚠️ <b>Command listener error</b>\n\n<code>{_html.escape(str(e))[:500]}</code>")
            except Exception:
                pass
        time.sleep(2)

# --- MAIN ---------------------------------------------------------------------
# ── Scan1 fixed schedule (IST HH:MM) ─────────────────────────────────────────
def _regular_grid(minute_a: int, minute_b: int, special: set, near_thresh: int = 5) -> set:
    """Every hour's :minute_a and :minute_b, minus any slot that exactly matches
    or falls within near_thresh minutes of a special time in the same hour
    (avoids running a near-duplicate scan right next to the special one)."""
    out = set()
    for h in range(24):
        for m in (minute_a, minute_b):
            if any(sh == h and abs(sm - m) <= near_thresh for sh, sm in special):
                continue
            out.add((h, m))
    return out

# Scan1: special times (Direct+Opus, tier-routed) + hourly :02/:23 regular grid (Aerolink+Opus, Signal-only)
SCAN1_SCHEDULE: list[tuple[int,int]] = []
# Scan2: special times (Direct+Opus, tier-routed) + hourly :07/:27 regular grid (Aerolink+Opus, Signal-only)
SCAN2_SCHEDULE: list[tuple[int,int]] = []
# TS1: special times (Direct+Opus, tier-routed) + hourly :09/:27 regular grid (Aerolink+Opus, Signal-only)
SCAN1_TEST_SCHEDULE: list[tuple[int,int]] = []
# TS2: independent schedule, same shape as TS1 but its own special/regular times
SCAN2_TEST_SCHEDULE: list[tuple[int,int]] = []
# Built via _rebuild_schedules() (single source of truth, also folds in any
# relocated/blacklisted times restored by _load_slot_state() above) instead
# of duplicating the same union/subtract formula here.
_rebuild_schedules()
_scan1_triggered_today: set[tuple[int,int]] = set()   # (hour,minute) pairs run today
_test_triggered_today:  set[tuple[int,int]] = set()

def _clear_own_triggers(tracker_set: set, ver: int):
    """_scan1_triggered_today and _test_triggered_today are each SHARED between
    two scan versions — ver 1's entries are raw (h,m) tuples, ver 2's are tagged
    ((h,m), 2). A schedule-editor command for one version must only clear its
    OWN entries — wiping the whole set would also drop the other version's
    dedup state, letting it auto-trigger a second time in the same minute."""
    if ver == 1:
        tracker_set.difference_update({e for e in list(tracker_set) if not isinstance(e[0], tuple)})
    else:
        tracker_set.difference_update({e for e in list(tracker_set) if isinstance(e[0], tuple)})
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
    send_admin(f"🔄 <b>Auto-Scan {lbl}</b>  {ist_str()}\n\nScheduled scan starting (~60s)...")
    # Clear cycle dedup set when scan1 starts (scan1 always starts first)
    if scan_ver == 1:
        with _scan_cycle_lock:
            _scan_cycle_placed.clear()
    cmd = "/scan1" if scan_ver == 1 else "/scan2"
    # Note: /scan2's actual work runs in its own background thread (_do_scan) that
    # handle_command() merely kicks off — it returns almost instantly. The special-
    # time flag is cleared by _do_scan itself once that thread finishes, NOT here,
    # otherwise it gets cleared before the real Claude call ever happens.
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

def _force_close_demo_trade(dver: int, symbol: str, result: str) -> str:
    """Admin /forceclose ts1|ts2 — same idea as _force_close_scan_trade but
    for TS1/TS2 demo trades. result: tp1/tp2/sl/be."""
    result = result.lower()
    if result not in ("tp1", "tp2", "sl", "be"):
        return "Result must be one of: tp1, tp2, sl, be."
    demo_list = demo_scan1_trades if dver == 1 else demo_scan2_trades
    t = next((x for x in demo_list if x.get("symbol", "").upper().startswith(symbol.upper())), None)
    if not t:
        return f"No open TS{dver} trade found matching '{symbol}'."
    sym = t.get("symbol", ""); sig = t.get("signal", "")
    entry = float(t.get("entry", 0)); sl = float(t.get("sl", 0))
    tp1 = float(t.get("tp1", 0)); tp2 = float(t.get("tp2", 0))
    tp1hit = t.get("tp1_hit", False); be_sl = float(t.get("be_sl", 0))
    created = float(t.get("created_at", time.time()))
    tier_routed = t.get("tier_routed", False); share_free = t.get("share_free", False)
    sig_id = t.get("sig_id", "")
    coin = sym.replace("-USDT", "")
    cp = get_bingx_price(sym) or entry
    _dtype = f"demo{dver}"

    if result == "tp2":
        _delete_trail_sl_messages(t)
        log_trade_event({"type": _dtype, "coin": sym, "direction": sig,
            "tp2_hit_time": _ist_str_now(), "result": "TP2",
            "entry_price": entry, "sl_price": sl, "tp1_price": tp1, "tp2_price": tp2})
        _msg = _scan_box(
            f"#{coin} TP2 Hit", f"🏆 TS{dver} {coin}-USDT",
            [[f"📊 {_smallcaps_title('Price')} @ TP2: <code>{cp:,.6g}</code>",
              f"🎯 {_smallcaps_title('Entry')}: <code>{entry:,.6g}</code>",
              f"🏆 TP2: <code>{tp2:,.6g}</code>",
              f"✅ {_smallcaps_title('Result')}: {_smallcaps_title('Full win')}"]],
            tag=sig_id)
        send_lifecycle_reply(_msg, t.get("reply_map"), include_ch2=True, tier_routed=tier_routed, share_free=share_free, reply_markup=_tp_buttons())
        ct.on_scan_tp2(sym)
        _track_daily_result(sym, "TP2", tier_routed=tier_routed, free_shown=share_free, entry_date=_ist_date_str(created), sig_id=sig_id)
        _notify_free_late(sym, t, "TP2")
        _slot_hm = _ist_hm_from_epoch(created)
        if _slot_hm: _slot_track(f"demo{dver}", _slot_hm, True)
        _log_demo_history(t, "TP2", cp, dver)
        _close_sig_snapshot(sig_id, "TP2")
        demo_list.remove(t); save_state()
        return f"✅ {sym} force-closed as TP2 @ {cp:,.4g} — announced, recorded, removed."

    if result == "tp1":
        if tp1hit:
            return f"{sym} already shows tp1_hit=True — nothing to do."
        be_sl_price = round(entry * 1.001 if sig == "SELL" else entry * 0.999, 6)
        t["tp1_hit"] = True; t["be_sl"] = be_sl_price
        _delete_trail_sl_messages(t)
        log_trade_event({"type": _dtype, "coin": sym, "direction": sig,
            "tp1_hit_time": _ist_str_now(), "result": "TP1_partial",
            "entry_price": entry, "sl_price": be_sl_price, "tp1_price": tp1, "tp2_price": tp2})
        _msg = _scan_box(
            f"#{coin} TP1 Hit", f"🎯 TS{dver} {coin}-USDT",
            [[f"📊 {_smallcaps_title('Price')} @ TP1: <code>{cp:,.6g}</code>",
              f"🛡️ {_smallcaps_title(f'{ct.TP1_CLOSE_PCT}% closed')}",
              f"🔒 BE SL: <code>{be_sl_price:,.6g}</code>",
              f"🚀 {_smallcaps_title('Runner TP2')}: <code>{tp2:,.6g}</code>"]],
            tag=sig_id)
        send_lifecycle_reply(_msg, t.get("reply_map"), include_ch2=True, tier_routed=tier_routed, share_free=share_free, reply_markup=_tp_buttons())
        ct.on_scan_tp1(sym)
        _track_daily_result(sym, "TP1", tier_routed=tier_routed, free_shown=share_free,
            tp1_detail={"tag": f"TS{dver}", "side": sig, "tp1": tp1, "sl_be": be_sl_price, "tp2": tp2},
            entry_date=_ist_date_str(created), sig_id=sig_id)
        _notify_free_late(sym, t, "TP1")
        save_state()
        return f"✅ {sym} force-marked TP1 hit @ {cp:,.4g} — SL moved to BE, trade stays open for TP2."

    # sl / be
    lbl = "BE" if tp1hit else "SL"
    close_result = "BREAKEVEN" if tp1hit else "LOSS"
    _sl_exit = be_sl if tp1hit and be_sl else sl
    _delete_trail_sl_messages(t)
    log_trade_event({"type": _dtype, "coin": sym, "direction": sig,
        "sl_hit_time": _ist_str_now(), "result": close_result,
        "entry_price": entry, "sl_price": _sl_exit, "tp1_price": tp1, "tp2_price": tp2})
    _msg = _scan_box(
        f"#{coin} {lbl} Hit", f"🚨 TS{dver} {coin}-USDT",
        [[f"📊 {_smallcaps_title('Price')} @ {lbl}: <code>{cp:,.6g}</code>",
          f"🎯 {_smallcaps_title('Entry')}: <code>{entry:,.6g}</code>",
          f"🛑 {lbl}: <code>{_sl_exit:,.6g}</code>",
          f"{'🛡️' if close_result == 'BREAKEVEN' else '❌'} {_smallcaps_title('Result')}: {_smallcaps_title(close_result)}"]],
        tag=sig_id)
    _send_sl_and_log(_msg, t.get("reply_map"), sig_id, lbl, include_ch2=False, tier_routed=tier_routed, share_free=share_free)
    ct.on_scan_sl(sym)
    if lbl == "SL":
        _track_daily_result(sym, "SL", tier_routed=tier_routed, free_shown=tier_routed and share_free, entry_date=_ist_date_str(created))
        _send_sl_reassurance(sym, f"TS{dver}", sig, entry,
            _sl_reassurance_channels(tier_routed, share_free), t.get("reply_map"), sig_id)
    _slot_hm = _ist_hm_from_epoch(created)
    if _slot_hm: _slot_track(f"demo{dver}", _slot_hm, close_result == "BREAKEVEN")
    _log_demo_history(t, lbl, cp, dver)
    _close_sig_snapshot(sig_id, close_result)
    demo_list.remove(t); save_state()
    return f"✅ {sym} force-closed as {lbl} @ {cp:,.4g} — announced, recorded, removed."

def _demo_monitor_loop():
    """Background thread: monitors demo trades every 30s. Sends TG alerts and,
    when Demo1/Demo2 copy trade is turned ON, closes matching real copy-user
    positions on TP1/TP2/SL/timeout via ct.on_scan_tp1/tp2/sl (symbol-based lookup —
    safe no-op if no copy user has that symbol open in a demo slot)."""
    import re as _re
    while True:
        try:
            time.sleep(30)
            if bot_paused.is_set(): continue
            now = time.time()
            for _dver, demo_list in ((1, demo_scan1_trades), (2, demo_scan2_trades)):
                _dtype = f"demo{_dver}"
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
                    tier_routed = t.get("tier_routed", False)
                    share_free = t.get("share_free", False)
                    is_d48 = t.get("is_d48", False)
                    sig_id = t.get("sig_id","")

                    if not sym or not entry: continue
                    cp = get_bingx_price(sym)
                    if cp <= 0: continue

                    _apply_trail_sl(2 + _dver, t, cp)  # ver 3=demo1, 4=demo2
                    sl = float(t.get("sl", 0))

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
                    arrow = "🟩" if sig == "BUY" else "🟥"
                    if tp2_now:
                        _delete_trail_sl_messages(t)
                        log_trade_event({"type":_dtype,"coin":sym,"direction":sig,
                            "tp2_hit_time":_ist_str_now(),"result":"TP2",
                            "entry_price":entry,"sl_price":sl,"tp1_price":tp1,"tp2_price":tp2})
                        _msg = _scan_box(
                            f"#{coin} TP2 Hit", f"🏆 TS{_dver} {coin}-USDT",
                            [[f"📊 {_smallcaps_title('Price')} @ TP2: <code>{cp:,.6g}</code>",
                              f"🎯 {_smallcaps_title('Entry')}: <code>{entry:,.6g}</code>",
                              f"🏆 TP2: <code>{tp2:,.6g}</code>",
                              f"✅ {_smallcaps_title('Result')}: {_smallcaps_title('Full win')}"]],
                            tag=sig_id)
                        send_lifecycle_reply(_msg, t.get("reply_map"), include_ch2=True, tier_routed=tier_routed, share_free=share_free, reply_markup=_tp_buttons())
                        ct.on_scan_tp2(sym)
                        _track_daily_result(sym, "TP2", tier_routed=tier_routed, free_shown=share_free, entry_date=_ist_date_str(created), sig_id=sig_id)
                        _notify_free_late(sym, t, "TP2")
                        _slot_hm = _ist_hm_from_epoch(created)
                        if _slot_hm: _slot_track(f"demo{_dver}", _slot_hm, True)
                        _log_demo_history(t, "TP2", cp, _dver)
                        _close_sig_snapshot(sig_id, "TP2")
                        to_remove.append(t)
                    elif sl_hit:
                        lbl = "BE" if tp1hit else "SL"
                        result = "BREAKEVEN" if tp1hit else "LOSS"
                        _sl_exit = be_sl if tp1hit and be_sl else sl
                        _delete_trail_sl_messages(t)
                        log_trade_event({"type":_dtype,"coin":sym,"direction":sig,
                            "sl_hit_time":_ist_str_now(),"result":result,
                            "entry_price":entry,"sl_price":_sl_exit,
                            "tp1_price":tp1,"tp2_price":tp2})
                        _msg = _scan_box(
                            f"#{coin} {lbl} Hit", f"🚨 TS{_dver} {coin}-USDT",
                            [[f"📊 {_smallcaps_title('Price')} @ {lbl}: <code>{cp:,.6g}</code>",
                              f"🎯 {_smallcaps_title('Entry')}: <code>{entry:,.6g}</code>",
                              f"🛑 {lbl}: <code>{_sl_exit:,.6g}</code>",
                              f"{'🛡️' if result == 'BREAKEVEN' else '❌'} {_smallcaps_title('Result')}: {_smallcaps_title(result)}"]],
                            tag=sig_id)
                        _send_sl_and_log(_msg, t.get("reply_map"), sig_id, lbl, include_ch2=False, tier_routed=tier_routed, share_free=share_free)
                        ct.on_scan_sl(sym)
                        if lbl == "SL":
                            _track_daily_result(sym, "SL", tier_routed=tier_routed, free_shown=tier_routed and share_free, entry_date=_ist_date_str(created))
                            _send_sl_reassurance(sym, f"TS{_dver}", sig, entry,
                                _sl_reassurance_channels(tier_routed, share_free), t.get("reply_map"), sig_id)
                        _slot_hm = _ist_hm_from_epoch(created)
                        if _slot_hm: _slot_track(f"demo{_dver}", _slot_hm, result == "BREAKEVEN")
                        _log_demo_history(t, lbl, cp, _dver)
                        _close_sig_snapshot(sig_id, result)
                        to_remove.append(t)
                    elif tp1_now:
                        be_sl_price = round(entry * 1.001 if sig == "SELL" else entry * 0.999, 6)
                        t["tp1_hit"] = True
                        t["be_sl"]   = be_sl_price
                        _delete_trail_sl_messages(t)
                        log_trade_event({"type":_dtype,"coin":sym,"direction":sig,
                            "tp1_hit_time":_ist_str_now(),"result":"TP1_partial",
                            "entry_price":entry,"sl_price":be_sl_price,"tp1_price":tp1,"tp2_price":tp2})
                        _msg = _scan_box(
                            f"#{coin} TP1 Hit", f"🎯 TS{_dver} {coin}-USDT",
                            [[f"📊 {_smallcaps_title('Price')} @ TP1: <code>{cp:,.6g}</code>",
                              f"🛡️ {_smallcaps_title(f'{ct.TP1_CLOSE_PCT}% closed')}",
                              f"🔒 BE SL: <code>{be_sl_price:,.6g}</code>",
                              f"🚀 {_smallcaps_title('Runner TP2')}: <code>{tp2:,.6g}</code>"]],
                            tag=sig_id)
                        send_lifecycle_reply(_msg, t.get("reply_map"), include_ch2=True, tier_routed=tier_routed, share_free=share_free, reply_markup=_tp_buttons())
                        ct.on_scan_tp1(sym)
                        _track_daily_result(sym, "TP1", tier_routed=tier_routed, free_shown=share_free,
                            tp1_detail={"tag": f"TS{_dver}", "side": sig, "tp1": tp1, "sl_be": be_sl_price, "tp2": tp2},
                            entry_date=_ist_date_str(created), sig_id=sig_id)
                        _notify_free_late(sym, t, "TP1")
                        save_state()  # persist tp1_hit + BE SL immediately — trade stays open (no to_remove append) so the loop's own save_state() below wouldn't otherwise run for this branch
                    elif timeout_hit:
                        pnl = (cp - entry) / entry * 100 * (1 if sig == "BUY" else -1)
                        _delete_trail_sl_messages(t)
                        log_trade_event({"type":_dtype,"coin":sym,"direction":sig,
                            "timeout_time":_ist_str_now(),"result":f"TIMEOUT({pnl:+.2f}%)",
                            "entry_price":entry,"sl_price":sl,"tp1_price":tp1,"tp2_price":tp2})
                        _timeout_line = (f"1ʜ ᴇʟᴀᴘꜱᴇᴅ ꜱɪɴᴄᴇ TP1 — {_smallcaps_title(f'Remaining {100-ct.TP1_CLOSE_PCT}% runner closed')}"
                                         if tp1hit else f"{_smallcaps_title('1H elapsed — no TP1/SL hit')}")
                        _msg = _scan_box(
                            f"#{coin} Timeout", f"⏰ TS{_dver} {coin}-USDT",
                            [[_timeout_line,
                              f"📊 {_smallcaps_title('Exit')}: <code>{cp:,.6g}</code>",
                              f"🎯 {_smallcaps_title('Entry')}: <code>{entry:,.6g}</code>",
                              f"📈 P/L: {pnl:+.2f}%"]],
                            tag=sig_id)
                        send_lifecycle_reply(_msg, t.get("reply_map"), include_ch2=False, tier_routed=tier_routed, share_free=share_free)
                        ct.on_scan_sl(sym)
                        _track_daily_result(sym, "TIMEOUT", tier_routed=tier_routed, free_shown=tier_routed and share_free, entry_date=_ist_date_str(created), pnl=pnl)
                        _slot_hm = _ist_hm_from_epoch(created)
                        if _slot_hm: _slot_track(f"demo{_dver}", _slot_hm, pnl >= 0)
                        _log_demo_history(t, f"TIMEOUT({pnl:+.2f}%)", cp, _dver)
                        _close_sig_snapshot(sig_id, f"TIMEOUT({pnl:+.2f}%)")
                        to_remove.append(t)

                with _demo_monitor_lock:
                    for t in to_remove:
                        if t in demo_scan1_trades: demo_scan1_trades.remove(t)
                        if t in demo_scan2_trades: demo_scan2_trades.remove(t)
                if to_remove:
                    save_state()
        except Exception as _e:
            print(f"  [DEMO MONITOR] Error: {_e}")

def _run_test_scan_and_clear_flag(cid, scan_ver: int):
    """Wrapper for the auto-scheduled TS1/TS2 triggers — TS1 and TS2 now run
    fully independent schedules/verified-time tracking (test1/test2), so each
    just clears its own run-mode flag once its run finishes."""
    _kind = f"test{scan_ver}"
    try:
        _run_test_scan(cid, scan_ver)
    finally:
        _scan_run_mode[_kind] = None

def _run_test_scan(cid, scan_ver: int):
    """CLEXER SCALP v1 test scan. Sends [DEMO] signal to TG. Places real copy
    trade orders too, if Demo1/Demo2 copy trade is turned ON."""
    import re as _re, math as _math
    lbl = "S1" if scan_ver == 1 else "S2"
    _kind = f"test{scan_ver}"
    send_admin(f"🧪 <b>[TEST] Scalp V1 Scan{lbl}</b>  {ist_str()}\n\nDemo scan starting...\n\n<i>- CLEXER TEST -</i>")

    demo_list = demo_scan1_trades if scan_ver == 1 else demo_scan2_trades
    _max_demo_slots = 6
    _demo_is_special_now = _scan_run_mode.get(_kind) == "special"
    with _demo_monitor_lock:
        if len(demo_list) >= _max_demo_slots and not _demo_is_special_now:
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

        MAX_TRIES = 3
        signal_placed = False
        tried = []
        _demo_api_fail_count = 0
        _aero_bad_keys = set()  # Aerolink keys that failed anywhere THIS scan cycle — see
        # the live scan loop's identical comment for why (skip instead of re-trying a
        # known-bad key from scratch on every coin).
        for candidate in candidate_order:
            if signal_placed: break
            if len(tried) >= MAX_TRIES: break
            chosen_sym  = candidate["sym"]
            chosen_base = candidate["base"]
            cp          = candidate["price"]
            tried.append(chosen_sym)
            # Same staleness fix as the live scan loop — candidate["price"] is a
            # snapshot from this cycle's initial ticker fetch, stale by the time
            # coin #2/#3 gets its turn after #1's full Claude analysis. Refetch here.
            _fresh_cp = get_bingx_price(chosen_sym)
            if _fresh_cp:
                cp = _fresh_cp

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
            _using_aero = _ai_aerolink("test", scan_ver)
            _retry_budget = _claude_retry_budget(_using_aero)
            for _attempt in range(_retry_budget):
                try:
                    if _attempt > 0:
                        # Refresh price on every retry — see the live scan loop's
                        # identical comment for why (stale price across retries).
                        _fresh_retry_px = get_bingx_price(chosen_sym)
                        if _fresh_retry_px:
                            cp = _fresh_retry_px
                            analysis_prompt = _build_scalp_v1_prompt(chosen_sym, cp, smc, candidate["vol"],
                                candidate["change"], struct=struct, age=age, age_4h=age_4h)
                    _gw_dbg = _aerolink_gw_debug_tag(_using_aero, _attempt, _aero_bad_keys)
                    print(f"  [TEST] attempt {_attempt+1}/{_retry_budget} using gateway={_gw_dbg} model={_ai_model('test', scan_ver)} run_mode={_scan_run_mode.get(f'test{scan_ver}')}")
                    _client, _used_key = _claude_client_skip("test", _attempt, _aero_bad_keys, scan_ver=scan_ver)
                    r2 = _client.messages.create(
                        model=_ai_model("test", scan_ver), max_tokens=500,
                        messages=[{"role":"user","content":analysis_prompt}])
                    _log_api_usage(f"demo{scan_ver}_{chosen_sym}", _ai_model("test", scan_ver),
                                   r2.usage.input_tokens, r2.usage.output_tokens,
                                   gateway="Aerolink" if _using_aero else "Direct")
                    analysis = _claude_text(r2)
                    _claude_ok = True; break
                except Exception as _ce:
                    print(f"  [TEST] Claude attempt {_attempt+1} FAIL (gateway={_gw_dbg}): {_ce}")
                    if _using_aero and _used_key:
                        _aero_bad_keys.add(_used_key)
                    if _attempt < _retry_budget - 1: time.sleep(10)
            if not _claude_ok:
                print(f"  [TEST] {chosen_sym}: Claude failed {_retry_budget} times — skipping"); _demo_api_fail_count += 1; continue

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
                f"<pre>{_html.escape(_preview[:600])}</pre>\n\n"
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

            # Same staleness issue as the real Scan1/Scan2 path — `cp` was captured
            # before the chart-fetch + Claude analysis, refetch so entry/SL/TP match
            # where price actually is now, not where it was minutes ago.
            scan_entry = get_bingx_price(chosen_sym) or cp
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
            _demo_sig_id = _gen_signal_id()
            demo_msg = _scan_box(
                "Alt Signal", f"📣 {coin}-USDT  |  TS{scan_ver} {_gw_model_tag('test', scan_ver)}",
                [[f"{arrow} — {_smallcaps_title('Market Entry')}"],
                 [f"🎯 {_smallcaps_title('Entry')}: <code>{scan_entry:,.4g}</code>",
                  f"🛑 SL: <code>{scan_sl:,.4g}</code>  ({sl_pct:.1f}%)",
                  f"📌 {_smallcaps_title('Swing Level')}: {swing_level_str}",
                  f"💰 TP1: <code>{scan_tp1:,.4g}</code>", f"🏆 TP2: <code>{scan_tp2:,.4g}</code>",
                  f"📊 RR: 1:2.0 (TP1) / 1:3.75 (TP2)",
                  f"⏰ {_smallcaps_title('Timeout')}: 1H | move_age: {age}c"]],
                tag=_demo_sig_id,
            )
            _demo_is_d48 = _gw_model_tag("test", scan_ver) == "D4.8"  # channel-2 only gets D4.8 (Direct+Opus4.8) signals
            # TS1 and TS2 each reach Free/VIP channels at their OWN independent
            # whitelisted special slot times (test1/test2) — everything else
            # (regular grid) stays on the legacy channel only.
            _demo1_tier_routed = _scan_run_mode.get(_kind) == "special"
            # Used to hardcode share_free=True (bypassing the daily quota entirely
            # for every TS1 special-time signal) — now respects the same quota as
            # everything else: within quota -> real signal to Free; exhausted ->
            # locked card instead of an unlimited real reveal.
            _demo_share_free = _free_quota_available() if _demo1_tier_routed else False
            if _demo_share_free: _consume_free_quota()
            _save_sig_snapshot(_demo_sig_id, chosen_sym, scan_signal_val, scan_entry, scan_sl, scan_tp1, scan_tp2, f"demo{scan_ver}")
            _demo_reply_map = send_entry_signal(demo_msg, include_ch2=False, tier_routed=_demo1_tier_routed, share_free=_demo_share_free,
                locked_text=_locked_signal_text(coin, f"TS{scan_ver} {_gw_model_tag('test', scan_ver)}", _demo_sig_id), sig_id=_demo_sig_id)
            for _k, _v in (_demo_reply_map or {}).items():
                if _k.startswith("free:"): _track_free_sl(_demo_sig_id, _k.split(":", 1)[1], "entry_mid", _v)

            slot_data = {
                "symbol": chosen_sym, "signal": scan_signal_val,
                "entry": scan_entry, "sl": scan_sl, "tp1": scan_tp1, "tp2": scan_tp2,
                "tp1_hit": False, "be_sl": 0, "created_at": time.time(), "entry_hit": True,
                "scan_ver": scan_ver,
                "tier_routed": _demo1_tier_routed,
                "share_free": _demo_share_free,
                "is_d48": _demo_is_d48,
                "sig_id": _demo_sig_id,
                "reply_map": _demo_reply_map,
            }
            with _demo_monitor_lock:
                demo_list.append(slot_data)
            save_state()  # mini app's /trades/active reads this — without it, demo trades never appeared there
            log_trade_event({"type":f"demo{scan_ver}","coin":chosen_sym,"direction":scan_signal_val,
                "signal_time":_ist_str_now(),"entry_price":scan_entry,
                "sl_price":scan_sl,"tp1_price":scan_tp1,"tp2_price":scan_tp2,
                "entry_trigger_time":_ist_str_now(),"result":"open"})
            # Copy trade only mirrors demo signals that were actually shown in
            # VIP/Free (same rule real Scan1/Scan2 already follow) — regular-grid
            # and unverified-special-time demo signals place no real orders,
            # even when Demo1/Demo2 copy trade is turned ON.
            _demo_ver = 3 if scan_ver == 1 else 4
            _demo_ct_on = ct.DEMO1_CT_ENABLED if scan_ver == 1 else ct.DEMO2_CT_ENABLED
            _demo_trigger_hm = _scan_trigger_hm.get(_kind)
            _demo_is_unverified = _demo1_tier_routed and _demo_trigger_hm in _SCAN_SPECIAL_NO_COPY.get(_kind, set())
            if _demo_ct_on and _demo1_tier_routed and not _demo_is_unverified:
                _demo_sd = {"ver": _demo_ver, "signal": scan_signal_val, "entry": scan_entry,
                             "sl": scan_sl, "tp1": scan_tp1, "tp2": scan_tp2}
                ct_results = ct.on_scan_signal(_demo_sd, chosen_sym, cp, True)
                send_admin(f"📋 <b>Demo{scan_ver} Copy Trade ({chosen_sym}):</b>\n" + "\n".join(ct_results[:5]))
            signal_placed = True
            print(f"  [TEST] {chosen_sym} {scan_signal_val} demo signal placed — scan{lbl}")

        if not signal_placed:
            tried_str = ", ".join(tried) if tried else "none"
            _test_is_special_now = _scan_run_mode.get(_kind) == "special"
            if _test_is_special_now:
                _trig_hm = _scan_trigger_hm.get(_kind)
                _trig_str = f"{_trig_hm[0]}:{_trig_hm[1]:02d}" if _trig_hm else "?"
                _label = f"TS{scan_ver}"
                _gw = _gw_model_tag("test", scan_ver)
                if _demo_api_fail_count > 0 and _demo_api_fail_count >= len(tried):
                    _no_sig_msg = _scan_box(
                        f"{_label} No Signal", f"⏸ {_label} {_gw}  |  {_trig_str} IST",
                        [[f"🔴 {_smallcaps_title(f'{_gw} Error')}",
                          f"{_smallcaps_title('Gateway/API failed — no chart was analyzed')}."]],
                    )
                else:
                    _no_sig_msg = _scan_box(
                        f"{_label} No Signal", f"⏸ {_label} {_gw}  |  {_trig_str} IST",
                        [[f"🔍 {_smallcaps_title('No Clear Trade Found')}",
                          f"{_smallcaps_title('Claude analyzed but no clean setup at this slot')}."]],
                    )
                send_to_tier_channels(_no_sig_msg, False)  # VIP only
            send_admin(
                f"⏸ <b>[TEST] No demo signal</b>  {ist_str()}\n\n"
                f"Tried: <b>{tried_str}</b>\n\n"
                f"All WAIT or failed gates (age/SL/RR).\n\n<i>- CLEXER SCALP V1 TEST -</i>")

    except Exception as e:
        send_admin(f"❌ [TEST] Scan error: {e}")
        import traceback as _tb3; print(_tb3.format_exc())

def main():
    global last_signal_scan_time, last_price_check_time, last_tick_time, last_scan_tick_time

    print(f"[CLEXER V17.8.5] Starting | {SYMBOL}")
    print(f"  TV Bridge: {TV_BRIDGE_URL or 'NOT SET - Binance-only'}")
    print(f"  Starting PAUSED - send /go to start scanning")
    load_users()
    ct.set_username_resolver(lambda uid: user_usernames.get(str(uid)))
    ct.load()
    load_settings()

    # Retroactive 1:3 sweep — catches any slot ALREADY sitting at a 1:3-
    # reducible ratio (e.g. from before this feature existed, or restored
    # from a redeploy) rather than only reacting to new trades going forward.
    # Runs here (inside main(), after every function including send_admin is
    # defined) rather than at raw module-load time, since a real hit fires an
    # admin notification.
    _blacklist_swept_any = False
    for _sk in list(_slot_stats.keys()):
        try:
            _k, _hm_str = _sk.split("|", 1)
            _h, _m = _hm_str.split(".")
            if _check_slot_blacklist(_k, (int(_h), int(_m))):
                _blacklist_swept_any = True
        except Exception as _e:
            print(f"[SLOT AUTO] startup 1:3 sweep error for {_sk}: {_e}")
    if _blacklist_swept_any:
        print("[SLOT AUTO] Retroactive 1:3 sweep found and retired at least one slot on startup")

    load_active_trade()
    _pull_csv_central("trade_history_csv", TRADE_LOG_CSV)
    _pull_csv_central("api_cost_log_csv", API_COST_LOG)

    # One-time bootstrap escape hatch: set FORCE_ACTIVE=1 on a server's env vars
    # to make it claim itself active in the central store immediately, without
    # needing Telegram polling to already be working — breaks the chicken-and-egg
    # deadlock where a brand-new central store (or an unreachable one) makes every
    # server default to standby, and nobody can send /server to fix it because
    # nothing is polling yet. Remove this env var again after first use — leaving
    # it set means this server will re-claim active on every future restart too.
    if os.getenv("FORCE_ACTIVE") == "1" and CLEXER_API_URL:
        set_active_server(SERVER_NAME)
        print(f"[SERVER] FORCE_ACTIVE=1 — '{SERVER_NAME}' claimed active in the central store. "
              f"Remove this env var now that it's done its job.")

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
    threading.Thread(target=_daily_summary_loop, daemon=True).start()
    threading.Thread(target=_liquidation_ws_loop, daemon=True).start()
    threading.Thread(target=_poll_payment_events, daemon=True).start()

    def _active_server_loop():
        while True:
            try:
                ct._is_active = is_active_server()
            except Exception as e:
                print(f"[SERVER] active-sync error: {e}")
            time.sleep(15)
    threading.Thread(target=_active_server_loop, daemon=True).start()

    def _wait_then_poll():
        """Telegram only allows ONE process to poll getUpdates for a given bot
        token at a time — if two servers both poll simultaneously, Telegram
        returns conflicts and commands behave unpredictably. So a standby
        server waits here (checking every 20s) until it's marked active
        before it ever starts polling. A fresh main with no CLEXER_API_URL
        set, or an empty/never-configured shared store, starts immediately —
        this only kicks in once multi-server mode is actually in use."""
        if CLEXER_API_URL:
            _warned = False
            while not is_active_server():
                if not _warned:
                    print(f"[SERVER] '{SERVER_NAME}' is in STANDBY (active server: "
                          f"'{get_active_server_name()}') — NOT polling Telegram. "
                          f"Run /server {SERVER_NAME} from the active server to switch.")
                    _warned = True
                time.sleep(20)
            print(f"[SERVER] '{SERVER_NAME}' is ACTIVE — starting Telegram polling now.")
        command_listener()
    threading.Thread(target=_wait_then_poll, daemon=True).start()
    threading.Thread(target=_demo_monitor_loop, daemon=True).start()
    threading.Thread(target=_chat_session_sweep_loop, daemon=True).start()

    # Start SL/TP monitor — checks all copy users' positions every 1 hour
    ct.start_monitor_loop(notify_fn=send_admin, ghost_close_fn=_ghost_confirm_close, interval_hours=1)

    # Start balance sync — refreshes every connected user's cached BingX balance
    # every 60s, so the Mini App's Portfolio balance/equity curve stays fresh
    # (api.py can't fetch this itself — it can't decrypt ct_users' API keys)
    ct.start_balance_sync_loop(interval_seconds=60)

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
                    elif cb.startswith("reset_scan_ghost_"):
                        sym = cb.split("_")[4] if len(cb.split("_")) > 4 else "?"
                        row.append({"text": f"🔄 Reset {sym.replace('-USDT','')}", "callback_data": f"sync_reset_scan_ghost:{uid}:{sym}"})
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
    if TV_BRIDGE_URL:
        if tv_bridge_state["online"] and tv_bridge_state["cdp_ok"]:   tv_status = "ONLINE"
        elif tv_bridge_state["online"]:                                 tv_status = "Bridge OK, TV down"
        else:                                                           tv_status = "OFFLINE"
    else:
        tv_status = "Not configured"
    source_status = "Binance Fallback" if not (TV_BRIDGE_URL and tv_bridge_state["online"] and tv_bridge_state["cdp_ok"]) else "TradingView"

    send_admin(_deploy_status_box(
        tv_status=tv_status, source_status=source_status,
        charts_on=SEND_CHARTS, news_on=SEND_NEWS, paused=True))

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
                    send_admin("😴 <b>Weekend Sleep Mode</b>\n\nAll bot activity paused.\nFri 10 PM → Sun 11 PM IST.\nOpen trades are safe — BingX orders still active.")
                    # Flush Friday's daily recap right now instead of waiting for the
                    # post-midnight window — if the process restarts/goes down anytime
                    # during the weekend (Railway restart, credit limit, etc.), the
                    # in-memory bucket would be lost before ever reaching that
                    # check, and Friday's report would silently never go out.
                    global _daily_summary_last_sent_date
                    _today_marker = now_ist().strftime("%Y-%m-%d")
                    _today_bucket = _daily_buckets.get(_today_marker)
                    if _daily_summary_last_sent_date != _today_marker and _today_bucket and _today_bucket.get("trades"):
                        try:
                            _send_daily_summary(_today_bucket)
                            _daily_summary_last_sent_date = _today_marker
                        except Exception as e:
                            print(f"  [DAILY SUMMARY] weekend-sleep flush error: {e}")
                time.sleep(60); continue
            elif _weekend_sleep_notified:
                _weekend_sleep_notified = False
                send_admin("✅ <b>Weekend Sleep Ended</b>\n\nBot resuming all activity.")

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
                    send_admin(f"<b>TradingView Offline</b>\n\nSwitched to Binance (OLD prompt).")
                elif not was_online and is_online:
                    print("  TV back ONLINE")
                    send_admin(f"<b>TradingView Back Online</b>\n\nSwitched back to TradingView (NEW prompt).")

            # News: the liquidation feed runs continuously in its own background
            # thread (_liquidation_ws_loop), started once at boot — nothing to
            # poll here anymore.

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

            # Scan1: special times (Direct) + hourly :02/:23 regular grid (Aerolink)
            if SCAN1_AUTO_ENABLED and not bot_paused.is_set() and not bot_stopped.is_set() and _cur_hm in SCAN1_SCHEDULE and _cur_hm not in _scan1_triggered_today:
                _scan1_triggered_today.add(_cur_hm)
                print(f"  [AUTO-SCAN1] {_ist_now.strftime('%H:%M')} IST")
                if ADMIN_CHAT_ID:
                    _scan_run_mode["scan1"] = "special" if _cur_hm in _SCAN_SPECIAL["scan1"] else "regular"
                    _scan_trigger_hm["scan1"] = _cur_hm
                    threading.Thread(target=lambda: _run_auto_scan(ADMIN_CHAT_ID, scan_ver=1), daemon=True).start()

            # Scan2: special times (Direct) + hourly :07/:27 regular grid (Aerolink)
            if SCAN2_AUTO_ENABLED and not bot_paused.is_set() and not bot_stopped.is_set():
                if _cur_hm in SCAN2_SCHEDULE and (_cur_hm, 2) not in _scan1_triggered_today:
                    _scan1_triggered_today.add((_cur_hm, 2))
                    print(f"  [AUTO-SCAN2] {_ist_now.strftime('%H:%M')} IST")
                    if ADMIN_CHAT_ID:
                        _scan_run_mode["scan2"] = "special" if _cur_hm in _SCAN_SPECIAL["scan2"] else "regular"
                        _scan_trigger_hm["scan2"] = _cur_hm
                        threading.Thread(target=lambda: _run_auto_scan(ADMIN_CHAT_ID, scan_ver=2), daemon=True).start()

            # TS1: own independent special times (Direct) + hourly :09/:27 regular grid (Aerolink)
            if TEST_SCAN_ENABLED and not bot_paused.is_set() and not bot_stopped.is_set() and _cur_hm in SCAN1_TEST_SCHEDULE and _cur_hm not in _test_triggered_today:
                _test_triggered_today.add(_cur_hm)
                print(f"  [TEST-SCAN] TS1 at {_ist_now.strftime('%H:%M')} IST")
                if ADMIN_CHAT_ID:
                    _scan_run_mode["test1"] = "special" if _cur_hm in _SCAN_SPECIAL["test1"] else "regular"
                    _scan_trigger_hm["test1"] = _cur_hm
                    threading.Thread(target=lambda: _run_test_scan_and_clear_flag(ADMIN_CHAT_ID, 1), daemon=True).start()

            # TS2: own independent special times + schedule — fully separate from TS1
            if TEST_SCAN_ENABLED and not bot_paused.is_set() and not bot_stopped.is_set() and _cur_hm in SCAN2_TEST_SCHEDULE and (_cur_hm, 2) not in _test_triggered_today:
                _test_triggered_today.add((_cur_hm, 2))
                print(f"  [TEST-SCAN] TS2 at {_ist_now.strftime('%H:%M')} IST")
                if ADMIN_CHAT_ID:
                    _scan_run_mode["test2"] = "special" if _cur_hm in _SCAN_SPECIAL["test2"] else "regular"
                    _scan_trigger_hm["test2"] = _cur_hm
                    threading.Thread(target=lambda: _run_test_scan_and_clear_flag(ADMIN_CHAT_ID, 2), daemon=True).start()

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
                    send_telegram("✅ <b>Cooldown over - scanning now!</b> 🔍")
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
                    f"Analyzing...")

            data = fetch_all_data()

            if not active_trade["signal"]:
                signal = analyze_with_claude(ticker, data, validate_trade=False)
                if signal and not signal.get("_hold"):
                    _share_free = _free_quota_available()
                    if _share_free: _consume_free_quota()
                    signal["reply_map"] = _send_btc_entry_signal(signal, _share_free)
                    set_trade(signal, _share_free)
                    results = ct.on_signal(signal, price, _share_free)
                    # MARKET orders filled instantly — send entry confirmation immediately
                    if signal.get("entry_type", "MARKET") == "MARKET":
                        send_telegram(
                            f"🚀 <b>ENTRY TRIGGERED!</b>  🕐 {ist_str()}\n\n"
                            f"{'🟩' if signal['signal']=='BUY' else '🟥'} <b>{signal['signal']} {SYMBOL}</b>\n"
                            f"🎯 Entry: <b>{signal['entry']:,.0f}</b>  ✅ MARKET FILLED\n"
                            f"🛑 SL:    <b>{signal['sl']:,.0f}</b>\n"
                            f"💰 TP1:   <b>{signal['tp1']:,.0f}</b>\n"
                            f"🏆 TP2:   <b>{signal['tp2']:,.0f}</b>",
                            include_ch2=False                        )
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
                            f"TP2: <b>{t['tp2']:,.0f}</b>")
                elif signal.get("_hold"):
                    send_admin(f"<b>Trade Validated - HOLD</b>  {ist_str()}\n\n"
                        f"{t['signal']} @ {t['entry']:,.0f}\n"
                        f"SL:{t['sl']:,.0f} | TP1:{t['tp1']:,.0f} | TP2:{t['tp2']:,.0f}\n\n"
                        f"<i>{_html.escape(signal.get('reasoning','Structure intact')[:250])}</i>")
                elif signal["signal"] != t["signal"]:
                    # Only flip if entry has already been hit — never flip a pending trade
                    if not t["entry_hit"]:
                        print(f"  [FLIP BLOCKED] Entry not hit yet — holding {t['signal']} @ {t['entry']:,.0f}")
                        send_admin(f"<b>Flip Blocked</b>\n\nClaude wanted to flip {t['signal']} -> {signal['signal']} but entry not hit yet.\nHolding original trade.")
                    else:
                        flip_reason = signal.get("reasoning","Structure flipped")
                        log_trade_outcome("STRUCTURE_FLIP", flip_reason[:100])
                        send_lifecycle_reply(f"🔄 <b>STRUCTURE FLIP!</b> 🚨  🕐 {ist_str()}\n\n"
                            f"❌ Closing: {t['signal']} @ {t['entry']:,.0f}\n"
                            f"💡 Why: <i>{_html.escape(flip_reason[:200])}</i>\n\n"
                            f"{'🟩' if signal['signal']=='BUY' else '🟥'} New: <b>{signal['signal']} @ {signal['entry']:,.0f}</b>",
                            t.get("reply_map"), include_ch2=False)
                        ct.on_close_all()
                        _close_sig_snapshot(t.get("sig_id",""), "STRUCTURE_FLIP")
                        reset_trade(); time.sleep(1)
                        _share_free = _free_quota_available()
                        if _share_free: _consume_free_quota()
                        signal["reply_map"] = _send_btc_entry_signal(signal, _share_free)
                        set_trade(signal, _share_free)
                        ct.on_signal(signal, price, _share_free)
                else:
                    if forced:
                        send_telegram(f"<b>Trade Update</b>  {ist_str()}\n\n"
                            f"Old: {t['signal']} @ {t['entry']:,.0f}\n"
                            f"New: {signal['signal']} @ {signal['entry']:,.0f}\n"
                            f"Bias confirmed.")
                    log_trade_outcome("REPLACED","same direction, updated levels")
                    ct.on_close_all()
                    _close_sig_snapshot(t.get("sig_id",""), "REPLACED")
                    reset_trade(); time.sleep(1)
                    _share_free = _free_quota_available()
                    if _share_free: _consume_free_quota()
                    signal["reply_map"] = _send_btc_entry_signal(signal, _share_free)
                    set_trade(signal, _share_free)
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
