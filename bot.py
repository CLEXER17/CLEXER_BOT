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
def get_session():
    mins = now_ist().hour * 60 + now_ist().minute
    if 450 <= mins < 990:         return "LONDON"
    if mins >= 1110 or mins < 60: return "NEW_YORK"
    return "ASIA"
def is_trading_hours(): return get_session() in ("LONDON", "NEW_YORK")
def is_ist_sleep():
    mins = now_ist().hour * 60 + now_ist().minute
    return 60 <= mins < 450

active_trade = {
    "signal": None, "entry": None, "sl": None,
    "tp1": None, "tp2": None, "tp1_hit": False,
    "entry_type": "MARKET", "entry_note": "",
    "entry_hit": False, "sl_wicked": False, "scan_count": 0,
}
scan1_trades = []   # list of active scan1 trade dicts (unlimited slots)
scan2_trades = []   # list of active scan2 trade dicts (unlimited slots)
last_scan_tick_time = 0
signal_history        = []
scan_history          = []   # closed scan trades — appended on TP/SL/missed
trade_outcomes        = []
force_scan            = threading.Event()
bot_paused            = threading.Event()
btc_analysis_enabled  = True   # toggle via /btcanalysis on|off or inline button
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
    global registered_users
    try:
        if os.path.exists(USER_DB_FILE):
            with open(USER_DB_FILE, "r") as f:
                registered_users = set(json.load(f))
    except Exception as e:
        print(f"[USERS] Load error: {e}"); registered_users = set()

def save_users():
    try:
        with open(USER_DB_FILE, "w") as f: json.dump(list(registered_users), f)
    except Exception as e: print(f"[USERS] Save error: {e}")

def register_user(chat_id):
    if chat_id not in registered_users:
        registered_users.add(chat_id); save_users()

broadcast_pending: dict = {}

trade_stats = {
    "consecutive_sl": 0, "cooldown_scans": 0,
    "total_sl": 0, "total_tp1": 0, "total_tp2": 0,
    "total_signals": 0, "missed_entries": 0, "stop_hunts": 0,
    "scan_sl": 0, "scan_tp1": 0, "scan_tp2": 0, "scan_signals": 0,
}

STATE_FILE = os.path.join(DATA_DIR, "clexer_state.json")

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
        now_utc   = _dt.datetime.utcnow()
        cur_year  = now_utc.year
        cur_month = now_utc.month
        cur_qnum  = (cur_month - 1) // 3  # 0=Q1,1=Q2,2=Q3,3=Q4

        def _bar_dt(b):
            ts = b["t"]
            if ts > 1e12: ts = ts / 1000
            return _dt.datetime.utcfromtimestamp(ts)

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

You are CLEXER - elite BTC trader.
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
            msg = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
                model="claude-opus-4-8", max_tokens=1200,
                messages=[{"role": "user", "content": content}])
            raw = msg.content[0].text.strip() if msg.content else ""
            if raw: break
            time.sleep(2)
        except Exception as e:
            print(f"  [CLAUDE ERROR] attempt {attempt+1}: {e}")
            if "image" in str(e).lower() and attempt == 0:
                print("  [CLAUDE] Retrying text-only...")
                content_text = [c for c in content if c["type"] == "text"]
                try:
                    msg = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
                        model="claude-opus-4-8", max_tokens=1200,
                        messages=[{"role": "user", "content": content_text}])
                    raw = msg.content[0].text.strip() if msg.content else ""
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
                f"{ist_str()}\n<i>- CLEXER V17.8.5 -</i>")
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
                f"<i>/resetsl to lower bar. - CLEXER V17.8.5 -</i>")
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
        msg = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
            model="claude-opus-4-8", max_tokens=2000,
            system="You are a trading signal bot. Respond with ONLY a JSON object. No reasoning, no steps, no text before or after the JSON.",
            messages=[{"role": "user", "content": prompt}])
        raw = msg.content[0].text.strip() if msg.content else ""
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
        f"Source:   <b>{get_current_source()}</b>\n\n<i>- CLEXER V17.8.5 -</i>")

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

# --- TELEGRAM -----------------------------------------------------------------
_SETTINGS_FILE = os.path.join(os.getenv("DATA_DIR", "."), "settings.json")

def load_settings():
    global channel_paused, SEND_CHARTS, CHART_TFS, SEND_NEWS, SIGNAL_SCAN_INTERVAL, BTC_PROMPT_MODE
    try:
        if os.path.exists(_SETTINGS_FILE):
            d = json.load(open(_SETTINGS_FILE))
            channel_paused.update(d.get("channel_paused", {}))
            SEND_CHARTS           = d.get("send_charts",       SEND_CHARTS)
            CHART_TFS             = d.get("chart_tfs",         CHART_TFS)
            SEND_NEWS             = d.get("send_news",         SEND_NEWS)
            SIGNAL_SCAN_INTERVAL  = d.get("scan_interval",     SIGNAL_SCAN_INTERVAL)
            BTC_PROMPT_MODE       = d.get("btc_prompt_mode",   BTC_PROMPT_MODE)
            print(f"[SETTINGS] Loaded — charts:{SEND_CHARTS} news:{SEND_NEWS} "
                  f"interval:{SIGNAL_SCAN_INTERVAL//3600}h "
                  f"btcmode:{BTC_PROMPT_MODE} "
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
        }, open(_SETTINGS_FILE, "w"), indent=2)
    except Exception as e:
        print(f"[SETTINGS] Save error: {e}")

channel_paused = {"1": False, "2": False}  # per-channel pause state

def send_telegram(text):
    success = False
    channels = [("1", TELEGRAM_CHANNEL_ID), ("2", os.getenv("TELEGRAM_CHANNEL_ID_2",""))]
    for key, cid in channels:
        if not cid: continue
        if channel_paused.get(key): continue
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
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
    except Exception as e: print(f"  [ADMIN MSG ERROR] {e}")

def send_reply(chat_id, text, reply_markup=None):
    try:
        payload = {"chat_id": chat_id, "text": text,
                   "parse_mode": "HTML", "disable_web_page_preview": True}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json=payload, timeout=10)
    except Exception as e: print(f"  [REPLY ERROR] {e}")

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
        return r.status_code == 200
    except Exception as e: print(f"  [USER SEND] {chat_id}: {e}"); return False

def do_broadcast(admin_chat_id, text, file_id=None, file_type=None):
    targets = list(registered_users) + [TELEGRAM_CHANNEL_ID]
    ok = 0; fail = 0
    for cid in targets:
        if send_to_user(cid, text, file_id, file_type): ok += 1
        else: fail += 1
        time.sleep(0.05)
    send_reply(admin_chat_id, f"<b>Broadcast Done</b>\n{ok} delivered | {fail} failed\n\n<i>- CLEXER V17.8.5 -</i>")

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
        + f"\n✨ <i>- CLEXER V17.8.5 -</i>\n⚠️ <i>Not financial advice</i>")

def fmt_update(status, price=None):
    t = active_trade; entry = t.get("entry") or 0
    msgs = {
        "SL_HIT":         (
            f"🚨 <b>TRADE CLOSED — SL HIT</b> 🚨\n\n"
            f"❌ Loss taken on {t.get('signal','?')} @ {t.get('entry',0):,.0f}\n\n"
            f"💀 <i>MAA CHUD GYI TRADE KI TOH SHITT YRR</i> 😭\n\n"
            f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
            f"🔍 Waiting for next valid setup...\n\n"
            f"<i>- CLEXER V17.8.5 -</i>"
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
    return f"📣 <b>{SYMBOL} UPDATE</b>  🕐 {ist_str()}\n\n{msgs.get(status,'✅ Trade running')}\n\n✨ <i>- CLEXER V17.8.5 -</i>"

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
                    f"✨ <i>- CLEXER V17.8.5 -</i>\n⚠️ <i>Not financial advice</i>")
            return False

        # TP2 — use candle high/low to catch spike
        tp2_hit = (sig=="BUY" and check_high >= tp2) or (sig=="SELL" and check_low <= tp2)
        if tp2_hit:
            trade_stats["total_tp2"] += 1; trade_stats["consecutive_sl"] = 0
            log_trade_outcome("TP2_HIT", f"closed at {tp2:,.0f}")
            send_telegram(f"🏆 <b>TP2 HIT!</b> 🎊💵  🕐 {ist_str()}\n\n"
                f"{'🟢' if sig=='BUY' else '🔴'} {sig} {SYMBOL}\n"
                f"🎯 Entry: {entry:,.0f} ✅ TP2: <b>{tp2:,.0f}</b>\n\n✨ <i>- CLEXER V17.8.5 -</i>")
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
                    f"🚀 Riding TP2: <b>{tp2:,.0f}</b>...\n\n✨ <i>- CLEXER V17.8.5 -</i>")

        # SL — use candle low/high to catch wick SL hits
        sl_margin = 80
        sl_hit = (sig=="BUY"  and check_low  < sl - sl_margin) or \
                 (sig=="SELL" and check_high > sl + sl_margin)
        if sl_hit:
            trade_stats["total_sl"] += 1; trade_stats["consecutive_sl"] += 1
            n = trade_stats["consecutive_sl"]
            log_trade_outcome("SL_HIT", f"{n} in a row, low:{check_low:,.0f} sl:{sl:,.0f}")
            if n >= 3:
                trade_stats["cooldown_scans"] = 2
                send_telegram(
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {sig} @ {entry:,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 2 scans...\n\n<i>- CLEXER V17.8.5 -</i>")
            elif n == 2:
                trade_stats["cooldown_scans"] = 1
                send_telegram(
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {sig} @ {entry:,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 1 scan...\n\n<i>- CLEXER V17.8.5 -</i>")
            else:
                send_telegram(fmt_update("SL_HIT"))
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
    sl_pct = abs(entry - sl) / entry * 100 if entry else 0
    arrow = "🟢 LONG" if sig == "BUY" else "🔴 SHORT"
    coin = sym.replace("-USDT","").replace("USDT","")
    return (
        f"<b>📣 {coin}-USDT</b>\n"
        f"<b>{'─'*22}</b>\n\n"
        f" SCAN SIGNAL\n"
        f"  🕐 {ist_str()}\n\n"
        f"{arrow} — <b>{'MARKET' if et=='MARKET' else 'PULLBACK'} ENTRY</b>\n\n"
        f"🎯 Entry: <b>{entry:,.4g}</b>\n"
        f"🛑 SL:    <b>{sl:,.4g}</b>  ({sl_pct:.1f}%)\n"
        f"💰 TP1:  <b>{tp1:,.4g}</b>\n"
        f"🏆 TP2:  <b>{tp2:,.4g}</b>\n\n"
        f"✨ <i>- CLEXER V17.8.5 -</i>\n⚠️ <i>Not financial advice</i>"
    )

def fmt_scan_update(status: str, price: float = 0, t: dict = None) -> str:
    if t is None: t = scan_active_trade
    sym  = t.get("symbol","?"); sig = t.get("signal","?")
    entry = t.get("entry") or 0; tp1 = t.get("tp1",0); tp2 = t.get("tp2",0)
    msgs = {
        "ENTRY_HIT": (
            f"🚀 <b>ENTRY TRIGGERED — {sym}</b>  🕐 {ist_str()}\n\n"
            f"{'🟢' if sig=='BUY' else '🔴'} <b>{sig}</b>\n"
            f"🎯 Entry: <b>{entry:,.4g}</b>  |  📊 Price: <b>{price:,.4g}</b>\n"
            f"🛑 SL:    <b>{t.get('sl',0):,.4g}</b>\n"
            f"💰 TP1:  <b>{tp1:,.4g}</b>\n"
            f"🏆 TP2:  <b>{tp2:,.4g}</b>\n\n"
            f"⚠️ <b>Trade is now LIVE</b>\n\n✨ <i>- CLEXER V17.8.5 -</i>"
        ),
        "TP1_HIT": (
            f"💰 <b>TP1 HIT — {sym}!</b> 🎉  🕐 {ist_str()}\n\n"
            f"🎊 <i>MAJA AAGYA BHAI YAYY!!!!</i>\n\n"
            f"{'🟢' if sig=='BUY' else '🔴'} {sig}\n"
            f"✅ TP1: <b>{tp1:,.4g}</b>\n"
            f"🛡️ SL moved to BE: <b>{entry:,.4g}</b>\n"
            f"🚀 Riding TP2: <b>{tp2:,.4g}</b>...\n\n✨ <i>- CLEXER V17.8.5 -</i>"
        ),
        "TP2_HIT": (
            f"🏆 <b>TP2 HIT — {sym}!</b> 🎊💵  🕐 {ist_str()}\n\n"
            f"🎊 <i>MAJA AAGYA BHAI YAYY!!!!</i>\n\n"
            f"{'🟢' if sig=='BUY' else '🔴'} {sig}\n"
            f"✅ Full profit @ TP2: <b>{tp2:,.4g}</b>\n\n✨ <i>- CLEXER V17.8.5 -</i>"
        ),
        "SL_HIT": (
            (
                f"🛡️ <b>BE EXIT — {sym}</b>  🕐 {ist_str()}\n\n"
                f"{'🟢' if sig=='BUY' else '🔴'} {sig}\n"
                f"✅ TP1 already hit — closed at entry <b>{entry:,.4g}</b>\n"
                f"📊 Result: <b>Breakeven</b> (no loss)\n\n"
                f"🔍 Waiting for next scan signal...\n\n✨ <i>- CLEXER V17.8.5 -</i>"
            ) if t.get("tp1_hit") else (
                f"🚨 <b>SL HIT — {sym}</b> 🚨  🕐 {ist_str()}\n\n"
                f"💀 <i>MAA CHUD GYI TRADE KI TOH SHITT YRR</i> 😭\n\n"
                f"❌ Loss on {sig} @ {entry:,.4g}\n\n"
                f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                f"🔍 Waiting for next scan signal...\n\n✨ <i>- CLEXER V17.8.5 -</i>"
            )
        ),
        "ENTRY_MISSED": (
            f"😔 <b>ENTRY MISSED — {sym}</b>  🕐 {ist_str()}\n\n"
            f"Price bypassed entry zone <b>{entry:,.4g}</b> without filling.\n"
            f"⛔ <b>DO NOT CHASE</b>\n\n✨ <i>- CLEXER V17.8.5 -</i>"
        ),
        "WAITING_ENTRY": (
            f"⏳ <b>Waiting Entry — {sym}</b>\n"
            f"🎯 Entry: <b>{entry:,.4g}</b>\n"
            f"🛑 SL:    <b>{t.get('sl',0):,.4g}</b>\n"
            f"💰 TP1:  <b>{tp1:,.4g}</b>\n"
            f"🏆 TP2:  <b>{tp2:,.4g}</b>\n"
            + (f"📊 Current: <b>{price:,.4g}</b> ({abs(price-entry)/entry*100:.2f}% away)" if price else "")
        ),
    }
    return msgs.get(status, f"✅ {sym} trade running")

def _tick_one(ver: int, t: dict) -> bool:
    """Tick check for one trade dict. Returns True if trade closed."""
    sym = t["symbol"]; sig = t["signal"]
    entry = t["entry"]; sl = t["sl"]; tp1 = t["tp1"]; tp2 = t["tp2"]
    try:
        price = get_bingx_price(sym)
        if price <= 0: return False
        df1m = bingx_klines(sym, "1m", 3)
        if df1m is not None and len(df1m) > 0:
            check_high = max(price, float(df1m["high"].max()))
            check_low  = min(price, float(df1m["low"].min()))
        else:
            check_high = price; check_low = price
        print(f"  [SCAN{ver} {sym}] {sig} price:{price:.4g} H:{check_high:.4g} L:{check_low:.4g}")

        # All entries are MARKET — entry_hit is always True from creation.
        # Nothing to wait for. SL/TP monitoring starts immediately.
        if not t["entry_hit"]:
            # Shouldn't happen for MARKET trades, but safety fallback
            t["entry_hit"] = True
            send_telegram(fmt_scan_update("ENTRY_HIT", price, t))

        tp2_hit = (sig == "BUY" and check_high >= tp2) or (sig == "SELL" and check_low <= tp2)
        if tp2_hit:
            trade_stats["scan_tp2"] += 1; trade_stats["scan_tp1"] += (0 if t["tp1_hit"] else 1)
            _log_scan_history(t, "TP2", price)
            send_telegram(fmt_scan_update("TP2_HIT", price, t))
            ct.on_scan_tp2(sym)
            _remove_scan_trade(ver, sym); return True

        if not t["tp1_hit"]:
            tp1_hit = (sig == "BUY" and check_high >= tp1) or (sig == "SELL" and check_low <= tp1)
            if tp1_hit:
                t["tp1_hit"] = True
                t["sl"] = entry
                sl = entry  # update local var so SL-at-BE check works in THIS tick
                trade_stats["scan_tp1"] += 1
                send_telegram(fmt_scan_update("TP1_HIT", price, t))
                ct.on_scan_tp1(sym)

        sl_margin = sl * 0.002  # margin based on current SL (BE after TP1), not entry
        sl_hit = (sig == "BUY"  and check_low  < sl - sl_margin) or \
                 (sig == "SELL" and check_high > sl + sl_margin)
        if sl_hit:
            trade_stats["scan_sl"] += 1
            result = "BE" if t["tp1_hit"] else "SL"
            _log_scan_history(t, result, price)
            send_telegram(fmt_scan_update("SL_HIT", price, t))
            ct.on_scan_sl(sym)
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
                    f"❄️ Cooling down 2 scans...\n\n<i>- CLEXER V17.8.5 -</i>")
            elif n == 2:
                trade_stats["cooldown_scans"] = 1
                send_telegram(
                    f"🚨 <b>TRADE CLOSED — SL HIT ({n} in a row)</b> 🚨\n\n"
                    f"❌ Loss taken on {active_trade.get('signal','?')} @ {active_trade.get('entry',0):,.0f}\n\n"
                    f"⛔ <b>DO NOT OPEN ANY TRADE NOW</b>\n"
                    f"⛔ <b>This is NOT a new signal</b>\n\n"
                    f"❄️ Cooling down 1 scan...\n\n<i>- CLEXER V17.8.5 -</i>")
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
            resp = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
                model="claude-haiku-4-5-20251001", max_tokens=600,
                messages=[{"role":"user","content":f"BTC: ${btc_price:,.0f}\n{news_block}\n\nReturn JSON array HIGH/MEDIUM impact only. Fields: index,impact(BULLISH/BEARISH/NEUTRAL),strength(HIGH/MEDIUM),reason. Empty [] if none. JSON only."}])
            analyzed = json.loads(resp.content[0].text.strip().replace("```json","").replace("```","").strip())
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
            f"{item['source']}\n<a href='{item['link']}'>Read article</a>\n\n<i>- CLEXER V17.8.5 · {ist_str()} -</i>")
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
        send_reply(chat_id, "<b>TV Status</b>\n\nTV_BRIDGE_URL not set.\nRunning on <b>Binance</b>.\n\n<i>- CLEXER V17.8.5 -</i>"); return
    send_reply(chat_id, f"Checking...\n<code>{TV_BRIDGE_URL}</code>")
    now = time.time(); health = tv_ping()
    if not health:
        ls = tv_bridge_state.get("last_seen",0)
        since = f"{int((now-ls)//60)}m ago" if ls else "never"
        send_reply(chat_id, f"<b>TV Status</b>\n\n🔴 Bridge OFFLINE\nLast seen: {since}\n\nUsing: <b>Binance</b>\n\n<i>- CLEXER V17.8.5 -</i>"); return
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
        f"Uptime: <b>{uptime_str}</b>\n\n{ist_str()}\n<i>- CLEXER V17.8.5 -</i>")

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
    "/scan","/scan1","/scan2","/coin","/ctclose","/closetrade","/closescan","/scancopy","/readindicators","/checktvdata","/tvstudies","/calcstudies","/scantv",
    "/compare","/charts","/chartson","/chartsoff","/force_reload","/miniapp","/ctstatus","/ctretry","/btcanalysis","/demo","/synccheck"}

def handle_command(text, chat_id, message=None):
    global SIGNAL_SCAN_INTERVAL, SEND_CHARTS, CHART_TFS, SEND_NEWS, last_force_scan_time, broadcast_pending, BTC_PROMPT_MODE, btc_analysis_enabled
    register_user(chat_id)
    parts = text.strip().split(); cmd = parts[0].lower().split("@")[0]
    is_admin = (str(chat_id)==str(ADMIN_CHAT_ID)) if ADMIN_CHAT_ID else True

    if cmd in ADMIN_COMMANDS and not is_admin:
        send_reply(chat_id, "<b>Admin only.</b>\n\nUse /help to see your commands."); return

    if not is_admin and cmd in FRIEND_COMMANDS:
        allowed, reset_str = check_rate_limit(chat_id, cmd)
        if not allowed:
            send_reply(chat_id, f"<b>Limit reached</b>\n\nUse <code>{cmd}</code> again at <b>{reset_str}</b>\n\n<i>Free tier: 2 uses/hr</i>"); return

    # -- Copy trade commands (user + admin) -----------------------------------
    if ct.is_ct_command(cmd, is_admin):
        uname = (message.get("from",{}).get("username","?") if message else "?")
        ct.handle(cmd, parts, chat_id, uname, send_reply, is_admin, scan_trades=scan1_trades+scan2_trades)
        return

    if cmd == "/synccheck" and is_admin:
        send_reply(chat_id, "🔍 Checking BingX vs bot state...")
        lines = ct.sync_check()
        send_reply(chat_id, "<b>Sync Check Result</b>\n\n" + "\n".join(lines))
        return

    if cmd in ("/start","/help"):
        send_reply(chat_id, ADMIN_HELP if is_admin else FRIEND_HELP)

    elif cmd in ("/go", "/resume"):
        bot_paused.clear()
        _go_ist = now_ist()
        _go_scan_hrs = {7, 11, 15, 19, 23}
        _go_next_alt = f"{_go_ist.hour}:{ALT_SCAN_MINUTE:02d}" if _go_ist.minute < ALT_SCAN_MINUTE else f"{(_go_ist.hour+1)%24}:{ALT_SCAN_MINUTE:02d}"
        send_reply(chat_id,
            f"<b>CLEXER Started</b>\n\n"
            f"✅ Bot is RUNNING\n"
            f"📡 BTC Scan: {'ON' if btc_analysis_enabled else 'OFF (use /btcanalysis on)'}\n"
            f"📊 Alt Scan: ON\n"
            f"⏰ Next BTC scan: <b>{next((f'{h}:21' for h in sorted({7,11,15,19,23}) if h > _go_ist.hour or (h == _go_ist.hour and _go_ist.minute < 21)), '07:21 tomorrow')} IST</b>\n"
            f"⏰ Next Alt scan: <b>{_go_next_alt} IST</b>\n\n"
            f"Tick: {TICK_INTERVAL}s | Source: {get_current_source()}\n\n"
            f"<i>- CLEXER V17.8.5 -</i>")

    elif cmd == "/demo" and is_admin:
        """
        /demo btc buy entry 66000 tp1 67000 tp2 68000 sl 65500  → open fake BTC trade
        /demo tp1        → simulate TP1 hit  (SL→BE)
        /demo tp2        → simulate TP2 hit  (close all)
        /demo sl         → simulate SL hit   (close all)
        /demo btc sl 67000  → move SL to 67000
        /demo btc close     → force close all positions
        """
        raw = " ".join(parts[1:]).lower()
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
                    "<i>- CLEXER V17.8.5 -</i>")
        except Exception as e:
            send_reply(chat_id, f"❌ Demo error: {e}")

    elif cmd == "/pause":
        bot_paused.set()
        send_reply(chat_id, "<b>Bot Paused</b>\n\nUse /go to resume.\n\n<i>- CLEXER V17.8.5 -</i>")

    elif cmd == "/btcanalysis":
        arg = parts[1].lower() if len(parts) > 1 else ("off" if btc_analysis_enabled else "on")
        btc_analysis_enabled = (arg == "on")
        status = "✅ ON" if btc_analysis_enabled else "⏸ OFF"
        send_reply(chat_id,
            f"<b>BTC Analysis {status}</b>\n\n"
            f"Scheduled scans: {'7:21, 11:21, 15:21, 19:21, 23:21 IST' if btc_analysis_enabled else 'paused'}\n"
            f"/signal still forces immediate scan.\n\n<i>- CLEXER V17.8.5 -</i>",
            reply_markup={"inline_keyboard": [[
                {"text": "▶ Enable Analysis", "callback_data": "btca_on"},
                {"text": "⏸ Disable Analysis", "callback_data": "btca_off"}
            ]]})

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
        t = active_trade; st = "PAUSED (/go to start)" if bot_paused.is_set() else "RUNNING"
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
                    f"SL:{sc['sl']:,.4g}  TP1:{sc['tp1']:,.4g}")
        _ist_now = now_ist()
        _scan_hrs = {7, 11, 15, 19, 23}
        _next_btc_scan = next((f"{h}:21" for h in sorted(_scan_hrs) if h > _ist_now.hour or (h == _ist_now.hour and _ist_now.minute < 21)), "07:21 tomorrow")
        _next_alt_scan = f"{_ist_now.hour}:{ALT_SCAN_MINUTE:02d}" if _ist_now.minute < ALT_SCAN_MINUTE else f"{(_ist_now.hour+1)%24}:{ALT_SCAN_MINUTE:02d}"
        _scan_status = "🟢 ON" if not bot_paused.is_set() and btc_analysis_enabled else "🔴 OFF"
        _alt_scan_status = "🟢 ON" if not bot_paused.is_set() else "🔴 OFF (bot paused)"
        send_reply(chat_id,
            f"<b>CLEXER V17.8.5</b>\n\nBot: {st}\n{cd}"
            f"Session: {get_session()} {'active' if is_trading_hours() else 'inactive'}\n"
            f"IST: {ist_str()}\n"
            f"BTC Scan: {_scan_status} | Alt Scan: {_alt_scan_status}\n"
            f"Next BTC scan: <b>{_next_btc_scan} IST</b> | Alt scan: <b>{_next_alt_scan} IST</b>\n"
            f"MinConf: {required_confidence()} | Consec SL: {trade_stats['consecutive_sl']}\n"
            + (f"Users: {len(registered_users)}\n" if is_admin else "")
            + f"\nSource: <b>{src}</b>\nMode: <b>{'NEW (TV)' if is_tv_online() else 'OLD (Binance)'}</b>\n"
            + (f"TV: {tv_status}\n" if is_admin else "")
            + (f"Charts: {'ON' if SEND_CHARTS else 'OFF'} | News: {'ON' if SEND_NEWS else 'OFF'}\n\n" if is_admin else "\n")
            + f"<b>BTC Trade:</b>\n{ti}"
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
                parts_out.append(
                    f"<b>Scan{_ver} Trade</b>\n\n{sc['signal']} - {sc['symbol']}\n{spl}"
                    f"Entry: <b>{sc['entry']:,.4g}</b> {'✅' if sc.get('entry_hit') else '⏳ pending'}\n"
                    f"SL:    <b>{sc['sl']:,.4g}</b>\n"
                    f"TP1:   <b>{sc['tp1']:,.4g}</b> {'✅ HIT' if sc.get('tp1_hit') else '⏳ pending'}\n"
                    f"TP2:   <b>{sc['tp2']:,.4g}</b>\nType:  {sc.get('entry_type','MARKET')}"
                )
        if parts_out:
            send_reply(chat_id, "\n\n──────────\n\n".join(parts_out))
        else:
            send_reply(chat_id, "No active trade.")

    elif cmd == "/history":
        lines = []
        if signal_history:
            lines.append("<b>BTC Signals (last 5)</b>")
            for s in reversed(signal_history[-5:]):
                lines.append(f"{'🟢' if s['signal']=='BUY' else '🔴'} {s['signal']} @ {s['entry']:,.0f}  "
                    f"R:R:{s.get('rr','?')}  {s.get('confidence','?')}\n"
                    f"   SL:{s['sl']:,.0f}  TP1:{s['tp1']:,.0f}  TP2:{s['tp2']:,.0f}\n"
                    f"   {s['time']}")
        if scan_history:
            lines.append("\n<b>Scan Signals (last 5)</b>")
            for s in reversed(scan_history[-5:]):
                res = s.get("result","?")
                emoji = "🏆" if res=="TP2" else ("💰" if res in ("TP1","BE") else "❌")
                lines.append(f"{emoji} {s['signal']} {s['symbol']} @ {s['entry']:,.4g}  → <b>{res}</b>\n"
                    f"   SL:{s['sl']:,.4g}  TP1:{s['tp1']:,.4g}  TP2:{s['tp2']:,.4g}\n"
                    f"   {s['time']}")
        if lines:
            send_reply(chat_id, "\n".join(lines))
        else:
            send_reply(chat_id, "No history yet.")

    elif cmd == "/stats":
        ts = trade_stats
        btc_total = ts['total_tp1'] + ts['total_tp2'] + ts['total_sl'] or 1
        btc_wr = (ts['total_tp1'] + ts['total_tp2']) / btc_total * 100
        sc_total = ts['scan_tp1'] + ts['scan_tp2'] + ts['scan_sl'] or 1
        sc_wr = (ts['scan_tp1'] + ts['scan_tp2']) / sc_total * 100
        send_reply(chat_id,
            f"<b>Statistics</b>\n\n"
            f"<b>BTC Trades</b>\n"
            f"Signals: {ts['total_signals']}\n"
            f"TP1: {ts['total_tp1']} | TP2: {ts['total_tp2']} | SL: {ts['total_sl']}\n"
            f"Win rate: {btc_wr:.0f}%\n"
            f"Stop hunts: {ts['stop_hunts']} | Missed: {ts['missed_entries']}\n"
            f"Consec SL: {ts['consecutive_sl']} | Cooldown: {ts['cooldown_scans']}\n\n"
            f"<b>Scan Trades</b>\n"
            f"Signals: {ts['scan_signals']}\n"
            f"TP1: {ts['scan_tp1']} | TP2: {ts['scan_tp2']} | SL: {ts['scan_sl']}\n"
            f"Win rate: {sc_wr:.0f}%")

    elif cmd == "/session":
        s = get_session()
        send_reply(chat_id, f"<b>Session</b>\n\n{s} {'Active' if is_trading_hours() else 'Inactive'}\n\n"
            f"London:  07:30-16:30 IST\nNY:      18:30-01:00 IST\nSleep:   01:00-07:29 IST\n\n{ist_str()}")

    elif cmd == "/users":
        uname = (message.get("from",{}).get("username","?") if message else "?")
        ct.handle(cmd, parts, chat_id, uname, send_reply, is_admin, scan_trades=scan1_trades+scan2_trades)

    elif cmd == "/miniapp":
        if not is_admin: return
        sub = parts[1].lower() if len(parts) > 1 else ""
        msg = " ".join(parts[2:]) if len(parts) > 2 else "Under Maintenance — back soon!"
        if sub in ("pause", "off", "maintenance"):
            on = True
        elif sub in ("resume", "on", "live"):
            on = False
            msg = "Live"
        else:
            send_reply(chat_id, "Usage:\n/miniapp pause [message]\n/miniapp resume")
            return
        if CLEXER_API_URL:
            try:
                hdrs = {"X-Push-Secret": PUSH_STATE_SECRET, "Content-Type": "application/json"} if PUSH_STATE_SECRET else {"Content-Type": "application/json"}
                r = requests.post(f"{CLEXER_API_URL}/maintenance", json={"on": on, "msg": msg}, headers=hdrs, timeout=5)
                send_reply(chat_id, f"🔧 Mini App {'PAUSED ⏸' if on else 'RESUMED ▶️'}\nMessage: {msg}")
            except Exception as e:
                send_reply(chat_id, f"Error: {e}")
        else:
            send_reply(chat_id, "CLEXER_API_URL not set")

    elif cmd == "/close":
        t = active_trade
        if not t["signal"]: send_reply(chat_id, "No active trade.")
        else:
            info = f"{t['signal']} @ {t['entry']:,.0f}"
            log_trade_outcome("MANUAL_CLOSE", f"closed by admin")
            ct.on_close_all()
            reset_trade(); send_telegram(f"<b>Trade Closed</b>\n{info}\n\n<i>- CLEXER V17.8.5 -</i>")
            send_reply(chat_id, f"Closed: {info}"); force_scan.set()

    elif cmd == "/sltobe":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        else:
            old = active_trade["sl"]; active_trade["sl"] = active_trade["entry"]
            ct.on_sl_to_be(active_trade["entry"])
            send_telegram(f"<b>SL -> BE</b>  {old:,.0f} -> <b>{active_trade['entry']:,.0f}</b>\n\n<i>- CLEXER V17.8.5 -</i>")
            send_reply(chat_id, f"SL -> {active_trade['entry']:,.0f}")

    elif cmd == "/setsl":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /setsl 61500")
        else:
            try:
                v = float(parts[1].replace(",","")); old = active_trade["sl"]
                active_trade["sl"] = v
                ct.on_update_sl(v)
                send_telegram(f"<b>SL</b>  {old:,.0f} -> <b>{v:,.0f}</b>\n\n<i>- CLEXER V17.8.5 -</i>")
                send_reply(chat_id, f"SL = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /setsl 61500")

    elif cmd == "/settp1":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp1 63000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp1"] = v
                send_telegram(f"<b>TP1 -> {v:,.0f}</b>\n\n<i>- CLEXER V17.8.5 -</i>")
                send_reply(chat_id, f"TP1 = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /settp1 63000")

    elif cmd == "/settp2":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp2 65000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp2"] = v
                send_telegram(f"<b>TP2 -> {v:,.0f}</b>\n\n<i>- CLEXER V17.8.5 -</i>")
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
                    msg = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
                        model="claude-opus-4-8", max_tokens=1000,
                        messages=[{"role":"user","content":prompt}])
                    raw = msg.content[0].text.strip()
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
                    msg = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
                        model="claude-opus-4-8", max_tokens=1000,
                        messages=[{"role":"user","content":prompt}])
                    raw = msg.content[0].text.strip()
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
        if len(parts) < 2:
            mode_label = "V7 CLASSIC" if BTC_PROMPT_MODE == "V7" else "V9 CURRENT"
            send_reply(chat_id,
                f"<b>BTC Prompt Mode</b>\n\n"
                f"Current: <b>{mode_label}</b>\n\n"
                f"/btcmode on  — V7 Classic\n"
                f"  TV/BingX split, no CRITICAL header\n"
                f"  Scan: narrated, min 2 pause candles, no Rule 8\n\n"
                f"/btcmode off — V9 Current (default)\n"
                f"  Always new prompt, CRITICAL JSON header\n"
                f"  Scan: silent, min 3 pause candles, Rule 8 hard block")
        elif parts[1].lower() == "on":
            BTC_PROMPT_MODE = "V7"; save_settings()
            send_reply(chat_id,
                f"<b>BTC Mode → V7 CLASSIC</b> ✅\n\n"
                f"Using CLEXER_V7_CLASSIC prompts:\n"
                f"• TV online → build_new_prompt_v7 (10-step, no CRITICAL)\n"
                f"• TV offline → build_old_prompt_v7 (BingX + session notes)\n\n"
                f"Scan logic: V7 mode\n"
                f"• Narrated 5 steps | min 2 pause candles\n"
                f"• No Rule 8 hard block | no pre-filter\n\n"
                f"<i>- CLEXER V17.8.5 -</i>")
        elif parts[1].lower() == "off":
            BTC_PROMPT_MODE = "V9"; save_settings()
            send_reply(chat_id,
                f"<b>BTC Mode → V9 CURRENT</b> ✅\n\n"
                f"Using CLEXER_V9_CURRENT prompts:\n"
                f"• Always new prompt (TV or BingX)\n"
                f"• CRITICAL JSON-only header\n\n"
                f"Scan logic: V9 mode\n"
                f"• Silent output | min 3 pause candles\n"
                f"• Rule 8 hard block | post-pump pre-filter\n\n"
                f"<i>- CLEXER V17.8.5 -</i>")
        else:
            send_reply(chat_id, "Usage: /btcmode on|off\n\non = V7 Classic\noff = V9 Current (default)")


    elif cmd == "/setinterval":
        if len(parts)<2: send_reply(chat_id, f"Current: {SIGNAL_SCAN_INTERVAL//3600}h\nUsage: /setinterval 4")
        else:
            try:
                h = float(parts[1])
                if h<1 or h>24: send_reply(chat_id, "1-24 hours only.")
                else: SIGNAL_SCAN_INTERVAL = int(h*3600); save_settings(); send_reply(chat_id, f"Scan interval -> {h}h")
            except: send_reply(chat_id, "Usage: /setinterval 4")

    elif cmd == "/images":
        if len(parts)<2:
            send_reply(chat_id, f"Charts: {'ON' if SEND_CHARTS else 'OFF'}\nTFs: {', '.join(CHART_TFS).upper()}\n\nUsage: /images on|off\n(OFF by default)")
        elif parts[1].lower()=="on":
            SEND_CHARTS = True; save_settings()
            send_reply(chat_id, f"Charts ON - posting to channel only\nTFs: {', '.join(CHART_TFS).upper()}")
        elif parts[1].lower()=="off":
            SEND_CHARTS = False; save_settings(); send_reply(chat_id, "Charts OFF")
        else: send_reply(chat_id, "Usage: /images on|off")

    elif cmd == "/setimages":
        if len(parts)<2: send_reply(chat_id, f"Current: {', '.join(CHART_TFS).upper()}\nUsage: /setimages weekly,4h,1h,5m")
        else:
            valid = {"weekly","4h","1h","5m"}
            chosen = [tf.strip().lower() for tf in parts[1].split(",") if tf.strip().lower() in valid]
            if not chosen: send_reply(chat_id, "No valid TFs. Use: weekly, 4h, 1h, 5m")
            else: CHART_TFS = chosen; save_settings(); send_reply(chat_id, f"Chart TFs: {', '.join(CHART_TFS).upper()}")

    elif cmd == "/news":
        if len(parts)<2:
            send_reply(chat_id, f"News: {'ON' if SEND_NEWS else 'OFF (default)'}\nUsage: /news on|off")
        elif parts[1].lower()=="on":  SEND_NEWS = True;  save_settings(); send_reply(chat_id, f"News ON - {len(NEWS_SOURCES)} sources")
        elif parts[1].lower()=="off": SEND_NEWS = False; save_settings(); send_reply(chat_id, "News OFF")
        else: send_reply(chat_id, "Usage: /news on|off")

    elif cmd == "/latestnews":
        send_reply(chat_id, "Fetching news (~15s)...")
        threading.Thread(target=check_news, args=(True,), daemon=True).start()

    elif cmd == "/broadcast":
        broadcast_pending[chat_id] = {"step":"waiting_message"}
        send_reply(chat_id, "<b>Broadcast Mode</b>\n\nSend message now (text/image/PDF).\n\n<i>/cancel to abort</i>")

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
        if len(parts) < 2 or parts[1] not in ("1","2"):
            send_reply(chat_id, "Usage: /pausechannel 1\nor /pausechannel 2"); return
        key = parts[1]
        channel_paused[key] = True
        save_settings()
        send_reply(chat_id, f"<b>Channel {key} PAUSED</b>\n\nNo signals will be sent to channel {key}.\nUse /resumechannel {key} to resume.")

    elif cmd == "/resumechannel":
        if len(parts) < 2 or parts[1] not in ("1","2"):
            send_reply(chat_id, "Usage: /resumechannel 1\nor /resumechannel 2"); return
        key = parts[1]
        channel_paused[key] = False
        save_settings()
        send_reply(chat_id, f"<b>Channel {key} RESUMED</b>\n\nSignals will now be sent to channel {key}.")

    elif cmd == "/cancel":
        if chat_id in broadcast_pending: del broadcast_pending[chat_id]; send_reply(chat_id, "Cancelled.")
        else: send_reply(chat_id, "Nothing to cancel.")

    elif cmd == "/ctclose" and is_admin:
        uname = (message.get("from",{}).get("username","?") if message else "?")
        ct.handle(cmd, parts, chat_id, uname, send_reply, is_admin, scan_trades=scan1_trades+scan2_trades)

    elif cmd == "/closetrade" and is_admin:
        if len(parts) < 2:
            send_reply(chat_id,
                "<b>Close Trade</b>\n\n"
                "Usage:\n"
                "<code>/closetrade BTC</code> — close BTC-USDT for all copy users\n"
                "<code>/closetrade ETH</code> — close ETH-USDT for all copy users\n"
                "<code>/closetrade SOL</code> — close SOL-USDT for all copy users\n"
                "<code>/closetrade all</code> — close ALL positions (every coin)\n\n"
                "<i>- CLEXER V17.8.5 -</i>"); return
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
                f"Admin closed all positions.\n\n<i>- CLEXER V17.8.5 -</i>")
            send_reply(chat_id, f"✅ All positions closed.\nBTC trade reset + Scan1 ({scan1_count}) + Scan2 ({scan2_count}) trades cleared.")
        else:
            results = ct.close_coin_all(coin)
            # If it's BTC and we have an active BTC trade, also reset it
            if coin in ("BTC","BTCUSDT","BTC-USDT") and active_trade["signal"]:
                log_trade_outcome("MANUAL_CLOSE", f"admin /closetrade {coin}")
                reset_trade()
            reply = f"<b>Close {coin.upper()}-USDT</b>\n\n" + "\n".join(results)
            send_reply(chat_id, reply + "\n\n<i>- CLEXER V17.8.5 -</i>")

    elif cmd == "/closescan" and is_admin:
        s1 = len(scan1_trades); s2 = len(scan2_trades)
        scan1_trades.clear(); scan2_trades.clear(); save_state()
        send_reply(chat_id,
            f"✅ <b>Scan trades cleared</b>\n\n"
            f"Scan1: {s1} removed\nScan2: {s2} removed\n\n<i>- CLEXER V17.8.5 -</i>")

    elif cmd == "/alt" and is_admin:
        global ALT_SCAN_MINUTE
        if len(parts) < 2:
            send_reply(chat_id,
                f"⏰ <b>Alt Scan Time</b>\n\n"
                f"Current: every hour at <b>:{ALT_SCAN_MINUTE:02d}</b>\n\n"
                f"Usage: <code>/alt 02</code> or <code>/alt 24</code>\n"
                f"Sets the minute (0–59) when alt scan fires each hour.\n\n"
                f"<i>- CLEXER V17.8.5 -</i>"); return
        try:
            new_min = int(parts[1])
            if not (0 <= new_min <= 59):
                raise ValueError
        except ValueError:
            send_reply(chat_id, "❌ Invalid minute. Use 0–59. Example: <code>/alt 02</code>"); return
        old_min = ALT_SCAN_MINUTE
        ALT_SCAN_MINUTE = new_min
        _auto_scan_last_hour = -1   # reset so it doesn't skip next trigger
        send_reply(chat_id,
            f"✅ <b>Alt Scan Time Updated</b>\n\n"
            f"Was: every hour at :{old_min:02d}\n"
            f"Now: every hour at <b>:{new_min:02d}</b>\n\n"
            f"<i>- CLEXER V17.8.5 -</i>"); return

    elif cmd == "/scancopy" and is_admin:
        if len(parts) < 2 or parts[1].lower() not in ("on","off"):
            state = "ON ✅" if ct.SCAN_CT_ENABLED else "OFF ❌"
            send_reply(chat_id,
                f"<b>Scan Copy Trade:</b> {state}\n\n"
                "Usage: <code>/scancopy on</code> or <code>/scancopy off</code>\n\n"
                "When ON — /scan will auto-place trades on copy users for the chosen alt coin.\n"
                "When OFF — /scan gives analysis only, no trade placed.\n\n"
                "<i>- CLEXER V17.8.5 -</i>"); return
        ct.set_scan_ct(parts[1].lower() == "on")
        state = "ON ✅" if ct.SCAN_CT_ENABLED else "OFF ❌"
        send_reply(chat_id, f"✅ Scan copy trade is now <b>{state}</b>\n\n<i>- CLEXER V17.8.5 -</i>")

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

    elif cmd in ("/scan", "/scan1", "/scan2") and is_admin:
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
                        f"TV mode is ON. Start TV bridge or run /scantv off to use BingX mode.\n\n<i>- CLEXER V17.8.5 -</i>")
                    return

                # ── Remind about running trades ───────────────────────────────
                my_list = _scan_list(scan_ver)
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

                for candidate in candidate_order[:MAX_TRIES + 3]:  # a few extras in case of skips
                    if signal_placed: break
                    if len(tried) >= MAX_TRIES: break

                    chosen_base = candidate["base"]
                    chosen_sym  = candidate["sym"]
                    cp          = candidate["price"]

                    # Skip if already in active trade
                    if chosen_sym in _all_active_scan_syms():
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

                    r2 = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
                        model="claude-opus-4-8", max_tokens=_max_tokens,
                        messages=[{"role":"user","content":content}])
                    analysis = r2.content[0].text.strip()

                    import re as _re
                    def _parse(label):
                        m = _re.search(rf"{label}[:\s]+([0-9.]+)", analysis, _re.IGNORECASE)
                        return float(m.group(1)) if m else 0.0
                    sig_m = _re.search(r"Signal[:\s]+(BUY|SELL|WAIT)", analysis, _re.IGNORECASE)
                    scan_signal_val = sig_m.group(1).upper() if sig_m else "WAIT"

                    emoji = "🟢" if candidate["change"] >= 0 else "🔴"
                    tv_src = "TV" if tv_switched else "BingX"
                    send_reply(cid,
                        f"{emoji} <b>{chosen_sym}</b> #{len(tried)}  {ist_str()}\n\n"
                        f"Price: <b>${cp:,.6g}</b> ({candidate['change']:+.2f}%) | {tv_src}\n\n"
                        f"<pre>{analysis[:900]}</pre>\n\n"
                        f"⚠️ <i>Not financial advice — CLEXER V17.8.5</i>")

                    if scan_signal_val == "WAIT":
                        print(f"  [SCAN] {chosen_sym} → WAIT — trying next coin")
                        continue   # try next candidate

                    # ── BUY or SELL — place trade ──────────────────────────────
                    scan_entry = cp   # always live price
                    scan_sl    = _parse("SL")
                    entry_type = "MARKET"

                    if scan_sl > 0:
                        sl_dist = abs(scan_entry - scan_sl)
                        min_sl  = scan_entry * 0.015
                        if sl_dist < min_sl:
                            sl_dist = min_sl
                            scan_sl = round(scan_entry - sl_dist if scan_signal_val == "BUY"
                                            else scan_entry + sl_dist, 6)
                        scan_tp1 = round(scan_entry + sl_dist*1.5 if scan_signal_val=="BUY"
                                         else scan_entry - sl_dist*1.5, 6)
                        scan_tp2 = round(scan_entry + sl_dist*3.0 if scan_signal_val=="BUY"
                                         else scan_entry - sl_dist*3.0, 6)

                        sd = {"signal":scan_signal_val,"entry":scan_entry,
                              "sl":scan_sl,"tp1":scan_tp1,"tp2":scan_tp2,"entry_type":"MARKET"}
                        slot_data = {
                            "symbol":chosen_sym,"signal":scan_signal_val,
                            "entry":scan_entry,"sl":scan_sl,"tp1":scan_tp1,"tp2":scan_tp2,
                            "entry_type":"MARKET","tp1_hit":False,
                            "entry_hit":True,"created_at":time.time(),
                        }
                        _scan_list(scan_ver).append(slot_data)
                        trade_stats["scan_signals"] += 1
                        save_state()
                        send_telegram(fmt_scan_signal(slot_data))
                        ct_results = ct.on_scan_signal(sd, chosen_sym, cp)
                        send_reply(cid, f"📋 <b>Copy Trade ({chosen_sym}):</b>\n"+"\n".join(ct_results[:5]))
                        signal_placed = True
                    else:
                        print(f"  [SCAN] {chosen_sym} — could not parse SL, trying next")
                        continue

                # ── No coin found after trying all candidates ──────────────────
                if not signal_placed:
                    tried_str = ", ".join(tried) if tried else "none"
                    send_reply(cid,
                        f"⏸ <b>No signal found</b>  {ist_str()}\n\n"
                        f"Tried {len(tried)} coin(s): <b>{tried_str}</b>\n\n"
                        f"None had clear 4H+1H+5M alignment for MARKET entry right now.\n"
                        f"Next auto-scan runs at :{ALT_SCAN_MINUTE:02d} IST.\n\n"
                        f"<i>- CLEXER V17.8.5 -</i>")

            except Exception as e:
                send_reply(cid, f"❌ Scan error: {e}")
                import traceback as _tb2; print(_tb2.format_exc())
        threading.Thread(target=lambda: _do_scan(cid=chat_id, scan_ver=ver), daemon=True).start()

    elif cmd == "/coin" and is_admin:
        if len(parts) < 2:
            send_reply(chat_id,
                "<b>Coin Lookup</b>\n\n"
                "Usage: <code>/coin ETH</code> or <code>/coin ETHUSDT</code>\n\n"
                "Searches BingX, shows matches, then analyzes the coin you pick.\n\n"
                "<i>- CLEXER V17.8.5 -</i>"); return
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
                        f"<i>- CLEXER V17.8.5 -</i>"); return

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
                    send_reply(cid, "\n".join(lines) + "\n\n<i>- CLEXER V17.8.5 -</i>"); return

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
                resp = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
                    model="claude-haiku-4-5-20251001", max_tokens=700,
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
                analysis = resp.content[0].text.strip()
                emoji = "🟢" if change >= 0 else "🔴"
                send_reply(cid,
                    f"{emoji} <b>{sym} Analysis</b>  {ist_str()}\n\n"
                    f"Price:  <b>${price:,.6g}</b>  ({change:+.2f}%)\n"
                    f"24H:   H:${high24:,.6g}  L:${low24:,.6g}\n"
                    f"Vol:   ${vol/1e6:.1f}M\n\n"
                    f"<b>Claude Analysis:</b>\n<i>{analysis[:900]}</i>\n\n"
                    f"⚠️ Not financial advice\n<i>- CLEXER V17.8.5 -</i>")
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
    del broadcast_pending[chat_id]
    send_reply(chat_id, f"Broadcasting to {len(registered_users)+1} targets...")
    threading.Thread(target=do_broadcast, args=(chat_id, text, file_id, file_type), daemon=True).start()

def command_listener():
    global last_update_id
    print("[CMD] Listener started")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook", timeout=10)
    except: pass
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"offset": last_update_id+1, "timeout": 20, "allowed_updates": ["message","callback_query"]}, timeout=25)
            data = r.json()
            if not data.get("ok"): time.sleep(5); continue
            for upd in data.get("result", []):
                last_update_id = upd["update_id"]

                # Handle inline button callbacks
                cb = upd.get("callback_query")
                if cb:
                    cb_data = cb.get("data",""); cb_cid = cb["from"]["id"]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                                  json={"callback_query_id": cb["id"]}, timeout=5)
                    if str(cb_cid) == str(ADMIN_CHAT_ID):
                        global btc_analysis_enabled
                        if cb_data == "btca_on":
                            btc_analysis_enabled = True
                            send_reply(cb_cid, "✅ <b>BTC Analysis ON</b>\n\nWill scan at 7:21, 11:21, 15:21, 19:21, 23:21 IST.\n\n<i>- CLEXER V17.8.5 -</i>",
                                reply_markup={"inline_keyboard": [[
                                    {"text": "▶ Enable Analysis", "callback_data": "btca_on"},
                                    {"text": "⏸ Disable Analysis", "callback_data": "btca_off"}]]})
                        elif cb_data == "btca_off":
                            btc_analysis_enabled = False
                            send_reply(cb_cid, "⏸ <b>BTC Analysis OFF</b>\n\nScheduled scans paused. /signal still forces a scan.\n\n<i>- CLEXER V17.8.5 -</i>",
                                reply_markup={"inline_keyboard": [[
                                    {"text": "▶ Enable Analysis", "callback_data": "btca_on"},
                                    {"text": "⏸ Disable Analysis", "callback_data": "btca_off"}]]})
                    continue

                msg = upd.get("message",{}); text = msg.get("text","") or ""
                cid = msg.get("chat",{}).get("id"); uname = msg.get("from",{}).get("username","?")
                if not cid: continue
                # Ignore messages from the second group (send-only)
                ch2 = os.getenv("TELEGRAM_CHANNEL_ID_2","")
                if ch2 and str(cid) == str(ch2): continue
                print(f"  [CMD] @{uname} ID:{cid}: {text[:50]}")
                register_user(cid)
                if cid in broadcast_pending and not text.startswith("/"):
                    handle_broadcast_message(cid, msg); continue
                if text.startswith("/"): handle_command(text, cid, msg)
        except Exception as e: print(f"  [CMD] {e}")
        time.sleep(2)

# --- MAIN ---------------------------------------------------------------------
_auto_scan_last_hour = -1   # tracks last IST hour auto-scan ran
ALT_SCAN_MINUTE = 2         # minute of each hour when alt scan fires — changeable via /alt MM

def _run_auto_scan(cid, scan_ver=2):
    """Auto-scan entry point — called from main loop at IST :02."""
    lbl = "V1" if scan_ver == 1 else "V2"
    send_admin(f"🔄 <b>Auto-Scan {lbl}</b>  {ist_str()}\n\nScheduled scan starting (~60s)...\n\n<i>- CLEXER V17.8.5 -</i>")
    # Reuse the same _do_scan logic by firing handle_command from within
    cmd = "/scan1" if scan_ver == 1 else "/scan2"
    handle_command(cmd, cid)

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

    if TV_BRIDGE_URL:
        print("  Checking TV bridge...")
        if tv_update_state():
            cdp = "TV connected" if tv_bridge_state["cdp_ok"] else "TV not connected yet"
            print(f"  Bridge ONLINE - {cdp}")
        else:
            print("  TV bridge OFFLINE - Binance fallback")

    threading.Thread(target=command_listener, daemon=True).start()

    # Start SL/TP monitor — checks all copy users' positions every 1 hour
    ct.start_monitor_loop(notify_fn=send_admin, interval_hours=1)

    # Startup sync check — alert admin if any orphan positions exist
    def _startup_sync():
        time.sleep(10)  # wait for db to load
        lines = ct.sync_check()
        has_orphan = any("ORPHAN" in l or "GHOST" in l for l in lines)
        if has_orphan:
            send_admin("🚨 <b>STARTUP SYNC ALERT</b>\n\n" + "\n".join(lines) + "\n\nUse /synccheck anytime to recheck.")
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
        f"<i>- CLEXER V17.8.5 -</i>")

    MAIN_TICK = 5   # loop runs every 5s — ticker checked every TICK_INTERVAL=10s

    while True:
        try:
            if bot_paused.is_set():
                time.sleep(MAIN_TICK); continue

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
                    send_admin(f"<b>TradingView Offline</b>\n\nSwitched to Binance (OLD prompt).\n\n<i>- CLEXER V17.8.5 -</i>")
                elif not was_online and is_online:
                    print("  TV back ONLINE")
                    send_admin(f"<b>TradingView Back Online</b>\n\nSwitched back to TradingView (NEW prompt).\n\n<i>- CLEXER V17.8.5 -</i>")

            # News
            if (now-last_news_check_time) >= NEWS_CHECK_INTERVAL and SEND_NEWS:
                last_news_check_time = now
                threading.Thread(target=check_news, daemon=True).start()

            # ── BTC scan at :21, Alt scan at ALT_SCAN_MINUTE — every hour ─────
            global _auto_scan_last_hour
            _ist_now = now_ist()
            _btc_fixed_hours = {7, 11, 15, 19, 23}
            # Alt scan: every hour at ALT_SCAN_MINUTE
            if (_ist_now.minute == ALT_SCAN_MINUTE and _auto_scan_last_hour != _ist_now.hour):
                _auto_scan_last_hour = _ist_now.hour
                print(f"  [AUTO-SCAN] Alt scan at {_ist_now.strftime(f'%H:{ALT_SCAN_MINUTE:02d} IST')}")
                if ADMIN_CHAT_ID:
                    threading.Thread(target=lambda: _run_auto_scan(ADMIN_CHAT_ID, scan_ver=1), daemon=True).start()
                    threading.Thread(target=lambda: _run_auto_scan(ADMIN_CHAT_ID, scan_ver=2), daemon=True).start()

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
            if not forced and (not _btc_scan_due or not btc_analysis_enabled):
                time.sleep(MAIN_TICK); continue

            # Cooldown
            if trade_stats["cooldown_scans"] > 0 and not forced:
                trade_stats["cooldown_scans"] -= 1
                if trade_stats["cooldown_scans"] == 0:
                    send_telegram("✅ <b>Cooldown over - scanning now!</b> 🔍\n\n✨ <i>- CLEXER V17.8.5 -</i>")
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
                    f"Analyzing...\n<i>- CLEXER V17.8.5 -</i>")

            data = fetch_all_data()

            if not active_trade["signal"]:
                signal = analyze_with_claude(ticker, data, validate_trade=False)
                if signal and not signal.get("_hold"):
                    send_telegram(fmt_signal(signal)); set_trade(signal)
                    results = ct.on_signal(signal, price)
                    # MARKET orders filled instantly — send entry confirmation immediately
                    if signal.get("entry_type", "MARKET") == "MARKET":
                        send_telegram(
                            f"🚀 <b>ENTRY TRIGGERED!</b>  🕐 {ist_str()}\n\n"
                            f"{'🟢' if signal['signal']=='BUY' else '🔴'} <b>{signal['signal']} {SYMBOL}</b>\n"
                            f"🎯 Entry: <b>{signal['entry']:,.0f}</b>  ✅ MARKET FILLED\n"
                            f"🛑 SL:    <b>{signal['sl']:,.0f}</b>\n"
                            f"💰 TP1:   <b>{signal['tp1']:,.0f}</b>\n"
                            f"🏆 TP2:   <b>{signal['tp2']:,.0f}</b>\n\n"
                            f"✨ <i>- CLEXER V17.8.5 -</i>"
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
                            f"TP2: <b>{t['tp2']:,.0f}</b>\n\n<i>- CLEXER V17.8.5 -</i>")
                elif signal.get("_hold"):
                    send_admin(f"<b>Trade Validated - HOLD</b>  {ist_str()}\n\n"
                        f"{t['signal']} @ {t['entry']:,.0f}\n"
                        f"SL:{t['sl']:,.0f} | TP1:{t['tp1']:,.0f} | TP2:{t['tp2']:,.0f}\n\n"
                        f"<i>{signal.get('reasoning','Structure intact')[:250]}</i>\n\n<i>- CLEXER V17.8.5 -</i>")
                elif signal["signal"] != t["signal"]:
                    # Only flip if entry has already been hit — never flip a pending trade
                    if not t["entry_hit"]:
                        print(f"  [FLIP BLOCKED] Entry not hit yet — holding {t['signal']} @ {t['entry']:,.0f}")
                        send_admin(f"<b>Flip Blocked</b>\n\nClaude wanted to flip {t['signal']} -> {signal['signal']} but entry not hit yet.\nHolding original trade.\n\n<i>- CLEXER V17.8.5 -</i>")
                    else:
                        flip_reason = signal.get("reasoning","Structure flipped")
                        log_trade_outcome("STRUCTURE_FLIP", flip_reason[:100])
                        send_telegram(f"🔄 <b>STRUCTURE FLIP!</b> 🚨  🕐 {ist_str()}\n\n"
                            f"❌ Closing: {t['signal']} @ {t['entry']:,.0f}\n"
                            f"💡 Why: <i>{flip_reason[:200]}</i>\n\n"
                            f"{'🟢' if signal['signal']=='BUY' else '🔴'} New: <b>{signal['signal']} @ {signal['entry']:,.0f}</b>\n\n✨ <i>- CLEXER V17.8.5 -</i>")
                        ct.on_close_all()
                        reset_trade(); time.sleep(1); send_telegram(fmt_signal(signal)); set_trade(signal)
                        ct.on_signal(signal, price)
                else:
                    if forced:
                        send_telegram(f"<b>Trade Update</b>  {ist_str()}\n\n"
                            f"Old: {t['signal']} @ {t['entry']:,.0f}\n"
                            f"New: {signal['signal']} @ {signal['entry']:,.0f}\n"
                            f"Bias confirmed.\n\n<i>- CLEXER V17.8.5 -</i>")
                    log_trade_outcome("REPLACED","same direction, updated levels")
                    ct.on_close_all()
                    reset_trade(); time.sleep(1); send_telegram(fmt_signal(signal)); set_trade(signal)
                    results = ct.on_signal(signal, price)
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
