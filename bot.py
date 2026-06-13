"""
CLEXER Signal Bot V7.0
"""

import os, time, json, base64, requests, anthropic, threading, re
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

SYMBOL               = "BTCUSDT"
TICK_INTERVAL        = 60
PRICE_CHECK_INTERVAL = 3600
SIGNAL_SCAN_INTERVAL = 14400
NEWS_CHECK_INTERVAL  = 1800
BINANCE_BASE         = "https://api1.binance.com/api/v3"
IST                  = timedelta(hours=5, minutes=30)

SEND_CHARTS       = False   # OFF by default - /images on to enable
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
signal_history        = []
trade_outcomes        = []
force_scan            = threading.Event()
bot_paused            = threading.Event()
last_update_id        = 0
last_force_scan_time  = 0
last_signal_scan_time = 0
last_price_check_time = 0
last_tick_time        = 0
last_news_check_time  = 0
posted_news_guids: set = set()
latest_news_context: list = []
trade_lock = threading.Lock()

USER_DB_FILE = "users.json"
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
}

def reset_trade():
    global active_trade
    with trade_lock:
        active_trade = {
            "signal": None, "entry": None, "sl": None,
            "tp1": None, "tp2": None, "tp1_hit": False,
            "entry_type": "MARKET", "entry_note": "",
            "entry_hit": False, "sl_wicked": False, "scan_count": 0,
        }

def set_trade(s: dict):
    global active_trade
    with trade_lock:
        active_trade = {
            "signal": s["signal"], "entry": s["entry"],
            "sl": s["sl"], "tp1": s["tp1"], "tp2": s["tp2"], "tp1_hit": False,
            "entry_type": s.get("entry_type", "MARKET"),
            "entry_note": s.get("entry_note", ""),
            "entry_hit": s.get("entry_type", "MARKET") == "MARKET",
            "sl_wicked": False, "scan_count": 0,
        }
    trade_stats["total_signals"] += 1
    signal_history.append({
        "time": ist_str(), "signal": s["signal"],
        "entry": s["entry"], "sl": s["sl"],
        "tp1": s["tp1"], "tp2": s["tp2"],
        "rr": s.get("rr", "?"), "confidence": s.get("confidence", "?"),
    })
    if len(signal_history) > 10: signal_history.pop(0)

def log_trade_outcome(reason: str, detail: str = ""):
    trade_outcomes.append({
        "time": ist_str(), "signal": active_trade.get("signal"),
        "entry": active_trade.get("entry"), "reason": reason, "detail": detail,
    })
    if len(trade_outcomes) > 5: trade_outcomes.pop(0)

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
    tv_map = {"weekly": "W", "4h": "4H", "1h": "1H", "5m": "5"}
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/candles",
            params={"symbol": SYMBOL, "interval": tv_map.get(interval, interval), "limit": limit},
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

def fetch_tv_screenshots():
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

def fetch_tv_indicators():
    if not TV_BRIDGE_URL or not tv_bridge_state["online"]: return {}
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/indicators", timeout=15)
        if r.status_code == 200:
            data = r.json()
            if "error" not in data:
                print(f"      [INDICATORS] {len(data.get('raw_studies',[]))} studies OK")
                return data
    except Exception as e: print(f"      [INDICATORS] {e}")
    return {}

def build_indicator_context(indicators: dict) -> str:
    if not indicators: return ""
    lines = ["\n\nINDICATOR VALUES FROM TRADINGVIEW CHART:"]
    clexer = indicators.get("clexer_sniper")
    if clexer: lines.append(f"CLEXER SNIPER: {clexer.get('text','active')[:150]}")
    spaceman = indicators.get("spaceman_levels", [])
    if spaceman:
        levels = sorted(set([round(l, 0) for l in spaceman]))
        lines.append(f"SPACEMAN KEY LEVELS: {levels[:10]}")
        lines.append("  (nearest above = resistance, nearest below = support)")
    poi = indicators.get("poi_vol_surge")
    if poi: lines.append(f"POI VOL SURGE: {poi.get('text','active')[:150]}")
    raw = indicators.get("raw_studies", [])
    if raw:
        lines.append("ALL ACTIVE INDICATORS:")
        for study in raw[:8]:
            if study.get("title"): lines.append(f"  {study['title']}: {study.get('value','')}")
    if len(lines) == 1: return ""
    lines += ["", "Use indicators as ADDITIONAL CONFIRMATION only.",
              "Do not rely on indicators alone - confirm with price structure."]
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════════════
#  BINANCE FALLBACK
# ═══════════════════════════════════════════════════════════════════════════════

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
            df = tv_get_candles(interval, limit)
            if df is not None and len(df) >= 2: return df
    return binance_get_candles(interval, limit)

def get_ticker():
    if TV_BRIDGE_URL:
        tv_update_state()
        if tv_bridge_state["online"]:
            tk = tv_get_ticker()
            if tk: return tk
    return binance_get_ticker()

def get_price_range_since(minutes):
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

def get_current_source():
    return "TradingView" if (TV_BRIDGE_URL and tv_bridge_state["online"]) else "Binance"

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
    h, l = find_swing_points(df, 3)
    if len(h) < 2 or len(l) < 2: return "NEUTRAL"
    hp = [x["price"] for x in h[-2:]]; lp = [x["price"] for x in l[-2:]]
    if hp[1] > hp[0] and lp[1] > lp[0]: return "BULLISH"
    if hp[1] < hp[0] and lp[1] < lp[0]: return "BEARISH"
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

def build_new_prompt(summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note):
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


def build_old_prompt(summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note, session_note):
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
    prompt_mode = "NEW (TradingView)" if tv_on else "OLD (Binance)"
    print(f"  [CLAUDE] Mode:{prompt_mode} | MinConf:{min_conf} | Validate:{validate_trade}")

    screenshots = {}
    if tv_on:
        print("  [CLAUDE] Fetching screenshots...")
        screenshots = fetch_tv_screenshots()

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

    if tv_on:
        prompt = build_new_prompt(full_summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note)
    else:
        prompt = build_old_prompt(full_summary, price, session, validate_ctx, news_ctx, outcome_ctx, conf_note, session_note)

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
                model="claude-opus-4-6", max_tokens=1200,
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
                        model="claude-opus-4-6", max_tokens=1200,
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
                f"{ist_str()}\n<i>- CLEXER V7.0 -</i>")
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
                f"<i>/resetsl to lower bar. - CLEXER V7.0 -</i>")
            return None

        tp2_dist = abs(entry-float(signal["tp2"]))
        signal["rr"] = f"1:{tp2_dist/sl_dist:.1f}" if sl_dist else "1:?"
        signal["data_source"] = src; signal["prompt_mode"] = prompt_mode
        print(f"  [OK] {sig_type} entry:{entry:,.0f} SL:{sl_raw:,.0f} ({sl_dist:.0f}pts) R:R:{signal['rr']} Conf:{conf}")
        return signal
    except Exception as e:
        print(f"  [CLAUDE PARSE ERROR] {e}"); import traceback; traceback.print_exc(); return None


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
        f"Source:   <b>{get_current_source()}</b>\n\n<i>- CLEXER V7.0 -</i>")

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

def send_reply(chat_id, text):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
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
    send_reply(admin_chat_id, f"<b>Broadcast Done</b>\n{ok} delivered | {fail} failed\n\n<i>- CLEXER V7.0 -</i>")

# --- MESSAGE FORMATS ----------------------------------------------------------
def fmt_signal(s):
    e  = "🟢" if s["signal"]=="BUY" else "🔴"
    ci = {"HIGH":"[HIGH]","MEDIUM":"[MED]","LOW":"[LOW]"}.get(s.get("confidence",""),"")
    el = f"Entry    <b>{s['entry']:,.0f}</b>"
    if s.get("entry_type")=="PULLBACK" and s.get("entry_note"):
        el += f"\n   <i>{s['entry_note']}</i>"
    wk = s.get("weekly_trend",""); s4h = s.get("structure_4h","")
    ez = s.get("entry_zone","");   rs  = s.get("reasoning","")
    src = s.get("data_source", get_current_source()); mode = s.get("prompt_mode","?")
    return (f"{e} <b>{s['signal']} - {SYMBOL}</b>  {ci}\n"
        f"{ist_str()}  |  {s.get('session',get_session())}\n"
        f"Source: <b>{src}</b> | Mode: {mode}\n\n"
        f"{el}\nSL       <b>{s['sl']:,.0f}</b>\nTP1     <b>{s['tp1']:,.0f}</b>\n"
        f"TP2     <b>{s['tp2']:,.0f}</b>\nR:R     <b>{s.get('rr','-')}</b>\n\n"
        + (f"Weekly: <i>{wk}</i>\n" if wk else "")
        + (f"4H:     <i>{s4h}</i>\n" if s4h else "")
        + (f"Zone:   <i>{ez}</i>\n"  if ez else "")
        + (f"\n<i>{rs}</i>\n"        if rs else "")
        + f"\n<i>- CLEXER V7.0 -</i>\n<i>Not financial advice</i>")

def fmt_update(status, price=None):
    t = active_trade; entry = t.get("entry") or 0
    msgs = {
        "SL_HIT":         "<b>SL HIT</b> - Finding next trade",
        "TP1_HIT":        f"<b>TP1 HIT!</b>\nSL -> Breakeven ({entry:,.0f})\nRiding to TP2 -> <b>{t.get('tp2',0):,.0f}</b>",
        "TP2_HIT":        "<b>TP2 HIT - Trade Complete!</b>",
        "STOP_HUNT":      "<b>STOP HUNT</b> - SL wicked, closed back. Holding.",
        "SETUP_INVALID":  "<b>Setup Invalid</b> - SL hit before entry. Resetting.",
        "ENTRY_MISSED":   f"<b>Entry Missed</b> - Price bypassed zone {entry:,.0f}. Resetting.",
        "STRUCTURE_FLIP": "<b>Structure Flipped</b> - Closing trade.",
        "WAITING_ENTRY":  (f"<b>Waiting Pullback</b>\nEntry zone: <b>{entry:,.0f}</b>\n"
            + (f"Current: <b>{price:,.0f}</b> ({abs((price or 0)-entry):,.0f} pts away)" if price else "")),
    }
    return f"<b>{SYMBOL} UPDATE</b>  {ist_str()}\n\n{msgs.get(status,'Trade running')}\n\n<i>- CLEXER V7.0 -</i>"

# --- TICK CHECK ---------------------------------------------------------------
def run_tick_check():
    if not active_trade["signal"]: return False
    try:
        ticker = get_ticker(); price = ticker["price"]
        t = active_trade; sig = t["signal"]; entry = t["entry"]; sl = t["sl"]; tp1 = t["tp1"]; tp2 = t["tp2"]
        if not t["entry_hit"]:
            tol = abs(entry-sl)*0.25
            if (sig=="BUY" and price<=entry+tol) or (sig=="SELL" and price>=entry-tol):
                active_trade["entry_hit"] = True
                send_telegram(f"<b>ENTRY TRIGGERED!</b>  {ist_str()}\n\n{sig} {SYMBOL}\n\n"
                    f"Entry:  <b>{entry:,.0f}</b>  Price: <b>{price:,.2f}</b>\n"
                    f"SL:     <b>{sl:,.0f}</b>  ({abs(price-sl):.0f} pts)\n"
                    f"TP1:    <b>{tp1:,.0f}</b>\nTP2:    <b>{tp2:,.0f}</b>\n\n<i>- CLEXER V7.0 -</i>")
            return False
        if (sig=="BUY" and price>=tp2) or (sig=="SELL" and price<=tp2):
            trade_stats["total_tp2"] += 1; trade_stats["consecutive_sl"] = 0
            log_trade_outcome("TP2_HIT", f"closed at {tp2:,.0f}")
            send_telegram(f"<b>TP2 HIT!</b>  {ist_str()}\n\n{sig} {SYMBOL}\n"
                f"Entry: {entry:,.0f} -> TP2: <b>{tp2:,.0f}</b>\n\n<i>- CLEXER V7.0 -</i>")
            ct.on_tp2(); reset_trade(); return True
        if not t["tp1_hit"]:
            if (sig=="BUY" and price>=tp1) or (sig=="SELL" and price<=tp1):
                active_trade["tp1_hit"] = True; active_trade["sl"] = entry
                trade_stats["total_tp1"] += 1; trade_stats["consecutive_sl"] = 0
                ct.on_tp1(entry)
                send_telegram(f"<b>TP1 HIT!</b>  {ist_str()}\n\n{sig} {SYMBOL}\n"
                    f"TP1: <b>{tp1:,.0f}</b> - SL moved to BE: <b>{entry:,.0f}</b>\n"
                    f"Riding TP2: <b>{tp2:,.0f}</b>...\n\n<i>- CLEXER V7.0 -</i>")
        sl_margin = 80
        if (sig=="BUY" and price<sl-sl_margin) or (sig=="SELL" and price>sl+sl_margin):
            trade_stats["total_sl"] += 1; trade_stats["consecutive_sl"] += 1
            n = trade_stats["consecutive_sl"]
            log_trade_outcome("SL_HIT", f"{n} in a row, price {price:,.0f} vs sl {sl:,.0f}")
            if n >= 3:   trade_stats["cooldown_scans"] = 2; send_telegram(f"<b>SL HIT</b> ({n} in a row)\nCooling down 2 scans.\n\n<i>- CLEXER V7.0 -</i>")
            elif n == 2: trade_stats["cooldown_scans"] = 1; send_telegram(f"<b>SL HIT</b> ({n} in a row)\nCooling down 1 scan.\n\n<i>- CLEXER V7.0 -</i>")
            else:        send_telegram(fmt_update("SL_HIT"))
            ct.on_sl(); reset_trade(); return True
    except Exception as e: print(f"  [TICK ERROR] {e}")
    return False

# --- 1-HOUR PRICE CHECK -------------------------------------------------------
def run_price_check():
    if not active_trade["signal"]: return False
    try:
        ticker = get_ticker(); price = ticker["price"]
        range_1h = get_price_range_since(60)
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
            ct.on_tp2(); send_telegram(fmt_update("TP2_HIT")); reset_trade(); return True
        elif status == "SL_HIT":
            trade_stats["total_sl"] += 1; trade_stats["consecutive_sl"] += 1
            n = trade_stats["consecutive_sl"]
            log_trade_outcome("SL_HIT", f"{n} in a row during 1H check")
            if n >= 3:   trade_stats["cooldown_scans"] = 2; send_telegram(f"<b>SL HIT</b> ({n} in a row)\nCooling 2 scans.\n\n<i>- CLEXER V7.0 -</i>")
            elif n == 2: trade_stats["cooldown_scans"] = 1; send_telegram(f"<b>SL HIT</b> ({n} in a row)\nCooling 1 scan.\n\n<i>- CLEXER V7.0 -</i>")
            else:        send_telegram(fmt_update("SL_HIT"))
            ct.on_sl(); reset_trade(); return True
        elif status == "TP1_HIT" and not active_trade["tp1_hit"]:
            active_trade["tp1_hit"] = True; active_trade["sl"] = active_trade["entry"]
            trade_stats["total_tp1"] += 1; trade_stats["consecutive_sl"] = 0
            ct.on_tp1(active_trade["entry"])
            send_telegram(fmt_update("TP1_HIT"))
        elif status in ("STOP_HUNT",):      send_telegram(fmt_update("STOP_HUNT"))
        elif status in ("ENTRY_MISSED","SETUP_INVALID"):
            log_trade_outcome(status, ""); ct.on_cancel_limits()
            send_telegram(fmt_update(status)); reset_trade(); return True
        elif status == "WAITING_ENTRY":
            active_trade["scan_count"] += 1; send_telegram(fmt_update("WAITING_ENTRY", price))
        elif status == "RUNNING":
            active_trade["scan_count"] += 1; send_telegram(price_only_advice(price))
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
            f"{item['source']}\n<a href='{item['link']}'>Read article</a>\n\n<i>- CLEXER V7.0 · {ist_str()} -</i>")
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
        send_reply(chat_id, "<b>TV Status</b>\n\nTV_BRIDGE_URL not set.\nRunning on <b>Binance</b>.\n\n<i>- CLEXER V7.0 -</i>"); return
    send_reply(chat_id, f"Checking...\n<code>{TV_BRIDGE_URL}</code>")
    now = time.time(); health = tv_ping()
    if not health:
        ls = tv_bridge_state.get("last_seen",0)
        since = f"{int((now-ls)//60)}m ago" if ls else "never"
        send_reply(chat_id, f"<b>TV Status</b>\n\n🔴 Bridge OFFLINE\nLast seen: {since}\n\nUsing: <b>Binance</b>\n\n<i>- CLEXER V7.0 -</i>"); return
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
        f"Uptime: <b>{uptime_str}</b>\n\n{ist_str()}\n<i>- CLEXER V7.0 -</i>")

# --- COMMANDS -----------------------------------------------------------------
ADMIN_HELP = """<b>CLEXER V7.0 - Admin Commands</b>
--------------------

<b>BOT CONTROL</b>
/go - START scanning (required after deploy)
/pause - STOP scanning
/resume - Same as /go
/signal - Force scan now
/resetsl - Reset SL streak + cooldown
/setinterval 4 - Set scan interval (hours)

<b>INFO</b>
/status - Bot status
/price - Live BTC price
/trade - Active trade
/history - Last 5 signals
/stats - Win/loss stats
/session - Current session
/tvstatus - TV connection

<b>TRADE CONTROL</b>
/close - Close trade
/sltobe - SL to breakeven
/setsl 61500
/settp1 63000
/settp2 65000

<b>CHANNELS</b>
/channels - show status
/pausechannel 1 or 2
/resumechannel 1 or 2

<b>CHARTS (off by default)</b>
/images on|off
/setimages weekly,4h,1h,5m

<b>NEWS (off by default)</b>
/news on|off
/latestnews

<b>COPY TRADE (ADMIN)</b>
/users - Copy trade users list
/allusers - Summary stats
/user ID - User detail + position
/kick ID - Remove user
/pauseuser ID - Pause/unpause user

/broadcast - Send to all
/help"""

FRIEND_HELP = """<b>CLEXER V7.0 Commands</b>
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
/setsize 50 - Trade size (USDT)
/setleverage 10 - Leverage
/mytrade - Your open position
/mysize - Your settings
/myhistory - Trade history

<i>Note: 2 uses per command per hour</i>"""

FRIEND_COMMANDS = {"/start","/help","/status","/price","/trade","/history","/stats","/session"}
ADMIN_COMMANDS  = {"/go","/signal","/pause","/resume","/resetsl","/setinterval",
    "/close","/sltobe","/setsl","/settp1","/settp2","/tvstatus",
    "/broadcast","/users","/allusers","/user","/kick","/pauseuser",
    "/images","/setimages","/news","/latestnews",
    "/pausechannel","/resumechannel","/channels"}

def handle_command(text, chat_id, message=None):
    global SIGNAL_SCAN_INTERVAL, SEND_CHARTS, CHART_TFS, SEND_NEWS, last_force_scan_time, broadcast_pending
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
        ct.handle(cmd, parts, chat_id, uname, send_reply, is_admin)
        return

    if cmd in ("/start","/help"):
        send_reply(chat_id, ADMIN_HELP if is_admin else FRIEND_HELP)

    elif cmd in ("/go", "/resume"):
        bot_paused.clear()
        send_reply(chat_id,
            f"<b>CLEXER Started</b>\n\n"
            f"✅ Scanning active\n"
            f"Tick: {TICK_INTERVAL}s | Price: {PRICE_CHECK_INTERVAL//60}m | Signal: {SIGNAL_SCAN_INTERVAL//3600}h\n"
            f"Charts: {'ON' if SEND_CHARTS else 'OFF'} | News: {'ON' if SEND_NEWS else 'OFF'}\n"
            f"Source: {get_current_source()}\n\n"
            f"<i>- CLEXER V7.0 -</i>")

    elif cmd == "/pause":
        bot_paused.set()
        send_reply(chat_id, "<b>Bot Paused</b>\n\nUse /go to resume.\n\n<i>- CLEXER V7.0 -</i>")

    elif cmd == "/tvstatus":
        cmd_tvstatus(chat_id)

    elif cmd == "/status":
        t = active_trade; st = "PAUSED (/go to start)" if bot_paused.is_set() else "RUNNING"
        cd = f"Cooldown: {trade_stats['cooldown_scans']} scans\n" if trade_stats["cooldown_scans"] else ""
        ti = (f"{t['signal']} @ {t['entry']:,.0f}\nSL:{t['sl']:,.0f}  TP1:{t['tp1']:,.0f}  TP2:{t['tp2']:,.0f}\n"
            f"Entry:{'OK' if t['entry_hit'] else 'pending'}  TP1:{'OK' if t['tp1_hit'] else 'no'}"
            ) if t["signal"] else "No active trade"
        src = get_current_source()
        tv_status = ("ONLINE" if (tv_bridge_state["online"] and tv_bridge_state["cdp_ok"])
            else "Bridge OK - TV not connected" if tv_bridge_state["online"] else "OFFLINE - Binance") if TV_BRIDGE_URL else "Not configured - Binance"
        send_reply(chat_id,
            f"<b>CLEXER V7.0</b>\n\nBot: {st}\n{cd}"
            f"Session: {get_session()} {'active' if is_trading_hours() else 'inactive'}\n"
            f"IST: {ist_str()}\nScan: {SIGNAL_SCAN_INTERVAL//3600}h | MinConf: {required_confidence()}\n"
            f"Consec SL: {trade_stats['consecutive_sl']}\n"
            + (f"Users: {len(registered_users)}\n" if is_admin else "")
            + f"\nSource: <b>{src}</b>\nMode: <b>{'NEW (TV)' if is_tv_online() else 'OLD (Binance)'}</b>\n"
            + (f"TV: {tv_status}\n" if is_admin else "")
            + (f"Charts: {'ON' if SEND_CHARTS else 'OFF'} | News: {'ON' if SEND_NEWS else 'OFF'}\n\n" if is_admin else "\n")
            + f"<b>Active Trade:</b>\n{ti}")

    elif cmd == "/price":
        try:
            tk = get_ticker()
            send_reply(chat_id, f"<b>BTCUSDT</b>\n\nPrice: <b>{tk['price']:,.2f}</b>\n"
                f"24h: {tk['change']:+.2f}% | Vol: ${tk['volume']/1e6:.1f}M\n"
                f"H:{tk['high24']:,.2f}  L:{tk['low24']:,.2f}\n"
                f"Source: {tk.get('source',get_current_source())}\n{ist_str()}")
        except Exception as e: send_reply(chat_id, f"Error: {e}")

    elif cmd == "/trade":
        t = active_trade
        if not t["signal"]: send_reply(chat_id, "No active trade.")
        else:
            try: tk = get_ticker(); pl = f"Current: <b>{tk['price']:,.2f}</b>\n"
            except: pl = ""
            send_reply(chat_id, f"<b>Active Trade</b>\n\n{t['signal']} - {SYMBOL}\n{pl}"
                f"Entry: <b>{t['entry']:,.0f}</b> {'OK' if t['entry_hit'] else 'pending'}\n"
                f"SL:    <b>{t['sl']:,.0f}</b>\nTP1:   <b>{t['tp1']:,.0f}</b> {'HIT' if t['tp1_hit'] else 'pending'}\n"
                f"TP2:   <b>{t['tp2']:,.0f}</b>\nType:  {t['entry_type']}\n"
                + (f"<i>{t['entry_note']}</i>" if t.get("entry_note") else ""))

    elif cmd == "/history":
        if not signal_history: send_reply(chat_id, "No history.")
        else:
            lines = ["<b>Last Signals</b>\n"]
            for s in reversed(signal_history[-5:]):
                lines.append(f"{s['signal']} @ {s['entry']:,.0f}  R:R:{s.get('rr','?')}  {s.get('confidence','?')}\n"
                    f"   SL:{s['sl']:,.0f}  TP1:{s['tp1']:,.0f}  TP2:{s['tp2']:,.0f}\n   {s['time']}\n")
            send_reply(chat_id, "\n".join(lines))

    elif cmd == "/stats":
        ts = trade_stats
        send_reply(chat_id, f"<b>Statistics</b>\n\nSignals: {ts['total_signals']}\n"
            f"TP1: {ts['total_tp1']} | TP2: {ts['total_tp2']}\nSL: {ts['total_sl']}\n"
            f"Stop hunts: {ts['stop_hunts']}\nMissed: {ts['missed_entries']}\n"
            f"Consec SL: {ts['consecutive_sl']}\nCooldown: {ts['cooldown_scans']}")

    elif cmd == "/session":
        s = get_session()
        send_reply(chat_id, f"<b>Session</b>\n\n{s} {'Active' if is_trading_hours() else 'Inactive'}\n\n"
            f"London:  07:30-16:30 IST\nNY:      18:30-01:00 IST\nSleep:   01:00-07:29 IST\n\n{ist_str()}")

    elif cmd == "/users":
        uname = (message.get("from",{}).get("username","?") if message else "?")
        ct.handle(cmd, parts, chat_id, uname, send_reply, is_admin)

    elif cmd == "/close":
        t = active_trade
        if not t["signal"]: send_reply(chat_id, "No active trade.")
        else:
            info = f"{t['signal']} @ {t['entry']:,.0f}"
            log_trade_outcome("MANUAL_CLOSE", f"closed by admin")
            ct.on_close_all()
            reset_trade(); send_telegram(f"<b>Trade Closed</b>\n{info}\n\n<i>- CLEXER V7.0 -</i>")
            send_reply(chat_id, f"Closed: {info}"); force_scan.set()

    elif cmd == "/sltobe":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        else:
            old = active_trade["sl"]; active_trade["sl"] = active_trade["entry"]
            ct.on_sl_to_be(active_trade["entry"])
            send_telegram(f"<b>SL -> BE</b>  {old:,.0f} -> <b>{active_trade['entry']:,.0f}</b>\n\n<i>- CLEXER V7.0 -</i>")
            send_reply(chat_id, f"SL -> {active_trade['entry']:,.0f}")

    elif cmd == "/setsl":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /setsl 61500")
        else:
            try:
                v = float(parts[1].replace(",","")); old = active_trade["sl"]
                active_trade["sl"] = v
                ct.on_update_sl(v)
                send_telegram(f"<b>SL</b>  {old:,.0f} -> <b>{v:,.0f}</b>\n\n<i>- CLEXER V7.0 -</i>")
                send_reply(chat_id, f"SL = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /setsl 61500")

    elif cmd == "/settp1":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp1 63000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp1"] = v
                send_telegram(f"<b>TP1 -> {v:,.0f}</b>\n\n<i>- CLEXER V7.0 -</i>")
                send_reply(chat_id, f"TP1 = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /settp1 63000")

    elif cmd == "/settp2":
        if not active_trade["signal"]: send_reply(chat_id, "No active trade.")
        elif len(parts)<2: send_reply(chat_id, "Usage: /settp2 65000")
        else:
            try:
                v = float(parts[1].replace(",","")); active_trade["tp2"] = v
                send_telegram(f"<b>TP2 -> {v:,.0f}</b>\n\n<i>- CLEXER V7.0 -</i>")
                send_reply(chat_id, f"TP2 = {v:,.0f}")
            except: send_reply(chat_id, "Usage: /settp2 65000")

    elif cmd == "/signal":
        if bot_paused.is_set(): send_reply(chat_id, "Bot paused. /go first.")
        else:
            now = time.time(); elapsed = now-last_force_scan_time
            if elapsed<900 and last_force_scan_time>0: send_reply(chat_id, f"Cooldown: {int((900-elapsed)/60)} min left")
            else:
                last_force_scan_time = now
                send_reply(chat_id, "Forcing scan (~15-30s)..."); force_scan.set()

    elif cmd == "/resetsl":
        trade_stats["consecutive_sl"] = 0; trade_stats["cooldown_scans"] = 0
        send_reply(chat_id, "SL streak + cooldown reset.")

    elif cmd == "/setinterval":
        if len(parts)<2: send_reply(chat_id, f"Current: {SIGNAL_SCAN_INTERVAL//3600}h\nUsage: /setinterval 4")
        else:
            try:
                h = float(parts[1])
                if h<1 or h>24: send_reply(chat_id, "1-24 hours only.")
                else: SIGNAL_SCAN_INTERVAL = int(h*3600); send_reply(chat_id, f"Scan interval -> {h}h")
            except: send_reply(chat_id, "Usage: /setinterval 4")

    elif cmd == "/images":
        if len(parts)<2:
            send_reply(chat_id, f"Charts: {'ON' if SEND_CHARTS else 'OFF'}\nTFs: {', '.join(CHART_TFS).upper()}\n\nUsage: /images on|off\n(OFF by default)")
        elif parts[1].lower()=="on":
            SEND_CHARTS = True; send_reply(chat_id, f"Charts ON - posting to channel only\nTFs: {', '.join(CHART_TFS).upper()}")
        elif parts[1].lower()=="off":
            SEND_CHARTS = False; send_reply(chat_id, "Charts OFF")
        else: send_reply(chat_id, "Usage: /images on|off")

    elif cmd == "/setimages":
        if len(parts)<2: send_reply(chat_id, f"Current: {', '.join(CHART_TFS).upper()}\nUsage: /setimages weekly,4h,1h,5m")
        else:
            valid = {"weekly","4h","1h","5m"}
            chosen = [tf.strip().lower() for tf in parts[1].split(",") if tf.strip().lower() in valid]
            if not chosen: send_reply(chat_id, "No valid TFs. Use: weekly, 4h, 1h, 5m")
            else: CHART_TFS = chosen; send_reply(chat_id, f"Chart TFs: {', '.join(CHART_TFS).upper()}")

    elif cmd == "/news":
        if len(parts)<2:
            send_reply(chat_id, f"News: {'ON' if SEND_NEWS else 'OFF (default)'}\nUsage: /news on|off")
        elif parts[1].lower()=="on":  SEND_NEWS = True;  send_reply(chat_id, f"News ON - {len(NEWS_SOURCES)} sources")
        elif parts[1].lower()=="off": SEND_NEWS = False; send_reply(chat_id, "News OFF")
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
        send_reply(chat_id, f"<b>Channel {key} PAUSED</b>\n\nNo signals will be sent to channel {key}.\nUse /resumechannel {key} to resume.")

    elif cmd == "/resumechannel":
        if len(parts) < 2 or parts[1] not in ("1","2"):
            send_reply(chat_id, "Usage: /resumechannel 1\nor /resumechannel 2"); return
        key = parts[1]
        channel_paused[key] = False
        send_reply(chat_id, f"<b>Channel {key} RESUMED</b>\n\nSignals will now be sent to channel {key}.")

    elif cmd == "/cancel":
        if chat_id in broadcast_pending: del broadcast_pending[chat_id]; send_reply(chat_id, "Cancelled.")
        else: send_reply(chat_id, "Nothing to cancel.")

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
                params={"offset": last_update_id+1, "timeout": 20, "allowed_updates": ["message"]}, timeout=25)
            data = r.json()
            if not data.get("ok"): time.sleep(5); continue
            for upd in data.get("result", []):
                last_update_id = upd["update_id"]
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
def main():
    global last_signal_scan_time, last_price_check_time, last_tick_time, last_news_check_time

    print(f"[CLEXER V7.0] Starting | {SYMBOL}")
    print(f"  TV Bridge: {TV_BRIDGE_URL or 'NOT SET - Binance-only'}")
    print(f"  Starting PAUSED - send /go to start scanning")
    load_users()
    ct.load()

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

    # Startup message → admin DM only
    tv_line = ""
    if TV_BRIDGE_URL:
        if tv_bridge_state["online"] and tv_bridge_state["cdp_ok"]:   tv_line = "TV: ONLINE ✅\n"
        elif tv_bridge_state["online"]:                                 tv_line = "TV: Bridge OK - TV not connected\n"
        else:                                                           tv_line = "TV: OFFLINE - Binance fallback\n"
    else:
        tv_line = "TV: Not configured - Binance-only\n"

    send_admin(
        f"<b>CLEXER V7.0 Deployed</b>\n"
        f"---------------------\n"
        f"{tv_line}"
        f"Charts: {'ON' if SEND_CHARTS else 'OFF (default)'}\n"
        f"News: {'ON' if SEND_NEWS else 'OFF (default)'}\n\n"
        f"⚠️ <b>Bot is PAUSED</b>\n"
        f"Send /go to start scanning.\n"
        f"---------------------\n"
        f"<i>- CLEXER V7.0 -</i>")

    MAIN_TICK = 60

    while True:
        try:
            if bot_paused.is_set():
                time.sleep(MAIN_TICK); continue

            now = time.time(); forced = force_scan.is_set()
            if forced: force_scan.clear()

            h_str = now_ist().strftime('%H:%M IST'); mode_str = "NEW" if is_tv_online() else "OLD"
            print(f"\n[{h_str}] {get_session()}{' FORCED' if forced else ''} | Src:{get_current_source()} | Mode:{mode_str}")

            # TV bridge health check
            if TV_BRIDGE_URL and (now-tv_bridge_state["last_check"]) >= tv_bridge_state["check_interval"]:
                was_online = tv_bridge_state["online"]
                is_online  = tv_update_state()
                if was_online and not is_online:
                    print("  TV OFFLINE - Binance fallback")
                    # Admin DM only - not channel
                    send_admin(f"<b>TradingView Offline</b>\n\nSwitched to Binance (OLD prompt).\n\n<i>- CLEXER V7.0 -</i>")
                elif not was_online and is_online:
                    print("  TV back ONLINE")
                    send_admin(f"<b>TradingView Back Online</b>\n\nSwitched back to TradingView (NEW prompt).\n\n<i>- CLEXER V7.0 -</i>")

            # News
            if (now-last_news_check_time) >= NEWS_CHECK_INTERVAL and SEND_NEWS:
                last_news_check_time = now
                threading.Thread(target=check_news, daemon=True).start()

            # Sleep hours
            if not forced and is_ist_sleep():
                print("  [SLEEP] 01:00-07:29 IST")
                time.sleep(MAIN_TICK); continue

            # 1-min tick
            if ((now-last_tick_time) >= TICK_INTERVAL or forced) and active_trade["signal"]:
                last_tick_time = now
                if run_tick_check():
                    forced = True; last_signal_scan_time = 0

            # 1-hour price check
            if (now-last_price_check_time) >= PRICE_CHECK_INTERVAL and active_trade["signal"]:
                last_price_check_time = now
                if run_price_check():
                    forced = True; last_signal_scan_time = 0

            # Scan due?
            if not forced and (now-last_signal_scan_time) < SIGNAL_SCAN_INTERVAL:
                time.sleep(MAIN_TICK); continue

            # Session check
            if not forced and not is_trading_hours() and not active_trade["signal"]:
                print(f"  [WAIT] {get_session()} - not London/NY")
                time.sleep(MAIN_TICK); continue

            # Cooldown
            if trade_stats["cooldown_scans"] > 0 and not forced:
                trade_stats["cooldown_scans"] -= 1
                if trade_stats["cooldown_scans"] == 0:
                    send_telegram("<b>Cooldown over - scanning now</b>\n\n<i>- CLEXER V7.0 -</i>")
                last_signal_scan_time = now; time.sleep(MAIN_TICK); continue

            # -- FULL CLAUDE SCAN ----------------------------------------------
            last_signal_scan_time = now
            print("  Fetching candles...")
            ticker = get_ticker(); price = ticker["price"]
            print(f"  BTC: {price:,.2f} | {ticker['change']:+.2f}% | {get_session()} | {get_current_source()}")

            if active_trade["signal"]:
                t = active_trade
                send_telegram(
                    f"<b>4H Scan - Active Trade</b>  {ist_str()}\n\n"
                    f"{t['signal']} @ {t['entry']:,.0f}\n"
                    f"SL:{t['sl']:,.0f} | TP1:{t['tp1']:,.0f} | TP2:{t['tp2']:,.0f}\n"
                    f"Current: {price:,.2f}\n"
                    f"Entry: {'YES' if t['entry_hit'] else 'pending'} | TP1: {'YES' if t['tp1_hit'] else 'no'}\n\n"
                    f"Analyzing...\n<i>- CLEXER V7.0 -</i>")

            data = fetch_all_data()

            if not active_trade["signal"]:
                signal = analyze_with_claude(ticker, data, validate_trade=False)
                if signal and not signal.get("_hold"):
                    send_telegram(fmt_signal(signal)); set_trade(signal)
                    ct.on_signal(signal, price)
                    print(f"  [SIGNAL SENT] {signal['signal']} R:R:{signal.get('rr','?')}")
            else:
                t = active_trade
                signal = analyze_with_claude(ticker, data, validate_trade=True)
                if signal is None:
                    if forced:
                        send_telegram(f"<b>Trade Status: HOLD</b>  {ist_str()}\n\n"
                            f"{t['signal']} @ {t['entry']:,.0f}\nStructure intact.\n"
                            f"TP2: <b>{t['tp2']:,.0f}</b>\n\n<i>- CLEXER V7.0 -</i>")
                elif signal.get("_hold"):
                    send_telegram(f"<b>Trade Validated - HOLD</b>  {ist_str()}\n\n"
                        f"{t['signal']} @ {t['entry']:,.0f}\n"
                        f"SL:{t['sl']:,.0f} | TP1:{t['tp1']:,.0f} | TP2:{t['tp2']:,.0f}\n\n"
                        f"<i>{signal.get('reasoning','Structure intact')[:250]}</i>\n\n<i>- CLEXER V7.0 -</i>")
                elif signal["signal"] != t["signal"]:
                    # Only flip if entry has already been hit — never flip a pending trade
                    if not t["entry_hit"]:
                        print(f"  [FLIP BLOCKED] Entry not hit yet — holding {t['signal']} @ {t['entry']:,.0f}")
                        send_admin(f"<b>Flip Blocked</b>\n\nClaude wanted to flip {t['signal']} -> {signal['signal']} but entry not hit yet.\nHolding original trade.\n\n<i>- CLEXER V7.0 -</i>")
                    else:
                        flip_reason = signal.get("reasoning","Structure flipped")
                        log_trade_outcome("STRUCTURE_FLIP", flip_reason[:100])
                        send_telegram(f"<b>STRUCTURE FLIP!</b>  {ist_str()}\n\n"
                            f"Closing: {t['signal']} @ {t['entry']:,.0f}\n"
                            f"Why: <i>{flip_reason[:200]}</i>\n\n"
                            f"New: <b>{signal['signal']} @ {signal['entry']:,.0f}</b>\n\n<i>- CLEXER V7.0 -</i>")
                        ct.on_close_all()
                        reset_trade(); time.sleep(1); send_telegram(fmt_signal(signal)); set_trade(signal)
                        ct.on_signal(signal, price)
                else:
                    if forced:
                        send_telegram(f"<b>Trade Update</b>  {ist_str()}\n\n"
                            f"Old: {t['signal']} @ {t['entry']:,.0f}\n"
                            f"New: {signal['signal']} @ {signal['entry']:,.0f}\n"
                            f"Bias confirmed.\n\n<i>- CLEXER V7.0 -</i>")
                    log_trade_outcome("REPLACED","same direction, updated levels")
                    ct.on_close_all()
                    reset_trade(); time.sleep(1); send_telegram(fmt_signal(signal)); set_trade(signal)
                    ct.on_signal(signal, price)

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("<b>CLEXER V7.0 Stopped</b>\n\n<i>- CLEXER -</i>"); break
        except Exception as e:
            print(f"  [MAIN ERROR] {e}"); import traceback; traceback.print_exc()

        time.sleep(MAIN_TICK)

if __name__ == "__main__":
    main()
