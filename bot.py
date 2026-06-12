"""
CLEXER Signal Bot V6.1
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DUAL PROMPT SYSTEM:
  TV ONLINE  → New 10-step SMC framework (volume + pullback confirmation)
  TV OFFLINE → Original SMC prompt (OB/FVG/BOS based)

SCAN TIERS:
  Every 1 min  → Tick: entry/SL/TP detection
  Every 1 hour → Price check: TV first, Binance fallback
  Every 4 hours→ Full Claude scan with trade assessment
  News         → OFF by default, enable with /news on

FRIEND ACCESS:
  Commands: /status /price /trade /history /stats /session /help
  Rate limit: 2 uses per command per hour
  Shows reset time when limit hit
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

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY",   "")
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN",  "")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")
ADMIN_CHAT_ID       = os.getenv("ADMIN_CHAT_ID",       "")
TV_BRIDGE_URL       = os.getenv("TV_BRIDGE_URL",       "").rstrip("/")

SYMBOL               = "BTCUSDT"
TICK_INTERVAL        = 60
PRICE_CHECK_INTERVAL = 3600
SIGNAL_SCAN_INTERVAL = 14400
NEWS_CHECK_INTERVAL  = 1800
BINANCE_BASE         = "https://api1.binance.com/api/v3"
IST                  = timedelta(hours=5, minutes=30)

SEND_CHARTS   = True
CHART_TFS     = ["weekly", "4h", "1h", "5m"]
SEND_NEWS     = False   # OFF by default — enable with /news on
MAX_NEWS_AGE  = 4
MAX_NEWS_PER_RUN = 3

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

# ─── TV BRIDGE STATE ──────────────────────────────────────────────────────────
tv_bridge_state = {
    "online": False, "cdp_ok": False,
    "last_seen": 0, "last_check": 0,
    "fail_count": 0, "source": "BINANCE",
    "tv_version": "", "tv_symbol": "",
    "cached_intervals": [], "check_interval": 60,
}

# ─── TIME HELPERS ─────────────────────────────────────────────────────────────
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

# ─── BOT STATE ────────────────────────────────────────────────────────────────
active_trade = {
    "signal": None, "entry": None, "sl": None,
    "tp1": None, "tp2": None, "tp1_hit": False,
    "entry_type": "MARKET", "entry_note": "",
    "entry_hit": False, "sl_wicked": False, "scan_count": 0,
    "open_time": None, "close_reason": None,
}
signal_history = []
force_scan     = threading.Event()
bot_paused     = threading.Event()
last_update_id = 0
last_force_scan_time  = 0
last_signal_scan_time = 0
last_price_check_time = 0
last_tick_time        = 0
last_news_check_time  = 0
posted_news_guids: set = set()
latest_news_context: list = []
trade_lock = threading.Lock()

trade_stats = {
    "consecutive_sl": 0, "cooldown_scans": 0,
    "total_sl": 0, "total_tp1": 0, "total_tp2": 0,
    "total_signals": 0, "missed_entries": 0, "stop_hunts": 0,
}

# ─── USER REGISTRY ────────────────────────────────────────────────────────────
USER_DB_FILE = "users.json"
registered_users: set = set()

def load_users():
    global registered_users
    try:
        if os.path.exists(USER_DB_FILE):
            with open(USER_DB_FILE, "r") as f:
                registered_users = set(json.load(f))
    except: registered_users = set()

def save_users():
    try:
        with open(USER_DB_FILE, "w") as f:
            json.dump(list(registered_users), f)
    except: pass

def register_user(chat_id):
    if chat_id not in registered_users:
        registered_users.add(chat_id)
        save_users()

# ─── FRIEND RATE LIMITER ──────────────────────────────────────────────────────
# {chat_id: {cmd: [timestamp, timestamp, ...]}}
friend_usage: dict = defaultdict(lambda: defaultdict(list))
FRIEND_LIMIT = 2       # max uses per command per hour
FRIEND_WINDOW = 3600   # 1 hour in seconds

def check_friend_rate(chat_id, cmd) -> tuple[bool, str]:
    """
    Returns (allowed, message).
    allowed=True  → proceed
    allowed=False → show reset time message
    """
    if str(chat_id) == str(ADMIN_CHAT_ID):
        return True, ""

    now = time.time()
    window_start = now - FRIEND_WINDOW
    # Clean old entries
    friend_usage[chat_id][cmd] = [
        t for t in friend_usage[chat_id][cmd] if t > window_start
    ]
    uses = friend_usage[chat_id][cmd]

    if len(uses) >= FRIEND_LIMIT:
        # Find when oldest use expires
        oldest = min(uses)
        reset_ts = oldest + FRIEND_WINDOW
        reset_dt = datetime.fromtimestamp(reset_ts, tz=timezone.utc) + IST
        reset_str = reset_dt.strftime("%I:%M %p IST")
        return False, f"⏳ Limit reached. Your access resets at <b>{reset_str}</b>"

    friend_usage[chat_id][cmd].append(now)
    return True, ""

# ─── BROADCAST ────────────────────────────────────────────────────────────────
broadcast_pending: dict = {}

# ─── TRADE HELPERS ────────────────────────────────────────────────────────────
def reset_trade(reason: str = ""):
    global active_trade
    with trade_lock:
        if reason and active_trade.get("signal"):
            active_trade["close_reason"] = reason
        active_trade = {
            "signal": None, "entry": None, "sl": None,
            "tp1": None, "tp2": None, "tp1_hit": False,
            "entry_type": "MARKET", "entry_note": "",
            "entry_hit": False, "sl_wicked": False, "scan_count": 0,
            "open_time": None, "close_reason": reason,
        }

def set_trade(s: dict):
    global active_trade
    with trade_lock:
        active_trade = {
            "signal":     s["signal"],
            "entry":      s["entry"],
            "sl":         s["sl"],
            "tp1":        s["tp1"],
            "tp2":        s["tp2"],
            "tp1_hit":    False,
            "entry_type": s.get("entry_type", "MARKET"),
            "entry_note": s.get("entry_note", ""),
            "entry_hit":  s.get("entry_type", "MARKET") == "MARKET",
            "sl_wicked":  False,
            "scan_count": 0,
            "open_time":  ist_str(),
            "close_reason": None,
        }
    trade_stats["total_signals"] += 1
    signal_history.append({
        "time": ist_str(), "signal": s["signal"],
        "entry": s["entry"], "sl": s["sl"],
        "tp1": s["tp1"], "tp2": s["tp2"],
        "rr": s.get("rr","?"), "confidence": s.get("confidence","?"),
        "source": s.get("data_source","?"),
    })
    if len(signal_history) > 10: signal_history.pop(0)

def required_confidence() -> str:
    n = trade_stats["consecutive_sl"]
    if n >= 2: return "HIGH"
    if n >= 1: return "MEDIUM"
    return "LOW"

# ─── TV BRIDGE ────────────────────────────────────────────────────────────────
def tv_ping() -> dict | None:
    if not TV_BRIDGE_URL: return None
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/health", timeout=5)
        if r.status_code == 200: return r.json()
    except: pass
    return None

def tv_update_state():
    result = tv_ping()
    now = time.time()
    tv_bridge_state["last_check"] = now
    if result:
        tv_bridge_state.update({
            "online": True, "last_seen": now, "fail_count": 0,
            "source": "TRADINGVIEW",
            "tv_version": result.get("tv_version",""),
            "tv_symbol": result.get("symbol",""),
            "cdp_ok": result.get("cdp_connected", False),
            "cached_intervals": result.get("cached_intervals",[]),
        })
        return True
    else:
        tv_bridge_state["fail_count"] += 1
        if tv_bridge_state["fail_count"] >= 2:
            tv_bridge_state["online"] = False
            tv_bridge_state["source"] = "BINANCE"
        return False

def tv_get_candles(interval: str, limit: int):
    if not TV_BRIDGE_URL or not tv_bridge_state["online"]: return None
    tv_map = {"weekly":"W","4h":"4H","1h":"1H","5m":"5"}
    tv_interval = tv_map.get(interval, interval)
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/candles",
            params={"symbol":SYMBOL,"interval":tv_interval,"limit":limit}, timeout=15)
        if r.status_code != 200: return None
        data = r.json()
        if not data.get("candles"): return None
        rows = [{
            "time": datetime.fromtimestamp(c["t"]/1000 if c["t"]>1e10 else c["t"], tz=timezone.utc),
            "open": float(c["o"]), "high": float(c["h"]),
            "low":  float(c["l"]), "close": float(c["c"]),
            "vol":  float(c.get("v",0)),
        } for c in data["candles"]]
        df = pd.DataFrame(rows).set_index("time")
        print(f"      [TV] {interval}: {len(df)} candles")
        return df
    except Exception as e:
        print(f"      [TV] {interval} error: {e}")
        return None

def tv_get_ticker() -> dict | None:
    if not TV_BRIDGE_URL or not tv_bridge_state["online"]: return None
    try:
        r = requests.get(f"{TV_BRIDGE_URL}/ticker", params={"symbol":SYMBOL}, timeout=8)
        if r.status_code == 200:
            d = r.json()
            price = float(d.get("price",0))
            if price > 0:
                return {
                    "price": price, "change": float(d.get("change_pct",0)),
                    "volume": float(d.get("volume",0)),
                    "high24": float(d.get("high24",price)),
                    "low24":  float(d.get("low24",price)),
                    "source": "TRADINGVIEW",
                }
    except: pass
    return None

def is_tv_online() -> bool:
    return bool(TV_BRIDGE_URL and tv_bridge_state["online"] and tv_bridge_state["cdp_ok"])

# ─── BINANCE ──────────────────────────────────────────────────────────────────
def binance_get_candles(interval: str, limit: int):
    iv_map = {"weekly":"1w","4h":"4h","1h":"1h","5m":"5m"}
    iv = iv_map.get(interval, interval)
    r = requests.get(f"{BINANCE_BASE}/klines",
        params={"symbol":SYMBOL,"interval":iv,"limit":limit}, timeout=15)
    r.raise_for_status()
    rows = [{
        "time":  datetime.fromtimestamp(c[0]/1000, tz=timezone.utc),
        "open":  float(c[1]), "high": float(c[2]),
        "low":   float(c[3]), "close": float(c[4]), "vol": float(c[5]),
    } for c in r.json()]
    df = pd.DataFrame(rows).set_index("time")
    print(f"      [BINANCE] {interval}: {len(df)} candles")
    return df

def binance_get_ticker() -> dict:
    r = requests.get(f"{BINANCE_BASE}/ticker/24hr",
        params={"symbol":SYMBOL}, timeout=10)
    r.raise_for_status()
    d = r.json()
    return {
        "price":  float(d["lastPrice"]),
        "change": float(d["priceChangePercent"]),
        "volume": float(d["quoteVolume"]),
        "high24": float(d["highPrice"]),
        "low24":  float(d["lowPrice"]),
        "source": "BINANCE",
    }

# ─── UNIFIED DATA ─────────────────────────────────────────────────────────────
def get_candles(interval: str, limit: int):
    if TV_BRIDGE_URL:
        tv_update_state()
        if tv_bridge_state["online"]:
            df = tv_get_candles(interval, limit)
            if df is not None and len(df) >= 2:
                return df
            print(f"      [TV→BINANCE] fallback for {interval}")
    return binance_get_candles(interval, limit)

def get_ticker() -> dict:
    if TV_BRIDGE_URL:
        tv_update_state()
        if tv_bridge_state["online"]:
            tk = tv_get_ticker()
            if tk: return tk
    return binance_get_ticker()

def get_price_range_since(minutes: int) -> dict:
    since_ms = int((time.time() - minutes*60)*1000)
    now_ms   = int(time.time()*1000)
    all_highs, all_lows = [], []
    chunk_ms = 5*60*1000; chunk_start = since_ms
    while chunk_start < now_ms:
        chunk_end = min(chunk_start+chunk_ms, now_ms)
        try:
            r = requests.get(f"{BINANCE_BASE}/aggTrades",
                params={"symbol":SYMBOL,"startTime":chunk_start,"endTime":chunk_end,"limit":1000},
                timeout=10)
            r.raise_for_status()
            trades = r.json()
            if trades:
                prices = [float(t["p"]) for t in trades]
                all_highs.append(max(prices)); all_lows.append(min(prices))
        except Exception as e:
            print(f"  [aggTrades] {e}")
        chunk_start = chunk_end+1; time.sleep(0.05)
    if not all_highs: return {"high":None,"low":None}
    return {"high":max(all_highs),"low":min(all_lows)}

def get_current_source() -> str:
    if is_tv_online(): return "TradingView"
    return "Binance"

def fetch_all_data() -> dict:
    data = {}
    specs = [("weekly",52,5),("4h",200,5),("1h",100,5),("5m",50,3)]
    for key, lim, lb in specs:
        df = get_candles(key, lim)
        data[key] = (df, lb)
        time.sleep(0.3)
        print(f"    {key}: {len(df)} candles  [{get_current_source()}]")
    return data

# ─── SMC CALCULATIONS ─────────────────────────────────────────────────────────
def find_swing_points(df, lookback=5):
    highs, lows = [], []
    for i in range(lookback, len(df)-lookback):
        if df["high"].iloc[i] == df["high"].iloc[i-lookback:i+lookback+1].max():
            highs.append({"idx":i,"price":df["high"].iloc[i],"time":df.index[i]})
        if df["low"].iloc[i] == df["low"].iloc[i-lookback:i+lookback+1].min():
            lows.append({"idx":i,"price":df["low"].iloc[i],"time":df.index[i]})
    return highs, lows

def detect_trend(df):
    h, l = find_swing_points(df, 3)
    if len(h)<2 or len(l)<2: return "NEUTRAL"
    hp=[x["price"] for x in h[-2:]]; lp=[x["price"] for x in l[-2:]]
    if hp[1]>hp[0] and lp[1]>lp[0]: return "BULLISH"
    if hp[1]<hp[0] and lp[1]<lp[0]: return "BEARISH"
    return "NEUTRAL"

def detect_bos_choch(df):
    events=[]; h,l=find_swing_points(df,3)
    if len(h)<2 or len(l)<2: return events
    for i in range(1,min(4,len(h))):
        idx=h[-i]["idx"]
        if idx<len(df)-1 and df["close"].iloc[idx+1]>h[-i-1]["price"]:
            events.append({"type":"BOS_BULL","price":h[-i-1]["price"],"idx":idx}); break
    for i in range(1,min(4,len(l))):
        idx=l[-i]["idx"]
        if idx<len(df)-1 and df["close"].iloc[idx+1]<l[-i-1]["price"]:
            events.append({"type":"BOS_BEAR","price":l[-i-1]["price"],"idx":idx}); break
    return events

def detect_order_blocks(df, n=5):
    obs=[]; c,o,h,l=df["close"].values,df["open"].values,df["high"].values,df["low"].values
    for i in range(3,len(df)-3):
        sz=h[i]-l[i]
        if sz==0: continue
        if c[i]<o[i] and max(c[i+1:i+4])-h[i]>sz*0.5:
            obs.append({"type":"BULL_OB","top":h[i],"bottom":l[i],"mid":(h[i]+l[i])/2,"idx":i})
        if c[i]>o[i] and l[i]-min(c[i+1:i+4])>sz*0.5:
            obs.append({"type":"BEAR_OB","top":h[i],"bottom":l[i],"mid":(h[i]+l[i])/2,"idx":i})
    return obs[-n:] if len(obs)>n else obs

def detect_fvgs(df, n=5):
    fvgs=[]
    for i in range(2,len(df)):
        if df["low"].iloc[i]>df["high"].iloc[i-2]:
            fvgs.append({"type":"BULL_FVG","top":df["low"].iloc[i],"bottom":df["high"].iloc[i-2],"idx":i})
        if df["high"].iloc[i]<df["low"].iloc[i-2]:
            fvgs.append({"type":"BEAR_FVG","top":df["low"].iloc[i-2],"bottom":df["high"].iloc[i],"idx":i})
    return fvgs[-n:] if len(fvgs)>n else fvgs

def calc_atr(df, period=14) -> float:
    try:
        high=df["high"]; low=df["low"]; close=df["close"]
        tr=pd.concat([high-low,(high-close.shift()).abs(),(low-close.shift()).abs()],axis=1).max(axis=1)
        return tr.rolling(period).mean().iloc[-1]
    except: return 0

# ─── CHART DRAWING ────────────────────────────────────────────────────────────
def draw_smc_chart(df, tf, obs, fvgs, bos_events, s_highs, s_lows_list, price=None) -> str:
    fig,(ax1,ax2)=plt.subplots(2,1,figsize=(18,10),gridspec_kw={"height_ratios":[4,1]},facecolor="#0d0d0d")
    ax1.set_facecolor("#0d0d0d"); ax2.set_facecolor("#0d0d0d")
    n=len(df)
    for i,(_,row) in enumerate(df.iterrows()):
        o,h,l,c=row["open"],row["high"],row["low"],row["close"]
        col="#26a69a" if c>=o else "#ef5350"
        ax1.plot([i,i],[l,h],color=col,linewidth=0.8,zorder=2)
        ax1.add_patch(plt.Rectangle((i-0.4,min(o,c)),0.8,abs(c-o),color=col,zorder=3))
    for ob in obs:
        idx=ob["idx"]
        if idx>=n: continue
        col="#1a6b3c" if ob["type"]=="BULL_OB" else "#6b1a1a"
        bc="#00e676" if ob["type"]=="BULL_OB" else "#ff5252"
        ax1.add_patch(plt.Rectangle((idx-0.5,ob["bottom"]),n-idx+2,ob["top"]-ob["bottom"],color=col,alpha=0.3,zorder=1))
        ax1.text(idx+0.5,ob["top"],"Bull OB" if ob["type"]=="BULL_OB" else "Bear OB",color=bc,fontsize=6.5,va="bottom",zorder=5)
    for fvg in fvgs:
        idx=fvg["idx"]
        if idx>=n: continue
        col="#1a3d6b" if fvg["type"]=="BULL_FVG" else "#6b4a1a"
        bc="#40c4ff" if fvg["type"]=="BULL_FVG" else "#ffab40"
        ax1.add_patch(plt.Rectangle((idx-2,fvg["bottom"]),n-idx+3,fvg["top"]-fvg["bottom"],color=col,alpha=0.35,zorder=1))
        ax1.text(idx+0.5,fvg["top"],"Bull FVG" if fvg["type"]=="BULL_FVG" else "Bear FVG",color=bc,fontsize=6.5,va="bottom",zorder=5)
    for sh in s_highs[-6:]:
        if sh["idx"]<n:
            ax1.plot(sh["idx"],sh["price"],"^",color="#ffeb3b",markersize=5,zorder=6)
            ax1.axhline(sh["price"],color="#ffeb3b",linewidth=0.5,linestyle=":",alpha=0.4)
    for sl_pt in s_lows_list[-6:]:
        if sl_pt["idx"]<n:
            ax1.plot(sl_pt["idx"],sl_pt["price"],"v",color="#ff9800",markersize=5,zorder=6)
            ax1.axhline(sl_pt["price"],color="#ff9800",linewidth=0.5,linestyle=":",alpha=0.4)
    for ev in bos_events:
        if ev["idx"]<n:
            col="#b2ff59" if "BULL" in ev["type"] else "#ff4081"
            ax1.axhline(ev["price"],color=col,linewidth=1.0,linestyle="-.",alpha=0.7)
            ax1.text(max(0,ev["idx"]-2),ev["price"],"BOS",color=col,fontsize=7,va="bottom",fontweight="bold")
    if price:
        ax1.axhline(price,color="#fff",linewidth=1.2,linestyle="--",alpha=0.9)
        ax1.text(n-1,price,f" {price:,.0f}",color="#fff",fontsize=8,va="center")
    for i,(_,row) in enumerate(df.iterrows()):
        ax2.bar(i,row["vol"],color="#26a69a" if row["close"]>=row["open"] else "#ef5350",alpha=0.7,width=0.8)
    step=max(1,n//10)
    ax2.set_xticks(np.arange(n)[::step])
    ax2.set_xticklabels([df.index[i].strftime("%m/%d %H:%M") for i in range(0,n,step)],rotation=30,fontsize=6,color="#aaa")
    ax1.set_xticks([]); ax1.set_xlim(-1,n+3); ax2.set_xlim(-1,n+3)
    for ax in (ax1,ax2):
        ax.tick_params(colors="#aaa",labelsize=7)
        for s in ax.spines.values(): s.set_color("#333")
    ax1.yaxis.tick_right()
    trend=detect_trend(df)
    col="#26a69a" if trend=="BULLISH" else ("#ef5350" if trend=="BEARISH" else "#fff")
    src_label=get_current_source()
    ax1.set_title(f"{SYMBOL} {tf}  |  {trend}  |  {src_label}",color=col,fontsize=11,fontweight="bold",loc="left",pad=6)
    ax1.legend(handles=[
        mpatches.Patch(color="#1a6b3c",label="Bull OB"),
        mpatches.Patch(color="#6b1a1a",label="Bear OB"),
        mpatches.Patch(color="#1a3d6b",label="Bull FVG"),
        mpatches.Patch(color="#6b4a1a",label="Bear FVG"),
    ],loc="upper left",facecolor="#1a1a1a",edgecolor="#444",labelcolor="#ccc",fontsize=7,framealpha=0.8)
    plt.tight_layout(pad=0.5)
    buf=BytesIO()
    plt.savefig(buf,format="png",dpi=120,bbox_inches="tight",facecolor="#0d0d0d")
    plt.close(fig); buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def _build_chart(df,lb,tf_key,price) -> str:
    obs=detect_order_blocks(df,6); fvgs=detect_fvgs(df,6)
    bos=detect_bos_choch(df); sh,sl_pts=find_swing_points(df,lb)
    return draw_smc_chart(df,tf_key.upper(),obs,fvgs,bos,sh[-8:],sl_pts[-8:],price)

def generate_all_charts(data:dict,price:float) -> dict:
    charts={}
    for tf_key,(df,lb) in data.items():
        charts[tf_key]=_build_chart(df,lb,tf_key,price)
    return charts

def send_charts_to_channel(charts:dict,label="SMC Analysis"):
    if not SEND_CHARTS: return
    for tf_key in CHART_TFS:
        b64=charts.get(tf_key)
        if not b64: continue
        try:
            img_bytes=base64.b64decode(b64)
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id":TELEGRAM_CHANNEL_ID,"caption":f"{label} — {tf_key.upper()} [{get_current_source()}]"},
                files={"photo":(f"chart_{tf_key}.png",img_bytes,"image/png")},
                timeout=20,
            )
            time.sleep(0.4)
        except Exception as e:
            print(f"  [CHART] {tf_key}: {e}")

def build_smc_summary(data:dict,ticker:dict) -> str:
    src=ticker.get("source","UNKNOWN")
    lines=[
        f"=== BTCUSDT LIVE ===",
        f"Price: {ticker['price']:,.2f} | 24h: {ticker['change']:+.2f}%",
        f"Vol: ${ticker['volume']/1e6:.1f}M | Session: {get_session()} | {ist_str()}",
        f"Data Source: {src}","",
    ]
    for tf_key,(df,lb) in data.items():
        trend=detect_trend(df); obs=detect_order_blocks(df,4); fvgs=detect_fvgs(df,4)
        bos=detect_bos_choch(df); sh,sl_pts=find_swing_points(df,lb)
        atr=calc_atr(df)
        lines.append(f"--- {tf_key.upper()} | {trend} | ATR:{atr:.0f} ---")
        for b in bos[-2:]: lines.append(f"  {b['type']}: {b['price']:,.2f}")
        bull_ob=[o for o in obs if o["type"]=="BULL_OB"]
        bear_ob=[o for o in obs if o["type"]=="BEAR_OB"]
        if bull_ob: lines.append(f"  Bull OB: {bull_ob[-1]['bottom']:,.2f}-{bull_ob[-1]['top']:,.2f}")
        if bear_ob: lines.append(f"  Bear OB: {bear_ob[-1]['bottom']:,.2f}-{bear_ob[-1]['top']:,.2f}")
        bf=[f for f in fvgs if f["type"]=="BULL_FVG"]
        brf=[f for f in fvgs if f["type"]=="BEAR_FVG"]
        if bf:  lines.append(f"  Bull FVG: {bf[-1]['bottom']:,.2f}-{bf[-1]['top']:,.2f}")
        if brf: lines.append(f"  Bear FVG: {brf[-1]['bottom']:,.2f}-{brf[-1]['top']:,.2f}")
        if sh:      lines.append(f"  Swing High: {sh[-1]['price']:,.2f}")
        if sl_pts:  lines.append(f"  Swing Low:  {sl_pts[-1]['price']:,.2f}")
        # Volume context
        try:
            last_vol = df["vol"].iloc[-1]
            avg_vol  = df["vol"].iloc[-20:].mean()
            vol_note = "HIGH" if last_vol > avg_vol*1.5 else ("LOW" if last_vol < avg_vol*0.5 else "NORMAL")
            lines.append(f"  Volume: {vol_note} (last:{last_vol:.0f} avg:{avg_vol:.0f})")
        except: pass
        lines.append("")
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════════════
#  DUAL PROMPT SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

NEW_PROMPT_FRAMEWORK = """
You are CLEXER — an elite BTC SMC trader. Follow this EXACT 10-step framework:

STEP 1 — CHECK CURRENT PRICE
- What is current price and volume?
- Is price rising or falling right now?

STEP 2 — CHECK WEEKLY TF
- What is the overall trend?
- Weekly high and low?
- Did price make new HH or LL this week?
- Weekly candle bullish or bearish?

STEP 3 — CHECK 4H TF (PRIMARY BIAS)
- Structure: bullish (HH+HL) or bearish (LH+LL)?
- Where is the last 4H Order Block?
- Volume on last big candle?
- Has CHoCH happened?

STEP 4 — CHECK 1H TF
- Confirms 4H bias?
- Nearest support/resistance?
- Any liquidity above or below?

STEP 5 — CHECK 5M TF
- What is price doing right now?
- Higher lows = bullish momentum
- Lower highs = bearish momentum
- Volume confirmation?

STEP 6 — DECIDE DIRECTION
LONG when ALL true:
  4H making HH and HL
  Price above 4H bullish OB
  Weekly support held
  5M higher lows forming
  Big green volume candle recently

SHORT when ALL true:
  4H making LH and LL
  Price below 4H bearish OB
  Weekly resistance holding
  5M lower highs forming
  Big red volume candle recently

NO TRADE when:
  Price in middle of range
  Low volume consolidation
  4H and 5M conflict

STEP 7 — FIND ENTRY (PULLBACK PREFERRED)
For LONG: Wait for price to pull back to nearest 4H HL or bullish OB
  Enter when 5M candle closes GREEN above that level WITH volume
For SHORT: Wait for price to bounce to nearest 4H LH or bearish OB
  Enter when 5M candle closes RED below that level WITH volume

NEVER enter:
  At top/bottom of big candle
  Without volume confirmation
  Against 4H trend direction

STEP 8 — SET SL
For LONG: SL = below last 4H HL, minimum 500 pts below entry, never less than 1x ATR
For SHORT: SL = above last 4H LH, minimum 500 pts above entry, never less than 1x ATR
NEVER: less than 300 pts, at round numbers, at obvious stop hunt zones
SL MAXIMUM: 3000 pts

STEP 9 — SET TP
TP1 = next key level (min 1:2 R:R)
TP2 = major level beyond (min 1:4 R:R)
For LONG: TP1 = nearest 4H resistance, TP2 = weekly high or major OB above
For SHORT: TP1 = nearest 4H support, TP2 = weekly low or major demand below

STEP 10 — VALIDATE
If giving WAIT: explain which specific condition failed
If giving signal: confirm all 6 conditions for that direction are met
"""

OLD_PROMPT_FRAMEWORK = """
You are CLEXER — elite BTC SMC trader. Analyse all 4 charts (Weekly, 4H, 1H, 5M).

STEP 1 — WEEKLY BIAS
  HH+HL = bullish | LH+LL = bearish | flat = neutral

STEP 2 — 4H PRIMARY BIAS (most important)
  BOS direction, CHoCH if any.
  Find last 4H Order Block.
  4H structure is PRIMARY bias. 1H is confirmation not veto.

STEP 3 — 1H CONFIRMATION
  Align with 4H. Minor lag = OK. Active opposition = BLOCK.

STEP 4 — 5M TIMING
  Higher lows = bullish. Lower highs = bearish. Context only.

WAIT CONDITIONS (strict):
  HARD BLOCK A: ALL THREE timeframes conflict simultaneously
  HARD BLOCK B: No OB or FVG within 1000 pts of price
  HARD BLOCK C: 4H completely flat/neutral

SL: minimum 500 pts, maximum 3000 pts
TP1 = entry ± (sl_dist × 2)
TP2 = entry ± (sl_dist × 4)
"""

def analyze_with_claude(ticker:dict, data:dict, validate_trade:bool=False, trade_assessment:str="") -> dict | None:
    price   = ticker["price"]
    session = get_session()
    min_conf= required_confidence()
    src     = get_current_source()
    tv_live = is_tv_online()

    print(f"  [CLAUDE] {'NEW PROMPT (TV)' if tv_live else 'OLD PROMPT (Binance)'} | MinConf:{min_conf} | Session:{session}")

    all_charts     = generate_all_charts(data, price)
    channel_charts = {k:v for k,v in all_charts.items() if k in CHART_TFS}
    send_charts_to_channel(channel_charts, "SMC Analysis")

    summary = build_smc_summary(data, ticker)

    news_ctx = ""
    if latest_news_context:
        news_ctx = "\n\nRECENT MARKET NEWS:\n" + "\n".join(latest_news_context[-3:])

    # Trade assessment context
    trade_ctx = ""
    if trade_assessment:
        trade_ctx = f"\n\nPREVIOUS TRADE ASSESSMENT:\n{trade_assessment}\n"

    validation_ctx = ""
    if validate_trade and active_trade["signal"]:
        t = active_trade
        trade_ctx += (
            f"\n\nACTIVE TRADE TO VALIDATE:\n"
            f"  {t['signal']} entry:{t['entry']:,.0f}  SL:{t['sl']:,.0f}  "
            f"TP1:{t['tp1']:,.0f}  TP2:{t['tp2']:,.0f}\n"
            f"  TP1 hit:{t['tp1_hit']}  Entry hit:{t['entry_hit']}\n"
            f"  Opened: {t.get('open_time','?')}\n\n"
            f"FIRST evaluate if this trade is still valid.\n"
            f"If still valid → return signal:HOLD\n"
            f"If structure flipped → return new signal in opposite direction\n"
            f"Explain WHY in reasoning field\n"
        )

    conf_note = ""
    if min_conf == "HIGH":   conf_note = "\n⚠️ 2+ consecutive SLs: HIGH confidence only."
    elif min_conf == "MEDIUM": conf_note = "\n⚠️ 1 recent SL: MEDIUM or HIGH only."

    session_note = ""
    if session == "NEW_YORK":
        session_note = "\n\nNY SESSION: 4H bias is primary. Minor 1H noise acceptable."
    elif session == "LONDON":
        session_note = "\n\nLONDON SESSION: Strong breakout session. 4H+1H alignment ideal."

    # Choose framework
    framework = NEW_PROMPT_FRAMEWORK if tv_live else OLD_PROMPT_FRAMEWORK

    pullback_rule = f"""
PULLBACK RULE (current price = {price:,.0f}):
  BUY pullback  → entry MUST be < {price:,.0f}, TP1 & TP2 MUST be > {price:,.0f}
  SELL pullback → entry MUST be > {price:,.0f}, TP1 & TP2 MUST be < {price:,.0f}
"""

    prompt = f"""{summary}{trade_ctx}{news_ctx}

Data Source: {src} | TV Online: {tv_live}
Session: {session}

{framework}
{pullback_rule}
{conf_note}{session_note}

Return ONLY valid JSON, no markdown:

WAIT:  {{"signal":"WAIT","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"","structure_4h":"","entry_zone":"","confidence":"LOW","session":"{session}","reasoning":"exact condition that failed","trade_valid":null,"old_trade_assessment":""}}

HOLD:  {{"signal":"HOLD","entry":0,"sl":0,"tp1":0,"tp2":0,"rr":"none","entry_type":"PULLBACK","entry_note":"","bias":"NEUTRAL","weekly_trend":"","structure_4h":"","entry_zone":"","confidence":"LOW","session":"{session}","reasoning":"why trade still valid","trade_valid":true,"old_trade_assessment":""}}

Trade: {{"signal":"BUY" or "SELL","entry":<price>,"sl":<price>,"tp1":<price>,"tp2":<price>,"rr":"1:X.X","entry_type":"MARKET" or "PULLBACK","entry_note":"","bias":"BULLISH" or "BEARISH","weekly_trend":"","structure_4h":"","entry_zone":"","confidence":"HIGH" or "MEDIUM" or "LOW","session":"{session}","reasoning":"all conditions met","trade_valid":null,"old_trade_assessment":"why previous trade closed/changed if applicable"}}"""

    try:
        msg = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
            model="claude-opus-4-6",
            max_tokens=1200,
            messages=[{"role":"user","content":[
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":all_charts["weekly"]}},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":all_charts["4h"]}},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":all_charts["1h"]}},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":all_charts["5m"]}},
                {"type":"text","text":prompt},
            ]}]
        )
        raw    = msg.content[0].text.strip().replace("```json","").replace("```","").strip()
        signal = json.loads(raw)
        sig_type = signal.get("signal","")

        if sig_type == "HOLD":
            print(f"  [HOLD] {signal.get('reasoning','')[:80]}")
            return None

        if sig_type == "WAIT":
            reason = signal.get("reasoning","no reason")
            bias   = signal.get("bias","?")
            old_assessment = signal.get("old_trade_assessment","")
            print(f"  [WAIT] {reason[:120]}")
            wait_msg = (
                f"🔍 <b>Scan Complete — No Signal</b>\n\n"
                f"Price: <b>{price:,.2f}</b> ({ticker['change']:+.2f}%)\n"
                f"Session: {session} | Bias: {bias}\n"
                f"Source: {src}\n"
                f"Framework: {'New 10-Step' if tv_live else 'Classic SMC'}\n\n"
                f"💭 <i>{reason[:200]}</i>\n"
            )
            if old_assessment:
                wait_msg += f"\n📋 <i>Previous trade: {old_assessment[:150]}</i>\n"
            wait_msg += f"\n🕐 {ist_str()}\nNext scan in {SIGNAL_SCAN_INTERVAL//3600}h. /signal to force.\n\n<i>— CLEXER V6.1 —</i>"
            send_telegram(wait_msg)
            return None

        if sig_type not in ("BUY","SELL"):
            print(f"  [REJECT] Unknown: {sig_type}")
            return None

        entry  = float(signal["entry"])
        sl_raw = float(signal["sl"])
        sl_dist= abs(entry-sl_raw)

        if sl_dist < 500:
            fix_dist = 650
            signal["sl"]  = round(entry-fix_dist if sig_type=="BUY" else entry+fix_dist, -1)
            sl_raw  = float(signal["sl"])
            sl_dist = abs(entry-sl_raw)
            signal["tp1"] = round(entry+sl_dist*2 if sig_type=="BUY" else entry-sl_dist*2, -1)
            signal["tp2"] = round(entry+sl_dist*4 if sig_type=="BUY" else entry-sl_dist*4, -1)

        if sl_dist > 3000:
            print(f"  [REJECT] SL too wide: {sl_dist:.0f}")
            return None

        etype = signal.get("entry_type","MARKET")
        if etype == "PULLBACK":
            if sig_type=="BUY" and float(signal["tp1"])<=price:
                signal["tp1"]=round(price+sl_dist*2,-1)
                signal["tp2"]=round(price+sl_dist*4,-1)
            elif sig_type=="SELL" and float(signal["tp1"])>=price:
                signal["tp1"]=round(price-sl_dist*2,-1)
                signal["tp2"]=round(price-sl_dist*4,-1)

        conf = signal.get("confidence","LOW")
        rank = {"HIGH":3,"MEDIUM":2,"LOW":1}
        if rank.get(conf,1) < rank.get(min_conf,1):
            print(f"  [SKIP] Confidence {conf} < required {min_conf}")
            send_telegram(
                f"⚠️ <b>Signal filtered — low confidence</b>\n\n"
                f"{'BUY' if sig_type=='BUY' else 'SELL'} @ {entry:,.0f}\n"
                f"Confidence: {conf} (required: {min_conf})\n"
                f"Source: {src}\n\n"
                f"<i>{signal.get('reasoning','')[:160]}</i>\n\n"
                f"<i>Use /resetsl to lower the bar.\n— CLEXER V6.1 —</i>"
            )
            return None

        tp2_dist     = abs(entry-float(signal["tp2"]))
        signal["rr"] = f"1:{tp2_dist/sl_dist:.1f}" if sl_dist else "1:?"
        signal["data_source"] = src
        signal["prompt_used"] = "NEW_10STEP" if tv_live else "OLD_CLASSIC"

        print(f"  [OK] {sig_type} entry:{entry:,.0f} SL:{sl_raw:,.0f} R:R:{signal['rr']} Conf:{conf}")
        return signal

    except Exception as e:
        print(f"  [CLAUDE ERROR] {e}")
        return None

# ─── TELEGRAM SEND ────────────────────────────────────────────────────────────
def send_telegram(text:str) -> bool:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id":TELEGRAM_CHANNEL_ID,"text":text,"parse_mode":"HTML","disable_web_page_preview":True},
            timeout=10,
        )
        r.raise_for_status(); return True
    except Exception as e:
        print(f"  [TG ERROR] {e}"); return False

def send_reply(chat_id, text:str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id":chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":True},
            timeout=10,
        )
    except Exception as e:
        print(f"  [REPLY ERROR] {e}")

def send_to_user(chat_id,text:str,file_id=None,file_type=None) -> bool:
    try:
        base=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
        if file_type=="photo":
            r=requests.post(f"{base}/sendPhoto",json={"chat_id":chat_id,"photo":file_id,"caption":text,"parse_mode":"HTML"},timeout=15)
        elif file_type=="document":
            r=requests.post(f"{base}/sendDocument",json={"chat_id":chat_id,"document":file_id,"caption":text,"parse_mode":"HTML"},timeout=15)
        else:
            r=requests.post(f"{base}/sendMessage",json={"chat_id":chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":True},timeout=10)
        return r.status_code==200
    except Exception as e:
        print(f"  [USER SEND] {chat_id}: {e}"); return False

def do_broadcast(admin_chat_id,text:str,file_id=None,file_type=None):
    targets=list(registered_users)+[TELEGRAM_CHANNEL_ID]
    ok=0; fail=0
    for cid in targets:
        if send_to_user(cid,text,file_id,file_type): ok+=1
        else: fail+=1
        time.sleep(0.05)
    send_reply(admin_chat_id,f"Broadcast Done\n{ok} delivered | {fail} failed\n\n<i>— CLEXER V6.1 —</i>")

# ─── MESSAGE FORMATS ──────────────────────────────────────────────────────────
def fmt_signal(s:dict) -> str:
    e  ="🟢" if s["signal"]=="BUY" else "🔴"
    ci ={"HIGH":"🔥","MEDIUM":"✨","LOW":"⚡"}.get(s.get("confidence",""),"")
    el =f"🎯 Entry    <b>{s['entry']:,.0f}</b>"
    if s.get("entry_type")=="PULLBACK" and s.get("entry_note"):
        el+=f"\n   ⏳ <i>{s['entry_note']}</i>"
    wk=s.get("weekly_trend",""); s4h=s.get("structure_4h","")
    ez=s.get("entry_zone","");   rs=s.get("reasoning","")
    src=s.get("data_source",get_current_source())
    pf=s.get("prompt_used","")
    old_assess=s.get("old_trade_assessment","")
    msg=(
        f"{e} <b>{s['signal']} — {SYMBOL}</b>  {ci}\n"
        f"🕐 {ist_str()}  |  📍 {s.get('session',get_session())}\n"
        f"📡 Source: <b>{src}</b>"+(f" | {pf}" if pf else "")+"\n\n"
        f"{el}\n"
        f"🛑 SL       <b>{s['sl']:,.0f}</b>\n"
        f"✅ TP1     <b>{s['tp1']:,.0f}</b>\n"
        f"✅ TP2     <b>{s['tp2']:,.0f}</b>\n"
        f"📊 R:R     <b>{s.get('rr','—')}</b>\n\n"
        +(f"🗓 Weekly: <i>{wk}</i>\n" if wk else "")
        +(f"🔷 4H:     <i>{s4h}</i>\n" if s4h else "")
        +(f"📍 Zone:   <i>{ez}</i>\n" if ez else "")
        +(f"\n💭 <i>{rs}</i>\n" if rs else "")
        +(f"\n📋 <i>Previous: {old_assess[:120]}</i>\n" if old_assess else "")
        +f"\n<i>— Signal by CLEXER V6.1 —</i>\n"
        f"⚠️ <i>Not financial advice</i>"
    )
    return msg

def fmt_update(status:str,price:float=None) -> str:
    t=active_trade; entry=t.get("entry") or 0
    msgs={
        "SL_HIT":        "🛑 <b>SL HIT</b> — Finding next trade...",
        "TP1_HIT":       f"✅ <b>TP1 HIT!</b>\nSL → Breakeven ({entry:,.0f})\nRiding to TP2 → <b>{t.get('tp2',0):,.0f}</b>",
        "TP2_HIT":       "🏆 <b>TP2 HIT — Trade Complete!</b>",
        "STOP_HUNT":     "⚡ <b>STOP HUNT</b> — SL wicked, closed above/below. Holding.",
        "SETUP_INVALID": "❌ <b>Setup Invalid</b> — SL hit before entry. Finding new trade.",
        "ENTRY_MISSED":  f"🚀 <b>Entry Missed</b> — Price bypassed zone {entry:,.0f}. Resetting.",
        "STRUCTURE_FLIP":"🔄 <b>Structure Flipped</b> — Closing trade, entering new direction.",
        "WAITING_ENTRY": (
            f"⏳ <b>Waiting Pullback</b>\n"
            f"Entry zone: <b>{entry:,.0f}</b>\n"
            +(f"Current:    <b>{price:,.0f}</b> ({abs((price or 0)-entry):,.0f} pts away)" if price else "")
        ),
    }
    return f"📡 <b>{SYMBOL} UPDATE</b>  {ist_str()}\n\n{msgs.get(status,'⏳ Trade running')}\n\n<i>— CLEXER V6.1 —</i>"

# ─── PRICE ADVICE ─────────────────────────────────────────────────────────────
def price_only_advice(price:float) -> str:
    t=active_trade
    sig=t["signal"]; entry=t["entry"]; sl=t["sl"]; tp1=t["tp1"]; tp2=t["tp2"]
    sl_dist=abs(entry-sl); tp2_dist=abs(entry-tp2) or 1
    if sig=="BUY":
        dist_to_sl=price-sl; dist_to_tp1=tp1-price; dist_to_tp2=tp2-price
        pct=(price-entry)/tp2_dist*100
    else:
        dist_to_sl=sl-price; dist_to_tp1=price-tp1; dist_to_tp2=price-tp2
        pct=(entry-price)/tp2_dist*100
    if pct>=75:   advice="HOLD — Consider trailing SL"
    elif pct>=40: advice="HOLD — Strong momentum"
    elif pct>=10: advice="HOLD — In profit"
    else:         advice="WAIT — Near entry, watch momentum"
    tp1_status="HIT — SL at breakeven" if t["tp1_hit"] else f"{abs(dist_to_tp1):.0f} pts away"
    return (
        f"🕐 <b>HOURLY CHECK</b>  {ist_str()}\n\n"
        f"{'🟢' if sig=='BUY' else '🔴'} <b>{sig} {SYMBOL}</b>\n\n"
        f"Price:    <b>{price:,.2f}</b>\n"
        f"Entry:    <b>{entry:,.0f}</b>\n"
        f"SL:       <b>{sl:,.0f}</b>  ({dist_to_sl:.0f} pts)\n"
        f"TP1:      <b>{tp1:,.0f}</b>  {tp1_status}\n"
        f"TP2:      <b>{tp2:,.0f}</b>  ({abs(dist_to_tp2):.0f} pts)\n"
        f"Progress: <b>{max(0,pct):.1f}%</b> to TP2\n"
        f"Advice:   <b>{advice}</b>\n"
        f"Source:   <b>{get_current_source()}</b>\n\n"
        f"<i>— CLEXER V6.1 —</i>"
    )

# ─── DETECTION HELPERS ────────────────────────────────────────────────────────
def detect_stop_hunt(df_5m) -> bool:
    t=active_trade
    if not t["signal"] or not t["entry_hit"]: return False
    sig=t["signal"]; sl=t["sl"]
    for i in range(-3,0):
        row=df_5m.iloc[i]
        if sig=="BUY"  and row["low"]<sl  and row["close"]>sl and row["close"]-row["low"]>100:   return True
        if sig=="SELL" and row["high"]>sl  and row["close"]<sl and row["high"]-row["close"]>100:  return True
    return False

def detect_entry_missed(price:float) -> bool:
    t=active_trade
    if t["entry_hit"] or t["entry_type"]!="PULLBACK": return False
    if t["signal"]=="BUY"  and price>=t["tp2"]: return True
    if t["signal"]=="SELL" and price<=t["tp2"]: return True
    return False

def detect_entry_invalidated(price:float,df_4h) -> bool:
    t=active_trade
    if t["entry_hit"]: return False
    last_close=df_4h["close"].iloc[-1]
    if t["signal"]=="BUY"  and last_close<t["sl"]: return True
    if t["signal"]=="SELL" and last_close>t["sl"]: return True
    return False

def check_price_status(price:float,high:float,low:float,df_5m=None) -> str:
    t=active_trade
    if not t["signal"]: return "NONE"
    sig,sl,tp1,tp2,entry=t["signal"],t["sl"],t["tp1"],t["tp2"],t["entry"]
    if not t["entry_hit"]:
        if (sig=="BUY"  and high>=tp2): return "ENTRY_MISSED"
        if (sig=="SELL" and low<=tp2):  return "ENTRY_MISSED"
        if (sig=="BUY"  and low<=sl):   return "SETUP_INVALID"
        if (sig=="SELL" and high>=sl):  return "SETUP_INVALID"
        tol=abs(entry-sl)*0.3
        if (sig=="BUY" and price<=entry+tol) or (sig=="SELL" and price>=entry-tol):
            active_trade["entry_hit"]=True
        else:
            return "WAITING_ENTRY"
    if df_5m is not None and not t["sl_wicked"]:
        if detect_stop_hunt(df_5m):
            active_trade["sl_wicked"]=True
            trade_stats["stop_hunts"]+=1
            return "STOP_HUNT"
    if (sig=="SELL" and high>=sl) or (sig=="BUY" and low<=sl):   return "SL_HIT"
    if (sig=="SELL" and low<=tp2) or (sig=="BUY" and high>=tp2): return "TP2_HIT"
    if not t["tp1_hit"]:
        if (sig=="SELL" and low<=tp1) or (sig=="BUY" and high>=tp1): return "TP1_HIT"
    return "RUNNING"

# ─── 1-MIN TICK ───────────────────────────────────────────────────────────────
def run_tick_check() -> bool:
    if not active_trade["signal"]: return False
    try:
        ticker=get_ticker(); price=ticker["price"]
        t=active_trade
        sig=t["signal"]; entry=t["entry"]; sl=t["sl"]; tp1=t["tp1"]; tp2=t["tp2"]
        if not t["entry_hit"]:
            tol=abs(entry-sl)*0.25
            if (sig=="BUY" and price<=entry+tol) or (sig=="SELL" and price>=entry-tol):
                active_trade["entry_hit"]=True
                send_telegram(
                    f"⚡ <b>ENTRY TRIGGERED!</b>  {ist_str()}\n\n"
                    f"{'🟢' if sig=='BUY' else '🔴'} <b>{sig} {SYMBOL}</b>\n\n"
                    f"Entry:  <b>{entry:,.0f}</b>  ←  Price: <b>{price:,.2f}</b>\n"
                    f"SL:     <b>{sl:,.0f}</b>  ({abs(price-sl):.0f} pts)\n"
                    f"TP1:    <b>{tp1:,.0f}</b>\n"
                    f"TP2:    <b>{tp2:,.0f}</b>\n\n"
                    f"<i>— CLEXER V6.1 —</i>"
                )
            return False
        if (sig=="BUY" and price>=tp2) or (sig=="SELL" and price<=tp2):
            trade_stats["total_tp2"]+=1; trade_stats["consecutive_sl"]=0
            send_telegram(
                f"🏆 <b>TP2 HIT!</b>  {ist_str()}\n\n"
                f"{'🟢' if sig=='BUY' else '🔴'} {sig} {SYMBOL}\n"
                f"Entry: {entry:,.0f} → TP2: <b>{tp2:,.0f}</b>\n\n"
                f"Full trade complete!\n\n<i>— CLEXER V6.1 —</i>"
            )
            reset_trade("TP2 HIT"); return True
        if not t["tp1_hit"]:
            if (sig=="BUY" and price>=tp1) or (sig=="SELL" and price<=tp1):
                active_trade["tp1_hit"]=True; active_trade["sl"]=entry
                trade_stats["total_tp1"]+=1; trade_stats["consecutive_sl"]=0
                send_telegram(
                    f"✅ <b>TP1 HIT!</b>  {ist_str()}\n\n"
                    f"{'🟢' if sig=='BUY' else '🔴'} {sig} {SYMBOL}\n"
                    f"Entry: {entry:,.0f} → TP1: <b>{tp1:,.0f}</b>\n"
                    f"SL moved to breakeven: <b>{entry:,.0f}</b>\n"
                    f"Riding to TP2: <b>{tp2:,.0f}</b>...\n\n<i>— CLEXER V6.1 —</i>"
                )
        sl_margin=80
        sl_clearly=(sig=="BUY" and price<sl-sl_margin) or (sig=="SELL" and price>sl+sl_margin)
        if sl_clearly:
            trade_stats["total_sl"]+=1; trade_stats["consecutive_sl"]+=1
            n=trade_stats["consecutive_sl"]
            if n>=3:
                trade_stats["cooldown_scans"]=2
                send_telegram(f"🛑 <b>SL HIT</b> ({n} in a row)\nCooling down 2 scans.\n\n<i>— CLEXER V6.1 —</i>")
            elif n==2:
                trade_stats["cooldown_scans"]=1
                send_telegram(f"🛑 <b>SL HIT</b> ({n} in a row)\nCooling down 1 scan.\n\n<i>— CLEXER V6.1 —</i>")
            else:
                send_telegram(fmt_update("SL_HIT"))
            reset_trade("SL HIT"); return True
    except Exception as e:
        print(f"  [TICK ERROR] {e}")
    return False

# ─── 1-HOUR PRICE CHECK ───────────────────────────────────────────────────────
def run_price_check() -> bool:
    if not active_trade["signal"]: return False
    try:
        ticker=get_ticker(); price=ticker["price"]
        range_1h=get_price_range_since(60)
        high_1h=range_1h["high"] or price
        low_1h =range_1h["low"]  or price
        print(f"  [1H] cur:{price:,.2f} H:{high_1h:,.2f} L:{low_1h:,.2f} src:{get_current_source()}")
        df_5m=get_candles("5m",50)
        df_4h=get_candles("4h",10)
        if detect_entry_missed(price):
            trade_stats["missed_entries"]+=1
            send_telegram(fmt_update("ENTRY_MISSED")); reset_trade("ENTRY MISSED"); return True
        if not active_trade["entry_hit"] and detect_entry_invalidated(price,df_4h):
            send_telegram(fmt_update("SETUP_INVALID")); reset_trade("SETUP INVALID"); return True
        status=check_price_status(price,high_1h,low_1h,df_5m)
        print(f"  [1H] {active_trade['signal']} | {status}")
        if status=="TP2_HIT":
            trade_stats["total_tp2"]+=1; trade_stats["consecutive_sl"]=0
            send_telegram(fmt_update("TP2_HIT")); reset_trade("TP2 HIT"); return True
        elif status=="SL_HIT":
            trade_stats["total_sl"]+=1; trade_stats["consecutive_sl"]+=1
            n=trade_stats["consecutive_sl"]
            if n>=3:
                trade_stats["cooldown_scans"]=2
                send_telegram(f"🛑 <b>SL HIT</b> ({n} in a row)\nCooling down 2 scans.\n\n<i>— CLEXER V6.1 —</i>")
            elif n==2:
                trade_stats["cooldown_scans"]=1
                send_telegram(f"🛑 <b>SL HIT</b> ({n} in a row)\nCooling down 1 scan.\n\n<i>— CLEXER V6.1 —</i>")
            else:
                send_telegram(fmt_update("SL_HIT"))
            reset_trade("SL HIT"); return True
        elif status=="TP1_HIT" and not active_trade["tp1_hit"]:
            active_trade["tp1_hit"]=True; active_trade["sl"]=active_trade["entry"]
            trade_stats["total_tp1"]+=1; trade_stats["consecutive_sl"]=0
            send_telegram(fmt_update("TP1_HIT"))
        elif status=="STOP_HUNT":
            send_telegram(fmt_update("STOP_HUNT"))
        elif status in ("ENTRY_MISSED","SETUP_INVALID"):
            send_telegram(fmt_update(status)); reset_trade(status); return True
        elif status=="WAITING_ENTRY":
            active_trade["scan_count"]+=1
            send_telegram(fmt_update("WAITING_ENTRY",price))
        elif status=="RUNNING":
            active_trade["scan_count"]+=1
            send_telegram(price_only_advice(price))
    except Exception as e:
        print(f"  [1H ERROR] {e}")
    return False

# ─── NEWS ─────────────────────────────────────────────────────────────────────
def get_article_image(entry) -> bytes | None:
    for field in ("media_content","media_thumbnail"):
        items=getattr(entry,field,[]) or entry.get(field,[])
        if items:
            url=items[0].get("url","") if isinstance(items[0],dict) else ""
            if url.startswith("http"):
                try:
                    r=requests.get(url,timeout=8,headers={"User-Agent":"Mozilla/5.0"})
                    if r.status_code==200 and len(r.content)>2000: return r.content
                except: pass
    link=entry.get("link","")
    if not link: return None
    try:
        r=requests.get(link,timeout=8,headers={"User-Agent":"Mozilla/5.0 Chrome/120"})
        if HAS_BS4:
            soup=BeautifulSoup(r.text,"html.parser")
            og=soup.find("meta",property="og:image")
            if og:
                img_url=og.get("content","")
                if img_url.startswith("http"):
                    r2=requests.get(img_url,timeout=8)
                    if r2.status_code==200 and len(r2.content)>2000: return r2.content
    except: pass
    return None

def check_news(force:bool=False):
    global latest_news_context
    if not SEND_NEWS and not force: return
    if not HAS_FEEDPARSER: return
    print(f"  [NEWS] Checking {len(NEWS_SOURCES)} RSS sources...")
    candidates=[]
    btc_kw=["bitcoin","btc","crypto","ethereum","eth","fed","federal reserve","interest rate",
            "inflation","sec","cftc","etf","regulation","whale","spot","halving","market",
            "rally","crash","bull","bear","exchange","hack","cpi","gdp","bank","liquidity",
            "blackrock","fidelity","coinbase","binance","tether","stablecoin"]
    for src in NEWS_SOURCES:
        try:
            feed=feedparser.parse(src["url"]); added=0
            for entry in feed.entries:
                title=(entry.get("title") or "").strip()
                link=entry.get("link",""); guid=entry.get("id",link or title)
                if not title or guid in posted_news_guids: continue
                pub=entry.get("published_parsed") or entry.get("updated_parsed")
                if pub and (time.time()-time.mktime(pub))/3600>MAX_NEWS_AGE: continue
                raw_sum=entry.get("summary") or entry.get("description") or ""
                if HAS_BS4: summary=BeautifulSoup(raw_sum,"html.parser").get_text()[:400]
                else:        summary=re.sub(r"<[^>]+>","",raw_sum)[:400]
                if not any(kw in (title+" "+summary).lower() for kw in btc_kw): continue
                candidates.append({"title":title,"link":link,"guid":guid,"summary":summary,"source":src["name"],"entry":entry})
                added+=1
            print(f"    {src['name']}: {added} new items")
        except Exception as e:
            print(f"    {src['name']}: {e}")
    if not candidates: return
    try:
        btc_price=get_ticker()["price"]
    except: btc_price=0
    to_post=[]
    for i in range(0,len(candidates),10):
        batch=candidates[i:i+10]
        news_block="\n\n".join(f"[{j}] {e['source']}\nTITLE: {e['title']}\nSUMMARY: {e['summary'][:200]}" for j,e in enumerate(batch))
        try:
            resp=anthropic.Anthropic(api_key=ANTHROPIC_API_KEY).messages.create(
                model="claude-haiku-4-5-20251001",max_tokens=600,
                messages=[{"role":"user","content":
                    f"BTC: ${btc_price:,.0f} | Session: {get_session()}\n\n{news_block}\n\n"
                    f"Return JSON array of MEDIUM/HIGH impact items only.\n"
                    f"Fields: index(int), impact(BULLISH/BEARISH/NEUTRAL), strength(HIGH/MEDIUM), reason(str)\n"
                    f"Empty array [] if none. JSON only."}]
            )
            raw=resp.content[0].text.strip().replace("```json","").replace("```","").strip()
            analyzed=json.loads(raw)
            for item in analyzed:
                idx=item.get("index",-1)
                if 0<=idx<len(batch):
                    batch[idx].update({"impact":item.get("impact","NEUTRAL"),"strength":item.get("strength","LOW"),"reason":item.get("reason","")})
                    to_post.append(batch[idx])
        except Exception as e:
            print(f"  [NEWS CLAUDE] {e}")
    if not to_post: return
    to_post.sort(key=lambda x:0 if x.get("strength")=="HIGH" else 1)
    latest_news_context=[
        f"• {e.get('impact','?')} ({e.get('strength','?')}): {e['title'][:80]} — {e.get('reason','')[:80]}"
        for e in to_post[:3]
    ]
    posted=0
    for item in to_post[:MAX_NEWS_PER_RUN]:
        impact=item.get("impact","NEUTRAL"); strength=item.get("strength","LOW")
        emoji="🟢" if impact=="BULLISH" else ("🔴" if impact=="BEARISH" else "⚪")
        fire="🔥" if strength=="HIGH" else "✨"
        msg_text=(
            f"📰 <b>MARKET NEWS</b>  {fire}\n\n"
            f"{emoji} <b>{impact}</b> for BTC\n"
            f"<b>{item['title'][:120]}</b>\n\n"
            f"💭 <i>{item.get('reason','')}</i>\n\n"
            f"📡 {item['source']}\n"
            f"🔗 <a href='{item['link']}'>Read article</a>\n\n"
            f"<i>— CLEXER V6.1 News · {ist_str()} —</i>"
        )
        img_bytes=None
        try: img_bytes=get_article_image(item["entry"])
        except: pass
        try:
            if img_bytes and len(img_bytes)>2000:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                    data={"chat_id":TELEGRAM_CHANNEL_ID,"caption":msg_text,"parse_mode":"HTML"},
                    files={"photo":("news.jpg",img_bytes,"image/jpeg")},timeout=20)
            else:
                send_telegram(msg_text)
            posted_news_guids.add(item["guid"])
            posted+=1; time.sleep(1)
        except Exception as e:
            print(f"  [NEWS POST] {e}")
    print(f"  [NEWS] Posted {posted} articles")

# ─── /tvstatus ────────────────────────────────────────────────────────────────
def cmd_tvstatus(chat_id):
    if not TV_BRIDGE_URL:
        send_reply(chat_id,
            "TradingView Status\n\nTV_BRIDGE_URL not set in Railway.\n"
            "Bot running on Binance data.\n\n<i>— CLEXER V6.1 —</i>")
        return
    send_reply(chat_id, f"Checking connection...\n<code>{TV_BRIDGE_URL}</code>")
    now=time.time(); health=tv_ping()
    if not health:
        send_reply(chat_id,
            f"TradingView Status\n\n🔴 Bridge OFFLINE\n"
            f"Can't reach {TV_BRIDGE_URL}\n\n"
            f"Fix:\n1. Run tv_bridge.py\n2. Run ngrok http 8765\n3. Update TV_BRIDGE_URL\n\n"
            f"Bot using Binance fallback.\n\n{ist_str()}\n<i>— CLEXER V6.1 —</i>")
        return
    tv_bridge_state.update({
        "online":True,"last_seen":now,
        "cdp_ok":health.get("cdp_connected",False),
        "tv_version":health.get("tv_version",""),
        "cached_intervals":health.get("cached_intervals",[]),
    })
    cdp_ok=health.get("cdp_connected",False)
    uptime=health.get("uptime_seconds",0)
    cached_ivs=health.get("cached_intervals",[])
    try:
        r2=requests.get(f"{TV_BRIDGE_URL}/status",timeout=8)
        detail=r2.json() if r2.status_code==200 else {}
    except: detail={}
    price_ok=False; price_val=0.0
    tk=tv_get_ticker()
    if tk and tk.get("price",0)>0: price_ok=True; price_val=tk["price"]
    candles_ok=False; candles_count=0
    df=tv_get_candles("1h",10)
    if df is not None and len(df)>0: candles_ok=True; candles_count=len(df)
    def tick(ok): return "✅" if ok else "❌"
    overall="ALL SYSTEMS GO — TradingView data flowing" if (cdp_ok and price_ok and candles_ok) else "PARTIAL — some issues"
    uptime_str=f"{uptime//3600:.0f}h {(uptime%3600)//60:.0f}m" if uptime>=3600 else f"{uptime//60:.0f}m {uptime%60:.0f}s"
    msg=(
        f"TradingView Status\n\n{overall}\n\n"
        f"{tick(True)} Step 1: Bridge reachable\n"
        f"{tick(cdp_ok)} Step 2: TradingView connected\n"
        f"{tick(price_ok)} Step 3: Price feed live" +(f" ({price_val:,.2f})" if price_ok else "")+"\n"
        f"{tick(candles_ok)} Step 4: Candles cached" +(f" ({candles_count} bars)" if candles_ok else "")+"\n\n"
        f"Cached: {', '.join(cached_ivs) if cached_ivs else 'none'}\n"
        f"TV Version: {health.get('tv_version','?')}\n"
        f"Uptime: {uptime_str}\n"
        f"Prompt: {'New 10-Step' if (cdp_ok and price_ok) else 'Classic SMC'}\n\n"
        f"{ist_str()}\n<i>— CLEXER V6.1 —</i>"
    )
    send_reply(chat_id, msg)

# ─── COMMANDS ─────────────────────────────────────────────────────────────────
FRIEND_COMMANDS_HELP = """🤖 <b>CLEXER V6.1</b>
━━━━━━━━━━━━━━━━━━━━

/status — Bot status
/price — Live BTC price
/trade — Active trade details
/history — Last 5 signals
/stats — Win/loss statistics
/session — Current session
/help — This menu

<i>Limit: 2 uses per command per hour</i>"""

ADMIN_COMMANDS_HELP = """🤖 <b>CLEXER V6.1 — Admin</b>
━━━━━━━━━━━━━━━━━━━━

INFO
/status /price /trade /history /stats /session /tvstatus

TRADE CONTROL
/close /sltobe /setsl 61500 /settp1 63000 /settp2 65000

BOT CONTROL
/signal — Force scan now
/pause /resume /resetsl
/setinterval 4 — Scan interval (hours)
/users — User count

CHARTS & NEWS
/images on|off
/setimages weekly,4h,1h,5m
/news on|off
/latestnews

/broadcast — Send to all users
/help — This menu"""

def handle_command(text:str, chat_id, message:dict=None):
    global SIGNAL_SCAN_INTERVAL, SEND_CHARTS, CHART_TFS, SEND_NEWS
    global last_force_scan_time, broadcast_pending

    register_user(chat_id)
    parts=text.strip().split()
    cmd=parts[0].lower().split("@")[0]
    is_admin=(not ADMIN_CHAT_ID) or (str(chat_id)==str(ADMIN_CHAT_ID))

    # Friend-accessible commands (rate limited)
    friend_cmds={"/status","/price","/trade","/history","/stats","/session","/help","/start"}
    # Admin-only commands
    admin_cmds={
        "/signal","/pause","/resume","/resetsl","/setinterval",
        "/close","/sltobe","/setsl","/settp1","/settp2",
        "/broadcast","/users","/images","/setimages","/news","/latestnews","/tvstatus",
    }

    # Block non-admin from admin commands
    if cmd in admin_cmds and not is_admin:
        send_reply(chat_id,"⛔ Admin only."); return

    # Rate limit friends on friend commands
    if cmd in friend_cmds and not is_admin:
        allowed, msg = check_friend_rate(chat_id, cmd)
        if not allowed:
            send_reply(chat_id, msg); return

    # ── COMMANDS ──────────────────────────────────────────────────────────────
    if cmd in ("/start","/help"):
        send_reply(chat_id, ADMIN_COMMANDS_HELP if is_admin else FRIEND_COMMANDS_HELP)

    elif cmd=="/tvstatus":
        cmd_tvstatus(chat_id)

    elif cmd=="/status":
        t=active_trade; st="PAUSED" if bot_paused.is_set() else "RUNNING"
        cd=f"Cooldown: {trade_stats['cooldown_scans']} scans\n" if trade_stats["cooldown_scans"] else ""
        ti=(
            f"{t['signal']} @ {t['entry']:,.0f}\n"
            f"SL:{t['sl']:,.0f}  TP1:{t['tp1']:,.0f}  TP2:{t['tp2']:,.0f}\n"
            f"Entry:{'✅' if t['entry_hit'] else '⏳'}  TP1:{'✅' if t['tp1_hit'] else '❌'}"
        ) if t["signal"] else "No active trade"
        tv_status="Online (New Prompt)" if is_tv_online() else "Offline (Classic Prompt)"
        send_reply(chat_id,
            f"CLEXER V6.1\n\n"
            f"Bot: {st}\n{cd}"
            f"Session: {get_session()} {'✅' if is_trading_hours() else '⏸'}\n"
            f"IST: {ist_str()}\n"
            f"Scan: {SIGNAL_SCAN_INTERVAL//3600}h | Min conf: {required_confidence()}\n"
            f"Consec SL: {trade_stats['consecutive_sl']}\n"
            f"Users: {len(registered_users)}\n\n"
            f"Source: {get_current_source()}\n"
            f"TV Bridge: {tv_status}\n"
            f"Charts: {'ON' if SEND_CHARTS else 'OFF'} | News: {'ON' if SEND_NEWS else 'OFF'}\n\n"
            f"Active Trade:\n{ti}"
        )

    elif cmd=="/price":
        try:
            tk=get_ticker()
            send_reply(chat_id,
                f"BTCUSDT\n\nPrice: <b>{tk['price']:,.2f}</b>\n"
                f"24h: {tk['change']:+.2f}% | Vol: ${tk['volume']/1e6:.1f}M\n"
                f"H:{tk['high24']:,.2f}  L:{tk['low24']:,.2f}\n"
                f"Source: {tk.get('source',get_current_source())}\n"
                f"{ist_str()}")
        except Exception as e: send_reply(chat_id,f"Error: {e}")

    elif cmd=="/trade":
        t=active_trade
        if not t["signal"]: send_reply(chat_id,"No active trade.")
        else:
            try: tk=get_ticker(); pl=f"Current: <b>{tk['price']:,.2f}</b>\n"
            except: pl=""
            sl_dist=abs(t['entry']-t['sl'])
            send_reply(chat_id,
                f"Active Trade\n\n"
                f"{'🟢 BUY' if t['signal']=='BUY' else '🔴 SELL'} — {SYMBOL}\n\n"
                f"{pl}"
                f"Entry: <b>{t['entry']:,.0f}</b> {'✅' if t['entry_hit'] else '⏳ waiting'}\n"
                f"SL:    <b>{t['sl']:,.0f}</b>  ({sl_dist:.0f} pts)\n"
                f"TP1:   <b>{t['tp1']:,.0f}</b> {'✅ HIT — SL at BE' if t['tp1_hit'] else '⏳'}\n"
                f"TP2:   <b>{t['tp2']:,.0f}</b>\n"
                f"Type:  {t['entry_type']}\n"
                f"Opened: {t.get('open_time','?')}\n"
                +(f"Note:  <i>{t['entry_note']}</i>" if t.get("entry_note") else ""))

    elif cmd=="/history":
        if not signal_history: send_reply(chat_id,"No history.")
        else:
            lines=["Last Signals\n"]
            for s in reversed(signal_history[-5:]):
                e="🟢" if s["signal"]=="BUY" else "🔴"
                lines.append(
                    f"{e} {s['signal']} @ {s['entry']:,.0f}  R:R:{s.get('rr','?')}  {s.get('confidence','?')}\n"
                    f"   SL:{s['sl']:,.0f}  TP1:{s['tp1']:,.0f}  TP2:{s['tp2']:,.0f}\n"
                    f"   Src:{s.get('source','?')}  {s['time']}\n"
                )
            send_reply(chat_id,"\n".join(lines))

    elif cmd=="/stats":
        ts=trade_stats
        send_reply(chat_id,
            f"Statistics\n\n"
            f"Total signals:  {ts['total_signals']}\n"
            f"TP1 hits:       {ts['total_tp1']} ✅\n"
            f"TP2 hits:       {ts['total_tp2']} 🏆\n"
            f"SL hits:        {ts['total_sl']} 🛑\n"
            f"Stop hunts:     {ts['stop_hunts']}\n"
            f"Missed entries: {ts['missed_entries']}\n"
            f"Consec SL:      {ts['consecutive_sl']}\n"
            f"Cooldown:       {ts['cooldown_scans']} scans")

    elif cmd=="/session":
        s=get_session()
        send_reply(chat_id,
            f"Session\n\n{s} {'✅ Active' if is_trading_hours() else '⏸ Inactive'}\n\n"
            f"London:  07:30–16:30 IST\n"
            f"NY:      18:30–01:00 IST\n"
            f"Sleep:   01:00–07:29 IST\n\n{ist_str()}")

    elif cmd=="/users":
        send_reply(chat_id,f"Users\n\nTotal: <b>{len(registered_users)}</b>")

    elif cmd=="/close":
        t=active_trade
        if not t["signal"]: send_reply(chat_id,"No active trade.")
        else:
            info=f"{t['signal']} @ {t['entry']:,.0f}"; reset_trade("MANUAL CLOSE")
            send_telegram(f"⛔ <b>Trade Manually Closed</b>\n{info}\n\n<i>— CLEXER V6.1 —</i>")
            send_reply(chat_id,f"Closed: {info}"); force_scan.set()

    elif cmd=="/sltobe":
        if not active_trade["signal"]: send_reply(chat_id,"No active trade.")
        else:
            old=active_trade["sl"]; active_trade["sl"]=active_trade["entry"]
            send_telegram(f"SL to Breakeven: {old:,.0f} → <b>{active_trade['entry']:,.0f}</b>\n\n<i>— CLEXER V6.1 —</i>")
            send_reply(chat_id,f"SL → {active_trade['entry']:,.0f}")

    elif cmd=="/setsl":
        if not active_trade["signal"]: send_reply(chat_id,"No active trade.")
        elif len(parts)<2: send_reply(chat_id,"Usage: /setsl 61500")
        else:
            try:
                v=float(parts[1].replace(",","")); old=active_trade["sl"]; active_trade["sl"]=v
                send_telegram(f"SL: {old:,.0f} → <b>{v:,.0f}</b>\n\n<i>— CLEXER V6.1 —</i>")
                send_reply(chat_id,f"SL = {v:,.0f}")
            except: send_reply(chat_id,"Error: /setsl 61500")

    elif cmd=="/settp1":
        if not active_trade["signal"]: send_reply(chat_id,"No active trade.")
        elif len(parts)<2: send_reply(chat_id,"Usage: /settp1 63000")
        else:
            try:
                v=float(parts[1].replace(",","")); active_trade["tp1"]=v
                send_telegram(f"TP1 → <b>{v:,.0f}</b>\n\n<i>— CLEXER V6.1 —</i>")
                send_reply(chat_id,f"TP1 = {v:,.0f}")
            except: send_reply(chat_id,"Error: /settp1 63000")

    elif cmd=="/settp2":
        if not active_trade["signal"]: send_reply(chat_id,"No active trade.")
        elif len(parts)<2: send_reply(chat_id,"Usage: /settp2 65000")
        else:
            try:
                v=float(parts[1].replace(",","")); active_trade["tp2"]=v
                send_telegram(f"TP2 → <b>{v:,.0f}</b>\n\n<i>— CLEXER V6.1 —</i>")
                send_reply(chat_id,f"TP2 = {v:,.0f}")
            except: send_reply(chat_id,"Error: /settp2 65000")

    elif cmd=="/signal":
        if bot_paused.is_set(): send_reply(chat_id,"Bot paused. /resume first.")
        else:
            now=time.time(); elapsed=now-last_force_scan_time
            if elapsed<900 and last_force_scan_time>0:
                send_reply(chat_id,f"Cooldown: {int((900-elapsed)/60)} min left")
            else:
                last_force_scan_time=now
                send_reply(chat_id,"Forcing full scan — charts + signal incoming (~30s)")
                force_scan.set()

    elif cmd=="/pause":
        bot_paused.set()
        send_telegram("⏸ <b>Bot Paused</b>\n\n<i>— CLEXER V6.1 —</i>")
        send_reply(chat_id,"Paused.")

    elif cmd=="/resume":
        bot_paused.clear()
        send_telegram("▶️ <b>Bot Resumed</b>\n\n<i>— CLEXER V6.1 —</i>")
        send_reply(chat_id,"Resumed.")

    elif cmd=="/resetsl":
        trade_stats["consecutive_sl"]=0; trade_stats["cooldown_scans"]=0
        send_reply(chat_id,"SL streak + cooldown reset.")

    elif cmd=="/setinterval":
        if len(parts)<2: send_reply(chat_id,f"Current: {SIGNAL_SCAN_INTERVAL//3600}h\nUsage: /setinterval 4")
        else:
            try:
                h=float(parts[1])
                if h<1 or h>24: send_reply(chat_id,"1–24 hours only.")
                else:
                    SIGNAL_SCAN_INTERVAL=int(h*3600)
                    send_reply(chat_id,f"Scan interval → {h}h")
            except: send_reply(chat_id,"Error: /setinterval 4")

    elif cmd=="/images":
        if len(parts)<2:
            send_reply(chat_id,f"Charts: {'ON' if SEND_CHARTS else 'OFF'}\nTFs: {', '.join(CHART_TFS).upper()}\n\nUsage: /images on|off")
        elif parts[1].lower()=="on":
            SEND_CHARTS=True; send_reply(chat_id,f"Chart images ON — {', '.join(CHART_TFS).upper()}")
        elif parts[1].lower()=="off":
            SEND_CHARTS=False; send_reply(chat_id,"Chart images OFF")
        else:
            send_reply(chat_id,"Usage: /images on|off")

    elif cmd=="/setimages":
        if len(parts)<2:
            send_reply(chat_id,f"Current: {', '.join(CHART_TFS).upper()}\nUsage: /setimages weekly,4h,1h,5m")
        else:
            valid={"weekly","4h","1h","5m"}
            chosen=[tf.strip().lower() for tf in parts[1].split(",") if tf.strip().lower() in valid]
            if not chosen: send_reply(chat_id,"No valid TFs. Available: weekly,4h,1h,5m")
            else:
                CHART_TFS=chosen
                send_reply(chat_id,f"Chart TFs → {', '.join(CHART_TFS).upper()}")

    elif cmd=="/news":
        if len(parts)<2:
            send_reply(chat_id,f"News: {'ON' if SEND_NEWS else 'OFF'}\nUsage: /news on|off")
        elif parts[1].lower()=="on":
            SEND_NEWS=True; send_reply(chat_id,f"News ON — {len(NEWS_SOURCES)} RSS sources")
        elif parts[1].lower()=="off":
            SEND_NEWS=False; send_reply(chat_id,"News OFF")
        else:
            send_reply(chat_id,"Usage: /news on|off")

    elif cmd=="/latestnews":
        send_reply(chat_id,"Fetching latest news... (~15s)")
        threading.Thread(target=check_news,args=(True,),daemon=True).start()

    elif cmd=="/broadcast":
        broadcast_pending[chat_id]={"step":"waiting_message"}
        send_reply(chat_id,"Broadcast Mode\n\nSend your message now.\n/cancel to abort")

    elif cmd=="/cancel":
        if chat_id in broadcast_pending:
            del broadcast_pending[chat_id]; send_reply(chat_id,"Cancelled.")
        else:
            send_reply(chat_id,"Nothing to cancel.")

    else:
        send_reply(chat_id,f"Unknown: {cmd}\n/help")

def handle_broadcast_message(chat_id,message:dict):
    text=message.get("text") or message.get("caption") or ""
    photo=message.get("photo"); doc=message.get("document")
    file_id=None; file_type=None
    if photo:  file_id=photo[-1]["file_id"]; file_type="photo"
    elif doc:  file_id=doc["file_id"];       file_type="document"
    if not text and not file_id:
        send_reply(chat_id,"Empty. Send text/image or /cancel."); return
    del broadcast_pending[chat_id]
    send_reply(chat_id,f"Broadcasting to {len(registered_users)+1} targets...")
    threading.Thread(target=do_broadcast,args=(chat_id,text,file_id,file_type),daemon=True).start()

# ─── COMMAND LISTENER ─────────────────────────────────────────────────────────
def command_listener():
    global last_update_id
    print("[CMD] Listener started")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook",timeout=10)
    except: pass
    while True:
        try:
            r=requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"offset":last_update_id+1,"timeout":20,"allowed_updates":["message"]},
                timeout=25,
            )
            data=r.json()
            if not data.get("ok"): time.sleep(5); continue
            for upd in data.get("result",[]):
                last_update_id=upd["update_id"]
                msg=upd.get("message",{}); text=msg.get("text","") or ""
                cid=msg.get("chat",{}).get("id"); uname=msg.get("from",{}).get("username","?")
                if not cid: continue
                print(f"  [CMD] @{uname} ID:{cid}: {text[:50]}")
                register_user(cid)
                if cid in broadcast_pending and not text.startswith("/"):
                    handle_broadcast_message(cid,msg); continue
                if text.startswith("/"): handle_command(text,cid,msg)
        except Exception as e: print(f"  [CMD] {e}")
        time.sleep(2)

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    global last_signal_scan_time,last_price_check_time,last_tick_time,last_news_check_time

    print(f"[CLEXER V6.1] Starting | {SYMBOL}")
    print(f"  TV Bridge: {TV_BRIDGE_URL or 'NOT SET — Binance-only'}")
    print(f"  Tick:{TICK_INTERVAL}s | Price:{PRICE_CHECK_INTERVAL//60}m | Signal:{SIGNAL_SCAN_INTERVAL//3600}h")
    print(f"  News: OFF by default (use /news on to enable)")
    load_users()

    if TV_BRIDGE_URL:
        if tv_update_state():
            print(f"  TV Bridge ONLINE — {'New 10-Step prompt' if is_tv_online() else 'Classic prompt'}")
        else:
            print("  TV Bridge OFFLINE — using Binance + Classic prompt")

    threading.Thread(target=command_listener,daemon=True).start()

    tv_status_line=(
        "TradingView: ONLINE (New 10-Step Prompt)\n" if is_tv_online()
        else "TradingView: OFFLINE (Classic Prompt)\n" if TV_BRIDGE_URL
        else "TradingView: Not configured — Binance only\n"
    )

    send_telegram(
        f"🤖 <b>CLEXER V6.1 Online</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"1-min:  entry/TP/SL instant alerts\n"
        f"1-hour: price check (TV or Binance)\n"
        f"4-hour: Claude SMC analysis\n"
        f"News:   OFF (use /news on)\n\n"
        f"Dual Prompt System:\n"
        f"  TV Online  → New 10-Step Framework\n"
        f"  TV Offline → Classic SMC Prompt\n\n"
        f"{tv_status_line}"
        f"Charts: {', '.join(CHART_TFS).upper()} | {'ON' if SEND_CHARTS else 'OFF'}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>— CLEXER V6.1 —</i>"
    )

    MAIN_TICK=60

    while True:
        try:
            if bot_paused.is_set(): time.sleep(MAIN_TICK); continue

            now=time.time()
            forced=force_scan.is_set()
            if forced: force_scan.clear()

            h_str=now_ist().strftime('%H:%M IST')
            print(f"\n[{h_str}] {get_session()}{' FORCED' if forced else ''} | Src:{get_current_source()} | Prompt:{'NEW' if is_tv_online() else 'OLD'}")

            # TV bridge health check
            if TV_BRIDGE_URL and (now-tv_bridge_state["last_check"])>=tv_bridge_state["check_interval"]:
                was_online=tv_bridge_state["online"]
                is_online=tv_update_state()
                if was_online and not is_online:
                    send_telegram(
                        f"⚠️ <b>TradingView Offline</b>\n\n"
                        f"Switched to Binance + Classic Prompt.\n\n<i>— CLEXER V6.1 —</i>"
                    )
                elif not was_online and is_online:
                    send_telegram(
                        f"✅ <b>TradingView Back Online</b>\n\n"
                        f"Switched to New 10-Step Prompt.\n\n<i>— CLEXER V6.1 —</i>"
                    )

            # News check (only if enabled)
            if SEND_NEWS and (now-last_news_check_time)>=NEWS_CHECK_INTERVAL:
                last_news_check_time=now
                threading.Thread(target=check_news,daemon=True).start()

            # Sleep hours
            if not forced and is_ist_sleep():
                print("  [SLEEP] 01:00–07:29 IST")
                time.sleep(MAIN_TICK); continue

            # 1-min tick check
            tick_due=(now-last_tick_time)>=TICK_INTERVAL
            if (tick_due or forced) and active_trade["signal"]:
                last_tick_time=now
                if run_tick_check():
                    forced=True; last_signal_scan_time=0

            # 1-hour price check
            price_due=(now-last_price_check_time)>=PRICE_CHECK_INTERVAL
            if price_due and active_trade["signal"]:
                last_price_check_time=now
                if run_price_check():
                    forced=True; last_signal_scan_time=0

            # Scan due?
            scan_due=(now-last_signal_scan_time)>=SIGNAL_SCAN_INTERVAL
            if not forced and not scan_due:
                time.sleep(MAIN_TICK); continue

            # Session check
            if not forced and not is_trading_hours() and not active_trade["signal"]:
                print(f"  [WAIT] {get_session()} — not London/NY")
                time.sleep(MAIN_TICK); continue

            # Cooldown
            if trade_stats["cooldown_scans"]>0 and not forced:
                trade_stats["cooldown_scans"]-=1
                remaining=trade_stats["cooldown_scans"]
                print(f"  [COOLDOWN] {remaining} scans remaining")
                if remaining==0:
                    send_telegram("🔍 <b>Cooldown over — scanning now</b>\n\n<i>— CLEXER V6.1 —</i>")
                last_signal_scan_time=now
                time.sleep(MAIN_TICK); continue

            # ── FULL CLAUDE SCAN ──────────────────────────────────────────
            last_signal_scan_time=now
            print("  Fetching candles...")
            ticker=get_ticker(); price=ticker["price"]
            print(f"  BTC: {price:,.2f} | {ticker['change']:+.2f}% | {get_session()} | {get_current_source()} | Prompt:{'NEW' if is_tv_online() else 'OLD'}")
            data=fetch_all_data()

            if not active_trade["signal"]:
                # No active trade — look for new signal
                signal=analyze_with_claude(ticker,data,validate_trade=False)
                if signal:
                    send_telegram(fmt_signal(signal))
                    set_trade(signal)
                    print(f"  [SIGNAL] {signal['signal']} R:R:{signal.get('rr','?')} Conf:{signal.get('confidence','?')}")
            else:
                # Active trade — assess current situation first
                t=active_trade
                try:
                    cur_price=get_ticker()["price"]
                    sl_dist=abs(cur_price-t["sl"])
                    tp1_dist=abs(cur_price-t["tp1"])
                    tp2_dist=abs(cur_price-t["tp2"])
                    progress=abs(cur_price-t["entry"])/abs(t["tp2"]-t["entry"])*100 if t["tp2"]!=t["entry"] else 0
                    trade_assessment=(
                        f"Active {t['signal']} @ {t['entry']:,.0f} | Current: {cur_price:,.2f}\n"
                        f"SL: {t['sl']:,.0f} ({sl_dist:.0f} pts away) | "
                        f"TP1: {t['tp1']:,.0f} ({'HIT' if t['tp1_hit'] else f'{tp1_dist:.0f} pts'}) | "
                        f"TP2: {t['tp2']:,.0f} ({tp2_dist:.0f} pts)\n"
                        f"Progress: {progress:.1f}% | Entry hit: {t['entry_hit']}"
                    )
                except: trade_assessment=""

                print(f"  Validating: {t['signal']} @ {t['entry']:,.0f}")
                signal=analyze_with_claude(ticker,data,validate_trade=True,trade_assessment=trade_assessment)

                if signal is None:
                    print("  [VALIDATE] Trade remains valid")
                    if forced:
                        send_telegram(
                            f"✅ <b>Trade Validated</b>  {ist_str()}\n\n"
                            f"{'🟢' if t['signal']=='BUY' else '🔴'} {t['signal']} @ {t['entry']:,.0f}\n"
                            f"Structure intact. TP2: <b>{t['tp2']:,.0f}</b>\n"
                            f"Source: {get_current_source()}\n\n<i>— CLEXER V6.1 —</i>"
                        )
                elif signal["signal"]!=t["signal"]:
                    old_info=f"{t['signal']} @ {t['entry']:,.0f}"
                    send_telegram(
                        f"🔄 <b>STRUCTURE FLIP!</b>  {ist_str()}\n\n"
                        f"Closing: {old_info}\n"
                        f"New: {signal['signal']} @ {signal['entry']:,.0f}\n\n"
                        f"<i>{signal.get('old_trade_assessment','')[:150]}</i>\n\n"
                        f"<i>— CLEXER V6.1 —</i>"
                    )
                    reset_trade("STRUCTURE FLIP")
                    time.sleep(1)
                    send_telegram(fmt_signal(signal))
                    set_trade(signal)
                else:
                    print(f"  [VALIDATE] Same direction — no change")
                    if forced:
                        send_telegram(
                            f"📊 <b>Analysis Update</b>\n\n"
                            f"Running: {t['signal']} @ {t['entry']:,.0f}\n"
                            f"Same bias confirmed.\n\n<i>— CLEXER V6.1 —</i>"
                        )

        except KeyboardInterrupt:
            print("\n[BOT] Stopped.")
            send_telegram("🛑 <b>CLEXER V6.1 Stopped</b>\n\n<i>— CLEXER —</i>")
            break
        except Exception as e:
            print(f"  [MAIN ERROR] {e}")
            import traceback; traceback.print_exc()

        time.sleep(MAIN_TICK)

if __name__=="__main__":
    main()
