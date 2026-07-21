"""
CLEXER V17.8.5 — BingX Copy Trade System
──────────────────────────────────────
Standalone module. Import into bot.py.

SETUP:
  Railway env vars needed:
    CT_ENCRYPT_KEY = any random 32-char string (for API key encryption)

  pip install cryptography  (add to requirements.txt)

INTEGRATION HOOKS (add to bot.py at each event):
  Signal sent      → ct.on_signal(signal, price)
  TP1 hit          → ct.on_tp1(entry)
  TP2 hit          → ct.on_tp2()
  SL hit           → ct.on_sl()
  Entry missed     → ct.on_cancel_limits()
  Setup invalid    → ct.on_cancel_limits()
  Admin /close     → ct.on_close_all()
  Admin /sltobe    → ct.on_sl_to_be(entry)
  Admin /setsl X   → ct.on_update_sl(new_sl)
  Structure flip   → ct.on_close_all() then ct.on_signal(new_signal, price)
"""

import os, json, time, hmac, hashlib, base64, requests, threading
from datetime import datetime, timezone, timedelta

_SMALLCAPS_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyz",
    "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ"
)

def _sc(text: str) -> str:
    """'Trade Size Set' -> 'Tʀᴀᴅᴇ Sɪᴢᴇ Sᴇᴛ' — first letter of each word stays a
    normal capital, the rest render in small-caps unicode glyphs. Acronyms
    (BingX, USDT, ...) that are already all-uppercase are left untouched."""
    words = text.split(" ")
    out = []
    for w in words:
        if not w:
            out.append(w); continue
        letters = [c for c in w if c.isalpha()]
        if letters and len(letters) > 1 and all(c.isupper() for c in letters):
            out.append(w)
        else:
            out.append(w[0].upper() + w[1:].lower().translate(_SMALLCAPS_MAP))
    return " ".join(out)

try:
    from cryptography.fernet import Fernet
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False
    print("[CT] cryptography not installed — API keys stored base64 only. Run: pip install cryptography")

_DATA_DIR      = os.getenv("DATA_DIR", ".")
CT_FILE        = os.path.join(_DATA_DIR, "copy_users.json")
CT_ENCRYPT_KEY = os.getenv("CT_ENCRYPT_KEY", "")
# Shared cross-server sync — same api.py service every main/co-server points at.
# When set, the users DB is pulled from / pushed to this central store instead of
# (in addition to) the local JSON file, so multiple Railway deployments always
# see the same copy-on/off, tier, and API-key state regardless of which one is
# currently the active trading server.
_API_URL       = os.getenv("CLEXER_API_URL", "").rstrip("/")
_API_SECRET    = os.getenv("PUSH_STATE_SECRET", "")

def _central_get(path: str, timeout: int = 8, retries: int = 3, delay: float = 2.5):
    """GET from _API_URL with a couple of retries — several startup pulls firing
    in quick succession can hit a freshly-started Postgres before it's fully
    ready (transient 503), which would otherwise silently start this server
    blank instead of restoring its real shared users DB."""
    if not _API_URL:
        return None
    hdrs = {"X-Push-Secret": _API_SECRET} if _API_SECRET else {}
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(f"{_API_URL}{path}", headers=hdrs, timeout=timeout)
            if r.ok:
                return r
            last_err = f"HTTP {r.status_code} — {r.text[:150]}"
            if r.status_code < 500:
                return r
        except Exception as e:
            last_err = str(e)
        if attempt < retries - 1:
            time.sleep(delay)
    print(f"[CT] central GET {path} failed after {retries} attempts: {last_err}")
    return None

def _kv_pick_newer(local_path: str, kv_body: dict, log_tag: str):
    """Compare a /kv/{key} response's updated_at against local_path's mtime —
    return the central data only if it's actually newer (or local doesn't
    exist yet); otherwise None, so a stale central pull can't silently
    clobber a local change made after the last /syncup."""
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

BINGX_BASE     = "https://open-api.bingx.com"
BINGX_SYMBOL   = "BTC-USDT"
IST            = timedelta(hours=5, minutes=30)
TP1_CLOSE_PCT = 50  # % of the position closed at TP1 — rest rides to TP2. Set via /tp1size (tap or type).

def _tp1_split(qty: float) -> tuple[float, float]:
    """Splits a position size into (tp1_qty, tp2_qty) per TP1_CLOSE_PCT."""
    tp1_qty = max(round(qty * (TP1_CLOSE_PCT / 100), 4), 0.0001)
    tp2_qty = max(round(qty - tp1_qty, 4), 0.0001)
    return tp1_qty, tp2_qty

SCAN_CT_ENABLED = True   # legacy combined flag — kept for backward compat, no longer gates anything
BTC_CT_ENABLED   = True  # toggle with /ctpause btc|on|off — copy trade for BTC signals
SCAN1_CT_ENABLED = True  # toggle with /ctpause scan1|on|off — copy trade for Scan1 signals
SCAN2_CT_ENABLED = True  # toggle with /ctpause scan2|on|off — copy trade for Scan2 signals
DEMO1_CT_ENABLED = False  # toggle from Copy Trade By Type screen — copy trade for Demo Scan1 signals
DEMO2_CT_ENABLED = False  # toggle from Copy Trade By Type screen — copy trade for Demo Scan2 signals

def _now_ist() -> str:
    return (datetime.now(timezone.utc) + IST).strftime("%d %b %I:%M %p IST")

# ─── ENCRYPTION ───────────────────────────────────────────────────────────────

def _get_fernet():
    if not HAS_CRYPTO or not CT_ENCRYPT_KEY:
        return None
    try:
        # Pad/trim key to 32 bytes, then base64url-encode for Fernet
        raw = CT_ENCRYPT_KEY.encode("utf-8")[:32].ljust(32, b"\x00")
        return Fernet(base64.urlsafe_b64encode(raw))
    except Exception as e:
        print(f"[CT] Fernet init error: {e}")
        return None

def _encrypt(plain: str) -> str:
    f = _get_fernet()
    if f:
        return f.encrypt(plain.encode()).decode()
    return base64.b64encode(plain.encode()).decode()   # fallback: not secure

def _decrypt(enc: str) -> str:
    if not enc:
        return ""
    f = _get_fernet()
    try:
        if f:
            return f.decrypt(enc.encode()).decode()
        return base64.b64decode(enc.encode()).decode()
    except Exception as e:
        print(f"[CT] Decrypt error: {e}")
        return ""

# ─── CLAUDE AI HELPER ────────────────────────────────────────────────────────

def _ask_claude_action(situation: str) -> dict:
    """
    Ask Claude for a structured action to execute immediately.
    Returns dict like:
      {"action": "place_sl", "sl_price": 63000}
      {"action": "place_sl_tp", "sl_price": 63000, "tp1_price": 67000, "tp2_price": 68000}
      {"action": "close_position"}
      {"action": "hold", "reason": "position looks safe"}
    """
    try:
        import anthropic, os, json as _json
        key = os.getenv("ANTHROPIC_API_KEY", "")
        if not key:
            return {"action": "hold", "reason": "no API key"}
        client = anthropic.Anthropic(api_key=key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": (
                    "You are an autonomous crypto risk manager for a BingX perpetual futures bot. "
                    "Respond ONLY with a JSON object — no explanation, no markdown.\n\n"
                    "Available actions:\n"
                    '{"action":"place_sl","sl_price":<number>}  — place stop loss at price\n'
                    '{"action":"place_sl_tp","sl_price":<n>,"tp1_price":<n>,"tp2_price":<n>}  — place SL + TPs\n'
                    '{"action":"close_position"}  — market close immediately\n'
                    '{"action":"hold","reason":"<short reason>"}  — do nothing\n\n'
                    f"Situation: {situation}"
                )
            }]
        )
        text = msg.content[0].text.strip()
        # Extract JSON from response
        import re as _re
        m = _re.search(r'\{.*\}', text, _re.DOTALL)
        if m:
            return _json.loads(m.group())
        return {"action": "hold", "reason": f"could not parse: {text[:80]}"}
    except Exception as e:
        return {"action": "hold", "reason": f"Claude error: {e}"}


def _execute_claude_action(action: dict, ak: str, ask: str, sym: str,
                            pos_side: str, pos_amt: float, notify_fn=None,
                            uname: str = "?", avg_price: float = 0, user: dict = None,
                            sl: float = 0, tp1: float = 0, tp2: float = 0):
    """
    Execute the action Claude recommended.
    Pass sl/tp1/tp2 directly — do NOT rely on user state (can be stale from other signals).
    """
    close_side = "SELL" if pos_side == "LONG" else "BUY"
    act = action.get("action", "hold")
    result = ""

    # Use explicitly passed prices first, fall back to user state only if not provided
    stored_sl  = sl  or float(user.get("scan_sl",  0) if user else 0) or float(user.get("sl",  0) if user else 0)
    stored_tp1 = tp1 or float(user.get("scan_tp1", 0) if user else 0) or float(user.get("tp1", 0) if user else 0)
    stored_tp2 = tp2 or float(user.get("scan_tp2", 0) if user else 0) or float(user.get("tp2", 0) if user else 0)


    # Only use risk-based SL if NO stored SL at all (orphan with zero signal data)
    def _fallback_sl() -> float:
        if not avg_price or not user: return 0
        risk = float(user.get("risk_usdt", 0.5))
        sl_dist = risk / pos_amt if pos_amt else avg_price * 0.02
        return round(avg_price - sl_dist, 6) if pos_side == "LONG" else round(avg_price + sl_dist, 6)

    def _fallback_tp(sl_price: float) -> tuple:
        if not avg_price or not sl_price: return 0, 0
        d = abs(avg_price - sl_price)
        if pos_side == "LONG":
            return round(avg_price + 2*d, 6), round(avg_price + 4*d, 6)
        return round(avg_price - 2*d, 6), round(avg_price - 4*d, 6)

    if act == "close_position":
        r = _bingx("POST", "/openApi/swap/v2/trade/closePosition", ak, ask,
                   {"symbol": sym, "positionSide": pos_side})
        if r.get("code") != 0:
            r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                "symbol": sym, "side": close_side, "positionSide": pos_side,
                "type": "MARKET", "quantity": round(pos_amt, 4)
            })
        ok = r.get("code") == 0
        result = f"{'✅' if ok else '❌'} CLOSED {sym} @{uname}: {r.get('msg','') or 'ok'}"

    elif act in ("place_sl", "place_sl_tp"):
        # Priority: stored signal SL → fallback risk-based
        sl_price = stored_sl or _fallback_sl()
        if sl_price:
            r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                "symbol": sym, "side": close_side, "positionSide": pos_side,
                "type": "STOP_MARKET", "quantity": round(pos_amt, 4),
                "stopPrice": sl_price,
            })
            ok = r.get("code") == 0
            result += f"{'✅' if ok else '❌'} SL@{sl_price} {r.get('msg','') or 'ok'} "

        half = max(round(pos_amt / 2, 4), 0.0001)
        fb_tp1, fb_tp2 = _fallback_tp(sl_price)
        tp1_price = stored_tp1 or fb_tp1
        tp2_price = stored_tp2 or fb_tp2
        for tp_price, tp_type in [(tp1_price, "TP1"), (tp2_price, "TP2")]:
            if tp_price:
                r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                    "symbol": sym, "side": close_side, "positionSide": pos_side,
                    "type": "TAKE_PROFIT_MARKET", "quantity": half,
                    "stopPrice": tp_price,
                })
                ok = r.get("code") == 0
                result += f"{'✅' if ok else '❌'} {tp_type}@{tp_price} "

    elif act == "hold":
        result = f"⏸ HOLD: {action.get('reason','')}"

    print(f"[CT] Claude action on {sym}: {act} → {result}")
    if notify_fn and result:
        notify_fn(f"🤖 <b>Claude acted on {sym} @{uname}</b>\n{result}")


# ─── USER DATABASE ────────────────────────────────────────────────────────────

_db: dict = {}        # str(chat_id) → user_dict
_lock = threading.Lock()
_scan_signal_lock = threading.Lock()   # one scan trade placed at a time — prevents race condition
_last_signal: dict = {}   # last active signal — cleared on SL/TP/cancel
_SIGNAL_FILE = os.path.join(_DATA_DIR, "ct_last_signal.json")

def _save_last_signal():
    """Local file only — central sync is manual, via /syncup."""
    try:
        with open(_SIGNAL_FILE, "w") as f:
            json.dump(_last_signal, f)
    except Exception as e:
        print(f"[CT] signal save error: {e}")

def _load_last_signal():
    global _last_signal
    try:
        d = None
        r = _central_get("/kv/ct_last_signal")
        if r is not None and r.ok:
            d = _kv_pick_newer(_SIGNAL_FILE, r.json(), "CT-SIGNAL")
        if d is None and os.path.exists(_SIGNAL_FILE):
            with open(_SIGNAL_FILE) as f:
                d = json.load(f)
        if d is not None:
            _last_signal = d
            if _last_signal:
                print(f"[CT] Restored last signal: {_last_signal.get('side')} entry={_last_signal.get('entry')}")
    except Exception as e:
        print(f"[CT] signal load error: {e}")

def _default_user(username: str = "?") -> dict:
    return {
        "username":       username,
        "api_key_enc":    "",
        "api_secret_enc": "",
        "connected":      False,
        "copy_on":        False,
        "size_usdt":      50.0,
        "leverage":       10,
        "risk_usdt":      None,   # if set, auto-calculates leverage per trade based on SL distance
        "sl_order_id":    "",    # BingX order ID of current SL order
        "tp_order_id":    "",    # BingX order ID of current TP2 order
        "limit_order_id": "",    # BingX order ID of pending limit entry
        "in_position":    False,
        "pos_side":       "",    # "BUY" or "SELL"
        "pos_qty":        0.0,   # full position qty in BTC (set on entry)
        "history":        {"total": 0, "profit": 0, "loss": 0,
                           "total_pnl": 0.0, "won_usdt": 0.0, "lost_usdt": 0.0},
        "paused_by_admin": False,
        "joined":         _now_ist(),
        "tier":           "free",  # "vip" or "free" — set via /setvip (admin) or a real VIP payment; every new user starts Free
        "vip_start":      "",     # "DD.MM.YYYY" — only meaningful when tier == "vip" with an expiry
        "vip_end":        "",     # "DD.MM.YYYY" — VIP auto-downgrades to free after this date
        "vip_grace_notified_at": 0,  # set once the 24h renew-or-removed reminder has been sent
    }


def _calc_auto_leverage(size_usdt: float, risk_usdt: float, entry: float, sl: float) -> int:
    """
    Auto-calculate leverage so that max loss at SL = risk_usdt.
    Formula: leverage = risk_usdt / (size_usdt × sl_pct)
    Clamped to 1–125x. Rounded down for safety.
    """
    if entry <= 0 or sl <= 0 or size_usdt <= 0 or risk_usdt <= 0:
        return 10  # safe fallback
    sl_pct = abs(entry - sl) / entry
    if sl_pct <= 0:
        return 10
    lev = risk_usdt / (size_usdt * sl_pct)
    lev = max(1, min(125, int(lev)))  # clamp 1–125, round down
    return lev

def _lev_display(user: dict) -> str:
    """Leverage line for status screens. In auto-risk mode there's no fixed
    leverage — it's recalculated per trade from that trade's SL distance — so
    showing the stale manual-mode `leverage` field there is misleading."""
    risk = user.get("risk_usdt")
    if risk:
        return f"{_sc('Auto-risk')}: <b>${risk}</b> {_sc('max loss (leverage varies per trade)')}"
    return f"{_sc('Leverage')}: <b>{user.get('leverage', 10)}x</b>"

def load():
    global _db
    # Central store first (shared across every server pointed at the same
    # CLEXER_API_URL) — falls back to the local file if unreachable/unset,
    # so the bot still works standalone with no central store configured.
    r = _central_get("/kv/ct_users")
    if r is not None and r.ok:
        _central_data = _kv_pick_newer(CT_FILE, r.json(), "CT")
        if _central_data is not None:
            _db = _central_data
            print(f"[CT] Loaded {len(_db)} copy users from central store")
            _load_last_signal()
            return
    try:
        if os.path.exists(CT_FILE):
            with open(CT_FILE) as f:
                _db = json.load(f)
            print(f"[CT] Loaded {len(_db)} copy users from local file")
    except Exception as e:
        print(f"[CT] Load error: {e}"); _db = {}
    _load_last_signal()

def _save():
    """Local file only — central sync is manual, via /syncup."""
    try:
        with open(CT_FILE, "w") as f:
            json.dump(_db, f, indent=2)
    except Exception as e:
        print(f"[CT] Save error: {e}")

def push_to_central() -> bool:
    """Force-push the current users DB to the central store. Called by /syncup.
    Raises if the server didn't actually accept it (e.g. secret mismatch = 403) —
    caller must not treat a non-2xx response as success."""
    if not _API_URL:
        return False
    hdrs = {"X-Push-Secret": _API_SECRET} if _API_SECRET else {}
    r = requests.post(f"{_API_URL}/kv/ct_users", json=_db, headers=hdrs, timeout=15)
    if not r.ok:
        raise Exception(f"HTTP {r.status_code} — {r.text[:150]}")
    return True

def _get(cid: str) -> dict:
    return _db.get(str(cid), {})

def _set(cid: str, user: dict):
    with _lock:
        _db[str(cid)] = user
        _save()
        try:
            push_to_central()
        except Exception as e:
            print(f"[CT] immediate central push error: {e}")

def active_count() -> int:
    return sum(1 for u in _db.values() if u.get("copy_on") and u.get("connected") and not u.get("paused_by_admin"))

def active_ids() -> list:
    return [cid for cid, u in _db.items() if u.get("copy_on") and u.get("connected") and not u.get("paused_by_admin")]

def reset_history(cid: str):
    """Zero out a user's copy-trade P&L history. Their connection/settings stay untouched."""
    user = _get(cid)
    if not user:
        return
    user["history"] = {"total": 0, "profit": 0, "loss": 0, "total_pnl": 0.0, "won_usdt": 0.0, "lost_usdt": 0.0}
    _set(cid, user)

def has_active_signal() -> bool:
    return bool(_last_signal)

# ─── BINGX API CLIENT ─────────────────────────────────────────────────────────


def _bingx(method: str, path: str, api_key: str, api_secret: str, params: dict = None) -> dict:
    from urllib.parse import urlencode
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    # Build query string the same way for both signing and URL — avoids requests re-encoding mismatch
    query = urlencode(sorted(params.items()))
    sig = hmac.new(api_secret.strip().encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    url = f"{BINGX_BASE}{path}?{query}&signature={sig}"
    headers = {"X-BX-APIKEY": api_key.strip()}
    try:
        if method == "GET":
            r = requests.get(url, headers=headers, timeout=15)
        elif method == "POST":
            r = requests.post(url, headers=headers, timeout=15)
        elif method == "DELETE":
            r = requests.delete(url, headers=headers, timeout=15)
        else:
            return {"code": -1, "msg": "unknown method"}
        return r.json()
    except Exception as e:
        return {"code": -1, "msg": str(e)}

def _test_api(api_key: str, api_secret: str) -> tuple[bool, str]:
    result = _bingx("GET", "/openApi/swap/v2/user/balance", api_key, api_secret, {})
    if result.get("code") == 0:
        return True, ""
    return False, result.get("msg", "invalid key")

def _get_balance(api_key: str, api_secret: str) -> dict:
    """Fetches real BingX USDT-M futures balance. Returns {} on any failure —
    callers must treat that as 'couldn't fetch', not 'balance is zero'."""
    r = _bingx("GET", "/openApi/swap/v2/user/balance", api_key, api_secret, {})
    if r.get("code") != 0:
        return {}
    bal = ((r.get("data") or {}).get("balance")) or {}
    if not bal:
        return {}
    try:
        return {
            "balance":   float(bal.get("balance", 0) or 0),
            "equity":    float(bal.get("equity", bal.get("balance", 0)) or 0),
            "available": float(bal.get("availableMargin", 0) or 0),
            "used":      float(bal.get("usedMargin", 0) or 0),
            "unrealized":float(bal.get("unrealizedProfit", 0) or 0),
        }
    except (TypeError, ValueError):
        return {}

def sync_all_balances():
    """Fetches and caches real BingX balance for every connected user (not just
    copy_on ones — Portfolio balance should show regardless of copy trade
    state) into their ct_users record, so api.py's Mini App endpoint can serve
    it without needing live BingX credentials itself (api.py can't decrypt
    ct_users' api_key_enc — different encryption key from its own table)."""
    for cid, user in list(_db.items()):
        if not user.get("connected") or not user.get("api_key_enc"):
            continue
        try:
            api_key = _decrypt(user["api_key_enc"]); api_secret = _decrypt(user["api_secret_enc"])
            bal = _get_balance(api_key, api_secret)
            if not bal:
                continue
            user["balance_usdt"]    = bal["equity"]
            user["avail_usdt"]      = bal["available"]
            user["intrade_usdt"]    = bal["used"]
            user["unrealized_usdt"] = bal["unrealized"]
            user["balance_updated_at"] = (datetime.now(timezone.utc) + IST).strftime("%Y-%m-%d %H:%M")
            _set(cid, user)
        except Exception as e:
            print(f"[CT] sync_all_balances {cid}: {e}")

def start_balance_sync_loop(interval_seconds: int = 60):
    """Background thread — refreshes every connected user's cached BingX
    balance on an interval, so the Mini App's Portfolio balance/equity curve
    is never more than ~1 minute stale."""
    import threading as _th
    def _loop():
        time.sleep(20)  # initial delay to let bot fully start
        while True:
            try:
                sync_all_balances()
            except Exception as e:
                print(f"[CT] balance sync loop error: {e}")
            time.sleep(interval_seconds)
    t = _th.Thread(target=_loop, daemon=True)
    t.start()
    print(f"[CT] Balance sync started — refreshes every {interval_seconds}s")

def _set_leverage(api_key: str, api_secret: str, side: str, leverage: int) -> bool:
    pos_side = "LONG" if side == "BUY" else "SHORT"
    r = _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret, {
        "symbol": BINGX_SYMBOL, "side": pos_side, "leverage": leverage,
    })
    return r.get("code") == 0

def _place_order(api_key: str, api_secret: str, side: str, order_type: str,
                 quantity: float, price: float = 0, stop_price: float = 0,
                 position_side: str = "") -> dict:
    pos_side = position_side if position_side else ("LONG" if side == "BUY" else "SHORT")
    params = {
        "symbol":       BINGX_SYMBOL,
        "side":         side,
        "positionSide": pos_side,
        "type":         order_type,
        "quantity":     round(quantity, 4),
    }
    if order_type == "LIMIT" and price:
        params["price"] = round(price, 1)
        params["timeInForce"] = "GTC"
    if stop_price and order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
        params["stopPrice"] = round(stop_price, 1)
    return _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, params)

def _sl_json(sl: float) -> str:
    """Position-level SL embedded in order — shows in Positions tab TP/SL column."""
    import json as _json
    return _json.dumps({
        "type":        "MARK_PRICE",
        "stopPrice":   str(round(sl, 1)),
        "price":       str(round(sl, 1)),
        "workingType": "MARK_PRICE",
    })

def _set_position_sl(api_key: str, api_secret: str, pos_side: str, sl: float) -> dict:
    """Set position-level SL via BingX positionTPSL endpoint (does not affect TP2 order)."""
    import json as _json
    from urllib.parse import urlencode, quote
    sl_payload = _json.dumps({
        "type": "MARK_PRICE",
        "stopPrice": str(round(sl, 2)),
        "price": "0",
        "workingType": "MARK_PRICE",
    }, separators=(",", ":"))
    # Sign only the non-nested params, then append stopLoss url-encoded separately
    base_params = {
        "symbol":       BINGX_SYMBOL,
        "positionSide": pos_side,
        "timestamp":    int(time.time() * 1000),
    }
    query = urlencode(sorted(base_params.items()))
    # Include stopLoss in signature string as-is
    sign_str = query + "&stopLoss=" + sl_payload
    sig = hmac.new(api_secret.strip().encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha256).hexdigest()
    url = (f"{BINGX_BASE}/openApi/swap/v2/trade/positionTPSL"
           f"?{query}&stopLoss={quote(sl_payload)}&signature={sig}")
    headers = {"X-BX-APIKEY": api_key.strip()}
    try:
        r = requests.post(url, headers=headers, timeout=15)
        return r.json()
    except Exception as e:
        return {"code": -1, "msg": str(e)}

def _cancel_order(api_key: str, api_secret: str, order_id: str) -> dict:
    if not order_id:
        return {"code": 0}
    return _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret, {
        "symbol": BINGX_SYMBOL, "orderId": order_id,
    })

def _cancel_all_orders(api_key: str, api_secret: str) -> dict:
    return _bingx("DELETE", "/openApi/swap/v2/trade/allOpenOrders", api_key, api_secret, {
        "symbol": BINGX_SYMBOL,
    })

def _close_position(api_key: str, api_secret: str, side: str) -> dict:
    pos_side = "LONG" if side == "BUY" else "SHORT"
    return _bingx("POST", "/openApi/swap/v2/trade/closePosition", api_key, api_secret, {
        "symbol": BINGX_SYMBOL, "positionSide": pos_side,
    })

def _get_position(api_key: str, api_secret: str) -> dict:
    r = _bingx("GET", "/openApi/swap/v2/user/positions", api_key, api_secret, {
        "symbol": BINGX_SYMBOL,
    })
    if r.get("code") == 0:
        data = r.get("data") or {}
        positions = data if isinstance(data, list) else data.get("positions", [])
        for pos in positions:
            if isinstance(pos, dict) and abs(float(pos.get("positionAmt", 0))) > 0:
                return pos
    return {}

def _get_all_positions(api_key: str, api_secret: str) -> list:
    """Same as _get_position but across EVERY symbol, not just BTC-USDT —
    _get_position alone misses any open Scan1/Scan2/TS1/TS2 copytrade position
    (those open on whatever coin the scan picked, never BTC), which was making
    /mytrade report 'No Open Position' for users who actually had live
    positions on BingX from scan-based copytrade."""
    r = _bingx("GET", "/openApi/swap/v2/user/positions", api_key, api_secret, {})
    out = []
    if r.get("code") == 0:
        data = r.get("data") or {}
        positions = data if isinstance(data, list) else data.get("positions", [])
        for pos in positions:
            if isinstance(pos, dict) and abs(float(pos.get("positionAmt", 0))) > 0:
                out.append(pos)
    return out

def _calc_qty(size_usdt: float, price: float, leverage: int) -> float:
    if price <= 0: return 0.001
    qty = (size_usdt * leverage) / price
    return max(round(qty, 4), 0.001)

def _min_qty_risk_note(size_usdt: float, price: float, leverage: int) -> str:
    """BingX won't place less than 0.001 BTC. If a user's margin/leverage combo
    calls for a smaller quantity than that, the bot silently rounds UP to 0.001 —
    which means the real position (and real max loss at SL) ends up bigger than
    their risk setting promised. Returns a warning string when that's happening,
    empty string otherwise."""
    if price <= 0 or leverage <= 0:
        return ""
    raw_qty = (size_usdt * leverage) / price
    if raw_qty >= 0.001:
        return ""
    real_margin = round((0.001 * price) / leverage, 2)
    return (f"⚠️ Margin ${size_usdt} at {leverage}x is below BingX's 0.001 BTC minimum order size — "
            f"the bot had to use ~${real_margin} effective margin instead, so the real max loss on "
            f"this trade will be higher than the risk setting targets.")

def _calc_pnl(side: str, entry: float, close_price: float, qty: float) -> float:
    if entry <= 0 or close_price <= 0 or qty <= 0: return 0.0
    raw = (close_price - entry) * qty if side == "BUY" else (entry - close_price) * qty
    return round(raw, 4)

def _record_pnl(user: dict, pnl: float, symbol: str = "BTC-USDT", side: str = "", result: str = ""):
    h = user.setdefault("history", {"total":0,"profit":0,"loss":0,
                                     "total_pnl":0.0,"won_usdt":0.0,"lost_usdt":0.0})
    # backfill missing keys for old users
    h.setdefault("total_pnl", 0.0); h.setdefault("won_usdt", 0.0); h.setdefault("lost_usdt", 0.0)
    h["total_pnl"] = round(h["total_pnl"] + pnl, 4)
    if pnl >= 0: h["won_usdt"]  = round(h["won_usdt"]  + pnl, 4)
    else:        h["lost_usdt"] = round(h["lost_usdt"] + abs(pnl), 4)
    # Per-trade closed-trade log — the Mini App's Portfolio "Recent Closed"
    # list, daily P/L bars, and equity curve had no per-user data source at
    # all before this (they read the bot's global stats, same for everyone).
    # Capped at 50 most recent closes per user.
    log = user.setdefault("trade_log", [])
    log.append({
        "symbol": symbol, "side": side, "pnl": pnl,
        "result": result or ("WIN" if pnl >= 0 else "LOSS"),
        "closed_at": (datetime.now(timezone.utc) + IST).strftime("%Y-%m-%d %H:%M"),
    })
    if len(log) > 50: del log[:-50]

# ─── COPY TRADE MIRROR ACTIONS ────────────────────────────────────────────────

_is_active = True  # set by bot.py from is_active_server() — False = standby,
                   # this server must never place/modify/close a real BingX order.

def _users_with_copy(share_free: bool = True) -> list[tuple[str, dict, str, str]]:
    """Yield (cid, user, api_key, api_secret) for all active copy users.
    share_free=False excludes free-tier users — used when a signal didn't make
    today's free-channel quota, so free users only ever copy what free channels got.
    Returns nothing at all while this server is in standby (multi-server failover)."""
    if not _is_active:
        return []
    out = []
    for cid, user in list(_db.items()):
        if not user.get("copy_on") or not user.get("connected") or user.get("paused_by_admin"):
            continue
        if not share_free and user.get("tier", "free") == "free":
            continue
        try:
            out.append((cid, user, _decrypt(user["api_key_enc"]), _decrypt(user["api_secret_enc"])))
        except Exception as e:
            print(f"[CT] decrypt error {cid}: {e}")
    return out

def on_signal(signal: dict, price: float, share_free: bool = True) -> list[str]:
    """
    Called when bot generates BUY/SELL signal.
    MARKET entry  → open position + set SL + set TP2
    PULLBACK entry → place limit order at entry level
    share_free: whether today's free-channel quota was met for this signal —
    free-tier users only copy when True.
    Returns list of result strings for admin notification.
    """
    global _last_signal
    if not BTC_CT_ENABLED:
        return ["[CT] BTC copy trade is OFF"]
    side        = signal["signal"]          # "BUY" or "SELL"
    entry       = float(signal["entry"])
    sl          = float(signal["sl"])
    tp1         = float(signal["tp1"])
    tp2         = float(signal["tp2"])
    entry_type  = signal.get("entry_type", "MARKET")
    close_side  = "SELL" if side == "BUY" else "BUY"
    # positionSide for close orders — opposite of order side in hedge mode
    trade_ps    = "LONG" if side == "BUY" else "SHORT"
    results     = []

    # Save last signal for /ctretry
    _last_signal = {
        "signal":     signal,
        "price":      price,
        "entry_type": entry_type,
        "side":       side,
        "entry":      entry,
        "sl":         sl,
        "tp2":        float(signal.get("tp2", 0)),
        "time":       _now_ist(),
    }
    _save_last_signal()

    for cid, user, api_key, api_secret in _users_with_copy(share_free):
        try:
            # Skip if user blocked BTC copy (/nocopy BTC)
            nocopy = set(user.get("nocopy_coins", []))
            if "BTC" in nocopy or "BTC-USDT" in nocopy:
                print(f"[CT] {cid} nocopy BTC — skipping")
                continue
            risk = user.get("risk_usdt")
            if risk:
                lev = _calc_auto_leverage(user["size_usdt"], risk, entry, sl)
                print(f"[CT] {cid} auto-leverage: risk=${risk} size=${user['size_usdt']} SL%={abs(entry-sl)/entry*100:.2f}% → {lev}x")
            else:
                lev = user.get("leverage", 10)
            qty = _calc_qty(user["size_usdt"], price, lev)
            _set_leverage(api_key, api_secret, side, lev)
            _qty_note = _min_qty_risk_note(user["size_usdt"], price, lev)

            if entry_type == "MARKET":
                pos_side_entry = "LONG" if side == "BUY" else "SHORT"
                r = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, {
                    "symbol":       BINGX_SYMBOL,
                    "side":         side,
                    "positionSide": pos_side_entry,
                    "type":         "MARKET",
                    "quantity":     round(qty, 4),
                })
                if r.get("code") == 0:
                    uname = user.get("username", "?")
                    warnings = [f"{_qty_note} (@{uname})"] if _qty_note else []
                    tp1_qty, tp2_qty = _tp1_split(qty)

                    # ── SL order — full qty STOP_MARKET ──
                    sl_r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                                        qty, stop_price=sl, position_side=trade_ps)
                    sl_ok  = sl_r.get("code") == 0
                    sl_oid = str((sl_r.get("data") or {}).get("order", {}).get("orderId", ""))
                    if not sl_ok:
                        warnings.append(f"⚠️ SL FAILED @{uname}: {sl_r.get('msg','?')}")

                    # ── TP1 order — TP1_CLOSE_PCT% of qty at tp1 price ──
                    tp1_r  = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                          tp1_qty, stop_price=tp1, position_side=trade_ps)
                    tp1_ok = tp1_r.get("code") == 0
                    tp1_oid = str((tp1_r.get("data") or {}).get("order", {}).get("orderId", ""))
                    if not tp1_ok:
                        warnings.append(f"⚠️ TP1 FAILED @{uname}: {tp1_r.get('msg','?')}")

                    # ── TP2 order — remaining qty at tp2 price ──
                    tp2_r  = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                          tp2_qty, stop_price=tp2, position_side=trade_ps)
                    tp2_ok = tp2_r.get("code") == 0
                    tp2_oid = str((tp2_r.get("data") or {}).get("order", {}).get("orderId", ""))
                    if not tp2_ok:
                        warnings.append(f"⚠️ TP2 FAILED @{uname}: {tp2_r.get('msg','?')}")

                    user["in_position"]    = True
                    user["pos_side"]       = side
                    user["pos_qty"]        = qty
                    user["sl_order_id"]    = sl_oid
                    user["tp_order_id"]    = tp2_oid
                    user["tp1_order_id"]   = tp1_oid
                    user["limit_order_id"] = ""
                    user["failed_copy"]    = False
                    _set(cid, user)

                    status = f"SL:{'✅' if sl_ok else '❌'} TP1:{'✅' if tp1_ok else '❌'} TP2:{'✅' if tp2_ok else '❌'}"
                    results.append(f"✅ @{uname} {side} {qty} BTC | {status}")
                    for w in warnings:
                        results.append(w)
                    print(f"[CT] on_signal {cid}: {status}")
                else:
                    user["failed_copy"] = True
                    _set(cid, user)
                    results.append(f"❌ @{user.get('username','?')}: {r.get('msg','?')}")

            else:  # PULLBACK — place limit order
                r = _place_order(api_key, api_secret, side, "LIMIT", qty, price=entry)
                if r.get("code") == 0:
                    oid = str((r.get("data") or {}).get("order", {}).get("orderId", ""))
                    user["in_position"]    = False
                    user["pos_side"]       = side
                    user["pos_qty"]        = qty
                    user["limit_order_id"] = oid
                    user["sl_order_id"]    = ""
                    user["tp_order_id"]    = ""
                    user["failed_copy"]    = False
                    _set(cid, user)
                    results.append(f"✅ @{user.get('username','?')} limit {side} {qty} BTC @ {entry:,.0f}")
                else:
                    user["failed_copy"] = True
                    _set(cid, user)
                    results.append(f"❌ @{user.get('username','?')}: {r.get('msg','?')}")

        except Exception as e:
            user["failed_copy"] = True
            _set(cid, user)
            results.append(f"❌ @{user.get('username','?')}: {e}")
            print(f"[CT] on_signal {cid}: {e}")

    print(f"[CT] on_signal: {len(results)} users → {results}")
    return results


def on_tp1(entry: float, tp1: float = 0):
    """TP1 hit — move SL to breakeven on the remaining position.
    BingX's own TP1 TAKE_PROFIT_MARKET order (placed at entry time) already closes
    50% automatically the instant price touches it. We only close manually as a
    fallback if that order somehow didn't fire — never unconditionally, or we'd
    double-close (BingX's 50% + our own 50% = the whole position gone)."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if not user.get("in_position"): continue
        try:
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            pos_side   = "LONG" if user["pos_side"] == "BUY" else "SHORT"
            full_qty   = user.get("pos_qty", 0.001)
            tp1_qty, tp2_qty = _tp1_split(full_qty)

            # Cancel TP1 order and OLD SL order first
            _cancel_order(api_key, api_secret, user.get("tp1_order_id", ""))
            _cancel_order(api_key, api_secret, user.get("sl_order_id", ""))

            # Check actual remaining position — BingX's TP1 order may have already closed its share
            time.sleep(1)
            pos_r = _bingx("GET", "/openApi/swap/v2/user/positions", api_key, api_secret, {"symbol": BINGX_SYMBOL})
            actual_qty = 0.0
            _pos_data = pos_r.get("data") or []
            _pos_list = _pos_data if isinstance(_pos_data, list) else _pos_data.get("positions", [])
            for pos in _pos_list:
                if isinstance(pos, dict) and pos.get("positionSide") == pos_side:
                    actual_qty = abs(float(pos.get("positionAmt", 0)))
                    break
            print(f"[CT] on_tp1 {cid}: stored_qty={full_qty} actual_qty={actual_qty}")

            close_price = tp1 if tp1 > 0 else entry
            # actual_qty close to full_qty (within half the TP1 share) means BingX's TP1 order didn't fire
            if actual_qty >= full_qty - (tp1_qty / 2):
                _place_order(api_key, api_secret, close_side, "MARKET", tp1_qty, position_side=pos_side)
                remaining_qty = max(round(full_qty - tp1_qty, 4), 0.0001)
            elif actual_qty >= 0.0001:
                # BingX already closed its share via the TP1 order — use the real remaining size
                remaining_qty = round(actual_qty, 4)
            else:
                # Position fully closed already (both parts gone some other way)
                print(f"[CT] on_tp1 {cid}: position already fully closed, skipping BE SL")
                user["tp1_order_id"] = ""; user["in_position"] = False; user["pos_side"] = ""
                _set(cid, user)
                continue

            # Record TP1 PnL on the portion that closed
            pnl = _calc_pnl(user["pos_side"], entry, close_price, tp1_qty)
            _record_pnl(user, pnl, "BTC-USDT", user["pos_side"], "TP1")
            user["history"]["total"] += 1; user["history"]["profit"] += 1

            # BE SL slightly inside entry so BingX accepts (SL must be < current price for LONG)
            be_sl_price = round(entry * 0.999, 2) if user["pos_side"] == "BUY" else round(entry * 1.001, 2)
            be_sl_r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                                   remaining_qty, stop_price=be_sl_price, position_side=pos_side)
            be_sl_ok  = be_sl_r.get("code") == 0
            be_sl_oid = str((be_sl_r.get("data") or {}).get("order", {}).get("orderId", ""))
            print(f"[CT] on_tp1 {cid}: BE SL@{be_sl_price:,.2f} qty={remaining_qty} code={be_sl_r.get('code')} msg={be_sl_r.get('msg','?')} oid={be_sl_oid}")

            user["tp1_order_id"] = ""
            user["sl_order_id"]  = be_sl_oid
            user["pos_qty"]      = remaining_qty
            _set(cid, user)
            print(f"[CT] on_tp1 {cid}: remaining={remaining_qty} BTC @ {close_price:,.0f} pnl={pnl:+.2f} SL→BE@{entry:,.0f}")
        except Exception as e:
            print(f"[CT] on_tp1 {cid}: {e}")


def on_tp2(entry: float = 0, tp2: float = 0):
    """TP2 hit — cancel remaining orders, force-close if needed, update records."""
    global _last_signal
    results = []
    for cid, user, api_key, api_secret in _users_with_copy():
        if not user.get("in_position"): continue
        uname = user.get("username", "?")
        try:
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            pos_side   = "LONG" if user["pos_side"] == "BUY" else "SHORT"
            _cancel_order(api_key, api_secret, user.get("tp1_order_id", ""))
            _cancel_order(api_key, api_secret, user.get("sl_order_id", ""))
            close_r = _close_position(api_key, api_secret, user["pos_side"])
            print(f"[CT] on_tp2 {cid}: closePosition code={close_r.get('code')} msg={close_r.get('msg','')}")
            if close_r.get("code") != 0:
                remaining = user.get("pos_qty", 0.001)
                close_r = _place_order(api_key, api_secret, close_side, "MARKET", remaining, position_side=pos_side)
                print(f"[CT] on_tp2 {cid}: fallback MARKET code={close_r.get('code')} msg={close_r.get('msg','')}")
            ok = close_r.get("code") == 0
            results.append(f"{'✅' if ok else '❌'} @{uname} closed: {close_r.get('msg','') or 'ok'}")
        except Exception as e:
            results.append(f"❌ @{uname}: {e}")
            print(f"[CT] on_tp2 {cid}: {e}")
        if entry > 0 and tp2 > 0:
            pnl = _calc_pnl(user["pos_side"], entry, tp2, user.get("pos_qty", 0.001))
            _record_pnl(user, pnl, "BTC-USDT", user["pos_side"], "TP2")
        user["in_position"] = False; user["pos_side"] = ""; user["pos_qty"] = 0.0
        user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["tp1_order_id"] = ""
        user["failed_copy"] = False
        user["history"]["total"] += 1; user["history"]["profit"] += 1
        _set(cid, user)
    _last_signal = {}
    _save_last_signal()
    return results or ["No users in position."]


def on_sl(entry: float = 0, sl: float = 0, tp1_hit: bool = False):
    """SL hit — force-close position on BingX, cancel open TP orders, update records.
    tp1_hit=True means this SL is actually a breakeven exit (SL was moved to entry
    after TP1 already banked a partial win) — not a genuine loss, so it must not
    count toward the loss stat even though the mechanism firing it is the SL order."""
    global _last_signal
    results = []
    for cid, user, api_key, api_secret in _users_with_copy():
        if not user.get("in_position"): continue
        uname = user.get("username", "?")
        try:
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            pos_side   = "LONG" if user["pos_side"] == "BUY" else "SHORT"
            # Cancel all open orders first
            _cancel_order(api_key, api_secret, user.get("tp1_order_id", ""))
            _cancel_order(api_key, api_secret, user.get("tp_order_id", ""))
            _cancel_order(api_key, api_secret, user.get("sl_order_id", ""))
            # Try closePosition endpoint
            close_r = _close_position(api_key, api_secret, user["pos_side"])
            print(f"[CT] on_sl {cid}: closePosition code={close_r.get('code')} msg={close_r.get('msg','')}")
            if close_r.get("code") != 0:
                # Fallback: explicit market order
                remaining = user.get("pos_qty", 0.001)
                close_r = _place_order(api_key, api_secret, close_side, "MARKET", remaining, position_side=pos_side)
                print(f"[CT] on_sl {cid}: fallback MARKET code={close_r.get('code')} msg={close_r.get('msg','')}")
            ok = close_r.get("code") == 0
            results.append(f"{'✅' if ok else '❌'} @{uname} closed: {close_r.get('msg','') or 'ok'}")
        except Exception as e:
            results.append(f"❌ @{uname}: {e}")
            print(f"[CT] on_sl {cid}: {e}")
        if entry > 0 and sl > 0:
            pnl = _calc_pnl(user["pos_side"], entry, sl, user.get("pos_qty", 0.001))
            _record_pnl(user, pnl, "BTC-USDT", user["pos_side"], "BE" if tp1_hit else "SL")
            if not tp1_hit:
                user["history"]["total"] += 1
                user["history"]["loss"] += 1
            # else: breakeven exit after a partial win — already counted once when
            # TP1 hit (on_tp1 increments total+profit) — this is the SAME trade's
            # second half, not a new one, so don't double-count "total" here.
            # so it's excluded from both buckets rather than skewing the win rate
        user["in_position"] = False; user["pos_side"] = ""; user["pos_qty"] = 0.0
        user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["tp1_order_id"] = ""
        user["failed_copy"] = False
        _set(cid, user)
    _last_signal = {}
    _save_last_signal()
    return results or ["No users in position."]


def on_cancel_limits():
    """Entry missed / setup invalid — cancel pending limit orders."""
    global _last_signal
    _last_signal = {}
    _save_last_signal()
    for cid, user, api_key, api_secret in _users_with_copy():
        if user.get("in_position"): continue
        try:
            oid = user.get("limit_order_id","")
            if oid:
                _cancel_order(api_key, api_secret, oid)
            user["limit_order_id"] = ""; user["pos_side"] = ""
            user["failed_copy"] = False
            _set(cid, user)
        except Exception as e:
            print(f"[CT] on_cancel_limits {cid}: {e}")


def on_entry_hit(entry: float, sl: float, tp1: float, tp2: float):
    """
    Pullback entry triggered — limit order should have filled.
    Place SL + TP1 + TP2 orders for copy users who had a pending limit order,
    same TP1_CLOSE_PCT-based split as the MARKET-entry path in on_signal() —
    previously this only placed a single 50%-qty TP2 order and never a TP1 at
    all, so pullback entries never actually closed the TP1_CLOSE_PCT% partial.
    Only acts on users with an active limit_order_id (TV signal copies).
    """
    for cid, user, api_key, api_secret in _users_with_copy():
        if user.get("in_position"): continue          # market-entry users already set
        if not user.get("pos_side"): continue         # no pending trade at all
        if not user.get("limit_order_id"): continue   # no pending limit — skip (not a TV copy)
        try:
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            pos_side   = "LONG" if user["pos_side"] == "BUY" else "SHORT"
            qty        = user.get("pos_qty", 0.001)
            tp1_qty, tp2_qty = _tp1_split(qty)

            # Place SL for full qty
            sl_r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                                qty, stop_price=sl, position_side=pos_side)
            # Place TP1 for TP1_CLOSE_PCT% of qty
            tp1_r = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                 tp1_qty, stop_price=tp1, position_side=pos_side)
            # Place TP2 for the remaining qty
            tp2_r = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                 tp2_qty, stop_price=tp2, position_side=pos_side)

            user["in_position"]    = True
            user["sl_order_id"]    = str((sl_r.get("data") or {}).get("order", {}).get("orderId", ""))
            user["tp1_order_id"]   = str((tp1_r.get("data") or {}).get("order", {}).get("orderId", ""))
            user["tp_order_id"]    = str((tp2_r.get("data") or {}).get("order", {}).get("orderId", ""))
            user["limit_order_id"] = ""
            _set(cid, user)
            print(f"[CT] on_entry_hit {cid}: SL@{sl:,.0f} TP1@{tp1:,.0f} TP2@{tp2:,.0f} placed")
        except Exception as e:
            print(f"[CT] on_entry_hit {cid}: {e}")


def clear_last_signal():
    """Catch-all: clear the cached 'active signal' shown by /ctstatus. Call this from
    every path that ends the BTC trade, so /ctstatus never shows a stale signal."""
    global _last_signal
    _last_signal = {}
    _save_last_signal()

def on_close_all():
    """Admin /close or structure flip — close all positions + cancel all orders."""
    results = []
    for cid, user, api_key, api_secret in _users_with_copy():
        try:
            uname = user.get("username","?")
            if user.get("in_position") and user.get("pos_side"):
                r = _close_position(api_key, api_secret, user["pos_side"])
                results.append(f"{'✅' if r.get('code')==0 else '❌'} @{uname} closed: {r.get('msg','') or 'ok'}")
            else:
                _cancel_all_orders(api_key, api_secret)
                results.append(f"✅ @{uname} orders cancelled (no open position)")
            user["in_position"] = False; user["pos_side"] = ""
            user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["limit_order_id"] = ""
            _set(cid, user)
        except Exception as e:
            results.append(f"❌ {cid}: {e}")
            print(f"[CT] on_close_all {cid}: {e}")
    clear_last_signal()
    return results or ["No copy users active."]


def close_coin_all(coin: str) -> list[str]:
    """
    Close a specific coin position + cancel its orders for ALL copy users.
    coin = "BTC" / "ETH" / "SOL" etc  (auto-converts to BTC-USDT format)
    """
    coin = coin.upper().replace("USDT","").replace("-","")
    symbol = f"{coin}-USDT"
    results = []
    for cid, user in list(_db.items()):
        if not user.get("connected"): continue
        try:
            ak  = _decrypt(user["api_key_enc"])
            ask = _decrypt(user["api_secret_enc"])
            # Cancel all open orders on this symbol
            _bingx("DELETE", "/openApi/swap/v2/trade/allOpenOrders", ak, ask,
                   {"symbol": symbol})
            # Close any open position — try both sides, also try MARKET order as fallback
            closed_any = False
            for ps in ("LONG", "SHORT"):
                r = _bingx("POST", "/openApi/swap/v2/trade/closePosition", ak, ask,
                           {"symbol": symbol, "positionSide": ps})
                if r.get("code") == 0:
                    closed_any = True
            # Fallback: place market close for both sides
            if not closed_any:
                pos_r = _bingx("GET", "/openApi/swap/v2/user/positions", ak, ask, {"symbol": symbol})
                positions = (pos_r.get("data") or [])
                for pos in positions:
                    pos_amt = abs(float(pos.get("positionAmt", 0)))
                    ps = pos.get("positionSide", "LONG")
                    if pos_amt > 0:
                        close_side = "SELL" if ps == "LONG" else "BUY"
                        _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask,
                               {"symbol": symbol, "side": close_side, "positionSide": ps,
                                "type": "MARKET", "quantity": round(pos_amt, 4)})
            # Clear bot state for this symbol
            if symbol == BINGX_SYMBOL:
                user["in_position"] = False; user["pos_side"] = ""; user["pos_qty"] = 0.0
                user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["limit_order_id"] = ""
                _set(cid, user)
            elif _ver_for_symbol(user, symbol):
                _clear_scan_state(cid, user, symbol)
            results.append(f"✅ @{user.get('username','?')} {symbol} closed")
        except Exception as e:
            results.append(f"❌ @{user.get('username','?')}: {e}")
    return results or [f"No users found"]


def on_close_user(cid: str) -> tuple[bool, str]:
    """Close position + cancel orders for one specific user."""
    user = _db.get(str(cid))
    if not user or not user.get("connected"):
        return False, "not connected"
    try:
        ak = _decrypt(user["api_key_enc"]); ask = _decrypt(user["api_secret_enc"])
        if user.get("in_position") and user.get("pos_side"):
            _close_position(ak, ask, user["pos_side"])
        _cancel_all_orders(ak, ask)
        user["in_position"] = False; user["pos_side"] = ""; user["pos_qty"] = 0.0
        user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["limit_order_id"] = ""
        _set(str(cid), user)
        return True, f"@{user.get('username','?')} closed"
    except Exception as e:
        return False, str(e)


def set_scan_ct(enabled: bool):
    """Enable or disable copy trade for /scan signals."""
    global SCAN_CT_ENABLED
    SCAN_CT_ENABLED = enabled

def set_btc_ct(enabled: bool):
    global BTC_CT_ENABLED
    BTC_CT_ENABLED = enabled

def set_scan1_ct(enabled: bool):
    global SCAN1_CT_ENABLED
    SCAN1_CT_ENABLED = enabled

def set_scan2_ct(enabled: bool):
    global SCAN2_CT_ENABLED
    SCAN2_CT_ENABLED = enabled

def set_demo1_ct(enabled: bool):
    global DEMO1_CT_ENABLED
    DEMO1_CT_ENABLED = enabled

def set_demo2_ct(enabled: bool):
    global DEMO2_CT_ENABLED
    DEMO2_CT_ENABLED = enabled

def is_scan_tp1_hit(symbol: str) -> bool:
    """Returns True if ANY copy user has tp1_hit=True for this symbol."""
    for cid, user, _, _ in _users_with_copy():
        p = _pfx_for_symbol(user, symbol)
        if p and user.get(f"{p}tp1_hit"):
            return True
    return False


def on_scan_signal(signal_dict: dict, symbol: str, price: float, share_free: bool = True) -> list[str]:
    """
    Place a scan-sourced trade (alt coin) for all copy users.
    ver is passed inside signal_dict["ver"]: 1→s1_* slot, 2→scan_* slot
    share_free: whether today's free-channel quota was met — free-tier users only copy when True.
    """
    ver = signal_dict.get("ver")
    if ver == 1 and not SCAN1_CT_ENABLED:
        return ["[CT] Scan1 copy trade is OFF"]
    if ver == 2 and not SCAN2_CT_ENABLED:
        return ["[CT] Scan2 copy trade is OFF"]
    if ver == 3 and not DEMO1_CT_ENABLED:
        return ["[CT] Demo1 copy trade is OFF"]
    if ver == 4 and not DEMO2_CT_ENABLED:
        return ["[CT] Demo2 copy trade is OFF"]

    with _scan_signal_lock:
        return _on_scan_signal_inner(signal_dict, symbol, price, share_free)


def _on_scan_signal_inner(signal_dict: dict, symbol: str, price: float, share_free: bool = True) -> list[str]:

    ver        = int(signal_dict.get("ver", 2))   # 1=scan1 slot, 2=scan2 slot
    side       = signal_dict["signal"]
    entry      = float(signal_dict["entry"])
    sl         = float(signal_dict["sl"])
    tp1        = float(signal_dict.get("tp1", 0))
    tp2        = float(signal_dict.get("tp2", 0))
    entry_type = signal_dict.get("entry_type", "MARKET")
    close_side = "SELL" if side == "BUY" else "BUY"
    trade_ps   = "LONG" if side == "BUY" else "SHORT"
    trade_side = side
    results    = []

    for cid, user, api_key, api_secret in _users_with_copy(share_free):
        try:
            # Find a free slot for this scan version
            p = _free_slot(user, ver)
            if not p:
                results.append(f"⏭ @{user.get('username','?')} scan{ver} slots full — skipping {symbol}")
                continue
            # Skip if this symbol already open in any slot
            # Skip if user blocked this coin (/nocopy BTC, /nocopy SOL, etc.)
            nocopy = set(user.get("nocopy_coins", []))
            base_coin = symbol.split("-")[0].upper()
            if base_coin in nocopy or symbol.upper() in nocopy:
                results.append(f"⏭ @{user.get('username','?')} nocopy {base_coin} — skipping")
                continue
            if _pfx_for_symbol(user, symbol):
                results.append(f"⏭ @{user.get('username','?')} already in {symbol} — skipping duplicate")
                continue
            risk = user.get("risk_usdt")
            if risk:
                lev = _calc_auto_leverage(user["size_usdt"], risk, entry, sl)
                print(f"[CT] {cid} scan auto-leverage: risk=${risk} size=${user['size_usdt']} SL%={abs(entry-sl)/entry*100:.2f}% → {lev}x")
            else:
                lev = user.get("leverage", 10)
            # Use entry price for qty calc (not current price) so margin = size_usdt exactly
            qty = _calc_qty(user["size_usdt"], entry, lev)

            def _place_alt(s, ot, q, pr=0, sp=0, cp=False, ps=""):
                ps = ps or ("LONG" if s == "BUY" else "SHORT")
                params = {"symbol": symbol, "side": s, "positionSide": ps, "type": ot}
                if cp:
                    params["closePosition"] = "true"
                else:
                    params["quantity"] = round(q, 4)
                if ot == "LIMIT" and pr:
                    params["price"] = round(pr, 6); params["timeInForce"] = "GTC"
                if sp and ot in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
                    params["stopPrice"] = round(sp, 6)
                return _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, params)

            uname = user.get("username", "?")

            # ── Step 1: Set leverage with fallback ──────────────────────────
            lev_set = False
            lev_r = _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret,
                           {"symbol": symbol, "side": trade_ps, "leverage": lev})
            if lev_r.get("code") == 0:
                lev_set = True
            else:
                for try_lev in [100, 75, 50, 25, 20, 10, 5, 2, 1]:
                    if try_lev >= lev: continue
                    r2 = _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret,
                                {"symbol": symbol, "side": trade_ps, "leverage": try_lev})
                    if r2.get("code") == 0:
                        lev = try_lev; lev_set = True
                        qty = _calc_qty(user["size_usdt"], entry, lev)
                        print(f"[CT] {cid} {symbol} leverage capped at {lev}x")
                        break

            if not lev_set:
                # All leverage attempts failed — ask Claude
                advice = _ask_claude_action(
                    f"{symbol} {trade_side} signal. All leverage attempts failed. "
                    f"User risk=${user.get('risk_usdt',0.5)} size=${user['size_usdt']}. "
                    f"Entry={entry} SL={sl}. Should we skip or try to open without leverage change?"
                )
                act = advice.get("action","hold")
                if act == "close_position" or act == "hold":
                    results.append(f"⏭ @{uname} {symbol}: leverage failed — SKIPPED (Claude: {advice.get('reason',act)})")
                    continue
                # Claude said proceed — try with leverage=1 as last resort
                _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret,
                       {"symbol": symbol, "side": trade_ps, "leverage": 1})
                lev = 1; qty = _calc_qty(user["size_usdt"], entry, lev)
                print(f"[CT] {cid} {symbol}: leverage forced to 1x by Claude advice")

            # ── Step 2: Place entry order with 3-min retry ──────────────────
            ENTRY_DEADLINE = 180  # 3 minutes
            entry_ok = False
            entry_r = {}
            entry_deadline = time.time() + ENTRY_DEADLINE
            attempt = 0
            while time.time() < entry_deadline:
                attempt += 1
                if entry_type == "MARKET":
                    entry_r = _place_alt(side, "MARKET", qty)
                else:
                    entry_r = _place_alt(side, "LIMIT", qty, pr=entry)
                if entry_r.get("code") == 0:
                    entry_ok = True; break
                err = entry_r.get("msg", "")
                print(f"  [CT] @{uname} {symbol} entry attempt {attempt} FAIL: {err}")
                time.sleep(10)

            if not entry_ok:
                # Ask Claude what to do after 3 min of failures
                advice = _ask_claude_action(
                    f"{symbol} {trade_side} entry failed after {attempt} attempts (3 min). "
                    f"Last error: {entry_r.get('msg','')}. Entry={entry} qty={qty} lev={lev}x. "
                    f"Skip or try different approach?"
                )
                results.append(
                    f"❌ @{uname} {symbol}: entry failed after 3min ({attempt} tries) "
                    f"— SKIPPED. Claude: {advice.get('reason', advice.get('action',''))}"
                )
                continue

            if entry_type != "MARKET":
                limit_oid = str((entry_r.get("data") or {}).get("order", {}).get("orderId", ""))
                user[f"{p}symbol"] = symbol; user[f"{p}side"] = side
                user[f"{p}entry"]  = entry;  user[f"{p}sl"]   = sl
                user[f"{p}tp1"]    = tp1;    user[f"{p}tp2"]  = tp2
                user[f"{p}qty"]    = qty;    user[f"{p}limit_oid"] = limit_oid
                user[f"{p}lev"]    = lev
                _set(cid, user)
                _qty_note = _min_qty_risk_note(user["size_usdt"], entry, lev)
                results.append(f"✅ @{uname} {symbol} LIMIT {side} {qty:.4f} @ {entry} oid={limit_oid} (attempt {attempt})" + (f"\n{_qty_note}" if _qty_note else ""))
                continue  # SL/TP placed when limit fills via on_scan_limit_filled

            # Use actual filled qty from BingX (avoids rounding mismatch with very small-price coins)
            filled_qty = float(((entry_r.get("data") or {}).get("order") or {}).get("executedQty") or qty)
            if filled_qty > 0:
                qty = round(filled_qty, 4)
            tp1_qty, tp2_qty = _tp1_split(qty)

            # ── Step 3: MARKET filled — place SL+TP (60s retry, backup logic) ───
            sl_ok = tp1_ok = tp2_ok = False
            deadline = time.time() + 60
            sl_attempt = 0
            while time.time() < deadline:
                sl_attempt += 1
                if not sl_ok:
                    sl_r = _place_alt(close_side, "STOP_MARKET", qty, sp=sl, ps=trade_ps)
                    sl_ok = sl_r.get("code") == 0
                    if not sl_ok:
                        print(f"  [CT] @{uname} SL attempt {sl_attempt} FAIL: {sl_r.get('msg','')}")
                if not tp1_ok and tp1:
                    tp1_r = _place_alt(close_side, "TAKE_PROFIT_MARKET", tp1_qty, sp=tp1, ps=trade_ps)
                    tp1_ok = tp1_r.get("code") == 0
                if not tp2_ok and tp2:
                    tp2_r = _place_alt(close_side, "TAKE_PROFIT_MARKET", tp2_qty, sp=tp2, ps=trade_ps)
                    tp2_ok = tp2_r.get("code") == 0
                if sl_ok and (tp1_ok or not tp1) and (tp2_ok or not tp2):
                    break
                time.sleep(6)

            if not sl_ok:
                _bingx("POST", "/openApi/swap/v2/trade/closePosition",
                       api_key, api_secret, {"symbol": symbol, "positionSide": trade_ps})
                results.append(
                    f"🚨 @{uname} {symbol} — SL failed after 60s ({sl_attempt} attempts)"
                    f" — POSITION AUTO-CLOSED for safety")
                continue

            # ── Save state only after SL placed successfully ─────────────────
            user[f"{p}symbol"]  = symbol; user[f"{p}side"]    = side
            user[f"{p}entry"]   = entry;  user[f"{p}sl"]      = sl
            user[f"{p}tp1"]     = tp1;    user[f"{p}tp2"]     = tp2
            user[f"{p}qty"]     = qty;    user[f"{p}tp1_hit"] = False
            user[f"{p}lev"]     = lev
            _set(cid, user)

            tp_warn = ""
            if tp1 and not tp1_ok: tp_warn += " ⚠️TP1 failed"
            if tp2 and not tp2_ok: tp_warn += " ⚠️TP2 failed"
            _qty_note = _min_qty_risk_note(user["size_usdt"], entry, lev)
            results.append(
                f"✅ @{uname} {symbol} {side} {qty:.4f} lev={lev}x"
                f" entry(att:{attempt}) SL=OK"
                f" TP1={'OK' if not tp1 or tp1_ok else 'FAIL'}"
                f" TP2={'OK' if not tp2 or tp2_ok else 'FAIL'}{tp_warn}"
                + (f"\n{_qty_note}" if _qty_note else ""))

        except Exception as e:
            results.append(f"❌ @{user.get('username','?')}: {e}")
            print(f"[CT] on_scan_signal {cid}: {e}")

    if not results:
        results = ["No copy users connected"]
    print(f"[CT] on_scan_signal {symbol}: {results}")
    return results


def on_scan_tp1(symbol: str):
    """Scan TP1 hit — cancel remaining orders, move SL to BE, re-place TP2.
    BingX's own TP1 TAKE_PROFIT_MARKET order handles the 50% close automatically.
    We only close 50% manually if BingX's order didn't fire (e.g. TP1 order failed at entry)."""
    for cid, user, api_key, api_secret in _users_with_copy():
        p = _pfx_for_symbol(user, symbol)
        if not p: continue
        try:
            side        = user[f"{p}side"]
            entry_price = float(user.get(f"{p}entry", 0))
            close_side  = "SELL" if side == "BUY" else "BUY"
            trade_ps    = "LONG" if side == "BUY" else "SHORT"
            qty         = float(user.get(f"{p}qty", 0))
            half_qty    = max(round(qty / 2, 4), 0.001)

            if not entry_price:
                print(f"[CT] on_scan_tp1 {cid} {symbol}: entry=0, cannot set BE SL")
                continue

            # Mark tp1_hit FIRST before any BingX calls — prevents monitor from re-placing TP1 if we crash
            user[f"{p}tp1_hit"] = True
            _set(cid, user)

            # Cancel ALL open orders for this symbol
            for o in _get_open_orders(api_key, api_secret, symbol):
                oid = str(o.get("orderId", ""))
                if oid:
                    _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                           {"symbol": symbol, "orderId": oid})

            # Check actual position size — BingX TP1 order may have already closed 50%
            time.sleep(1)
            pos_r = _bingx("GET", "/openApi/swap/v2/user/positions", api_key, api_secret, {"symbol": symbol})
            actual_qty = 0.0
            _pos_data = pos_r.get("data") or []
            _pos_list = _pos_data if isinstance(_pos_data, list) else _pos_data.get("positions", [])
            for pos in _pos_list:
                if isinstance(pos, dict) and pos.get("positionSide") == trade_ps:
                    actual_qty = abs(float(pos.get("positionAmt", 0)))
                    break
            print(f"[CT] on_scan_tp1 {cid} {symbol}: stored_qty={qty} actual_qty={actual_qty}")

            if actual_qty >= qty * 0.75:
                # BingX TP1 order didn't fire — close 50% manually
                close_r = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, {
                    "symbol": symbol, "side": close_side, "positionSide": trade_ps,
                    "type": "MARKET", "quantity": round(half_qty, 4)
                })
                print(f"[CT] on_scan_tp1 {cid} {symbol}: manual close50% code={close_r.get('code')} msg={close_r.get('msg','')}")
                time.sleep(2)
                remaining_qty = half_qty
            elif actual_qty >= 0.001:
                # BingX already closed ~50% via TP1 order — use actual remaining size
                remaining_qty = round(actual_qty, 4)
                print(f"[CT] on_scan_tp1 {cid} {symbol}: BingX TP1 already closed 50%, remaining={remaining_qty}")
            else:
                # Position fully closed (both halves gone)
                print(f"[CT] on_scan_tp1 {cid} {symbol}: position already fully closed, skipping BE SL")
                user[f"{p}tp1_hit"] = True
                _set(cid, user)
                continue

            # BE SL for remaining position with 0.1% buffer so BingX accepts
            be_sl_price = round(entry_price * 0.999, 6) if side == "BUY" else round(entry_price * 1.001, 6)
            sl_r = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, {
                "symbol": symbol, "side": close_side, "positionSide": trade_ps,
                "type": "STOP_MARKET", "quantity": round(remaining_qty, 4),
                "stopPrice": be_sl_price
            })
            print(f"[CT] on_scan_tp1 {cid} {symbol}: BE SL@{be_sl_price} qty={remaining_qty} code={sl_r.get('code')} msg={sl_r.get('msg','')}")

            # Re-place TP2 for remaining qty
            tp2 = float(user.get(f"{p}tp2", 0))
            if tp2:
                tp2_r = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, {
                    "symbol": symbol, "side": close_side, "positionSide": trade_ps,
                    "type": "TAKE_PROFIT_MARKET", "quantity": round(remaining_qty, 4),
                    "stopPrice": round(tp2, 6)
                })
                print(f"[CT] on_scan_tp1 {cid} {symbol}: TP2@{tp2} code={tp2_r.get('code')} msg={tp2_r.get('msg','')}")

            # Record PnL for the portion that actually closed at TP1
            tp1_price  = float(user.get(f"{p}tp1", 0))
            closed_qty = round(qty - remaining_qty, 4)
            if tp1_price and closed_qty > 0:
                pnl = _calc_pnl(side, entry_price, tp1_price, closed_qty)
                _record_pnl(user, pnl, symbol, side, "TP1")
                user["history"]["total"] += 1; user["history"]["profit"] += 1

            user[f"{p}qty"]     = remaining_qty
            user[f"{p}sl"]      = be_sl_price
            user[f"{p}tp1_hit"] = True
            _set(cid, user)
            print(f"[CT] on_scan_tp1 {cid} {symbol}: done — remaining={remaining_qty} SL→BE@{be_sl_price}")
        except Exception as e:
            print(f"[CT] on_scan_tp1 {cid} {symbol}: {e}")


def on_scan_tp2(symbol: str):
    """Scan TP2 hit — cancel remaining orders, force-close position, clear scan state."""
    for cid, user, api_key, api_secret in _users_with_copy():
        p = _pfx_for_symbol(user, symbol)
        if not p: continue
        try:
            trade_ps = "LONG" if user[f"{p}side"] == "BUY" else "SHORT"
            for o in _get_open_orders(api_key, api_secret, symbol):
                oid = str(o.get("orderId", ""))
                if oid:
                    _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                           {"symbol": symbol, "orderId": oid})
            close_r = _bingx("POST", "/openApi/swap/v2/trade/closePosition", api_key, api_secret,
                              {"symbol": symbol, "positionSide": trade_ps})
            if close_r.get("code") != 0:
                rem = float(user.get(f"{p}qty", 0.001))
                close_side = "SELL" if user[f"{p}side"] == "BUY" else "BUY"
                _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret,
                       {"symbol": symbol, "side": close_side, "positionSide": trade_ps,
                        "type": "MARKET", "quantity": round(rem, 4)})
            print(f"[CT] on_scan_tp2 {cid} {symbol}: closed code={close_r.get('code')}")
        except Exception as e:
            print(f"[CT] on_scan_tp2 {cid} {symbol}: {e}")
        try:
            entry_price = float(user.get(f"{p}entry", 0))
            tp2_price   = float(user.get(f"{p}tp2", 0))
            remaining_qty = float(user.get(f"{p}qty", 0))
            if entry_price and tp2_price and remaining_qty > 0:
                pnl = _calc_pnl(user[f"{p}side"], entry_price, tp2_price, remaining_qty)
                _record_pnl(user, pnl, symbol, user[f"{p}side"], "TP2")
                user["history"]["total"] += 1; user["history"]["profit"] += 1
        except Exception as e:
            print(f"[CT] on_scan_tp2 {cid} {symbol} pnl record: {e}")
        _clear_scan_state(cid, user, symbol)


def update_scan_sl(symbol: str, new_sl: float) -> list[str]:
    """Admin custom SL edit on a specific open scan-coin trade — cancels the old
    stop order and places a new one at new_sl for every copy user holding it."""
    results = []
    for cid, user, api_key, api_secret in _users_with_copy():
        p = _pfx_for_symbol(user, symbol)
        if not p: continue
        try:
            uname = user.get("username", "?")
            trade_ps  = "LONG" if user[f"{p}side"] == "BUY" else "SHORT"
            close_side = "SELL" if user[f"{p}side"] == "BUY" else "BUY"
            qty = float(user.get(f"{p}qty", 0))
            if qty <= 0: continue
            for o in _get_open_orders(api_key, api_secret, symbol):
                if o.get("type") == "STOP_MARKET":
                    oid = str(o.get("orderId", ""))
                    if oid:
                        _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                               {"symbol": symbol, "orderId": oid})
            r = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret,
                       {"symbol": symbol, "side": close_side, "positionSide": trade_ps,
                        "type": "STOP_MARKET", "quantity": round(qty, 4), "stopPrice": round(new_sl, 6)})
            user[f"{p}sl"] = new_sl
            _set(cid, user)
            results.append(f"{'✅' if r.get('code')==0 else '❌'} @{uname} SL→{new_sl:,.4f}")
        except Exception as e:
            results.append(f"❌ {cid}: {e}")
            print(f"[CT] update_scan_sl {cid}/{symbol}: {e}")
    return results or ["No copy users holding this coin."]

def scan_sl_to_be(symbol: str, entry: float) -> list[str]:
    return update_scan_sl(symbol, entry)

def update_scan_tp(symbol: str, which: str, new_price: float) -> list[str]:
    """Admin custom TP1/TP2 edit on a specific open scan-coin trade — cancels the
    existing take-profit order(s) and re-places at the (possibly just-edited)
    TP1/TP2 prices for every copy user holding it."""
    results = []
    for cid, user, api_key, api_secret in _users_with_copy():
        p = _pfx_for_symbol(user, symbol)
        if not p: continue
        try:
            uname = user.get("username", "?")
            trade_ps   = "LONG" if user[f"{p}side"] == "BUY" else "SHORT"
            close_side = "SELL" if user[f"{p}side"] == "BUY" else "BUY"
            qty = float(user.get(f"{p}qty", 0))
            if qty <= 0: continue
            tp1_hit = user.get(f"{p}tp1_hit", False)
            user[f"{p}{which}"] = new_price
            new_tp1 = user.get(f"{p}tp1", 0)
            new_tp2 = user.get(f"{p}tp2", 0)
            for o in _get_open_orders(api_key, api_secret, symbol):
                if o.get("type") == "TAKE_PROFIT_MARKET":
                    oid = str(o.get("orderId", ""))
                    if oid:
                        _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                               {"symbol": symbol, "orderId": oid})
            ok = True
            if tp1_hit:
                if new_tp2:
                    r = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret,
                               {"symbol": symbol, "side": close_side, "positionSide": trade_ps,
                                "type": "TAKE_PROFIT_MARKET", "quantity": round(qty, 4), "stopPrice": round(new_tp2, 6)})
                    ok = r.get("code") == 0
            else:
                tp1_qty, tp2_qty = _tp1_split(qty)
                if new_tp1:
                    r1 = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret,
                                {"symbol": symbol, "side": close_side, "positionSide": trade_ps,
                                 "type": "TAKE_PROFIT_MARKET", "quantity": tp1_qty, "stopPrice": round(new_tp1, 6)})
                    ok = ok and r1.get("code") == 0
                if new_tp2:
                    r2 = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret,
                                {"symbol": symbol, "side": close_side, "positionSide": trade_ps,
                                 "type": "TAKE_PROFIT_MARKET", "quantity": tp2_qty, "stopPrice": round(new_tp2, 6)})
                    ok = ok and r2.get("code") == 0
            _set(cid, user)
            results.append(f"{'✅' if ok else '❌'} @{uname} {which.upper()}→{new_price:,.6f}")
        except Exception as e:
            results.append(f"❌ {cid}: {e}")
            print(f"[CT] update_scan_tp {cid}/{symbol}: {e}")
    return results or ["No copy users holding this coin."]

def update_tp(which: str, new_price: float, full_remaining: bool = False) -> list[str]:
    """Admin custom TP1/TP2 edit for the single active BTC trade — cancels the
    existing TP order and re-places it at the new price for every user in position."""
    field = "tp1_order_id" if which == "tp1" else "tp_order_id"
    results = []
    for cid, user, api_key, api_secret in _users_with_copy():
        if not user.get("in_position"): continue
        try:
            uname = user.get("username", "?")
            trade_ps   = "LONG" if user["pos_side"] == "BUY" else "SHORT"
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            qty = float(user.get("pos_qty", 0.001))
            place_qty = qty if full_remaining else max(round(qty / 2, 4), 0.0001)
            _cancel_order(api_key, api_secret, user.get(field, ""))
            r = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                             place_qty, stop_price=new_price, position_side=trade_ps)
            user[field] = str((r.get("data") or {}).get("order", {}).get("orderId", ""))
            _set(cid, user)
            results.append(f"{'✅' if r.get('code')==0 else '❌'} @{uname} {which.upper()}→{new_price:,.2f}")
        except Exception as e:
            results.append(f"❌ {cid}: {e}")
            print(f"[CT] update_tp {cid}: {e}")
    return results or ["No users in position."]

def on_scan_sl(symbol: str):
    """Scan SL hit — cancel all orders, force-close position, clear scan state."""
    for cid, user, api_key, api_secret in _users_with_copy():
        p = _pfx_for_symbol(user, symbol)
        if not p: continue
        try:
            trade_ps = "LONG" if user[f"{p}side"] == "BUY" else "SHORT"
            for o in _get_open_orders(api_key, api_secret, symbol):
                oid = str(o.get("orderId", ""))
                if oid:
                    _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                           {"symbol": symbol, "orderId": oid})
            close_r = _bingx("POST", "/openApi/swap/v2/trade/closePosition", api_key, api_secret,
                              {"symbol": symbol, "positionSide": trade_ps})
            if close_r.get("code") != 0:
                rem = float(user.get(f"{p}qty", 0.001))
                close_side = "SELL" if user[f"{p}side"] == "BUY" else "BUY"
                _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret,
                       {"symbol": symbol, "side": close_side, "positionSide": trade_ps,
                        "type": "MARKET", "quantity": round(rem, 4)})
            print(f"[CT] on_scan_sl {cid} {symbol}: closed code={close_r.get('code')}")
        except Exception as e:
            print(f"[CT] on_scan_sl {cid} {symbol}: {e}")
        try:
            entry_price = float(user.get(f"{p}entry", 0))
            sl_price    = float(user.get(f"{p}sl", 0))  # already BE-adjusted if TP1 had hit
            close_qty   = float(user.get(f"{p}qty", 0))
            tp1_hit     = bool(user.get(f"{p}tp1_hit", False))
            if entry_price and sl_price and close_qty > 0:
                pnl = _calc_pnl(user[f"{p}side"], entry_price, sl_price, close_qty)
                _record_pnl(user, pnl, symbol, user[f"{p}side"], "BE" if tp1_hit else "SL")
                if not tp1_hit:
                    user["history"]["total"] += 1
                    user["history"]["loss"] += 1
                # else: breakeven exit after a partial win — already counted once
                # when TP1 hit (on_scan_tp1 increments total+profit) — this is the
                # SAME trade's second half, don't double-count "total" here too.
        except Exception as e:
            print(f"[CT] on_scan_sl {cid} {symbol} pnl record: {e}")
        _clear_scan_state(cid, user, symbol)


def on_scan_limit_filled(symbol: str, side: str, entry: float, sl: float, tp1: float, tp2: float):
    """Called when a PULLBACK LIMIT order fills — place SL and TP orders for all copy users."""
    close_side = "SELL" if side == "BUY" else "BUY"
    trade_ps = "LONG" if side == "BUY" else "SHORT"
    for cid, user, api_key, api_secret in _users_with_copy():
        if user.get("scan_symbol") != symbol: continue
        try:
            uname = user.get("username", "?")
            qty = float(user.get("scan_qty", 0))
            if qty <= 0: continue
            tp1_qty, tp2_qty = _tp1_split(qty)

            def _p(s, ot, q, sp=0):
                params = {"symbol": symbol, "side": s, "positionSide": trade_ps,
                          "type": ot, "quantity": round(q, 4)}
                if sp: params["stopPrice"] = round(sp, 6)
                return _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, params)

            # Retry for 60s
            sl_ok = tp1_ok = tp2_ok = False
            deadline = time.time() + 60
            attempt = 0
            while time.time() < deadline:
                attempt += 1
                if not sl_ok:
                    r = _p(close_side, "STOP_MARKET", qty, sp=sl)
                    sl_ok = r.get("code") == 0
                    if not sl_ok:
                        print(f"  [CT] @{uname} LIMIT-fill SL attempt {attempt}: {r.get('msg','')}")
                if not tp1_ok and tp1:
                    r = _p(close_side, "TAKE_PROFIT_MARKET", tp1_qty, sp=tp1)
                    tp1_ok = r.get("code") == 0
                if not tp2_ok and tp2:
                    r = _p(close_side, "TAKE_PROFIT_MARKET", tp2_qty, sp=tp2)
                    tp2_ok = r.get("code") == 0
                if sl_ok and (tp1_ok or not tp1) and (tp2_ok or not tp2):
                    break
                time.sleep(6)

            if not sl_ok:
                # SL failed — auto-close
                _bingx("POST", "/openApi/swap/v2/trade/closePosition", api_key, api_secret,
                       {"symbol": symbol, "positionSide": trade_ps})
                print(f"[CT] LIMIT-fill @{uname} {symbol}: SL failed after {attempt} attempts — AUTO-CLOSED")
            else:
                print(f"[CT] LIMIT-fill @{uname} {symbol}: SL✅ TP1={'✅' if tp1_ok else '❌'} TP2={'✅' if tp2_ok else '❌'} ({attempt} attempts)")
        except Exception as e:
            print(f"[CT] on_scan_limit_filled {cid} {symbol}: {e}")


def on_scan_entry_missed(symbol: str):
    """Scan PULLBACK entry missed — cancel ALL open orders for this symbol, clear scan state."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if user.get("scan_symbol") != symbol:
            # Also try users where scan_symbol might not match but have a limit order for this symbol
            if not user.get("scan_limit_oid"): continue
        try:
            cancelled = []
            # Cancel by stored limit order ID first
            stored_oid = str(user.get("scan_limit_oid", ""))
            if stored_oid:
                r = _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                           {"symbol": symbol, "orderId": stored_oid})
                cancelled.append(f"oid={stored_oid} code={r.get('code')}")
            # Also cancel all open orders for this symbol (catches any extras)
            for o in _get_open_orders(api_key, api_secret, symbol):
                oid = str(o.get("orderId", ""))
                if oid and oid != stored_oid:
                    r = _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                               {"symbol": symbol, "orderId": oid})
                    cancelled.append(f"oid={oid} code={r.get('code')}")
            print(f"[CT] on_scan_entry_missed {cid} {symbol}: cancelled {cancelled}")
        except Exception as e:
            print(f"[CT] on_scan_entry_missed {cid} {symbol}: {e}")
        _clear_scan_state(cid, user)


_SCAN_SLOTS = {
    1: ["s1_", "s1b_", "s1c_", "s1d_", "s1e_", "s1f_"],  # scan1 — 6 slots
    2: ["scan_", "s2b_", "s2c_", "s2d_", "s2e_", "s2f_"],   # scan2 — 6 slots
    3: ["d1_", "d1b_", "d1c_", "d1d_", "d1e_", "d1f_"],     # demo1 — 6 slots
    4: ["d2_", "d2b_", "d2c_", "d2d_", "d2e_", "d2f_"],     # demo2 — 6 slots
}
_ALL_SLOT_PREFIXES = [p for _ver, _prefixes in _SCAN_SLOTS.items() for p in _prefixes]

def _pfx(ver: int) -> str:
    """Legacy — returns slot A prefix for scan version."""
    if ver == 3: return "d1_"
    if ver == 4: return "d2_"
    return "s1_" if ver == 1 else "scan_"

def _free_slot(user: dict, ver: int) -> str:
    """Return first free slot prefix for this scan version, or '' if both slots full."""
    for p in _SCAN_SLOTS[ver]:
        if not user.get(f"{p}symbol", ""):
            return p
    return ""

def _pfx_for_symbol(user: dict, symbol: str) -> str:
    """Return slot prefix that owns this symbol, or '' if not found."""
    for p in _ALL_SLOT_PREFIXES:
        if user.get(f"{p}symbol") == symbol:
            return p
    return ""

def _ver_for_symbol(user: dict, symbol: str) -> int:
    """Find which scan version owns this symbol (1=scan1, 2=scan2, 3=demo1, 4=demo2). Returns 0 if not found."""
    for ver, prefixes in _SCAN_SLOTS.items():
        for p in prefixes:
            if user.get(f"{p}symbol") == symbol: return ver
    return 0

def _clear_scan_state(cid: str, user: dict, symbol: str = "", ver: int = 0):
    p = _pfx_for_symbol(user, symbol) if symbol else (_pfx(ver) if ver else "")
    if not p:
        p = "scan_"  # fallback
    sym = symbol or user.get(f"{p}symbol", "")
    user[f"{p}symbol"] = ""; user[f"{p}side"] = ""
    user[f"{p}entry"] = 0; user[f"{p}sl"] = 0; user[f"{p}tp1"] = 0
    user[f"{p}tp2"] = 0; user[f"{p}qty"] = 0; user[f"{p}tp1_hit"] = False
    adopted = user.get("adopted_symbols", {})
    if sym in adopted:
        del adopted[sym]; user["adopted_symbols"] = adopted
    # Also clear legacy scan_symbol key if it points to this symbol
    if user.get("scan_symbol") == sym:
        user["scan_symbol"] = ""; user["scan_side"] = ""
        user["scan_entry"] = 0; user["scan_sl"] = 0
        user["scan_tp1"] = 0; user["scan_tp2"] = 0; user["scan_qty"] = 0
    _set(cid, user)


def reset_ghost_state(cid: str, kind: str, symbol: str = "") -> str:
    """Admin-triggered fix for a stale ghost state flagged by sync_check —
    the position no longer exists on BingX (closed manually, liquidated, or
    missed by a monitor tick around a redeploy), so there's nothing to touch
    on the exchange side; this only clears the bot's own stale local record.
    kind: 'btc' or 'scan'."""
    user = _get(cid)
    if not user:
        return f"No copytrade user found for {cid}."
    if kind == "btc":
        user["in_position"] = False; user["pos_side"] = ""; user["pos_qty"] = 0.0
        _set(cid, user)
        return f"✅ BTC ghost state cleared for {cid}."
    p = _pfx_for_symbol(user, symbol)
    if not p:
        return f"No open slot found holding {symbol} for {cid} — may already be cleared."
    _clear_scan_state(cid, user, symbol)
    return f"✅ {symbol} ghost state cleared for {cid} (slot {p.rstrip('_')})."


def _set_position_sl_sym(api_key: str, api_secret: str, symbol: str, pos_side: str, sl_price: float, qty: float = 0):
    """Place new BE SL order for alt-coin after TP1 hit."""
    close_side = "SELL" if pos_side == "LONG" else "BUY"
    q = max(round(qty, 4), 0.001) if qty > 0 else 0.001
    return _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret,
                  {"symbol": symbol, "side": close_side, "positionSide": pos_side,
                   "type": "STOP_MARKET", "quantity": q,
                   "stopPrice": round(sl_price, 6)})


def _fetch_bingx_realized_pnl(api_key: str, api_secret: str, days: int = 90) -> float:
    """Sum of realized PnL from BingX's own income history — the actual money moved
    on the account, independent of whatever the bot's own event tracking recorded."""
    try:
        start_ms = int((time.time() - days * 86400) * 1000)
        r = _bingx("GET", "/openApi/swap/v2/user/income", api_key, api_secret,
                   {"incomeType": "REALIZED_PNL", "startTime": start_ms, "limit": 1000})
        if r.get("code") != 0:
            return None
        rows = r.get("data") or []
        return round(sum(float(row.get("income", 0)) for row in rows), 4)
    except Exception as e:
        print(f"[CT] _fetch_bingx_realized_pnl: {e}")
        return None

def _get_open_orders(api_key: str, api_secret: str, symbol: str) -> list:
    """Fetch all open orders for a symbol."""
    r = _bingx("GET", "/openApi/swap/v2/trade/openOrders", api_key, api_secret,
               {"symbol": symbol})
    return (r.get("data") or {}).get("orders", [])


def _get_all_positions(api_key: str, api_secret: str) -> list:
    """Fetch all open positions (all symbols)."""
    r = _bingx("GET", "/openApi/swap/v2/user/positions", api_key, api_secret, {})
    if r.get("code") == 0:
        data = r.get("data") or []
        # BingX returns data as list directly or as {"positions": [...]}
        if isinstance(data, dict):
            data = data.get("positions", [])
        return [p for p in data if abs(float(p.get("positionAmt", 0))) > 0]
    return []


def monitor_sl_tp(notify_fn=None, ghost_close_fn=None):
    """
    Runs every minute. For every connected user:
    1. Fetches all real BingX positions
    2. If position exists but bot has no state → adopt it (sync state + place SL/TP from stored signal)
    3. If bot thinks position open but BingX shows nothing → clear ghost state
    4. If position exists with known state → verify SL+TP orders, re-place any missing
    """
    fixes = []
    for cid, user in list(_db.items()):
        if not user.get("connected"): continue
        try:
            ak    = _decrypt(user["api_key_enc"])
            ask   = _decrypt(user["api_secret_enc"])
            uname = user.get("username", cid)

            positions  = _get_all_positions(ak, ask)
            pos_by_sym = {p.get("symbol",""): p for p in positions if abs(float(p.get("positionAmt",0))) > 0}

            def _detect_close_reason(sym: str, entry: float, sl: float, tp1: float, tp2: float) -> str:
                """Check BingX recent trade history to figure out why position closed."""
                if not entry:
                    return "closed (no entry price stored)"
                try:
                    import time as _t
                    since_ms = int((_t.time() - 3600) * 1000)  # last 1 hour only
                    h = _bingx("GET", "/openApi/swap/v2/trade/allOrders", ak, ask,
                               {"symbol": sym, "limit": 10, "startTime": since_ms})
                    orders = (h.get("data") or {}).get("orders", [])
                    filled = [o for o in orders if o.get("status") == "FILLED"
                              and o.get("type") in ("STOP_MARKET","TAKE_PROFIT_MARKET","MARKET")]
                    if not filled:
                        return "closed (no recent orders found)"
                    last = sorted(filled, key=lambda o: int(o.get("updateTime",0)), reverse=True)[0]
                    otype = last.get("type","")
                    price = float(last.get("avgPrice", 0) or last.get("stopPrice", 0))
                    if otype == "STOP_MARKET":
                        return f"SL hit @ {price}"
                    elif otype == "TAKE_PROFIT_MARKET":
                        if tp2 and price >= tp2 * 0.99:
                            return f"TP2 hit @ {price} 🏆"
                        return f"TP1/BE hit @ {price} 💰"
                    return f"closed @ {price}"
                except:
                    return "closed (reason unknown)"

            # ── Ghost state: bot thinks BTC open but BingX has nothing ──
            if user.get("in_position") and BINGX_SYMBOL not in pos_by_sym:
                entry = float(user.get("entry", 0))
                sl    = float(user.get("sl", 0))
                tp1   = float(user.get("tp1", 0))
                tp2   = float(user.get("tp2", 0))
                reason = _detect_close_reason(BINGX_SYMBOL, entry, sl, tp1, tp2)
                msg = f"🔔 @{uname} BTC trade {reason}"
                fixes.append(msg); print(f"[CT] {msg}")
                if notify_fn: notify_fn(f"📊 <b>BTC trade closed @{uname}</b>\n{reason}")
                _ghost_handled = False
                if ghost_close_fn and "hit" in reason:
                    try:
                        ghost_close_fn(BINGX_SYMBOL, reason)
                        _ghost_handled = True
                    except Exception as e: print(f"[CT] ghost_close_fn BTC: {e}")
                if not _ghost_handled:
                    user["in_position"] = False; user["pos_side"] = ""; user["pos_qty"] = 0
                    user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["tp1_order_id"] = ""
                    _set(cid, user)

            # ── Ghost state: bot thinks scan open but BingX has nothing (all 4 slots) ──
            for _gp in _ALL_SLOT_PREFIXES:
                scan_sym = user.get(f"{_gp}symbol", "")
                if not scan_sym: continue
                _scan_pos_qty = abs(float((pos_by_sym.get(scan_sym) or {}).get("positionAmt", 0)))
                if _scan_pos_qty < 0.0001:
                    entry = float(user.get(f"{_gp}entry", 0))
                    sl    = float(user.get(f"{_gp}sl", 0))
                    tp1   = float(user.get(f"{_gp}tp1", 0))
                    tp2   = float(user.get(f"{_gp}tp2", 0))
                    reason = _detect_close_reason(scan_sym, entry, sl, tp1, tp2)
                    msg = f"🔔 @{uname} {scan_sym} ({_gp.rstrip('_')}) {reason}"
                    fixes.append(msg); print(f"[CT] {msg}")
                    if notify_fn: notify_fn(f"📊 <b>{scan_sym} trade closed @{uname}</b>\n{reason}")
                    if ghost_close_fn and "hit" in reason:
                        # Downstream (on_scan_sl/on_scan_tp1/on_scan_tp2, called via
                        # _force_close_scan_trade/_force_close_demo_trade) needs this
                        # user's slot data (entry/sl/qty) to still be present to record
                        # their P&L into trade_log — it clears state itself once done.
                        # Clearing it here FIRST was a race that made this exact user's
                        # closed trade silently never make it into their Portfolio.
                        try: ghost_close_fn(scan_sym, reason)
                        except Exception as e: print(f"[CT] ghost_close_fn {scan_sym}: {e}")
                        else: continue
                    _clear_scan_state(cid, user, scan_sym)

            # ── Check every real BingX position ──
            for sym, pos in pos_by_sym.items():
                pos_side  = pos.get("positionSide","")
                pos_amt   = abs(float(pos.get("positionAmt", 0)))
                avg_price = float(pos.get("avgPrice", 0))
                close_side = "SELL" if pos_side == "LONG" else "BUY"
                trade_side = "BUY" if pos_side == "LONG" else "SELL"

                is_btc  = (sym == BINGX_SYMBOL)
                is_scan = any(user.get(f"{p}symbol", "") == sym for p in _ALL_SLOT_PREFIXES)
                is_known = is_btc or is_scan

                # Skip entire position if user has this coin in nocopy — they manage it manually
                _nocopy = set(user.get("nocopy_coins", []))
                _base   = sym.split("-")[0].upper()
                if _base in _nocopy or sym.upper() in _nocopy:
                    continue

                # ── Orphan: BingX has position, bot has no state → adopt it ──
                # Also check adopted_symbols (multi-position tracking)
                adopted = user.get("adopted_symbols", {})
                if not is_known and sym not in adopted:
                    # Track via adopted_symbols only — do NOT write scan_symbol (legacy key)
                    # to avoid double-slot detection with s1_/s1b_/etc.
                    adopted[sym] = {"side": trade_side, "entry": avg_price, "qty": pos_amt}
                    user["adopted_symbols"] = adopted
                    _set(cid, user)
                    msg = f"🔄 @{uname} ADOPTED orphan {sym} {trade_side} {pos_amt} @ {avg_price} — placing 2% emergency SL"
                    fixes.append(msg); print(f"[CT] {msg}")
                    # Place emergency 2% SL — no Claude involved
                    emergency_sl = round(avg_price * (0.98 if pos_side=="LONG" else 1.02), 6)
                    _set(cid, user)
                    _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                        "symbol": sym, "side": close_side, "positionSide": pos_side,
                        "type": "STOP_MARKET", "quantity": round(pos_amt, 4),
                        "stopPrice": emergency_sl,
                    })
                    is_scan = True
                elif sym in adopted:
                    is_scan = True  # already adopted, treat as known

                # ── Get SL/TP prices from state ──
                if is_btc:
                    # BTC uses order IDs — just check orders exist
                    open_orders = _get_open_orders(ak, ask, sym)
                    has_sl  = any(o.get("type")=="STOP_MARKET"        and o.get("positionSide")==pos_side for o in open_orders)
                    has_tp  = any(o.get("type")=="TAKE_PROFIT_MARKET" and o.get("positionSide")==pos_side for o in open_orders)
                    if not has_sl:
                        emergency_sl = avg_price * (0.98 if pos_side=="LONG" else 1.02)
                        r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                            "symbol": sym, "side": close_side, "positionSide": pos_side,
                            "type": "STOP_MARKET", "quantity": round(pos_amt,4),
                            "stopPrice": round(emergency_sl, 2),
                        })
                        ok = r.get("code") == 0
                        msg = f"{'🔧' if ok else '❌'} @{uname} BTC SL {'restored @'+str(round(emergency_sl,2)) if ok else 'FAILED:'+r.get('msg','')[:40]}"
                        fixes.append(msg); print(f"[CT] {msg}")
                        if not ok:
                            pnl = float(pos.get("unrealizedProfit",0))
                            action = _ask_claude_action(f"BTC {trade_side} size={pos_amt} avg={avg_price} PnL={pnl:+.2f}. SL placement failed: {r.get('msg','')}. Protect this position.")
                            _execute_claude_action(action, ak, ask, sym, pos_side, pos_amt, notify_fn, uname, avg_price=avg_price, user=user)
                        elif ok and notify_fn:
                            notify_fn(f"✅ {msg}")
                    if not has_tp:
                        pnl = float(pos.get("unrealizedProfit",0))
                        action = _ask_claude_action(f"BTC {trade_side} size={pos_amt} avg={avg_price} PnL={pnl:+.2f} has NO TP orders. Place TP1 and TP2 or hold?")
                        _execute_claude_action(action, ak, ask, sym, pos_side, pos_amt, notify_fn, uname, avg_price=avg_price, user=user)
                        fixes.append(f"🤖 Claude acted on BTC no-TP: {action.get('action')}")
                    continue

                # ── Scan: verify/place SL + TP ──
                # Find which slot owns this symbol (all 4 slots)
                _sp = _pfx_for_symbol(user, sym) or "scan_"
                sl_price  = float(user.get(f"{_sp}sl",  0))
                tp1_price = float(user.get(f"{_sp}tp1", 0))
                tp2_price = float(user.get(f"{_sp}tp2", 0))
                stored_qty = float(user.get(f"{_sp}qty", 0))
                tp1_already_hit = bool(user.get(f"{_sp}tp1_hit", False))

                # ── TP1 detection: position dropped to ~50% (price may have come back) ──
                _side_stored = user.get(f"{_sp}side", "")
                _tp1_price   = float(user.get(f"{_sp}tp1", 0))

                if (not tp1_already_hit and stored_qty > 0
                        and pos_amt < stored_qty * 0.65
                        and pos_amt > stored_qty * 0.05):
                    print(f"[CT] [Monitor] @{uname} {sym}: TP1 detected (pos={pos_amt} stored={stored_qty} price={avg_price} tp1={_tp1_price}) — triggering on_scan_tp1")
                    # Mark tp1_hit immediately to stop spam before on_scan_tp1 runs
                    user[f"{_sp}tp1_hit"] = True; _set(cid, user)
                    if notify_fn:
                        notify_fn(f"🎯 [Monitor] @{uname} {sym}: TP1 detected → placing BE SL")
                    on_scan_tp1(sym)
                    continue  # on_scan_tp1 handles SL/TP2 — skip normal check

                # Emergency SL if no stored price (2% from avg entry)
                if not sl_price:
                    sl_price = round(avg_price * (0.98 if pos_side=="LONG" else 1.02), 6)
                    user[f"{_sp}sl"] = sl_price; _set(cid, user)

                open_orders = _get_open_orders(ak, ask, sym)
                has_sl  = any(o.get("type")=="STOP_MARKET"        and o.get("positionSide")==pos_side for o in open_orders)
                tp_ords = [o for o in open_orders if o.get("type")=="TAKE_PROFIT_MARKET" and o.get("positionSide")==pos_side]
                has_tp1 = len(tp_ords) >= 1
                has_tp2 = len(tp_ords) >= 2
                tp1_qty, tp2_qty = _tp1_split(pos_amt)
                placed = []

                if not has_sl:
                    r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                        "symbol": sym, "side": close_side, "positionSide": pos_side,
                        "type": "STOP_MARKET", "quantity": round(pos_amt, 4),
                        "stopPrice": round(sl_price, 6),
                    })
                    placed.append(f"SL {'✅' if r.get('code')==0 else '❌'+r.get('msg','')[:250]}")

                if not has_tp1 and tp1_price:
                    r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                        "symbol": sym, "side": close_side, "positionSide": pos_side,
                        "type": "TAKE_PROFIT_MARKET", "quantity": tp1_qty,
                        "stopPrice": round(tp1_price, 6),
                    })
                    placed.append(f"TP1 {'✅' if r.get('code')==0 else '❌'+r.get('msg','')[:250]}")

                if not has_tp2 and tp2_price:
                    r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                        "symbol": sym, "side": close_side, "positionSide": pos_side,
                        "type": "TAKE_PROFIT_MARKET", "quantity": tp2_qty,
                        "stopPrice": round(tp2_price, 6),
                    })
                    placed.append(f"TP2 {'✅' if r.get('code')==0 else '❌'+r.get('msg','')[:250]}")

                if placed:
                    msg = f"🔧 @{uname} {sym}: {', '.join(placed)}"
                    fixes.append(msg); print(f"[CT] {msg}")
                    if notify_fn:
                        notify_fn(f"🔧 <b>Auto-fixed {sym}</b>\n@{uname}: {', '.join(placed)}")
                else:
                    print(f"[CT] [Monitor] @{uname} {sym}: SL+TP OK ✅")

        except Exception as e:
            print(f"[CT] monitor {cid}: {e}")

    return fixes


def sync_check() -> list[str]:
    """
    Compare actual BingX positions vs bot state for every connected user.
    Returns list of status lines for admin.
    Detects: orphan positions (BingX open but bot thinks closed) and ghost state (bot thinks open but BingX closed).
    """
    lines = []
    for cid, user in list(_db.items()):
        if not user.get("connected"): continue
        try:
            ak   = _decrypt(user["api_key_enc"])
            ask  = _decrypt(user["api_secret_enc"])
            uname = user.get("username", cid)
            positions = _get_all_positions(ak, ask)
            pos_symbols = {p.get("symbol","") for p in positions}

            # ── BTC ──
            if user.get("in_position"):
                if BINGX_SYMBOL not in pos_symbols:
                    lines.append(f"⚠️ @{uname} GHOST STATE: bot thinks BTC position open but BingX shows NONE")
                    lines.append(f"__BTN__reset_ghost:{cid}")
                else:
                    lines.append(f"✅ @{uname} BTC position confirmed on BingX")
            else:
                if BINGX_SYMBOL in pos_symbols:
                    btc_pos = next(p for p in positions if p.get("symbol") == BINGX_SYMBOL)
                    amt = float(btc_pos.get("positionAmt", 0))
                    pnl = float(btc_pos.get("unrealizedProfit", 0))
                    lines.append(f"🚨 @{uname} ORPHAN BTC POSITION: {amt} BTC PnL={pnl:+.2f} USDT — bot state says NO TRADE")
                    lines.append(f"__BTN__close_btc:{cid}|adopt_btc:{cid}")

            # ── Scan (all 4 slots) ──
            known_scan_syms = {user.get(f"{p}symbol","") for p in _ALL_SLOT_PREFIXES if user.get(f"{p}symbol","")}
            for scan_sym in known_scan_syms:
                if scan_sym not in pos_symbols:
                    lines.append(f"⚠️ @{uname} GHOST SCAN: bot thinks {scan_sym} open but BingX shows NONE")
                    lines.append(f"__BTN__reset_scan_ghost_{cid}_{scan_sym}:{cid}")
                else:
                    lines.append(f"✅ @{uname} {scan_sym} scan position confirmed on BingX")
            # Orphan scan positions (BingX open, bot doesn't know)
            for sym in pos_symbols:
                if sym == BINGX_SYMBOL: continue
                if sym not in known_scan_syms:
                    orphan = next(p for p in positions if p.get("symbol") == sym)
                    amt = float(orphan.get("positionAmt", 0))
                    pnl = float(orphan.get("unrealizedProfit", 0))
                    lines.append(f"🚨 @{uname} ORPHAN SCAN: {sym} {amt} PnL={pnl:+.2f} USDT — bot has NO record")
                    lines.append(f"__BTN__ctretry_{cid}_{sym.replace('-USDT','')}:{cid}|closescan_{sym}:{cid}")

        except Exception as e:
            lines.append(f"❌ {cid}: {e}")

    return lines or ["✅ All users in sync — no orphan positions found"]


_pause_event = None  # set by bot.py after import

_uname_resolver = None  # set by bot.py after import — looks up a user's REAL current Telegram
# username (captured on every /start, regardless of BingX connection) via bot.py's own
# user_usernames dict. ct_users' own "username" field is only captured at connect-time and,
# for many older/never-connected records, was stored as a fallback (the numeric id itself) —
# so admin screens that only read user["username"] show raw IDs instead of real @handles.

def set_username_resolver(fn):
    global _uname_resolver
    _uname_resolver = fn

def _display_uname(uid: str, user: dict) -> str:
    """Best available display name for a copy-trade user: bot.py's live-tracked
    Telegram username first (freshest, works even if never BingX-connected),
    then ct_users' own stored username (if it isn't just the id), then the id."""
    if _uname_resolver:
        try:
            real = _uname_resolver(uid)
            if real:
                return f"@{real}"
        except Exception:
            pass
    stored = user.get("username")
    if stored and stored != str(uid):
        return f"@{stored}"
    return f"ID {uid}"

def start_monitor_loop(notify_fn=None, ghost_close_fn=None, interval_hours: int = 1):
    """Start background thread that runs monitor_sl_tp every 30 seconds."""
    import threading as _th
    def _loop():
        time.sleep(30)  # initial delay to let bot fully start
        while True:
            try:
                if _pause_event and _pause_event.is_set():
                    time.sleep(30); continue
                monitor_sl_tp(notify_fn, ghost_close_fn)
            except Exception as e:
                print(f"[CT] monitor loop error: {e}")
            time.sleep(30)
    t = _th.Thread(target=_loop, daemon=True)
    t.start()
    print(f"[CT] SL/TP monitor started — checks every 30 seconds")


def on_sl_to_be(entry: float):
    """Admin /sltobe — move SL to a new price for all users in position."""
    results = []
    for cid, user, api_key, api_secret in _users_with_copy():
        if not user.get("in_position"): continue
        try:
            uname      = user.get("username","?")
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            pos_side   = "LONG" if user["pos_side"] == "BUY" else "SHORT"
            remaining  = user.get("pos_qty", 0.001)
            _cancel_order(api_key, api_secret, user.get("sl_order_id", ""))
            r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                             remaining, stop_price=entry, position_side=pos_side)
            ok = r.get("code") == 0
            user["sl_order_id"] = str((r.get("data") or {}).get("order", {}).get("orderId", ""))
            _set(cid, user)
            results.append(f"{'✅' if ok else '❌'} @{uname} SL→{entry:,.0f}: {r.get('msg','') or 'ok'}")
        except Exception as e:
            results.append(f"❌ {cid}: {e}")
            print(f"[CT] on_sl_to_be {cid}: {e}")
    return results or ["No users in position."]


def on_update_sl(new_sl: float):
    """Admin /setsl — cancel old SL, place new one at new_sl for remaining qty."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if not user.get("in_position"): continue
        try:
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            pos_side   = "LONG" if user["pos_side"] == "BUY" else "SHORT"
            remaining  = user.get("pos_qty", 0.001)
            _cancel_order(api_key, api_secret, user.get("sl_order_id",""))
            r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                             remaining, stop_price=new_sl, position_side=pos_side)
            user["sl_order_id"] = str((r.get("data") or {}).get("order", {}).get("orderId", ""))
            _set(cid, user)
        except Exception as e:
            print(f"[CT] on_update_sl {cid}: {e}")


# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────

CT_USER_COMMANDS  = {"/connect", "/disconnect", "/setsize", "/setleverage", "/setrisk",
                     "/copytrade", "/mytrade", "/mysize", "/myhistory",
                     "/nocopy"}
CT_ADMIN_COMMANDS = {"/allusers", "/user", "/kick", "/pauseuser",
                     "/ctretry", "/ctstatus", "/ctclose", "/setvip", "/setfree"}

def is_ct_command(cmd: str, is_admin: bool) -> bool:
    if cmd in CT_USER_COMMANDS: return True
    if is_admin and cmd in CT_ADMIN_COMMANDS: return True
    return False

def handle(cmd: str, parts: list, chat_id, username: str,
           send_reply_fn, is_admin: bool, scan_trades: list = None):
    """Route a copy-trade command. Call this from bot.py handle_command()."""
    cid = str(chat_id)
    scan_trades = scan_trades or []

    # ── USER COMMANDS ─────────────────────────────────────────────────────────

    if cmd == "/connect":
        if len(parts) < 3:
            send_reply_fn(chat_id,
                f"<b>Connect BingX</b>\n\n<blockquote>{_sc('Usage')}:\n<code>/connect API_KEY API_SECRET</code>\n\n"
                f"⚠️ {_sc('Use read + trade permissions only. NEVER enable withdrawal on the key.')}</blockquote>")
            return
        api_key = parts[1]; api_secret = parts[2]
        send_reply_fn(chat_id, f"{_sc('Testing API key')}...")
        ok, err = _test_api(api_key, api_secret)
        if not ok:
            send_reply_fn(chat_id, f"<b>Connection Failed</b>\n\n<blockquote>{err}\n\n{_sc('Check key + secret and try again.')}</blockquote>")
            return
        user = _get(cid) or _default_user(username)
        user["api_key_enc"]    = _encrypt(api_key)
        user["api_secret_enc"] = _encrypt(api_secret)
        user["connected"]      = True
        user["username"]       = username
        _set(cid, user)
        send_reply_fn(chat_id,
            "<b>BingX Connected!</b> 🎉\n\n"
            f"<blockquote>✅ {_sc('API verified')}\n\n"
            f"{_sc('Margin per trade')}: <b>${user['size_usdt']} USDT</b>\n"
            f"{_sc('Leverage')}: <b>{user['leverage']}x</b> ({_sc('manual')})\n\n"
            f"{_sc('Head to your Copy Trade menu to turn on auto-copy, change your margin per trade, set an auto-risk max loss')} ⭐ "
            f"{_sc('or set leverage manually.')}\n\n"
            "<i>🛡️ Capital protected</i></blockquote>")

    elif cmd == "/disconnect":
        user = _get(cid)
        if not user:
            send_reply_fn(chat_id, f"{_sc('No account connected.')}"); return
        user["api_key_enc"] = ""; user["api_secret_enc"] = ""
        user["connected"] = False; user["copy_on"] = False
        _set(cid, user)
        send_reply_fn(chat_id,
            "<b>Disconnected</b>\n\n"
            f"<blockquote>{_sc('BingX API keys removed. Open positions remain open — manage them manually.')}\n\n"
            "<i>🛡️ Capital protected</i></blockquote>")

    elif cmd == "/setsize":
        if len(parts) < 2:
            user = _get(cid) or {}
            send_reply_fn(chat_id,
                f"{_sc('Current size')}: <b>${user.get('size_usdt',50)} USDT</b>\n\n"
                f"{_sc('Tap the Set Margin button and enter your new amount.')}"); return
        try:
            size = float(parts[1])
            if size < 0.25 or size > 10000:
                send_reply_fn(chat_id, _sc("Size must be $0.25–$10,000 USDT")); return
            user = _get(cid) or _default_user(username)
            user["size_usdt"] = size; _set(cid, user)
            _exposure_line = (f"{_sc('Exposure per trade')}: <b>${size * user['leverage']:.0f}</b>\n"
                if not user.get("risk_usdt") else "")
            send_reply_fn(chat_id,
                f"<b>Trade Size Set</b>\n\n"
                f"<blockquote>{_sc('Size')}: <b>${size} USDT</b> | {_lev_display(user)}\n"
                f"{_exposure_line}\n"
                f"<i>🛡️ Capital protected</i></blockquote>")
        except: send_reply_fn(chat_id, _sc("Please enter a valid number."))

    elif cmd == "/setleverage":
        if len(parts) < 2:
            user = _get(cid) or {}
            send_reply_fn(chat_id,
                f"{_sc('Current leverage')}: <b>{user.get('leverage',10)}x</b>\n\n"
                f"{_sc('Tap the Set Leverage button to change it.')}\n\n"
                f"<i>{_sc('Tip: Auto-Risk sets your leverage automatically based on max loss per trade — try that instead.')}</i>"); return
        try:
            lev = int(parts[1])
            if lev < 1 or lev > 125:
                send_reply_fn(chat_id, _sc("Leverage must be 1–125x")); return
            user = _get(cid) or _default_user(username)
            user["leverage"] = lev
            user["risk_usdt"] = None  # disable auto-leverage mode when manual leverage is set
            _set(cid, user)
            send_reply_fn(chat_id,
                f"<b>Leverage Set (Manual)</b>\n\n"
                f"<blockquote>{_sc('Leverage')}: <b>{lev}x</b> | {_sc('Size')}: <b>${user['size_usdt']} USDT</b>\n"
                f"{_sc('Exposure per trade')}: <b>${user['size_usdt']*lev:.0f}</b>\n\n"
                f"<i>{_sc('Auto-risk mode disabled. Turn it back on anytime from the Auto-Risk button.')}</i>\n\n"
                f"<i>🛡️ Capital protected</i></blockquote>")
        except: send_reply_fn(chat_id, _sc("Please enter a valid whole number."))

    elif cmd == "/setrisk":
        if len(parts) < 2:
            user = _get(cid) or {}
            risk = user.get("risk_usdt")
            size = user.get("size_usdt", 50)
            if risk:
                send_reply_fn(chat_id,
                    f"<b>Auto-Risk Mode: ON ✅</b>\n\n"
                    f"<blockquote>{_sc('Max loss per trade')}: <b>${risk} USDT</b>\n"
                    f"{_sc('Margin per trade')}: <b>${size} USDT</b>\n\n"
                    f"{_sc('Leverage is auto-calculated each trade based on SL distance.')}\n\n"
                    f"{_sc('Tap the Auto-Risk button to change your max loss, or turn it off there.')}\n\n"
                    f"<i>🛡️ Capital protected</i></blockquote>")
            else:
                send_reply_fn(chat_id,
                    f"<b>Auto-Risk Mode: OFF</b>\n\n"
                    f"<blockquote>{_sc('Currently using manual leverage')}: <b>{user.get('leverage',10)}x</b>\n\n"
                    f"{_sc('Tap the Auto-Risk button to turn it on and set your max loss per trade ($1–$50).')}\n\n"
                    f"<i>🛡️ Capital protected</i></blockquote>")
            return
        arg = parts[1].lower()
        if arg == "off":
            user = _get(cid) or _default_user(username)
            user["risk_usdt"] = None
            _set(cid, user)
            send_reply_fn(chat_id,
                f"<b>Auto-Risk Mode: OFF</b>\n\n"
                f"<blockquote>{_sc('Using manual leverage')}: <b>{user.get('leverage',10)}x</b>\n\n"
                f"<i>🛡️ Capital protected</i></blockquote>")
            return
        try:
            risk = float(arg)
            if risk < 0.25 or risk > 50:
                send_reply_fn(chat_id, _sc("Risk must be $0.25 – $50 per trade")); return
            user = _get(cid) or _default_user(username)
            size = user.get("size_usdt", 50)
            user["risk_usdt"] = risk
            _set(cid, user)
            # Show example with a typical 2% SL
            example_lev = _calc_auto_leverage(size, risk, 100, 98)  # 2% SL example
            send_reply_fn(chat_id,
                f"<b>Auto-Risk Mode: ON ✅</b>\n\n"
                f"<blockquote>{_sc('Max loss per trade')}: <b>${risk} USDT</b>\n"
                f"{_sc('Margin per trade')}: <b>${size} USDT</b>\n\n"
                f"<b>{_sc('How it works')}:</b>\n"
                f"{_sc('Leverage is auto-calculated per trade based on SL distance.')}\n"
                f"{_sc('Example (2% SL): leverage')} = {example_lev}x → {_sc('max loss')} ≈ ${size * example_lev * 0.02:.2f}\n\n"
                f"<i>{_sc('Closer SL = higher leverage | Wider SL = lower leverage')}</i>\n\n"
                f"<i>🛡️ Capital protected</i></blockquote>")
        except: send_reply_fn(chat_id, _sc("Please enter a valid number."))

    elif cmd == "/copytrade":
        user = _get(cid)
        _ct_btns = {"inline_keyboard": [[
            {"text": "🟢  Turn ON",  "callback_data": "copytrade_on",  "style": "success"},
            {"text": "🔴  Turn OFF", "callback_data": "copytrade_off", "style": "danger"}]]}
        if len(parts) < 2:
            user = user or {}
            st = "✅ ON" if user.get("copy_on") else "❌ OFF"
            send_reply_fn(chat_id,
                f"<b>Copy Trade</b>\n\n<blockquote>{_sc('Status')}: <b>{st}</b>\n\n"
                f"{_sc('Margin')}: <b>${user.get('size_usdt', 50)} USDT</b> | {_lev_display(user)}\n\n"
                f""
                f"<i>🛡️ Capital protected</i></blockquote>", reply_markup=_ct_btns); return
        if not user or not user.get("connected"):
            send_reply_fn(chat_id,
                f"{_sc('Connect BingX first')}:\n<code>/connect API_KEY API_SECRET</code>"); return
        if user.get("paused_by_admin"):
            send_reply_fn(chat_id, _sc("Your copy trade is paused by admin.")); return
        state = parts[1].lower()
        if state == "on":
            user["copy_on"] = True; _set(cid, user)
            send_reply_fn(chat_id,
                "<b>Copy Trade ON ✅</b>\n\n"
                "<blockquote>"
                f"{_sc('Auto-copying all CLEXER signals.')}\n"
                f"{_sc('Size')}: <b>${user['size_usdt']} USDT</b> | {_lev_display(user)}\n\n"
                f"<b>⚠️ {_sc('Warning')}:</b> {_sc('Real money. You are responsible for your trades.')}\n\n"
                "<i>🛡️ Capital protected</i></blockquote>", reply_markup=_ct_btns)
        elif state == "off":
            user["copy_on"] = False; _set(cid, user)
            send_reply_fn(chat_id,
                f"<b>Copy Trade OFF ❌</b>\n\n<blockquote>{_sc('No more auto-copies.')}\n"
                f"{_sc('Open positions remain open — manage them on BingX.')}\n\n"
                "<i>🛡️ Capital protected</i></blockquote>", reply_markup=_ct_btns)
        else:
            send_reply_fn(chat_id, "Tap a button below to turn Copy Trade on or off:", reply_markup=_ct_btns)

    elif cmd == "/mytrade":
        user = _get(cid)
        if not user or not user.get("connected"):
            _connect_btn = {"inline_keyboard": [[{"text": "🔗  Connect Account", "callback_data": "help_cmd:/connect"}]]}
            send_reply_fn(chat_id,
                f"<b>No Account Connected</b>\n\n<blockquote>{_sc('Connect your BingX account to see your open position.')}</blockquote>",
                reply_markup=_connect_btn); return
        try:
            api_key = _decrypt(user["api_key_enc"]); api_secret = _decrypt(user["api_secret_enc"])
            positions = _get_all_positions(api_key, api_secret)
            if not positions:
                _no_pos_line = _sc("You don't have an open position yet.")
                send_reply_fn(chat_id, f"<b>No Open Position</b>\n\n<blockquote>{_no_pos_line}\n\n<i>🛡️ Capital protected</i></blockquote>")
            else:
                _blocks = []
                for pos in positions:
                    amt   = float(pos.get("positionAmt", 0))
                    pnl   = float(pos.get("unrealizedProfit", 0))
                    entry = float(pos.get("avgPrice", 0))
                    lev   = pos.get("leverage","?")
                    sym   = pos.get("symbol", BINGX_SYMBOL).replace("-USDT","")
                    side  = "LONG" if amt > 0 else "SHORT"
                    pnl_s = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
                    _blocks.append(
                        f"{'🟢' if side=='LONG' else '🔴'} {side} {abs(amt):.4f} {sym}\n"
                        f"{_sc('Entry')}:    <b>{entry:,.4g}</b>\n"
                        f"{_sc('Leverage')}: <b>{lev}x</b>\n"
                        f"{_sc('PnL')}:      <b>{pnl_s}</b>")
                _title = "Your BingX Position" if len(_blocks) == 1 else f"Your BingX Positions ({len(_blocks)})"
                send_reply_fn(chat_id,
                    f"<b>{_title}</b>\n\n<blockquote>" + "\n\n".join(_blocks) +
                    f"\n\n<i>🛡️ Capital protected</i></blockquote>")
        except Exception as e:
            send_reply_fn(chat_id, f"{_sc('Error')}: {e}")

    elif cmd == "/mysize":
        user = _get(cid) or {}
        size = user.get("size_usdt", 50)
        risk = user.get("risk_usdt")
        lev  = user.get("leverage", 10)
        if risk:
            lev_line = f"{_sc('Leverage')}: <b>{_sc('Auto')} (max ${risk} loss/trade)</b>"
            exp_line = f"{_sc('Max loss/trade')}: <b>${risk} USDT</b>"
        else:
            lev_line = f"{_sc('Leverage')}: <b>{lev}x</b>"
            exp_line = f"{_sc('Exposure/trade')}: <b>${size * lev:.0f}</b>"
        _size_btns = {"inline_keyboard": [
            [{"text": "💵  Set Size",     "callback_data": "mysize_setsize"},
             {"text": "⚡  Set Leverage", "callback_data": "mysize_setlev"}],
            [{"text": "🛡  Set Risk",     "callback_data": "mysize_setrisk"}]]}
        send_reply_fn(chat_id,
            f"<b>Your Settings</b>\n\n"
            f"<blockquote>{_sc('BingX')}: {'✅ ' + _sc('Connected') if user.get('connected') else '❌ ' + _sc('Not connected')}\n"
            f"{_sc('Copy Trade')}: <b>{'✅ ON' if user.get('copy_on') else '❌ OFF'}</b>\n"
            f"{_sc('Margin per trade')}: <b>${size} USDT</b>\n"
            f"{lev_line}\n"
            f"{exp_line}\n\n"
            f"<i>🛡️ Capital protected</i></blockquote>", reply_markup=_size_btns)

    elif cmd == "/myhistory":
        user = _get(cid) or {}
        h = user.get("history", {"total":0,"profit":0,"loss":0,"total_pnl":0.0,"won_usdt":0.0,"lost_usdt":0.0})
        h.setdefault("total_pnl", 0.0); h.setdefault("won_usdt", 0.0); h.setdefault("lost_usdt", 0.0)
        wr   = f"{h['profit']/h['total']*100:.0f}%" if h["total"] else "—"
        pnl  = h["total_pnl"]
        pnl_s = f"+${pnl:.2f} 🟢" if pnl > 0 else (f"-${abs(pnl):.2f} 🔴" if pnl < 0 else "$0.00")
        _bingx_pnl_line = ""
        if user.get("connected"):
            try:
                _bx_pnl = _fetch_bingx_realized_pnl(_decrypt(user["api_key_enc"]), _decrypt(user["api_secret_enc"]))
                if _bx_pnl is not None:
                    _bx_s = f"+${_bx_pnl:.2f} 🟢" if _bx_pnl > 0 else (f"-${abs(_bx_pnl):.2f} 🔴" if _bx_pnl < 0 else "$0.00")
                    _bingx_pnl_line = f"{_sc('BingX Realized PnL (last 90d)')}: <b>{_bx_s}</b>\n\n"
            except Exception as e:
                print(f"[CT] /myhistory bingx pnl fetch: {e}")
        _myh_btns = {"inline_keyboard": [[{"text": "🗑 Reset My P&L History", "callback_data": "myhistory_reset"}]]}
        send_reply_fn(chat_id,
            f"<b>Your Copy Trade History</b>\n\n"
            f"<blockquote>{_sc('Total trades')}: <b>{h['total']}</b>\n"
            f"{_sc('Wins')}:         <b>{h['profit']}</b>  (+${h['won_usdt']:.2f})\n"
            f"{_sc('Losses')}:       <b>{h['loss']}</b>  (-${h['lost_usdt']:.2f})\n"
            f"{_sc('Win rate')}:     <b>{wr}</b>\n\n"
            f"{_sc('Bot-Tracked PnL')}: <b>{pnl_s}</b>\n"
            f"{_bingx_pnl_line}"
            f"{_sc('Size')}: ${user.get('size_usdt',50)} | {_lev_display(user)}\n\n"
            f"<i>🛡️ Capital protected</i></blockquote>", reply_markup=_myh_btns)

    elif cmd == "/nocopy":
        user = _get(cid) or {}
        nocopy = list(user.get("nocopy_coins", []))
        arg = parts[1].upper() if len(parts) > 1 else ""

        def _nocopy_menu():
            """Build the nocopy menu with active trade coin buttons + type button."""
            # Active scan trade coins from scan_trades arg
            active_coins = []
            for t in scan_trades:
                sym = t.get("symbol","")
                if sym:
                    base = sym.split("-")[0].upper()
                    if base not in active_coins:
                        active_coins.append(base)
            # Also add BTC as always available
            if "BTC" not in active_coins:
                active_coins.insert(0, "BTC")

            rows = []
            # Row per coin: Block or Unblock button
            coin_row = []
            for coin in active_coins:
                if coin in nocopy:
                    coin_row.append({"text": f"✅ {coin} (unblock)", "callback_data": f"nocopy_clr:{coin}"})
                else:
                    coin_row.append({"text": f"🚫 Block {coin}", "callback_data": f"nocopy_blk:{coin}"})
                if len(coin_row) == 2:
                    rows.append(coin_row); coin_row = []
            if coin_row:
                rows.append(coin_row)

            # Type coin manually button
            rows.append([{"text": "⌨️  Type a Coin Name", "callback_data": "nocopy_type"}])
            if nocopy:
                rows.append([{"text": "🔓  Unblock All", "callback_data": "nocopy_clr:ALL"}])

            blocked_str = (", ".join(f"<b>{c}</b>" for c in nocopy)) if nocopy else f"<i>{_sc('none')}</i>"
            text = (
                f"🚫 <b>No-Copy Settings</b>\n\n"
                f"<blockquote>{_sc('Currently blocked')}: {blocked_str}\n\n"
                f"<i>{_sc('Tap a coin to block/unblock it.')}\n{_sc('Blocked coins: bot skips signal, you trade manually.')}</i></blockquote>"
            )
            send_reply_fn(chat_id, text, reply_markup={"inline_keyboard": rows})

        if not arg or arg in ("LIST", ""):
            _nocopy_menu(); return

        if arg == "CLEAR":
            target = parts[2].upper() if len(parts) > 2 else ""
            if target == "ALL":
                user["nocopy_coins"] = []; _set(cid, user)
                send_reply_fn(chat_id, f"✅ <b>{_sc('All coins unblocked.')}</b>\n\n<blockquote>{_sc('All signals will be copied again.')}</blockquote>")
            elif target:
                if target in nocopy:
                    nocopy.remove(target); user["nocopy_coins"] = nocopy; _set(cid, user)
                    send_reply_fn(chat_id, f"✅ <b>{target} {_sc('unblocked.')}</b>")
                else:
                    send_reply_fn(chat_id, f"ℹ️ <b>{target}</b> {_sc('was not blocked.')}")
            else:
                send_reply_fn(chat_id, f"{_sc('Usage')}: <code>/nocopy clear BTC</code>")
            return

        # Block the coin
        if arg not in nocopy:
            nocopy.append(arg); user["nocopy_coins"] = nocopy; _set(cid, user)
        coins_str = ", ".join(f"<b>{c}</b>" for c in nocopy)
        send_reply_fn(chat_id,
            f"🚫 <b>{arg} {_sc('blocked')}</b>\n\n<blockquote>{_sc('Currently blocked')}: {coins_str}\n\n"
            f"{_sc('To unblock')}: <code>/nocopy clear {arg}</code></blockquote>")

    # ── ADMIN COMMANDS ────────────────────────────────────────────────────────

    elif cmd == "/allusers" and is_admin:
        total     = len(_db)
        connected = sum(1 for u in _db.values() if u.get("connected"))
        active    = sum(1 for u in _db.values() if u.get("copy_on") and u.get("connected"))
        exposure  = sum(u.get("size_usdt",0)*u.get("leverage",1)
                        for u in _db.values() if u.get("copy_on") and u.get("connected"))
        in_pos    = sum(1 for u in _db.values() if u.get("in_position"))
        send_reply_fn(chat_id,
            f"<b>Users Summary</b>\n\n"
            f"<blockquote>Total registered:   {total}\n"
            f"BingX connected:    {connected}\n"
            f"Copy trade active:  {active}\n"
            f"In position now:    {in_pos}\n"
            f"Total exposure:     <b>${exposure:,.0f}</b>\n\n"
            f"<i>🛡️ Capital protected</i></blockquote>")

    elif cmd == "/users" and is_admin:
        if not _db:
            send_reply_fn(chat_id, "No copy trade users yet."); return
        lines = [f"<b>Copy Trade Users ({len(_db)})</b>\n"]
        for i, (uid, user) in enumerate(_db.items(), 1):
            uname    = _display_uname(uid, user)
            bingx_ok = "✅" if user.get("connected") else "❌"
            copy_s   = "ON" if user.get("copy_on") else "OFF"
            pos_line = f"\n     Pos: {user.get('pos_side','?')}" if user.get("in_position") else ""
            paused   = " ⛔" if user.get("paused_by_admin") else ""
            risk     = user.get("risk_usdt")
            lev_str  = f"auto (max ${risk} loss)" if risk else f"{user.get('leverage',1)}x manual"
            tier     = user.get("tier", "free")
            tier_tag = "⭐ VIP" if tier == "vip" else "🆓 FREE"
            lines.append(
                f"{i}. {uname}{paused} | <code>{uid}</code>  {tier_tag}\n"
                f"   BingX:{bingx_ok} Copy:{copy_s} | "
                f"${user.get('size_usdt',0):.0f} {lev_str}"
                f"{pos_line}\n")
        send_reply_fn(chat_id, "\n".join(lines))

    elif cmd == "/user" and is_admin:
        def _user_btns(cb_prefix):
            rows = []
            row = []
            for uid, u in list(_db.items()):
                label = _display_uname(uid, u) + (" ⛔" if u.get("paused_by_admin") else ("  🟢" if u.get("copy_on") else ""))
                row.append({"text": label, "callback_data": f"{cb_prefix}:{uid}"})
                if len(row) == 2:
                    rows.append(row); row = []
            if row: rows.append(row)
            return {"inline_keyboard": rows} if rows else None
        if len(parts) < 2:
            mkp = _user_btns("userinfo")
            send_reply_fn(chat_id, "👥 <b>Select a user:</b>", reply_markup=mkp); return
        target = str(parts[1]); user = _db.get(target)
        if not user:
            send_reply_fn(chat_id, f"User {target} not found."); return
        pos_info = ""
        if user.get("in_position") and user.get("connected"):
            try:
                pos = _get_position(_decrypt(user["api_key_enc"]), _decrypt(user["api_secret_enc"]))
                if pos:
                    amt   = float(pos.get("positionAmt",0))
                    pnl   = float(pos.get("unrealizedProfit",0))
                    entry = float(pos.get("avgPrice",0))
                    side  = "LONG" if amt > 0 else "SHORT"
                    pnl_s = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
                    pos_info = f"\nPosition: {side} {abs(amt):.4f} BTC | Entry:{entry:,.0f} | PnL:{pnl_s}"
            except: pos_info = "\nPosition: (fetch error)"
        h = user.get("history",{"total":0,"profit":0,"loss":0,"total_pnl":0.0,"won_usdt":0.0,"lost_usdt":0.0})
        h.setdefault("total_pnl", 0.0); h.setdefault("won_usdt", 0.0); h.setdefault("lost_usdt", 0.0)
        pnl   = h["total_pnl"]
        pnl_s = f"+${pnl:.2f} 🟢" if pnl > 0 else (f"-${abs(pnl):.2f} 🔴" if pnl < 0 else "$0.00")
        wr    = f"{h['profit']/h['total']*100:.0f}%" if h["total"] else "—"
        paused = "\n⚠️ PAUSED BY ADMIN" if user.get("paused_by_admin") else ""
        _risk = user.get("risk_usdt")
        _lev_line = f"Auto-Risk: <b>max ${_risk} loss/trade</b> (leverage recalculated per trade)" if _risk else f"Leverage: <b>{user.get('leverage',1)}x manual</b>"
        _tier = user.get("tier", "free")
        _tier_line = "⭐ <b>VIP</b>" + (f" (until {user['vip_end']})" if user.get("vip_end") else "") if _tier == "vip" else "🆓 <b>FREE</b>"
        _bingx_pnl_line = ""
        if user.get("connected"):
            try:
                _bx_pnl = _fetch_bingx_realized_pnl(_decrypt(user["api_key_enc"]), _decrypt(user["api_secret_enc"]))
                if _bx_pnl is not None:
                    _bx_s = f"+${_bx_pnl:.2f} 🟢" if _bx_pnl > 0 else (f"-${abs(_bx_pnl):.2f} 🔴" if _bx_pnl < 0 else "$0.00")
                    _bingx_pnl_line = f"BingX Realized PnL (last 90d): <b>{_bx_s}</b>\n"
            except Exception as e:
                print(f"[CT] /user bingx pnl fetch: {e}")
        send_reply_fn(chat_id,
            f"<b>{_display_uname(target, user)}</b> | <code>{target}</code>{paused}\n\n"
            f"<blockquote>Tier: {_tier_line}\n"
            f"BingX: {'✅ Connected' if user.get('connected') else '❌ Not connected'}\n"
            f"Copy Trade: {'ON' if user.get('copy_on') else 'OFF'}\n"
            f"Size: <b>${user.get('size_usdt',0)} USDT</b> | {_lev_line}"
            f"{pos_info}\n\n"
            f"Trades: {h['total']} | Wins: {h['profit']} | Losses: {h['loss']} | WR: {wr}\n"
            f"Won:  +${h['won_usdt']:.2f}  |  Lost: -${h['lost_usdt']:.2f}\n"
            f"Bot-Tracked PnL: <b>{pnl_s}</b>\n"
            f"{_bingx_pnl_line}\n"
            f"Joined: {user.get('joined','?')}\n\n"
            f"<i>🛡️ Capital protected</i></blockquote>")

    elif cmd == "/kick" and is_admin:
        if len(parts) < 2:
            rows = [[{"text": _display_uname(uid, u), "callback_data": f"kick:{uid}"}] for uid, u in list(_db.items())]
            send_reply_fn(chat_id, "🚫 <b>Select user to kick:</b>", reply_markup={"inline_keyboard": rows} if rows else None); return
        target = str(parts[1]); user = _db.get(target)
        if not user:
            send_reply_fn(chat_id, f"User {target} not found."); return
        if user.get("connected"):
            try:
                _cancel_all_orders(_decrypt(user["api_key_enc"]), _decrypt(user["api_secret_enc"]))
            except: pass
        with _lock:
            del _db[target]
            _save()
            try:
                push_to_central()
            except Exception as e:
                print(f"[CT] /kick central push error: {e}")
        send_reply_fn(chat_id,
            f"<b>User Removed</b>\n\n"
            f"<blockquote>{_display_uname(target, user)} (ID:{target})\n"
            f"Orders cancelled. API keys deleted.\n\n"
            f"<i>🛡️ Capital protected</i></blockquote>")

    elif cmd == "/pauseuser" and is_admin:
        if len(parts) < 2:
            rows = []
            for uid, u in list(_db.items()):
                state = "⛔ Paused" if u.get("paused_by_admin") else "✅ Active"
                rows.append([{"text": f"{_display_uname(uid, u)}  {state}", "callback_data": f"pauseuser:{uid}"}])
            send_reply_fn(chat_id, "⏸ <b>Select user to pause/unpause:</b>", reply_markup={"inline_keyboard": rows} if rows else None); return
        target = str(parts[1]); user = _db.get(target)
        if not user:
            send_reply_fn(chat_id, f"User {target} not found."); return
        user["paused_by_admin"] = not user.get("paused_by_admin", False)
        if user["paused_by_admin"]:
            user["copy_on"] = False
        _set(target, user)
        state = "PAUSED ⛔" if user["paused_by_admin"] else "UNPAUSED ✅"
        send_reply_fn(chat_id,
            f"<b>User {state}</b>\n\n"
            f"<blockquote>{_display_uname(target, user)} (ID:{target})\n\n"
            f"<i>🛡️ Capital protected</i></blockquote>")

    elif cmd == "/setvip" and is_admin:
        if len(parts) < 2:
            rows = [[{"text": _display_uname(uid, u), "callback_data": f"vip_pick:{uid}"}] for uid, u in list(_db.items())]
            send_reply_fn(chat_id, "⭐ <b>Promote to VIP</b>\n\n<blockquote>Choose a user:</blockquote>", reply_markup={"inline_keyboard": rows} if rows else None); return
        if len(parts) < 4:
            send_reply_fn(chat_id, "Usage: /setvip <chat_id> <DD.MM.YYYY start> <DD.MM.YYYY end>"); return
        target = str(parts[1])
        user = _db.get(target) or _default_user(parts[4] if len(parts) > 4 else target)
        import re as _re
        if not _re.match(r"^\d{2}\.\d{2}\.\d{4}$", parts[2]) or not _re.match(r"^\d{2}\.\d{2}\.\d{4}$", parts[3]):
            send_reply_fn(chat_id, "Dates must be DD.MM.YYYY, e.g. 17.08.2026"); return
        user["tier"] = "vip"; user["vip_start"] = parts[2]; user["vip_end"] = parts[3]; user["vip_grace_notified_at"] = 0
        _set(target, user)
        send_reply_fn(chat_id,
            f"<b>⭐ {_display_uname(target, user)} promoted to VIP</b>\n\n"
            f"<blockquote>From <b>{parts[2]}</b> to <b>{parts[3]}</b>\n\n<i>🛡️ Capital protected</i></blockquote>")

    elif cmd == "/setfree" and is_admin:
        if len(parts) < 2:
            rows = [[{"text": _display_uname(uid, u), "callback_data": f"free_set:{uid}"}] for uid, u in list(_db.items())]
            send_reply_fn(chat_id, "🆓 <b>Demote to Free</b>\n\n<blockquote>Choose a user:</blockquote>", reply_markup={"inline_keyboard": rows} if rows else None); return
        target = str(parts[1])
        user = _db.get(target) or _default_user(parts[2] if len(parts) > 2 else target)
        user["tier"] = "free"; user["vip_start"] = ""; user["vip_end"] = ""; user["vip_grace_notified_at"] = 0
        _set(target, user)
        send_reply_fn(chat_id, f"<b>🆓 {_display_uname(target, user)} set to Free tier</b>\n\n<blockquote><i>🛡️ Capital protected</i></blockquote>")

    elif cmd == "/ctstatus" and is_admin:
        # Show failed copy users and current active signal
        failed = [(cid, u) for cid, u in _db.items() if u.get("failed_copy")]
        sig_info = ""
        if _last_signal:
            sig_info = (
                f"\n\n<b>Active Signal:</b>\n"
                f"Direction: <b>{_last_signal.get('side','?')}</b>\n"
                f"Entry: {_last_signal.get('entry',0):,.0f} | "
                f"SL: {_last_signal.get('sl',0):,.0f} | "
                f"TP2: {_last_signal.get('tp2',0):,.0f}\n"
                f"Type: {_last_signal.get('entry_type','?')}\n"
                f"Time: {_last_signal.get('time','?')}"
            )
        else:
            sig_info = "\n\n<b>No active signal</b> — /ctretry will be blocked."
        if not failed:
            send_reply_fn(chat_id, f"<b>Copy Trade Status</b>\n\n<blockquote>No failed copies.{sig_info}\n\n<i>🛡️ Capital protected</i></blockquote>")
            return
        lines = [f"<b>Failed Copy Users ({len(failed)})</b>"]
        for cid, u in failed:
            lines.append(f"- @{u.get('username','?')} | ID: <code>{cid}</code>\n"
                         f"  Use: /ctretry {cid}")
        send_reply_fn(chat_id, "\n".join(lines) + sig_info + "\n\n<i>🛡️ Capital protected</i>")

    elif cmd == "/ctretry" and is_admin:
        """Retry copy trade for a specific user.
        /ctretry USER_ID          → retry BTC trade
        /ctretry USER_ID SOL      → retry SOL-USDT scan trade
        /ctretry USER_ID all      → retry ALL active scan trades
        """
        if len(parts) < 2:
            failed = [(uid, u) for uid, u in _db.items() if u.get("failed_copy")]
            rows = [[{"text": f"🔄 @{u.get('username',uid)}", "callback_data": f"ctretry:{uid}"}] for uid, u in (failed or list(_db.items())[:10])]
            send_reply_fn(chat_id, "🔄 <b>Select user to retry:</b>", reply_markup={"inline_keyboard": rows} if rows else None); return

        target = str(parts[1])
        user = _db.get(target)
        if not user:
            send_reply_fn(chat_id, f"User <code>{target}</code> not found."); return
        if not user.get("connected"):
            send_reply_fn(chat_id, f"@{user.get('username','?')} has no BingX connected."); return

        # Determine mode: btc / specific coin / all scan trades
        mode = parts[2].upper() if len(parts) > 2 else "BTC"

        # ── SCAN COIN RETRY ────────────────────────────────────────────────────
        if mode != "BTC":
            targets_scan = []
            if mode == "ALL":
                targets_scan = scan_trades  # all active scan trades
            else:
                sym = mode if "-USDT" in mode else f"{mode}-USDT"
                targets_scan = [t for t in scan_trades if t.get("symbol") == sym]
                if not targets_scan:
                    send_reply_fn(chat_id, f"No active scan trade found for {sym}."); return

            results = []
            api_key    = _decrypt(user["api_key_enc"])
            api_secret = _decrypt(user["api_secret_enc"])
            uname      = user.get("username", "?")
            risk       = user.get("risk_usdt")

            for st in targets_scan:
                sym     = st["symbol"]
                # Skip if user already has an open position for this symbol
                if user.get("scan_symbol") == sym:
                    results.append(f"⏭ {sym} — already in position, skipping"); continue
                side    = st["signal"]
                entry    = float(st["entry"])
                sl       = float(st["sl"])   # already = entry if tp1_hit
                tp1      = float(st.get("tp1", 0))
                tp2      = float(st.get("tp2", 0))
                tp1_hit  = bool(st.get("tp1_hit", False))
                trade_ps = "LONG" if side == "BUY" else "SHORT"
                close_side = "SELL" if side == "BUY" else "BUY"
                try:
                    lev = _calc_auto_leverage(user["size_usdt"], risk, entry, sl) if risk else user.get("leverage", 10)
                    qty = _calc_qty(user["size_usdt"], entry, lev)
                    tp1_qty, tp2_qty = _tp1_split(qty)

                    lev_r = _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret,
                                   {"symbol": sym, "side": trade_ps, "leverage": lev})
                    if lev_r.get("code") != 0:
                        for try_lev in [100, 75, 50, 25, 20, 10, 5, 2, 1]:
                            if try_lev >= lev: continue
                            if _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret,
                                      {"symbol": sym, "side": trade_ps, "leverage": try_lev}).get("code") == 0:
                                lev = try_lev; qty = _calc_qty(user["size_usdt"], entry, lev)
                                tp1_qty, tp2_qty = _tp1_split(qty); break

                    def _alt(s, ot, q, sp=0, ps=""):
                        ps = ps or trade_ps
                        p = {"symbol": sym, "side": s, "positionSide": ps, "type": ot,
                             "quantity": round(q, 4)}
                        if sp and ot in ("STOP_MARKET","TAKE_PROFIT_MARKET"): p["stopPrice"] = round(sp, 6)
                        return _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, p)

                    r = _alt(side, "MARKET", qty)
                    if r.get("code") == 0:
                        # Retry loop — position may not be confirmed immediately
                        sl_ok = tp1_ok = tp2_ok = False
                        deadline = time.time() + 60
                        attempt = 0
                        while time.time() < deadline:
                            attempt += 1
                            if not sl_ok:
                                sl_r = _alt(close_side, "STOP_MARKET", qty, sp=sl)
                                sl_ok = sl_r.get("code") == 0
                                if not sl_ok: print(f"  [CT] scan retry SL attempt {attempt}: {sl_r.get('msg','')}")
                            if tp1 and not tp1_ok:
                                tp1_ok = _alt(close_side, "TAKE_PROFIT_MARKET", tp1_qty, sp=tp1).get("code") == 0
                            if tp2 and not tp2_ok:
                                tp2_ok = _alt(close_side, "TAKE_PROFIT_MARKET", tp2_qty, sp=tp2).get("code") == 0
                            if sl_ok and (tp1_ok or not tp1) and (tp2_ok or not tp2):
                                break
                            time.sleep(6)

                        if not sl_ok:
                            # SL failed — close position to protect user
                            _bingx("POST", "/openApi/swap/v2/trade/closePosition",
                                   api_key, api_secret, {"symbol": sym, "positionSide": trade_ps})
                            results.append(f"🚨 {sym} — SL failed after 60s, position auto-closed")
                            continue

                        user["scan_symbol"] = sym; user["scan_side"] = side
                        user["scan_entry"] = entry; user["scan_sl"] = sl
                        user["scan_tp1"] = tp1; user["scan_tp2"] = tp2
                        user["scan_qty"] = tp2_qty if tp1_hit else qty  # if TP1 already hit, only the TP2 share remains
                        _set(target, user)
                        warn = ("" if tp1_ok else " ⚠️TP1 failed") + ("" if tp2_ok else " ⚠️TP2 failed")
                        results.append(f"✅ {sym} {side} {qty:.4f} lev={lev}x{warn}")
                    else:
                        results.append(f"❌ {sym}: {r.get('msg','?')}")
                except Exception as e:
                    results.append(f"❌ {sym}: {e}")

            send_reply_fn(chat_id,
                f"<b>Scan Retry — @{uname}</b>\n\n<blockquote>" + "\n".join(results) + "\n\n<i>🛡️ Capital protected</i></blockquote>")
            return

        # ── BTC RETRY (existing logic below) ──────────────────────────────────
        if not _last_signal:
            send_reply_fn(chat_id,
                "<b>Retry Blocked</b>\n\n"
                "<blockquote>No active BTC signal — trade already closed or no signal yet.\n\n"
                "<i>🛡️ Capital protected</i></blockquote>"); return

        if user.get("in_position"):
            send_reply_fn(chat_id, f"@{user.get('username','?')} already in BTC position — no retry needed."); return

        try:
            api_key    = _decrypt(user["api_key_enc"])
            api_secret = _decrypt(user["api_secret_enc"])
            side       = _last_signal["side"]
            entry      = float(_last_signal.get("entry", _last_signal.get("price", 0)))
            sl         = _last_signal["sl"]
            tp2        = _last_signal["tp2"]
            close_side = "SELL" if side == "BUY" else "BUY"
            trade_ps   = "LONG" if side == "BUY" else "SHORT"
            risk = user.get("risk_usdt")
            if risk:
                lev = _calc_auto_leverage(user["size_usdt"], risk, entry, sl)
            else:
                lev = user.get("leverage", 10)

            # Get live price for accurate qty calculation
            import requests as _req
            try:
                _tk = _req.get("https://open-api.bingx.com/openApi/swap/v2/quote/price",
                               params={"symbol": BINGX_SYMBOL}, timeout=5).json()
                live_price = float((_tk.get("data") or {}).get("price", _last_signal["price"]))
            except Exception:
                live_price = _last_signal["price"]

            qty      = _calc_qty(user["size_usdt"], live_price, lev)
            tp1_qty, tp2_qty = _tp1_split(qty)
            _set_leverage(api_key, api_secret, side, lev)

            tp1 = float(_last_signal.get("tp1", 0))
            r = _place_order(api_key, api_secret, side, "MARKET", qty)
            if r.get("code") == 0:
                sl_r   = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                                      qty, stop_price=sl, position_side=trade_ps)
                tp1_r  = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                      tp1_qty, stop_price=tp1, position_side=trade_ps) if tp1 else {}
                tp2_r  = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                      tp2_qty, stop_price=tp2, position_side=trade_ps)
                user["in_position"]    = True
                user["pos_side"]       = side
                user["pos_qty"]        = qty
                user["sl_order_id"]    = str((sl_r.get("data") or {}).get("order", {}).get("orderId", ""))
                user["tp1_order_id"]   = str((tp1_r.get("data") or {}).get("order", {}).get("orderId", ""))
                user["tp_order_id"]    = str((tp2_r.get("data") or {}).get("order", {}).get("orderId", ""))
                user["limit_order_id"] = ""
                user["failed_copy"]    = False
                _set(target, user)
                send_reply_fn(chat_id,
                    f"<b>Retry Successful!</b>\n\n"
                    f"<blockquote>✅ @{user.get('username','?')} entered {side} {qty} BTC\n\n"
                    f"SL:  {sl:,.0f} (100%)\n"
                    f"TP1: {tp1:,.0f} (50%)\n"
                    f"TP2: {tp2:,.0f} (50%)\n\n"
                    f"<i>🛡️ Capital protected</i></blockquote>")
            else:
                err = r.get("msg", "unknown error")
                send_reply_fn(chat_id,
                    f"<b>Retry Failed</b>\n\n"
                    f"<blockquote>❌ @{user.get('username','?')}: {err}\n\n"
                    f"Check their BingX margin balance.\n\n"
                    f"<i>🛡️ Capital protected</i></blockquote>")
        except Exception as e:
            send_reply_fn(chat_id, f"❌ Retry error: {e}")
            print(f"[CT] /ctretry {target}: {e}")

    elif cmd == "/ctclose" and is_admin:
        if len(parts) < 2:
            rows = [[{"text": f"❌ Close @{u.get('username',uid)}", "callback_data": f"ctclose:{uid}"}] for uid, u in _db.items() if u.get("in_position") or u.get("copy_on")]
            rows.append([{"text": "❌ Close ALL users", "callback_data": "ctclose:all"}])
            send_reply_fn(chat_id, "⚠️ <b>Select user to close:</b>", reply_markup={"inline_keyboard": rows}); return
        if len(parts) >= 2 and parts[1].lower() != "all":
            # Close one specific user
            target = str(parts[1])
            ok, msg = on_close_user(target)
            send_reply_fn(chat_id,
                f"<b>CT Close</b>\n\n<blockquote>{'✅' if ok else '❌'} {msg}\n\n<i>🛡️ Capital protected</i></blockquote>")
        else:
            # Close all copy trade positions
            results = []
            for cid, user, api_key, api_secret in _users_with_copy():
                ok, msg = on_close_user(cid)
                results.append(f"{'✅' if ok else '❌'} {msg}")
            if not results:
                send_reply_fn(chat_id, "<b>CT Close All</b>\n\n<blockquote>No active copy users.\n\n<i>🛡️ Capital protected</i></blockquote>")
            else:
                send_reply_fn(chat_id,
                    f"<b>CT Close All</b>\n\n<blockquote>" + "\n".join(results) +
                    f"\n\n<i>🛡️ Capital protected</i></blockquote>")

    else:
        send_reply_fn(chat_id, f"Unknown command: {cmd}")
