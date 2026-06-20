"""
CLEXER V9.0 — BingX Copy Trade System
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

try:
    from cryptography.fernet import Fernet
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False
    print("[CT] cryptography not installed — API keys stored base64 only. Run: pip install cryptography")

_DATA_DIR      = os.getenv("DATA_DIR", ".")
CT_FILE        = os.path.join(_DATA_DIR, "copy_users.json")
CT_ENCRYPT_KEY = os.getenv("CT_ENCRYPT_KEY", "")
BINGX_BASE     = "https://open-api.bingx.com"
BINGX_SYMBOL   = "BTC-USDT"
IST            = timedelta(hours=5, minutes=30)
SCAN_CT_ENABLED = True   # toggle with /scancopy on|off

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

# ─── USER DATABASE ────────────────────────────────────────────────────────────

_db: dict = {}        # str(chat_id) → user_dict
_lock = threading.Lock()
_last_signal: dict = {}   # last active signal — cleared on SL/TP/cancel
_SIGNAL_FILE = os.path.join(_DATA_DIR, "ct_last_signal.json")

def _save_last_signal():
    try:
        with open(_SIGNAL_FILE, "w") as f:
            json.dump(_last_signal, f)
    except Exception as e:
        print(f"[CT] signal save error: {e}")

def _load_last_signal():
    global _last_signal
    try:
        if os.path.exists(_SIGNAL_FILE):
            with open(_SIGNAL_FILE) as f:
                _last_signal = json.load(f)
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

def load():
    global _db
    try:
        if os.path.exists(CT_FILE):
            with open(CT_FILE) as f:
                _db = json.load(f)
            print(f"[CT] Loaded {len(_db)} copy users")
    except Exception as e:
        print(f"[CT] Load error: {e}"); _db = {}
    _load_last_signal()

def _save():
    try:
        with open(CT_FILE, "w") as f:
            json.dump(_db, f, indent=2)
    except Exception as e:
        print(f"[CT] Save error: {e}")

def _get(cid: str) -> dict:
    return _db.get(str(cid), {})

def _set(cid: str, user: dict):
    with _lock:
        _db[str(cid)] = user
        _save()

def active_count() -> int:
    return sum(1 for u in _db.values() if u.get("copy_on") and u.get("connected") and not u.get("paused_by_admin"))

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
        for pos in (r.get("data") or {}).get("positions", []):
            if abs(float(pos.get("positionAmt", 0))) > 0:
                return pos
    return {}

def _calc_qty(size_usdt: float, price: float, leverage: int) -> float:
    if price <= 0: return 0.001
    qty = (size_usdt * leverage) / price
    return max(round(qty, 4), 0.001)

def _calc_pnl(side: str, entry: float, close_price: float, qty: float) -> float:
    if entry <= 0 or close_price <= 0 or qty <= 0: return 0.0
    raw = (close_price - entry) * qty if side == "BUY" else (entry - close_price) * qty
    return round(raw, 4)

def _record_pnl(user: dict, pnl: float):
    h = user.setdefault("history", {"total":0,"profit":0,"loss":0,
                                     "total_pnl":0.0,"won_usdt":0.0,"lost_usdt":0.0})
    # backfill missing keys for old users
    h.setdefault("total_pnl", 0.0); h.setdefault("won_usdt", 0.0); h.setdefault("lost_usdt", 0.0)
    h["total_pnl"] = round(h["total_pnl"] + pnl, 4)
    if pnl >= 0: h["won_usdt"]  = round(h["won_usdt"]  + pnl, 4)
    else:        h["lost_usdt"] = round(h["lost_usdt"] + abs(pnl), 4)

# ─── COPY TRADE MIRROR ACTIONS ────────────────────────────────────────────────

def _users_with_copy() -> list[tuple[str, dict, str, str]]:
    """Yield (cid, user, api_key, api_secret) for all active copy users."""
    out = []
    for cid, user in list(_db.items()):
        if not user.get("copy_on") or not user.get("connected") or user.get("paused_by_admin"):
            continue
        try:
            out.append((cid, user, _decrypt(user["api_key_enc"]), _decrypt(user["api_secret_enc"])))
        except Exception as e:
            print(f"[CT] decrypt error {cid}: {e}")
    return out

def on_signal(signal: dict, price: float) -> list[str]:
    """
    Called when bot generates BUY/SELL signal.
    MARKET entry  → open position + set SL + set TP2
    PULLBACK entry → place limit order at entry level
    Returns list of result strings for admin notification.
    """
    global _last_signal
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

    for cid, user, api_key, api_secret in _users_with_copy():
        try:
            risk = user.get("risk_usdt")
            if risk:
                lev = _calc_auto_leverage(user["size_usdt"], risk, entry, sl)
                print(f"[CT] {cid} auto-leverage: risk=${risk} size=${user['size_usdt']} SL%={abs(entry-sl)/entry*100:.2f}% → {lev}x")
            else:
                lev = user.get("leverage", 10)
            qty = _calc_qty(user["size_usdt"], price, lev)
            _set_leverage(api_key, api_secret, side, lev)

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
                    warnings = []
                    half_qty = max(round(qty / 2, 4), 0.001)

                    # ── SL order — full qty STOP_MARKET ──
                    sl_r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                                        qty, stop_price=sl, position_side=trade_ps)
                    sl_ok  = sl_r.get("code") == 0
                    sl_oid = str((sl_r.get("data") or {}).get("order", {}).get("orderId", ""))
                    if not sl_ok:
                        warnings.append(f"⚠️ SL FAILED @{uname}: {sl_r.get('msg','?')}")

                    # ── TP1 order — 50% qty at tp1 price ──
                    tp1_r  = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                          half_qty, stop_price=tp1, position_side=trade_ps)
                    tp1_ok = tp1_r.get("code") == 0
                    tp1_oid = str((tp1_r.get("data") or {}).get("order", {}).get("orderId", ""))
                    if not tp1_ok:
                        warnings.append(f"⚠️ TP1 FAILED @{uname}: {tp1_r.get('msg','?')}")

                    # ── TP2 order — remaining 50% qty at tp2 price ──
                    tp2_r  = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                          half_qty, stop_price=tp2, position_side=trade_ps)
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
    """TP1 hit — cancel TP1 order, close 50% at market, move position SL to breakeven."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if not user.get("in_position"): continue
        try:
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            pos_side   = "LONG" if user["pos_side"] == "BUY" else "SHORT"
            full_qty   = user.get("pos_qty", 0.001)
            half_qty   = max(round(full_qty / 2, 4), 0.001)

            # Cancel TP1 order and OLD SL order
            _cancel_order(api_key, api_secret, user.get("tp1_order_id", ""))
            _cancel_order(api_key, api_secret, user.get("sl_order_id", ""))

            # Close 50% at market
            _place_order(api_key, api_secret, close_side, "MARKET", half_qty,
                         position_side=pos_side)

            # Record TP1 PnL
            close_price = tp1 if tp1 > 0 else entry
            pnl = _calc_pnl(user["pos_side"], entry, close_price, half_qty)
            _record_pnl(user, pnl)
            user["history"]["total"] += 1; user["history"]["profit"] += 1

            # Remaining qty after closing half (use actual remainder, not half_qty again)
            remaining_qty = max(round(full_qty - half_qty, 4), 0.0001)
            # BE SL slightly inside entry so BingX accepts (SL must be < current price for LONG)
            be_sl_price = round(entry * 0.999, 2) if user["pos_side"] == "BUY" else round(entry * 1.001, 2)
            be_sl_r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                                   remaining_qty, stop_price=be_sl_price, position_side=pos_side)
            be_sl_ok  = be_sl_r.get("code") == 0
            be_sl_oid = str((be_sl_r.get("data") or {}).get("order", {}).get("orderId", ""))
            print(f"[CT] on_tp1 {cid}: BE SL@{be_sl_price:,.2f} qty={remaining_qty} code={be_sl_r.get('code')} msg={be_sl_r.get('msg','?')} oid={be_sl_oid}")

            user["tp1_order_id"] = ""
            user["sl_order_id"]  = be_sl_oid
            user["pos_qty"]      = half_qty
            _set(cid, user)
            print(f"[CT] on_tp1 {cid}: closed {half_qty} BTC @ {close_price:,.0f} pnl={pnl:+.2f} SL→BE@{entry:,.0f}")
        except Exception as e:
            print(f"[CT] on_tp1 {cid}: {e}")


def on_tp2(entry: float = 0, tp2: float = 0):
    """TP2 hit — cancel remaining orders, force-close if needed, update records."""
    global _last_signal
    _last_signal = {}
    _save_last_signal()
    print(f"[CT] on_tp2: users_with_copy={len(_users_with_copy())}")
    for cid, user, api_key, api_secret in _users_with_copy():
        print(f"[CT] on_tp2 {cid}: in_position={user.get('in_position')} pos_side={user.get('pos_side')}")
        if not user.get("in_position"): continue
        try:
            # Cancel any leftover TP1/SL orders
            _cancel_order(api_key, api_secret, user.get("tp1_order_id", ""))
            _cancel_order(api_key, api_secret, user.get("sl_order_id", ""))
            # Force-close remaining position (TP2 order auto-closes 50%, this closes any remainder)
            _close_position(api_key, api_secret, user["pos_side"])
        except Exception as e:
            print(f"[CT] on_tp2 {cid} close error: {e}")
        if entry > 0 and tp2 > 0:
            pnl = _calc_pnl(user["pos_side"], entry, tp2, user.get("pos_qty", 0.001))
            _record_pnl(user, pnl)
            print(f"[CT] on_tp2 {cid}: pnl={pnl:+.2f}")
        user["in_position"] = False; user["pos_side"] = ""; user["pos_qty"] = 0.0
        user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["tp1_order_id"] = ""
        user["failed_copy"] = False
        user["history"]["total"] += 1; user["history"]["profit"] += 1
        _set(cid, user)


def on_sl(entry: float = 0, sl: float = 0):
    """SL hit — force-close position on BingX, cancel open TP orders, update records."""
    global _last_signal
    _last_signal = {}
    _save_last_signal()
    print(f"[CT] on_sl: users_with_copy={len(_users_with_copy())}")
    for cid, user, api_key, api_secret in _users_with_copy():
        print(f"[CT] on_sl {cid}: in_position={user.get('in_position')} pos_side={user.get('pos_side')}")
        if not user.get("in_position"): continue
        try:
            pos_side = "LONG" if user["pos_side"] == "BUY" else "SHORT"
            # Cancel open TP1/TP2 orders first so they don't fire after close
            _cancel_order(api_key, api_secret, user.get("tp1_order_id", ""))
            _cancel_order(api_key, api_secret, user.get("tp_order_id", ""))
            _cancel_order(api_key, api_secret, user.get("sl_order_id", ""))
            # Force-close via closePosition endpoint
            close_r = _close_position(api_key, api_secret, user["pos_side"])
            print(f"[CT] on_sl {cid}: closePosition code={close_r.get('code')} msg={close_r.get('msg','')}")
            # Fallback: if closePosition fails, place market order for remaining qty
            if close_r.get("code") != 0:
                close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
                pos_side_str = "LONG" if user["pos_side"] == "BUY" else "SHORT"
                remaining = user.get("pos_qty", 0.001)
                fb_r = _place_order(api_key, api_secret, close_side, "MARKET", remaining, position_side=pos_side_str)
                print(f"[CT] on_sl {cid}: fallback MARKET close qty={remaining} code={fb_r.get('code')} msg={fb_r.get('msg','')}")
        except Exception as e:
            print(f"[CT] on_sl {cid} close error: {e}")
        # Record PnL
        if entry > 0 and sl > 0:
            pnl = _calc_pnl(user["pos_side"], entry, sl, user.get("pos_qty", 0.001))
            _record_pnl(user, pnl)
            print(f"[CT] on_sl {cid}: pnl={pnl:+.2f}")
        user["in_position"] = False; user["pos_side"] = ""; user["pos_qty"] = 0.0
        user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["tp1_order_id"] = ""
        user["failed_copy"] = False
        user["history"]["total"] += 1; user["history"]["loss"] += 1
        _set(cid, user)


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


def on_entry_hit(entry: float, sl: float, tp2: float):
    """
    Pullback entry triggered — limit order should have filled.
    Place SL + TP orders for copy users who had a pending limit order.
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
            half_qty   = max(round(qty / 2, 4), 0.001)

            # Place SL for full qty
            sl_r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                                qty, stop_price=sl, position_side=pos_side)
            # Place TP2 for 50% — TP1 will close the other half
            tp_r = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                half_qty, stop_price=tp2, position_side=pos_side)

            user["in_position"]    = True
            user["sl_order_id"]    = str((sl_r.get("data") or {}).get("order", {}).get("orderId", ""))
            user["tp_order_id"]    = str((tp_r.get("data") or {}).get("order", {}).get("orderId", ""))
            user["limit_order_id"] = ""
            _set(cid, user)
            print(f"[CT] on_entry_hit {cid}: SL@{sl:,.0f} TP2@{tp2:,.0f} placed")
        except Exception as e:
            print(f"[CT] on_entry_hit {cid}: {e}")


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
            # Close any open position
            for ps in ("LONG", "SHORT"):
                _bingx("POST", "/openApi/swap/v2/trade/closePosition", ak, ask,
                       {"symbol": symbol, "positionSide": ps})
            # Clear local state only if it matches this coin (BINGX_SYMBOL is BTC-USDT)
            if symbol == BINGX_SYMBOL:
                user["in_position"] = False; user["pos_side"] = ""; user["pos_qty"] = 0.0
                user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["limit_order_id"] = ""
                _set(cid, user)
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


def on_scan_signal(signal_dict: dict, symbol: str, price: float) -> list[str]:
    """
    Place a scan-sourced trade (alt coin) for all copy users.
    symbol = "ETH-USDT" / "SOL-USDT" etc
    signal_dict = {"signal":"BUY"/"SELL", "entry":float, "sl":float,
                   "tp1":float, "tp2":float, "entry_type":"MARKET"/"LIMIT"}
    """
    if not SCAN_CT_ENABLED:
        return ["[CT] scan copy trade is OFF (/scancopy on to enable)"]

    side       = signal_dict["signal"]           # BUY or SELL
    entry      = float(signal_dict["entry"])
    sl         = float(signal_dict["sl"])
    tp1        = float(signal_dict.get("tp1", 0))
    tp2        = float(signal_dict.get("tp2", 0))
    entry_type = signal_dict.get("entry_type", "MARKET")
    close_side = "SELL" if side == "BUY" else "BUY"
    trade_ps   = "LONG" if side == "BUY" else "SHORT"
    results    = []

    for cid, user, api_key, api_secret in _users_with_copy():
        try:
            risk = user.get("risk_usdt")
            if risk:
                lev = _calc_auto_leverage(user["size_usdt"], risk, entry, sl)
                print(f"[CT] {cid} scan auto-leverage: risk=${risk} size=${user['size_usdt']} SL%={abs(entry-sl)/entry*100:.2f}% → {lev}x")
            else:
                lev = user.get("leverage", 10)
            qty = _calc_qty(user["size_usdt"], price, lev)

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

            # Set leverage — if BingX rejects (symbol max < lev), binary search down to find max allowed
            lev_r = _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret,
                           {"symbol": symbol, "side": trade_ps, "leverage": lev})
            if lev_r.get("code") != 0:
                # BingX rejected leverage — find max supported by trying lower values
                for try_lev in [100, 75, 50, 25, 20, 10, 5, 2, 1]:
                    if try_lev >= lev: continue
                    r2 = _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret,
                                {"symbol": symbol, "side": trade_ps, "leverage": try_lev})
                    if r2.get("code") == 0:
                        lev = try_lev
                        qty = _calc_qty(user["size_usdt"], price, lev)
                        half_qty = max(round(qty / 2, 4), 0.001)
                        print(f"[CT] {cid} {symbol} leverage capped at {lev}x")
                        break

            if entry_type == "MARKET":
                r = _place_alt(side, "MARKET", qty)
                if r.get("code") == 0:
                    half = max(round(qty / 2, 4), 0.001)
                    uname = user.get("username", "?")

                    # ── Retry loop: up to 60s to place SL + TPs ──────────────
                    sl_ok = tp1_ok = tp2_ok = False
                    deadline = time.time() + 60
                    attempt = 0
                    while time.time() < deadline:
                        attempt += 1
                        if not sl_ok:
                            sl_r = _place_alt(close_side, "STOP_MARKET", qty, sp=sl, ps=trade_ps)
                            sl_ok = sl_r.get("code") == 0
                            if not sl_ok:
                                print(f"  [CT] @{uname} SL attempt {attempt} FAIL: {sl_r.get('msg','')}")
                        if not tp1_ok and tp1:
                            tp1_r = _place_alt(close_side, "TAKE_PROFIT_MARKET", half, sp=tp1, ps=trade_ps)
                            tp1_ok = tp1_r.get("code") == 0
                        if not tp2_ok and tp2:
                            tp2_r = _place_alt(close_side, "TAKE_PROFIT_MARKET", half, sp=tp2, ps=trade_ps)
                            tp2_ok = tp2_r.get("code") == 0
                        if sl_ok and (tp1_ok or not tp1) and (tp2_ok or not tp2):
                            break  # all placed
                        time.sleep(6)

                    if not sl_ok:
                        # SL still failing after 60s — close position to protect user
                        _bingx("POST", "/openApi/swap/v2/trade/closePosition",
                               api_key, api_secret, {"symbol": symbol, "positionSide": trade_ps})
                        results.append(
                            f"🚨 @{uname} {symbol} — SL failed after 60s ({attempt} attempts)"
                            f" — POSITION AUTO-CLOSED for safety")
                        continue

                    # Store scan trade details for monitor loop
                    user["scan_symbol"] = symbol
                    user["scan_side"]   = side
                    user["scan_entry"]  = entry
                    user["scan_sl"]     = sl
                    user["scan_tp1"]    = tp1
                    user["scan_tp2"]    = tp2
                    user["scan_qty"]    = qty
                    _set(cid, user)

                    tp_warn = ""
                    if tp1 and not tp1_ok: tp_warn += " ⚠️TP1 still failed"
                    if tp2 and not tp2_ok: tp_warn += " ⚠️TP2 still failed"
                    results.append(
                        f"✅ @{uname} {symbol} {side} {qty:.4f}"
                        f" SL=OK TP1={'OK' if not tp1 or tp1_ok else 'FAIL'}"
                        f" TP2={'OK' if not tp2 or tp2_ok else 'FAIL'}"
                        f" (attempts:{attempt}){tp_warn}")
                else:
                    results.append(f"❌ @{user.get('username','?')} {symbol}: {r.get('msg','?')}")
            else:
                r = _place_alt(side, "LIMIT", qty, pr=entry)
                if r.get("code") == 0:
                    results.append(f"✅ @{user.get('username','?')} {symbol} LIMIT {side} {qty:.4f} @ {entry}")
                else:
                    results.append(f"❌ @{user.get('username','?')} {symbol}: {r.get('msg','?')}")

        except Exception as e:
            results.append(f"❌ @{user.get('username','?')}: {e}")
            print(f"[CT] on_scan_signal {cid}: {e}")

    if not results:
        results = ["No copy users connected"]
    print(f"[CT] on_scan_signal {symbol}: {results}")
    return results


def on_scan_tp1(symbol: str):
    """Scan TP1 hit — cancel open orders, close 50% at market, move SL to BE."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if user.get("scan_symbol") != symbol: continue
        try:
            side        = user["scan_side"]
            entry_price = float(user.get("scan_entry", 0))
            close_side  = "SELL" if side == "BUY" else "BUY"
            trade_ps    = "LONG" if side == "BUY" else "SHORT"
            qty         = float(user.get("scan_qty", 0))
            half_qty    = max(round(qty / 2, 4), 0.001)

            # Cancel all open orders for this symbol first
            for o in _get_open_orders(api_key, api_secret, symbol):
                oid = str(o.get("orderId", ""))
                if oid:
                    _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                           {"symbol": symbol, "orderId": oid})

            if not entry_price:
                print(f"[CT] on_scan_tp1 {cid} {symbol}: scan_entry=0, cannot set BE SL")
                continue

            # Close 50% at market
            params = {"symbol": symbol, "side": close_side, "positionSide": trade_ps,
                      "type": "MARKET", "quantity": round(half_qty, 4)}
            close_r = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, params)
            print(f"[CT] on_scan_tp1 {cid} {symbol}: close50% code={close_r.get('code')} msg={close_r.get('msg','')}")

            # Wait for position to update before placing new SL
            time.sleep(3)

            # Move SL to BE (entry price) — use remaining half qty
            sl_r = _set_position_sl_sym(api_key, api_secret, symbol, trade_ps, entry_price, qty=half_qty)
            print(f"[CT] on_scan_tp1 {cid} {symbol}: BE SL@{entry_price} code={sl_r.get('code') if sl_r else '?'} msg={sl_r.get('msg','') if sl_r else ''}")

            # Re-place TP2 for remaining half
            tp2 = float(user.get("scan_tp2", 0))
            if tp2:
                params2 = {"symbol": symbol, "side": close_side, "positionSide": trade_ps,
                           "type": "TAKE_PROFIT_MARKET", "quantity": round(half_qty, 4),
                           "stopPrice": round(tp2, 6)}
                tp2_r = _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, params2)
                print(f"[CT] on_scan_tp1 {cid} {symbol}: TP2@{tp2} code={tp2_r.get('code')} msg={tp2_r.get('msg','')}")

            user["scan_qty"] = half_qty
            _set(cid, user)
            print(f"[CT] on_scan_tp1 {cid} {symbol}: done — closed {half_qty} SL→BE@{entry_price}")
        except Exception as e:
            print(f"[CT] on_scan_tp1 {cid} {symbol}: {e}")


def on_scan_tp2(symbol: str):
    """Scan TP2 hit — cancel remaining orders, force-close position, clear scan state."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if user.get("scan_symbol") != symbol: continue
        try:
            trade_ps = "LONG" if user["scan_side"] == "BUY" else "SHORT"

            # Cancel all open orders for symbol
            for o in _get_open_orders(api_key, api_secret, symbol):
                oid = str(o.get("orderId", ""))
                if oid:
                    _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                           {"symbol": symbol, "orderId": oid})

            # Force-close any remaining position
            _bingx("POST", "/openApi/swap/v2/trade/closePosition", api_key, api_secret,
                   {"symbol": symbol, "positionSide": trade_ps})

            print(f"[CT] on_scan_tp2 {cid} {symbol}: closed")
        except Exception as e:
            print(f"[CT] on_scan_tp2 {cid} {symbol}: {e}")
        _clear_scan_state(cid, user)


def on_scan_sl(symbol: str):
    """Scan SL hit — cancel all orders, force-close position, clear scan state."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if user.get("scan_symbol") != symbol: continue
        try:
            trade_ps = "LONG" if user["scan_side"] == "BUY" else "SHORT"

            for o in _get_open_orders(api_key, api_secret, symbol):
                oid = str(o.get("orderId", ""))
                if oid:
                    _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                           {"symbol": symbol, "orderId": oid})

            _bingx("POST", "/openApi/swap/v2/trade/closePosition", api_key, api_secret,
                   {"symbol": symbol, "positionSide": trade_ps})

            print(f"[CT] on_scan_sl {cid} {symbol}: closed")
        except Exception as e:
            print(f"[CT] on_scan_sl {cid} {symbol}: {e}")
        _clear_scan_state(cid, user)


def on_scan_entry_missed(symbol: str):
    """Scan PULLBACK entry missed — cancel limit orders for this symbol, clear scan state."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if user.get("scan_symbol") != symbol: continue
        try:
            for o in _get_open_orders(api_key, api_secret, symbol):
                oid = str(o.get("orderId", ""))
                if oid:
                    _bingx("DELETE", "/openApi/swap/v2/trade/order", api_key, api_secret,
                           {"symbol": symbol, "orderId": oid})
            print(f"[CT] on_scan_entry_missed {cid} {symbol}: limit cancelled")
        except Exception as e:
            print(f"[CT] on_scan_entry_missed {cid} {symbol}: {e}")
        _clear_scan_state(cid, user)


def _clear_scan_state(cid: str, user: dict):
    user["scan_symbol"] = ""; user["scan_side"] = ""
    user["scan_entry"] = 0; user["scan_sl"] = 0; user["scan_tp1"] = 0
    user["scan_tp2"] = 0; user["scan_qty"] = 0
    _set(cid, user)


def _set_position_sl_sym(api_key: str, api_secret: str, symbol: str, pos_side: str, sl_price: float, qty: float = 0):
    """Place new BE SL order for alt-coin after TP1 hit."""
    close_side = "SELL" if pos_side == "LONG" else "BUY"
    q = max(round(qty, 4), 0.001) if qty > 0 else 0.001
    return _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret,
                  {"symbol": symbol, "side": close_side, "positionSide": pos_side,
                   "type": "STOP_MARKET", "quantity": q,
                   "stopPrice": round(sl_price, 6)})


def _get_open_orders(api_key: str, api_secret: str, symbol: str) -> list:
    """Fetch all open orders for a symbol."""
    r = _bingx("GET", "/openApi/swap/v2/trade/openOrders", api_key, api_secret,
               {"symbol": symbol})
    return (r.get("data") or {}).get("orders", [])


def _get_all_positions(api_key: str, api_secret: str) -> list:
    """Fetch all open positions (all symbols)."""
    r = _bingx("GET", "/openApi/swap/v2/user/positions", api_key, api_secret, {})
    if r.get("code") == 0:
        return [p for p in (r.get("data") or {}).get("positions", [])
                if abs(float(p.get("positionAmt", 0))) > 0]
    return []


def monitor_sl_tp(notify_fn=None):
    """
    Check every connected user's open positions and verify SL + TP orders exist.
    Re-places any missing ones. Call this every hour from bot main loop.
    notify_fn(text) — optional callback to send admin alert.
    """
    fixes = []
    for cid, user in list(_db.items()):
        if not user.get("connected"): continue
        try:
            ak  = _decrypt(user["api_key_enc"])
            ask = _decrypt(user["api_secret_enc"])
            uname = user.get("username", cid)

            positions = _get_all_positions(ak, ask)
            if not positions:
                continue

            for pos in positions:
                sym        = pos.get("symbol", "")
                pos_side   = pos.get("positionSide", "")   # LONG or SHORT
                pos_amt    = abs(float(pos.get("positionAmt", 0)))
                if pos_amt <= 0 or not sym or not pos_side: continue

                close_side = "SELL" if pos_side == "LONG" else "BUY"

                # Get expected SL/TP from stored state
                is_btc  = (sym == BINGX_SYMBOL)
                is_scan = (sym == user.get("scan_symbol", ""))

                if is_btc:
                    sl_price  = user.get("scan_sl", 0)   # fallback
                    tp1_price = 0
                    tp2_price = 0
                    # BTC SL/TP tracked by order IDs — just check existence
                elif is_scan:
                    sl_price  = float(user.get("scan_sl",  0))
                    tp1_price = float(user.get("scan_tp1", 0))
                    tp2_price = float(user.get("scan_tp2", 0))
                else:
                    continue  # unknown position, skip

                # Fetch open orders for this symbol
                open_orders = _get_open_orders(ak, ask, sym)
                has_sl  = any(o.get("type") == "STOP_MARKET"        and o.get("positionSide") == pos_side for o in open_orders)
                has_tp1 = any(o.get("type") == "TAKE_PROFIT_MARKET" and o.get("positionSide") == pos_side for o in open_orders)
                tp_orders = [o for o in open_orders if o.get("type") == "TAKE_PROFIT_MARKET" and o.get("positionSide") == pos_side]
                has_tp2 = len(tp_orders) >= 2

                placed = []

                if not has_sl and sl_price > 0:
                    r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                        "symbol": sym, "side": close_side, "positionSide": pos_side,
                        "type": "STOP_MARKET", "quantity": round(pos_amt, 4),
                        "stopPrice": round(sl_price, 6),
                    })
                    if r.get("code") == 0:
                        placed.append("SL fixed")
                    else:
                        placed.append(f"SL fix FAIL:{r.get('msg','')[:30]}")

                half = max(round(pos_amt / 2, 4), 0.001)

                if not has_tp1 and tp1_price > 0:
                    r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                        "symbol": sym, "side": close_side, "positionSide": pos_side,
                        "type": "TAKE_PROFIT_MARKET", "quantity": half,
                        "stopPrice": round(tp1_price, 6),
                    })
                    if r.get("code") == 0:
                        placed.append("TP1 fixed")
                    else:
                        placed.append(f"TP1 fix FAIL:{r.get('msg','')[:30]}")

                if not has_tp2 and tp2_price > 0:
                    r = _bingx("POST", "/openApi/swap/v2/trade/order", ak, ask, {
                        "symbol": sym, "side": close_side, "positionSide": pos_side,
                        "type": "TAKE_PROFIT_MARKET", "quantity": half,
                        "stopPrice": round(tp2_price, 6),
                    })
                    if r.get("code") == 0:
                        placed.append("TP2 fixed")
                    else:
                        placed.append(f"TP2 fix FAIL:{r.get('msg','')[:30]}")

                if placed:
                    msg = f"🔧 [Monitor] @{uname} {sym}: {', '.join(placed)}"
                    fixes.append(msg)
                    print(f"[CT] {msg}")
                else:
                    print(f"[CT] [Monitor] @{uname} {sym}: SL+TP OK")

        except Exception as e:
            print(f"[CT] monitor {cid}: {e}")

    if fixes and notify_fn:
        notify_fn("🔧 <b>SL/TP Monitor fixed:</b>\n" + "\n".join(fixes))
    elif not fixes:
        print("[CT] [Monitor] All positions have SL+TP ✅")

    return fixes


def start_monitor_loop(notify_fn=None, interval_hours: int = 1):
    """Start background thread that runs monitor_sl_tp every interval_hours."""
    import threading as _th
    def _loop():
        while True:
            time.sleep(interval_hours * 3600)
            print(f"[CT] Running SL/TP monitor check...")
            try:
                monitor_sl_tp(notify_fn)
            except Exception as e:
                print(f"[CT] monitor loop error: {e}")
    t = _th.Thread(target=_loop, daemon=True)
    t.start()
    print(f"[CT] SL/TP monitor started — checks every {interval_hours}h")


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
                     "/copytrade", "/mytrade", "/mysize", "/myhistory"}
CT_ADMIN_COMMANDS = {"/allusers", "/user", "/kick", "/pauseuser",
                     "/ctretry", "/ctstatus", "/ctclose"}

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
                "<b>Connect BingX</b>\n\nUsage:\n<code>/connect API_KEY API_SECRET</code>\n\n"
                "⚠️ Use <b>read + trade</b> permissions only.\nNEVER enable withdrawal on the key.")
            return
        api_key = parts[1]; api_secret = parts[2]
        send_reply_fn(chat_id, "Testing API key...")
        ok, err = _test_api(api_key, api_secret)
        if not ok:
            send_reply_fn(chat_id, f"<b>Connection Failed</b>\n\n{err}\n\nCheck key + secret and try again.")
            return
        user = _get(cid) or _default_user(username)
        user["api_key_enc"]    = _encrypt(api_key)
        user["api_secret_enc"] = _encrypt(api_secret)
        user["connected"]      = True
        user["username"]       = username
        _set(cid, user)
        send_reply_fn(chat_id,
            "<b>BingX Connected!</b>\n\n"
            "✅ API verified\n\n"
            f"Margin per trade: <b>${user['size_usdt']} USDT</b>\n"
            f"Leverage: <b>{user['leverage']}x</b> (manual)\n\n"
            "/copytrade on — enable auto-copy\n"
            "/setsize 50 — change margin per trade\n"
            "/setrisk 2 — auto-leverage (max $2 loss per trade) ⭐\n"
            "/setleverage 10 — manual leverage\n\n"
            "<i>— CLEXER V9.0 —</i>")

    elif cmd == "/disconnect":
        user = _get(cid)
        if not user:
            send_reply_fn(chat_id, "No account connected."); return
        user["api_key_enc"] = ""; user["api_secret_enc"] = ""
        user["connected"] = False; user["copy_on"] = False
        _set(cid, user)
        send_reply_fn(chat_id,
            "<b>Disconnected</b>\n\n"
            "BingX API keys removed. Open positions remain open — manage them manually.\n\n"
            "<i>— CLEXER V9.0 —</i>")

    elif cmd == "/setsize":
        if len(parts) < 2:
            user = _get(cid) or {}
            send_reply_fn(chat_id,
                f"Current size: <b>${user.get('size_usdt',50)} USDT</b>\n\n"
                f"Usage: /setsize 50"); return
        try:
            size = float(parts[1])
            if size < 0.5 or size > 10000:
                send_reply_fn(chat_id, "Size must be $0.50–$10,000 USDT"); return
            user = _get(cid) or _default_user(username)
            user["size_usdt"] = size; _set(cid, user)
            send_reply_fn(chat_id,
                f"<b>Trade Size Set</b>\n\n"
                f"Size: <b>${size} USDT</b> | Leverage: <b>{user['leverage']}x</b>\n"
                f"Exposure per trade: <b>${size * user['leverage']:.0f}</b>\n\n"
                f"<i>— CLEXER V9.0 —</i>")
        except: send_reply_fn(chat_id, "Usage: /setsize 50")

    elif cmd == "/setleverage":
        if len(parts) < 2:
            user = _get(cid) or {}
            send_reply_fn(chat_id,
                f"Current leverage: <b>{user.get('leverage',10)}x</b>\n\n"
                f"Usage: /setleverage 10\n\n"
                f"<i>Tip: Use /setrisk to auto-set leverage by max loss per trade instead.</i>"); return
        try:
            lev = int(parts[1])
            if lev < 1 or lev > 125:
                send_reply_fn(chat_id, "Leverage must be 1–125x"); return
            user = _get(cid) or _default_user(username)
            user["leverage"] = lev
            user["risk_usdt"] = None  # disable auto-leverage mode when manual leverage is set
            _set(cid, user)
            send_reply_fn(chat_id,
                f"<b>Leverage Set (Manual)</b>\n\n"
                f"Leverage: <b>{lev}x</b> | Size: <b>${user['size_usdt']} USDT</b>\n"
                f"Exposure per trade: <b>${user['size_usdt']*lev:.0f}</b>\n\n"
                f"<i>Auto-risk mode disabled. Use /setrisk to enable it.</i>\n\n"
                f"<i>— CLEXER V9.0 —</i>")
        except: send_reply_fn(chat_id, "Usage: /setleverage 10")

    elif cmd == "/setrisk":
        if len(parts) < 2:
            user = _get(cid) or {}
            risk = user.get("risk_usdt")
            size = user.get("size_usdt", 50)
            if risk:
                send_reply_fn(chat_id,
                    f"<b>Auto-Risk Mode: ON ✅</b>\n\n"
                    f"Max loss per trade: <b>${risk} USDT</b>\n"
                    f"Margin per trade: <b>${size} USDT</b>\n\n"
                    f"Leverage is auto-calculated each trade based on SL distance.\n\n"
                    f"Usage: /setrisk 2  — set max $2 loss per trade\n"
                    f"/setrisk off — disable, use manual leverage\n\n"
                    f"<i>— CLEXER V9.0 —</i>")
            else:
                send_reply_fn(chat_id,
                    f"<b>Auto-Risk Mode: OFF</b>\n\n"
                    f"Currently using manual leverage: <b>{user.get('leverage',10)}x</b>\n\n"
                    f"Usage: /setrisk 2  — auto-set leverage so max loss = $2 per trade\n"
                    f"Range: $1 – $50\n\n"
                    f"<i>— CLEXER V9.0 —</i>")
            return
        arg = parts[1].lower()
        if arg == "off":
            user = _get(cid) or _default_user(username)
            user["risk_usdt"] = None
            _set(cid, user)
            send_reply_fn(chat_id,
                f"<b>Auto-Risk Mode: OFF</b>\n\n"
                f"Using manual leverage: <b>{user.get('leverage',10)}x</b>\n\n"
                f"<i>— CLEXER V9.0 —</i>")
            return
        try:
            risk = float(arg)
            if risk < 0.5 or risk > 50:
                send_reply_fn(chat_id, "Risk must be $0.50 – $50 per trade"); return
            user = _get(cid) or _default_user(username)
            size = user.get("size_usdt", 50)
            user["risk_usdt"] = risk
            _set(cid, user)
            # Show example with a typical 2% SL
            example_lev = _calc_auto_leverage(size, risk, 100, 98)  # 2% SL example
            send_reply_fn(chat_id,
                f"<b>Auto-Risk Mode: ON ✅</b>\n\n"
                f"Max loss per trade: <b>${risk} USDT</b>\n"
                f"Margin per trade: <b>${size} USDT</b>\n\n"
                f"<b>How it works:</b>\n"
                f"Leverage is auto-calculated per trade based on SL distance.\n"
                f"Example (2% SL): leverage = {example_lev}x → max loss ≈ ${size * example_lev * 0.02:.2f}\n\n"
                f"<i>Closer SL = higher leverage | Wider SL = lower leverage</i>\n\n"
                f"<i>— CLEXER V9.0 —</i>")
        except: send_reply_fn(chat_id, "Usage: /setrisk 2")

    elif cmd == "/copytrade":
        if len(parts) < 2:
            user = _get(cid) or {}
            send_reply_fn(chat_id,
                f"Copy Trade: <b>{'ON' if user.get('copy_on') else 'OFF'}</b>\n\n"
                f"Usage: /copytrade on|off"); return
        user = _get(cid)
        if not user or not user.get("connected"):
            send_reply_fn(chat_id,
                "Connect BingX first:\n<code>/connect API_KEY API_SECRET</code>"); return
        if user.get("paused_by_admin"):
            send_reply_fn(chat_id, "Your copy trade is paused by admin."); return
        state = parts[1].lower()
        if state == "on":
            user["copy_on"] = True; _set(cid, user)
            send_reply_fn(chat_id,
                "<b>Copy Trade ON ✅</b>\n\n"
                f"You will auto-copy all CLEXER signals.\n"
                f"Size: <b>${user['size_usdt']} USDT</b> | Leverage: <b>{user['leverage']}x</b>\n\n"
                "<b>⚠️ Warning:</b> Real money. You are responsible for your trades.\n\n"
                "<i>— CLEXER V9.0 —</i>")
        elif state == "off":
            user["copy_on"] = False; _set(cid, user)
            send_reply_fn(chat_id,
                "<b>Copy Trade OFF</b>\n\nNo more auto-copies.\n"
                "Open positions remain open — manage them on BingX.\n\n"
                "<i>— CLEXER V9.0 —</i>")
        else:
            send_reply_fn(chat_id, "Usage: /copytrade on|off")

    elif cmd == "/mytrade":
        user = _get(cid)
        if not user or not user.get("connected"):
            send_reply_fn(chat_id,
                "No BingX connected.\n\n<code>/connect API_KEY API_SECRET</code>"); return
        try:
            api_key = _decrypt(user["api_key_enc"]); api_secret = _decrypt(user["api_secret_enc"])
            pos = _get_position(api_key, api_secret)
            if not pos:
                send_reply_fn(chat_id, "<b>No Open Position</b>\n\nBingX account clear.\n\n<i>— CLEXER V9.0 —</i>")
            else:
                amt   = float(pos.get("positionAmt", 0))
                pnl   = float(pos.get("unrealizedProfit", 0))
                entry = float(pos.get("avgPrice", 0))
                lev   = pos.get("leverage","?")
                side  = "LONG" if amt > 0 else "SHORT"
                pnl_s = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
                send_reply_fn(chat_id,
                    f"<b>Your BingX Position</b>\n\n"
                    f"{'🟢' if side=='LONG' else '🔴'} {side} {abs(amt):.4f} BTC\n\n"
                    f"Entry:    <b>{entry:,.2f}</b>\n"
                    f"Leverage: <b>{lev}x</b>\n"
                    f"PnL:      <b>{pnl_s}</b>\n\n"
                    f"<i>— CLEXER V9.0 —</i>")
        except Exception as e:
            send_reply_fn(chat_id, f"Error: {e}")

    elif cmd == "/mysize":
        user = _get(cid) or {}
        size = user.get("size_usdt", 50)
        risk = user.get("risk_usdt")
        lev  = user.get("leverage", 10)
        if risk:
            lev_line = f"Leverage: <b>Auto (max ${risk} loss/trade)</b>"
            exp_line = f"Max loss per trade: <b>${risk} USDT</b>"
        else:
            lev_line = f"Leverage: <b>{lev}x</b>"
            exp_line = f"Exposure per trade: <b>${size * lev:.0f}</b>"
        send_reply_fn(chat_id,
            f"<b>Your Settings</b>\n\n"
            f"BingX: {'✅ Connected' if user.get('connected') else '❌ Not connected'}\n"
            f"Copy Trade: <b>{'ON' if user.get('copy_on') else 'OFF'}</b>\n"
            f"Margin per trade: <b>${size} USDT</b>\n"
            f"{lev_line}\n"
            f"{exp_line}\n\n"
            f"<i>/setrisk 2 — auto leverage by max loss\n"
            f"/setleverage 10 — manual leverage\n"
            f"/setsize 50 — change margin</i>\n\n"
            f"<i>— CLEXER V9.0 —</i>")

    elif cmd == "/myhistory":
        user = _get(cid) or {}
        h = user.get("history", {"total":0,"profit":0,"loss":0,"total_pnl":0.0,"won_usdt":0.0,"lost_usdt":0.0})
        h.setdefault("total_pnl", 0.0); h.setdefault("won_usdt", 0.0); h.setdefault("lost_usdt", 0.0)
        wr   = f"{h['profit']/h['total']*100:.0f}%" if h["total"] else "—"
        pnl  = h["total_pnl"]
        pnl_s = f"+${pnl:.2f} 🟢" if pnl > 0 else (f"-${abs(pnl):.2f} 🔴" if pnl < 0 else "$0.00")
        send_reply_fn(chat_id,
            f"<b>Your Copy Trade History</b>\n\n"
            f"Total trades: <b>{h['total']}</b>\n"
            f"Wins:         <b>{h['profit']}</b>  (+${h['won_usdt']:.2f})\n"
            f"Losses:       <b>{h['loss']}</b>  (-${h['lost_usdt']:.2f})\n"
            f"Win rate:     <b>{wr}</b>\n\n"
            f"Total PnL:    <b>{pnl_s}</b>\n\n"
            f"Size: ${user.get('size_usdt',50)} | Leverage: {user.get('leverage',10)}x\n\n"
            f"<i>— CLEXER V9.0 —</i>")

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
            f"Total registered:   {total}\n"
            f"BingX connected:    {connected}\n"
            f"Copy trade active:  {active}\n"
            f"In position now:    {in_pos}\n"
            f"Total exposure:     <b>${exposure:,.0f}</b>\n\n"
            f"<i>— CLEXER V9.0 —</i>")

    elif cmd == "/users" and is_admin:
        if not _db:
            send_reply_fn(chat_id, "No copy trade users yet."); return
        lines = [f"<b>Copy Trade Users ({len(_db)})</b>\n"]
        for i, (uid, user) in enumerate(_db.items(), 1):
            uname    = f"@{user.get('username','?')}"
            bingx_ok = "✅" if user.get("connected") else "❌"
            copy_s   = "ON" if user.get("copy_on") else "OFF"
            pos_line = f"\n     Pos: {user.get('pos_side','?')}" if user.get("in_position") else ""
            paused   = " ⛔" if user.get("paused_by_admin") else ""
            lines.append(
                f"{i}. {uname}{paused} | <code>{uid}</code>\n"
                f"   BingX:{bingx_ok} Copy:{copy_s} | "
                f"${user.get('size_usdt',0):.0f} {user.get('leverage',1)}x"
                f"{pos_line}\n")
        send_reply_fn(chat_id, "\n".join(lines))

    elif cmd == "/user" and is_admin:
        if len(parts) < 2:
            send_reply_fn(chat_id, "Usage: /user TELEGRAM_ID"); return
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
        send_reply_fn(chat_id,
            f"<b>@{user.get('username','?')}</b> | <code>{target}</code>{paused}\n\n"
            f"BingX: {'✅ Connected' if user.get('connected') else '❌ Not connected'}\n"
            f"Copy Trade: {'ON' if user.get('copy_on') else 'OFF'}\n"
            f"Size: <b>${user.get('size_usdt',0)} USDT</b> | Leverage: <b>{user.get('leverage',1)}x</b>"
            f"{pos_info}\n\n"
            f"Trades: {h['total']} | Wins: {h['profit']} | Losses: {h['loss']} | WR: {wr}\n"
            f"Won:  +${h['won_usdt']:.2f}  |  Lost: -${h['lost_usdt']:.2f}\n"
            f"Total PnL: <b>{pnl_s}</b>\n\n"
            f"Joined: {user.get('joined','?')}\n\n"
            f"<i>— CLEXER V9.0 —</i>")

    elif cmd == "/kick" and is_admin:
        if len(parts) < 2:
            send_reply_fn(chat_id, "Usage: /kick TELEGRAM_ID"); return
        target = str(parts[1]); user = _db.get(target)
        if not user:
            send_reply_fn(chat_id, f"User {target} not found."); return
        if user.get("connected"):
            try:
                _cancel_all_orders(_decrypt(user["api_key_enc"]), _decrypt(user["api_secret_enc"]))
            except: pass
        with _lock:
            del _db[target]; _save()
        send_reply_fn(chat_id,
            f"<b>User Removed</b>\n\n"
            f"@{user.get('username','?')} (ID:{target})\n"
            f"Orders cancelled. API keys deleted.\n\n"
            f"<i>— CLEXER V9.0 —</i>")

    elif cmd == "/pauseuser" and is_admin:
        if len(parts) < 2:
            send_reply_fn(chat_id, "Usage: /pauseuser TELEGRAM_ID"); return
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
            f"@{user.get('username','?')} (ID:{target})\n\n"
            f"<i>— CLEXER V9.0 —</i>")

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
            send_reply_fn(chat_id, f"<b>Copy Trade Status</b>\n\nNo failed copies.{sig_info}\n\n<i>— CLEXER V9.0 —</i>")
            return
        lines = [f"<b>Failed Copy Users ({len(failed)})</b>"]
        for cid, u in failed:
            lines.append(f"- @{u.get('username','?')} | ID: <code>{cid}</code>\n"
                         f"  Use: /ctretry {cid}")
        send_reply_fn(chat_id, "\n".join(lines) + sig_info + "\n\n<i>— CLEXER V9.0 —</i>")

    elif cmd == "/ctretry" and is_admin:
        """Retry copy trade for a specific user.
        /ctretry USER_ID          → retry BTC trade
        /ctretry USER_ID SOL      → retry SOL-USDT scan trade
        /ctretry USER_ID all      → retry ALL active scan trades
        """
        if len(parts) < 2:
            send_reply_fn(chat_id,
                "<b>Retry Copy Trade</b>\n\n"
                "<code>/ctretry USER_ID</code> — retry BTC trade\n"
                "<code>/ctretry USER_ID SOL</code> — retry SOL scan trade\n"
                "<code>/ctretry USER_ID all</code> — retry all active scan trades\n\n"
                "Use /ctstatus to see failed users."); return

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
                    half = max(round(qty / 2, 4), 0.001)

                    lev_r = _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret,
                                   {"symbol": sym, "side": trade_ps, "leverage": lev})
                    if lev_r.get("code") != 0:
                        for try_lev in [100, 75, 50, 25, 20, 10, 5, 2, 1]:
                            if try_lev >= lev: continue
                            if _bingx("POST", "/openApi/swap/v2/trade/leverage", api_key, api_secret,
                                      {"symbol": sym, "side": trade_ps, "leverage": try_lev}).get("code") == 0:
                                lev = try_lev; qty = _calc_qty(user["size_usdt"], entry, lev)
                                half = max(round(qty / 2, 4), 0.001); break

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
                                tp1_ok = _alt(close_side, "TAKE_PROFIT_MARKET", half, sp=tp1).get("code") == 0
                            if tp2 and not tp2_ok:
                                tp2_ok = _alt(close_side, "TAKE_PROFIT_MARKET", half, sp=tp2).get("code") == 0
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
                        user["scan_qty"] = half if tp1_hit else qty  # if TP1 already hit, only 50% remains
                        _set(target, user)
                        warn = ("" if tp1_ok else " ⚠️TP1 failed") + ("" if tp2_ok else " ⚠️TP2 failed")
                        results.append(f"✅ {sym} {side} {qty:.4f} lev={lev}x{warn}")
                    else:
                        results.append(f"❌ {sym}: {r.get('msg','?')}")
                except Exception as e:
                    results.append(f"❌ {sym}: {e}")

            send_reply_fn(chat_id,
                f"<b>Scan Retry — @{uname}</b>\n\n" + "\n".join(results) + "\n\n<i>— CLEXER V9.0 —</i>")
            return

        # ── BTC RETRY (existing logic below) ──────────────────────────────────
        if not _last_signal:
            send_reply_fn(chat_id,
                "<b>Retry Blocked</b>\n\n"
                "No active BTC signal — trade already closed or no signal yet.\n\n"
                "<i>— CLEXER V9.0 —</i>"); return

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
            half_qty = max(round(qty / 2, 4), 0.001)
            _set_leverage(api_key, api_secret, side, lev)

            tp1 = float(_last_signal.get("tp1", 0))
            r = _place_order(api_key, api_secret, side, "MARKET", qty)
            if r.get("code") == 0:
                sl_r   = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                                      qty, stop_price=sl, position_side=trade_ps)
                tp1_r  = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                      half_qty, stop_price=tp1, position_side=trade_ps) if tp1 else {}
                tp2_r  = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                      half_qty, stop_price=tp2, position_side=trade_ps)
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
                    f"✅ @{user.get('username','?')} entered {side} {qty} BTC\n\n"
                    f"SL:  {sl:,.0f} (100%)\n"
                    f"TP1: {tp1:,.0f} (50%)\n"
                    f"TP2: {tp2:,.0f} (50%)\n\n"
                    f"<i>— CLEXER V9.0 —</i>")
            else:
                err = r.get("msg", "unknown error")
                send_reply_fn(chat_id,
                    f"<b>Retry Failed</b>\n\n"
                    f"❌ @{user.get('username','?')}: {err}\n\n"
                    f"Check their BingX margin balance.\n\n"
                    f"<i>— CLEXER V9.0 —</i>")
        except Exception as e:
            send_reply_fn(chat_id, f"❌ Retry error: {e}")
            print(f"[CT] /ctretry {target}: {e}")

    elif cmd == "/ctclose" and is_admin:
        if len(parts) >= 2 and parts[1].lower() != "all":
            # Close one specific user
            target = str(parts[1])
            ok, msg = on_close_user(target)
            send_reply_fn(chat_id,
                f"<b>CT Close</b>\n\n{'✅' if ok else '❌'} {msg}\n\n<i>— CLEXER V9.0 —</i>")
        else:
            # Close all copy trade positions
            results = []
            for cid, user, api_key, api_secret in _users_with_copy():
                ok, msg = on_close_user(cid)
                results.append(f"{'✅' if ok else '❌'} {msg}")
            if not results:
                send_reply_fn(chat_id, "<b>CT Close All</b>\n\nNo active copy users.\n\n<i>— CLEXER V9.0 —</i>")
            else:
                send_reply_fn(chat_id,
                    f"<b>CT Close All</b>\n\n" + "\n".join(results) +
                    f"\n\n<i>— CLEXER V9.0 —</i>")

    else:
        send_reply_fn(chat_id, f"Unknown command: {cmd}")
