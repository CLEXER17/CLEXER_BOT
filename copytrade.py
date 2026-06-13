"""
CLEXER V7.0 — BingX Copy Trade System
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

CT_FILE        = "copy_users.json"
CT_ENCRYPT_KEY = os.getenv("CT_ENCRYPT_KEY", "")
BINGX_BASE     = "https://open-api.bingx.com"
BINGX_SYMBOL   = "BTC-USDT"
IST            = timedelta(hours=5, minutes=30)

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

def _default_user(username: str = "?") -> dict:
    return {
        "username":       username,
        "api_key_enc":    "",
        "api_secret_enc": "",
        "connected":      False,
        "copy_on":        False,
        "size_usdt":      50.0,
        "leverage":       10,
        "sl_order_id":    "",    # BingX order ID of current SL order
        "tp_order_id":    "",    # BingX order ID of current TP2 order
        "limit_order_id": "",    # BingX order ID of pending limit entry
        "in_position":    False,
        "pos_side":       "",    # "BUY" or "SELL"
        "history":        {"total": 0, "profit": 0, "loss": 0},
        "paused_by_admin": False,
        "joined":         _now_ist(),
    }

def load():
    global _db
    try:
        if os.path.exists(CT_FILE):
            with open(CT_FILE) as f:
                _db = json.load(f)
            print(f"[CT] Loaded {len(_db)} copy users")
    except Exception as e:
        print(f"[CT] Load error: {e}")
        _db = {}

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

# ─── BINGX API CLIENT ─────────────────────────────────────────────────────────

def _sign(params: dict, secret: str) -> str:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()

def _bingx(method: str, path: str, api_key: str, api_secret: str, params: dict = None) -> dict:
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params, api_secret)
    headers = {"X-BX-APIKEY": api_key}
    url = BINGX_BASE + path
    try:
        if method == "GET":
            r = requests.get(url, params=params, headers=headers, timeout=15)
        elif method == "POST":
            r = requests.post(url, params=params, headers=headers, timeout=15)
        elif method == "DELETE":
            r = requests.delete(url, params=params, headers=headers, timeout=15)
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
                 close_position: bool = False) -> dict:
    pos_side = "LONG" if side == "BUY" else "SHORT"
    params = {
        "symbol":       BINGX_SYMBOL,
        "side":         side,
        "positionSide": pos_side,
        "type":         order_type,
    }
    if close_position:
        params["closePosition"] = "true"
    else:
        params["quantity"] = round(quantity, 4)
    if order_type == "LIMIT" and price:
        params["price"] = round(price, 1)
        params["timeInForce"] = "GTC"
    if stop_price and order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
        params["stopPrice"] = round(stop_price, 1)
    return _bingx("POST", "/openApi/swap/v2/trade/order", api_key, api_secret, params)

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
    side        = signal["signal"]          # "BUY" or "SELL"
    entry       = float(signal["entry"])
    sl          = float(signal["sl"])
    tp2         = float(signal["tp2"])
    entry_type  = signal.get("entry_type", "MARKET")
    close_side  = "SELL" if side == "BUY" else "BUY"
    results     = []

    for cid, user, api_key, api_secret in _users_with_copy():
        try:
            lev = user.get("leverage", 10)
            qty = _calc_qty(user["size_usdt"], price, lev)
            _set_leverage(api_key, api_secret, side, lev)

            if entry_type == "MARKET":
                r = _place_order(api_key, api_secret, side, "MARKET", qty)
                if r.get("code") == 0:
                    # Place SL (STOP_MARKET, close full position)
                    sl_r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                                        qty, stop_price=sl, close_position=True)
                    # Place TP2 (TAKE_PROFIT_MARKET)
                    tp_r = _place_order(api_key, api_secret, close_side, "TAKE_PROFIT_MARKET",
                                        qty, stop_price=tp2, close_position=True)
                    user["in_position"]    = True
                    user["pos_side"]       = side
                    user["sl_order_id"]    = str((sl_r.get("data") or {}).get("order", {}).get("orderId", ""))
                    user["tp_order_id"]    = str((tp_r.get("data") or {}).get("order", {}).get("orderId", ""))
                    user["limit_order_id"] = ""
                    _set(cid, user)
                    results.append(f"✅ @{user.get('username','?')} opened {side} {qty} BTC")
                else:
                    results.append(f"❌ @{user.get('username','?')}: {r.get('msg','?')}")

            else:  # PULLBACK — place limit order
                r = _place_order(api_key, api_secret, side, "LIMIT", qty, price=entry)
                if r.get("code") == 0:
                    oid = str((r.get("data") or {}).get("order", {}).get("orderId", ""))
                    user["in_position"]    = False
                    user["pos_side"]       = side
                    user["limit_order_id"] = oid
                    user["sl_order_id"]    = ""
                    user["tp_order_id"]    = ""
                    _set(cid, user)
                    results.append(f"✅ @{user.get('username','?')} limit {side} {qty} BTC @ {entry:,.0f}")
                else:
                    results.append(f"❌ @{user.get('username','?')}: {r.get('msg','?')}")

        except Exception as e:
            results.append(f"❌ @{user.get('username','?')}: {e}")
            print(f"[CT] on_signal {cid}: {e}")

    print(f"[CT] on_signal: {len(results)} users → {results}")
    return results


def on_tp1(entry: float):
    """TP1 hit — move SL to breakeven for all copy users."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if not user.get("in_position"): continue
        try:
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            # Cancel old SL
            _cancel_order(api_key, api_secret, user.get("sl_order_id",""))
            # Place new SL at entry (breakeven)
            r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                             0, stop_price=entry, close_position=True)
            user["sl_order_id"] = str((r.get("data") or {}).get("order", {}).get("orderId", ""))
            _set(cid, user)
        except Exception as e:
            print(f"[CT] on_tp1 {cid}: {e}")


def on_tp2():
    """TP2 hit — BingX TAKE_PROFIT_MARKET auto-closes. Update records."""
    for cid, user, _, _ in _users_with_copy():
        if not user.get("in_position"): continue
        user["in_position"] = False; user["pos_side"] = ""
        user["sl_order_id"] = ""; user["tp_order_id"] = ""
        user["history"]["total"] += 1; user["history"]["profit"] += 1
        _set(cid, user)


def on_sl():
    """SL hit — BingX STOP_MARKET auto-closes. Update records."""
    for cid, user, _, _ in _users_with_copy():
        if not user.get("in_position"): continue
        user["in_position"] = False; user["pos_side"] = ""
        user["sl_order_id"] = ""; user["tp_order_id"] = ""
        user["history"]["total"] += 1; user["history"]["loss"] += 1
        _set(cid, user)


def on_cancel_limits():
    """Entry missed / setup invalid — cancel pending limit orders."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if user.get("in_position"): continue
        try:
            oid = user.get("limit_order_id","")
            if oid:
                _cancel_order(api_key, api_secret, oid)
            user["limit_order_id"] = ""; user["pos_side"] = ""
            _set(cid, user)
        except Exception as e:
            print(f"[CT] on_cancel_limits {cid}: {e}")


def on_close_all():
    """Admin /close or structure flip — close all positions + cancel all orders."""
    for cid, user, api_key, api_secret in _users_with_copy():
        try:
            if user.get("in_position") and user.get("pos_side"):
                _close_position(api_key, api_secret, user["pos_side"])
            _cancel_all_orders(api_key, api_secret)
            user["in_position"] = False; user["pos_side"] = ""
            user["sl_order_id"] = ""; user["tp_order_id"] = ""; user["limit_order_id"] = ""
            _set(cid, user)
        except Exception as e:
            print(f"[CT] on_close_all {cid}: {e}")


def on_sl_to_be(entry: float):
    """Admin /sltobe — same as on_tp1."""
    on_tp1(entry)


def on_update_sl(new_sl: float):
    """Admin /setsl — cancel old SL, place new one at new_sl."""
    for cid, user, api_key, api_secret in _users_with_copy():
        if not user.get("in_position"): continue
        try:
            close_side = "SELL" if user["pos_side"] == "BUY" else "BUY"
            _cancel_order(api_key, api_secret, user.get("sl_order_id",""))
            r = _place_order(api_key, api_secret, close_side, "STOP_MARKET",
                             0, stop_price=new_sl, close_position=True)
            user["sl_order_id"] = str((r.get("data") or {}).get("order", {}).get("orderId", ""))
            _set(cid, user)
        except Exception as e:
            print(f"[CT] on_update_sl {cid}: {e}")


# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────

CT_USER_COMMANDS  = {"/connect", "/disconnect", "/setsize", "/setleverage",
                     "/copytrade", "/mytrade", "/mysize", "/myhistory"}
CT_ADMIN_COMMANDS = {"/allusers", "/user", "/kick", "/pauseuser"}

def is_ct_command(cmd: str, is_admin: bool) -> bool:
    if cmd in CT_USER_COMMANDS: return True
    if is_admin and cmd in CT_ADMIN_COMMANDS: return True
    return False

def handle(cmd: str, parts: list, chat_id, username: str,
           send_reply_fn, is_admin: bool):
    """Route a copy-trade command. Call this from bot.py handle_command()."""
    cid = str(chat_id)

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
            f"Size: <b>${user['size_usdt']} USDT</b> | Leverage: <b>{user['leverage']}x</b>\n"
            f"Exposure per trade: <b>${user['size_usdt']*user['leverage']:.0f}</b>\n\n"
            "/copytrade on — enable auto-copy\n"
            "/setsize 50 — change trade size\n"
            "/setleverage 10 — change leverage\n\n"
            "<i>— CLEXER V7.0 —</i>")

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
            "<i>— CLEXER V7.0 —</i>")

    elif cmd == "/setsize":
        if len(parts) < 2:
            user = _get(cid) or {}
            send_reply_fn(chat_id,
                f"Current size: <b>${user.get('size_usdt',50)} USDT</b>\n\n"
                f"Usage: /setsize 50"); return
        try:
            size = float(parts[1])
            if size < 5 or size > 10000:
                send_reply_fn(chat_id, "Size must be $5–$10,000 USDT"); return
            user = _get(cid) or _default_user(username)
            user["size_usdt"] = size; _set(cid, user)
            send_reply_fn(chat_id,
                f"<b>Trade Size Set</b>\n\n"
                f"Size: <b>${size} USDT</b> | Leverage: <b>{user['leverage']}x</b>\n"
                f"Exposure per trade: <b>${size * user['leverage']:.0f}</b>\n\n"
                f"<i>— CLEXER V7.0 —</i>")
        except: send_reply_fn(chat_id, "Usage: /setsize 50")

    elif cmd == "/setleverage":
        if len(parts) < 2:
            user = _get(cid) or {}
            send_reply_fn(chat_id,
                f"Current leverage: <b>{user.get('leverage',10)}x</b>\n\n"
                f"Usage: /setleverage 10"); return
        try:
            lev = int(parts[1])
            if lev < 1 or lev > 125:
                send_reply_fn(chat_id, "Leverage must be 1–125x"); return
            user = _get(cid) or _default_user(username)
            user["leverage"] = lev; _set(cid, user)
            send_reply_fn(chat_id,
                f"<b>Leverage Set</b>\n\n"
                f"Leverage: <b>{lev}x</b> | Size: <b>${user['size_usdt']} USDT</b>\n"
                f"Exposure per trade: <b>${user['size_usdt']*lev:.0f}</b>\n\n"
                f"<i>— CLEXER V7.0 —</i>")
        except: send_reply_fn(chat_id, "Usage: /setleverage 10")

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
                "<i>— CLEXER V7.0 —</i>")
        elif state == "off":
            user["copy_on"] = False; _set(cid, user)
            send_reply_fn(chat_id,
                "<b>Copy Trade OFF</b>\n\nNo more auto-copies.\n"
                "Open positions remain open — manage them on BingX.\n\n"
                "<i>— CLEXER V7.0 —</i>")
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
                send_reply_fn(chat_id, "<b>No Open Position</b>\n\nBingX account clear.\n\n<i>— CLEXER V7.0 —</i>")
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
                    f"<i>— CLEXER V7.0 —</i>")
        except Exception as e:
            send_reply_fn(chat_id, f"Error: {e}")

    elif cmd == "/mysize":
        user = _get(cid) or {}
        send_reply_fn(chat_id,
            f"<b>Your Settings</b>\n\n"
            f"BingX: {'✅ Connected' if user.get('connected') else '❌ Not connected'}\n"
            f"Copy Trade: <b>{'ON' if user.get('copy_on') else 'OFF'}</b>\n"
            f"Size: <b>${user.get('size_usdt',50)} USDT</b>\n"
            f"Leverage: <b>{user.get('leverage',10)}x</b>\n"
            f"Exposure per trade: <b>${user.get('size_usdt',50)*user.get('leverage',10):.0f}</b>\n\n"
            f"<i>— CLEXER V7.0 —</i>")

    elif cmd == "/myhistory":
        user = _get(cid) or {}
        h = user.get("history", {"total":0,"profit":0,"loss":0})
        wr = f"{h['profit']/h['total']*100:.0f}%" if h["total"] else "—"
        send_reply_fn(chat_id,
            f"<b>Your Copy Trade History</b>\n\n"
            f"Total trades: {h['total']}\n"
            f"Profit:       {h['profit']}\n"
            f"Loss:         {h['loss']}\n"
            f"Win rate:     {wr}\n\n"
            f"<i>— CLEXER V7.0 —</i>")

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
            f"<i>— CLEXER V7.0 —</i>")

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
        h = user.get("history",{"total":0,"profit":0,"loss":0})
        paused = "\n⚠️ PAUSED BY ADMIN" if user.get("paused_by_admin") else ""
        send_reply_fn(chat_id,
            f"<b>@{user.get('username','?')}</b> | <code>{target}</code>{paused}\n\n"
            f"BingX: {'✅ Connected' if user.get('connected') else '❌ Not connected'}\n"
            f"Copy Trade: {'ON' if user.get('copy_on') else 'OFF'}\n"
            f"Size: <b>${user.get('size_usdt',0)} USDT</b> | Leverage: <b>{user.get('leverage',1)}x</b>"
            f"{pos_info}\n\n"
            f"History: {h['total']} trades | {h['profit']} profit | {h['loss']} loss\n"
            f"Joined: {user.get('joined','?')}\n\n"
            f"<i>— CLEXER V7.0 —</i>")

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
            f"<i>— CLEXER V7.0 —</i>")

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
            f"<i>— CLEXER V7.0 —</i>")

    else:
        send_reply_fn(chat_id, f"Unknown command: {cmd}")
