"""
CLEXER Mini App — API backend (separate Railway service)
Deploy this as a NEW Railway service alongside CLEXER_BOT.

Env vars needed:
  DATABASE_URL         — postgres.railway.internal connection string
  TELEGRAM_BOT_TOKEN   — same token as bot service
  ENCRYPTION_KEY       — Fernet key for BingX API secrets (generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
  DATA_DIR             — path to /data volume (same volume mounted on bot service)
"""

import os
import json
import hmac
import hashlib
import time
import urllib.parse
import requests
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import Optional
import psycopg2
import psycopg2.extras
from cryptography.fernet import Fernet

# ── env ──────────────────────────────────────────────────────────────────────
DATABASE_URL    = os.environ["DATABASE_URL"]
BOT_TOKEN       = os.environ["TELEGRAM_BOT_TOKEN"]
ENCRYPTION_KEY  = os.environ["ENCRYPTION_KEY"].encode()
DATA_DIR        = os.environ.get("DATA_DIR", "/data")
STATE_FILE      = os.path.join(DATA_DIR, "clexer_state.json")
CRYPTO_PAY_API_TOKEN = os.environ.get("CRYPTO_PAY_API_TOKEN", "")   # @CryptoBot — verifies incoming webhook signatures
ADMIN_CHAT_ID   = os.environ.get("ADMIN_CHAT_ID", "")   # same value as bot.py's — must be set here too for /trades/active's admin view
IST = timezone(timedelta(hours=5, minutes=30))

fernet = Fernet(ENCRYPTION_KEY)

app = FastAPI(title="CLEXER API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_state (
                    id          INT PRIMARY KEY DEFAULT 1,
                    state_json  JSONB NOT NULL,
                    updated_at  TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS kv_store (
                    key         TEXT PRIMARY KEY,
                    data_json   JSONB NOT NULL,
                    updated_at  TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS users (
                    tg_id       BIGINT PRIMARY KEY,
                    username    TEXT,
                    first_name  TEXT,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS bingx_credentials (
                    tg_id           BIGINT PRIMARY KEY REFERENCES users(tg_id) ON DELETE CASCADE,
                    api_key_enc     BYTEA NOT NULL,
                    api_secret_enc  BYTEA NOT NULL,
                    connected_at    TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS copy_settings (
                    tg_id       BIGINT PRIMARY KEY REFERENCES users(tg_id) ON DELETE CASCADE,
                    auto_copy   BOOLEAN DEFAULT FALSE,
                    size_mode   TEXT DEFAULT 'fixed',
                    size_val    NUMERIC DEFAULT 50,
                    lev_mode    TEXT DEFAULT 'manual',
                    leverage    INT DEFAULT 10,
                    risk_usdt   NUMERIC,
                    updated_at  TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS copy_trades (
                    id          SERIAL PRIMARY KEY,
                    tg_id       BIGINT REFERENCES users(tg_id) ON DELETE CASCADE,
                    symbol      TEXT NOT NULL,
                    side        TEXT NOT NULL,
                    entry       NUMERIC,
                    tp          NUMERIC,
                    sl          NUMERIC,
                    size_usdt   NUMERIC,
                    leverage    INT,
                    status      TEXT DEFAULT 'OPEN',
                    opened_at   TIMESTAMPTZ DEFAULT NOW(),
                    closed_at   TIMESTAMPTZ,
                    close_price NUMERIC,
                    pnl_usdt    NUMERIC
                );

                -- Append-only queue: the CryptoBot webhook only ever INSERTs here
                -- (never touches ct_users directly), so there's no read-modify-write
                -- race with bot.py, which owns the user DB in-process and polls this
                -- table to apply wallet credits / VIP grants safely.
                CREATE TABLE IF NOT EXISTS payment_events (
                    id          SERIAL PRIMARY KEY,
                    cid         TEXT NOT NULL,
                    event_type  TEXT NOT NULL,
                    amount      NUMERIC NOT NULL,
                    meta        JSONB DEFAULT '{}',
                    created_at  TIMESTAMPTZ DEFAULT NOW(),
                    processed   BOOLEAN DEFAULT FALSE
                );
            """)
            # Migrations — safe to run repeatedly (IF NOT EXISTS / DO NOTHING)
            for sql in [
                "ALTER TABLE copy_settings ADD COLUMN IF NOT EXISTS lev_mode TEXT DEFAULT 'manual'",
                "ALTER TABLE copy_settings ADD COLUMN IF NOT EXISTS risk_usdt NUMERIC",
                "ALTER TABLE copy_settings DROP COLUMN IF EXISTS risk_mode",
                "ALTER TABLE copy_settings DROP COLUMN IF EXISTS risk_val",
            ]:
                try:
                    cur.execute(sql)
                except Exception:
                    pass
        conn.commit()

# ── initData auth ─────────────────────────────────────────────────────────────
def verify_init_data(init_data: str) -> dict:
    """
    Verify Telegram WebApp initData HMAC.
    Returns parsed user dict if valid, raises 401 if not.
    """
    parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        raise HTTPException(401, "Missing hash")

    data_check = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    expected   = hmac.new(secret_key, data_check.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(expected, received_hash):
        raise HTTPException(401, "Invalid initData")

    # optional: reject if older than 1 hour
    auth_date = int(parsed.get("auth_date", 0))
    if time.time() - auth_date > 86400:
        raise HTTPException(401, "initData expired")

    user_json = parsed.get("user", "{}")
    return json.loads(user_json)


def get_current_user(request: Request) -> dict:
    init_data = request.headers.get("X-Telegram-Init-Data", "")
    if not init_data:
        # Allow dev bypass when running locally without Telegram
        if os.environ.get("DEV_MODE") == "1":
            return {"id": 0, "first_name": "Dev", "username": "dev"}
        raise HTTPException(401, "No initData")
    return verify_init_data(init_data)


def upsert_user(user: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (tg_id, username, first_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (tg_id) DO UPDATE
                  SET username   = EXCLUDED.username,
                      first_name = EXCLUDED.first_name
            """, (user["id"], user.get("username"), user.get("first_name")))
        conn.commit()

# ── state reader (DB-first, file fallback) ────────────────────────────────────
def read_state() -> dict:
    # Try PostgreSQL first (bot pushes state here after every save)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT state_json FROM bot_state WHERE id = 1")
                row = cur.fetchone()
        if row:
            return dict(row["state_json"])
    except Exception:
        pass
    # File fallback (works if same Railway volume is mounted)
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


# ── push_state (called by bot after every save_state) ─────────────────────────
PUSH_STATE_SECRET = os.environ.get("PUSH_STATE_SECRET", "")

@app.get("/push_state")
def get_state(request: Request):
    """Any server (main or a co-server) calls this on startup to restore the
    active BTC trade / Scan1 / Scan2 slots from the shared store. Includes
    updated_at so a caller can compare it against its own local file's mtime
    and use whichever is actually newer, instead of always trusting central."""
    if PUSH_STATE_SECRET:
        if request.headers.get("X-Push-Secret", "") != PUSH_STATE_SECRET:
            raise HTTPException(403, "Forbidden")
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT state_json, updated_at FROM bot_state WHERE id = 1")
                row = cur.fetchone()
        if row:
            return {"state": dict(row["state_json"]), "updated_at": row["updated_at"].isoformat()}
    except Exception:
        pass
    return {"state": read_state(), "updated_at": None}

@app.post("/push_state")
async def push_state(request: Request):
    """Bot calls this endpoint after every save_state() to sync state to DB."""
    # Simple shared-secret auth (bot sets X-Push-Secret header)
    if PUSH_STATE_SECRET:
        secret = request.headers.get("X-Push-Secret", "")
        if secret != PUSH_STATE_SECRET:
            raise HTTPException(403, "Forbidden")
    body = await request.json()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_state (id, state_json, updated_at)
                    VALUES (1, %s, NOW())
                    ON CONFLICT (id) DO UPDATE
                      SET state_json = EXCLUDED.state_json,
                          updated_at = NOW()
                """, (json.dumps(body),))
            conn.commit()
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")
    return {"ok": True}

# ── generic key-value sync (copytrade users DB, bot settings, active-server flag) ──
# Lets multiple Railway deployments (main + any number of co-servers) share one
# source of truth by talking to this single api.py service instead of each
# keeping its own local files. Any server can push a full blob under a key, and
# any server can pull the latest blob for that key.
def _kv_check_secret(request: Request):
    if PUSH_STATE_SECRET:
        if request.headers.get("X-Push-Secret", "") != PUSH_STATE_SECRET:
            raise HTTPException(403, "Forbidden")

@app.post("/kv/{key}")
async def kv_push(key: str, request: Request):
    _kv_check_secret(request)
    body = await request.json()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO kv_store (key, data_json, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE
                      SET data_json  = EXCLUDED.data_json,
                          updated_at = NOW()
                """, (key, json.dumps(body)))
            conn.commit()
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")
    return {"ok": True}

@app.get("/kv/{key}")
def kv_pull(key: str):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT data_json, updated_at FROM kv_store WHERE key = %s", (key,))
                row = cur.fetchone()
        if row:
            return {"found": True, "data": row["data_json"], "updated_at": row["updated_at"].isoformat()}
        return {"found": False, "data": None}
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

# ═════════════════════════════════════════════════════════════════════════════
# CRYPTO PAY (@CryptoBot) webhook + payment event queue
#
# The webhook only ever INSERTs a row (see payment_events table above) and
# sends an instant "payment received" DM — it never touches ct_users/wallet
# balances directly. bot.py (the one process that owns the user DB in memory)
# polls /payment_events?processed=false, applies each event via its existing
# ct._get/_set, then acks it. This avoids a read-modify-write race between
# this stateless webhook process and bot.py's long-running one.
# ═════════════════════════════════════════════════════════════════════════════

def _send_telegram_dm(chat_id, text: str):
    """Direct Bot API call — api.py has BOT_TOKEN already (used for WebApp
    initData verification) but never sent a message with it before now."""
    try:
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"[CRYPTOPAY] DM send error: {e}")

@app.post("/cryptopay/webhook")
async def cryptopay_webhook(request: Request):
    raw = await request.body()
    if CRYPTO_PAY_API_TOKEN:
        sig = request.headers.get("crypto-pay-api-signature", "")
        secret = hashlib.sha256(CRYPTO_PAY_API_TOKEN.encode()).digest()
        expected = hmac.new(secret, raw, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            raise HTTPException(401, "Invalid signature")
    body = json.loads(raw)
    if body.get("update_type") != "invoice_paid":
        return {"ok": True}   # ignore other update types (invoice_created, etc.)
    invoice = body.get("payload", {})   # CryptoBot nests the actual invoice under "payload"
    try:
        meta = json.loads(invoice.get("payload", "{}"))   # our own metadata string, set at createInvoice time
    except Exception:
        meta = {}
    cid = str(meta.get("cid", ""))
    event_type = meta.get("type", "")
    amount = float(invoice.get("amount", 0) or meta.get("amount", 0) or 0)
    if not cid or not event_type:
        print(f"[CRYPTOPAY] webhook missing cid/type in payload: {invoice.get('payload')}")
        return {"ok": True}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO payment_events (cid, event_type, amount, meta)
                    VALUES (%s, %s, %s, %s)
                """, (cid, event_type, amount, json.dumps(meta)))
            conn.commit()
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")
    _send_telegram_dm(cid, f"✅ <b>Payment received</b> — ${amount:,.2f}\n\nProcessing shortly...\n\n<i>🛡️ Capital protected</i>")
    return {"ok": True}

@app.get("/payment_events")
def get_payment_events(request: Request, processed: bool = False):
    _kv_check_secret(request)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM payment_events WHERE processed = %s ORDER BY id ASC LIMIT 100", (processed,))
                rows = cur.fetchall()
        return {"events": [dict(r) | {"created_at": r["created_at"].isoformat()} for r in rows]}
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

@app.post("/payment_events/{event_id}/ack")
def ack_payment_event(event_id: int, request: Request):
    _kv_check_secret(request)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE payment_events SET processed = TRUE WHERE id = %s", (event_id,))
            conn.commit()
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")
    return {"ok": True}

# ── startup ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    init_db()

# ── health ────────────────────────────────────────────────────────────────────
@app.get("/app")
def serve_miniapp():
    # Telegram's in-app WebView caches web_app pages aggressively across
    # sessions — a stale cached copy previously kept serving a dead API URL
    # long after the file was fixed and redeployed. Explicit no-cache headers
    # force it (and any browser) to always fetch the latest deployed version.
    return FileResponse("clexer-miniapp.html", media_type="text/html",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"})

@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

# ── maintenance mode ──────────────────────────────────────────────────────────
# Starts ON by default — send /miniapp resume from Telegram to go live
_maintenance = {"on": True, "msg": "Under Maintenance — send /miniapp resume to go live"}

_MAINTENANCE_EXEMPT_PREFIXES = ("/maintenance", "/kv/", "/push_state", "/health", "/cryptopay/",
                                "/payment_events",
                                # The public marketing website is not the mini app. /miniapp pause
                                # is about the Telegram web app; it used to black out the website
                                # too, which has no user data and nothing to pause.
                                "/public/", "/price")

@app.middleware("http")
async def maintenance_gate(request: Request, call_next):
    """Block mini-app-facing requests when in maintenance mode — but NEVER the
    internal bot-to-bot sync endpoints (/kv/*, /push_state) or /health. Those
    must always work regardless of mini-app maintenance state, otherwise a
    freshly-restarted api.py (which resets maintenance to ON by default) would
    silently cut off every bot server's shared data sync until someone happens
    to send /miniapp resume — which can itself require a bot to be polling."""
    if _maintenance["on"] and not request.url.path.startswith(_MAINTENANCE_EXEMPT_PREFIXES):
        return JSONResponse({"error": "maintenance", "msg": _maintenance["msg"]}, status_code=503)
    return await call_next(request)

@app.get("/maintenance")
def get_maintenance():
    return _maintenance

@app.post("/maintenance")
async def set_maintenance(request: Request):
    if PUSH_STATE_SECRET:
        if request.headers.get("X-Push-Secret","") != PUSH_STATE_SECRET:
            raise HTTPException(403, "Forbidden")
    body = await request.json()
    _maintenance["on"]  = bool(body.get("on", False))
    _maintenance["msg"] = body.get("msg", "Under Maintenance")
    return _maintenance

# ── price (server-side BingX fetch — avoids browser CORS block) ───────────────
_daily_open_cache: dict = {}     # sym -> (open_price, fetched_at)

def _daily_change_pct(sym: str, price: float) -> float:
    """Real 24h change, taken from the current daily candle's open.

    Cached for 5 minutes: the daily open only changes once a day, so a ticker
    poll must not turn into a second upstream request every time."""
    try:
        hit = _daily_open_cache.get(sym)
        if hit and time.time() - hit[1] < 300:
            op = hit[0]
        else:
            r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/klines",
                             params={"symbol": sym, "interval": "1d", "limit": 1}, timeout=8)
            rows = (r.json() or {}).get("data") or []
            op = float(rows[-1]["open"]) if rows else 0.0
            _daily_open_cache[sym] = (op, time.time())
        return (price / op - 1) * 100 if op > 0 else 0.0
    except Exception:
        return 0.0


@app.get("/price")
def get_price(sym: str = "BTC-USDT"):
    """Fetch live price from BingX server-side. No auth required."""
    try:
        r = requests.get(
            "https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
            params={"symbol": sym}, timeout=8)
        d = r.json().get("data", {})
        if isinstance(d, list): d = d[0] if d else {}
        price = float(d.get("lastPrice", 0))
        if price > 0:
            # Neither of this endpoint's own change fields is usable:
            # priceChangePercent comes back as 0 on some calls and as a
            # FRACTION (0.01 meaning 1%) on others, and openPrice is the current
            # candle's open, not the 24h open. The daily candle is the only
            # consistent source, so it is the one we use.
            chg = _daily_change_pct(sym, price)
            return {
                "price":  price,
                "change": chg,
                "high24": float(d.get("highPrice", price)),
                "low24":  float(d.get("lowPrice",  price)),
                "volume": float(d.get("quoteVolume", 0)),
                "source": "BingX",
                "sym":    sym,
            }
    except Exception as e:
        print(f"[API /price] BingX error: {e}")
    # Binance fallback
    try:
        sym_b = sym.replace("-USDT", "USDT").replace("-", "")
        r2 = requests.get(f"https://api1.binance.com/api/v3/ticker/24hr",
                          params={"symbol": sym_b}, timeout=8)
        d2 = r2.json()
        return {
            "price":  float(d2["lastPrice"]),
            "change": float(d2["priceChangePercent"]),
            "high24": float(d2["highPrice"]),
            "low24":  float(d2["lowPrice"]),
            "volume": float(d2["quoteVolume"]),
            "source": "Binance",
            "sym":    sym,
        }
    except Exception as e2:
        raise HTTPException(status_code=502, detail=f"Price fetch failed: {e2}")

# ═════════════════════════════════════════════════════════════════════════════
# TRADES endpoints
# ═════════════════════════════════════════════════════════════════════════════

_SLOT_SCHEDULE_KIND = {"scan1": "scan1", "scan2": "scan2", "demo1": "test1", "demo2": "test2"}

# Mirrors copytrade.py's _SCAN_SLOTS — the per-user field prefixes for each of
# the (up to 6 concurrent) positions per scan type. Used to find symbols the
# calling user personally has an open copytrade position on, so their OWN
# money in a trade is never hidden by the verified/unverified/nonspecial tier
# filter below (that filter is for the shared public feed, not personal positions).
_ALL_SLOT_PREFIXES = (
    ["s1_", "s1b_", "s1c_", "s1d_", "s1e_", "s1f_"] +
    ["scan_", "s2b_", "s2c_", "s2d_", "s2e_", "s2f_"] +
    ["d1_", "d1b_", "d1c_", "d1d_", "d1e_", "d1f_"] +
    ["d2_", "d2b_", "d2c_", "d2d_", "d2e_", "d2f_"]
)

def _kv_dict(key: str) -> dict:
    """Reads a kv_store blob directly (same table kv_pull/kv_push use) —
    used here to pull ct_users (tier lookup) and slot_auto_state (special/
    unverified schedule sets) without an internal HTTP round-trip."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT data_json FROM kv_store WHERE key = %s", (key,))
                row = cur.fetchone()
        return (row["data_json"] if row else {}) or {}
    except Exception:
        return {}

def _trade_category(kind: str, created_at, special: dict, no_copy: dict) -> str:
    """Same verified/unverified/non-special classification bot.py's /status
    uses, reimplemented here since api.py is a separate process with no
    access to bot.py's in-memory _SCAN_SPECIAL/_SCAN_SPECIAL_NO_COPY sets —
    slot_auto_state (pushed by bot.py's _save_slot_state) carries the same
    data centrally instead."""
    if not created_at:
        return "nonspecial"
    try:
        dt = datetime.fromtimestamp(float(created_at), timezone.utc) + IST
        hm = [dt.hour, dt.minute]
    except Exception:
        return "nonspecial"
    sched_kind = _SLOT_SCHEDULE_KIND.get(kind, kind)
    if hm in no_copy.get(sched_kind, []):
        return "unverified"
    if hm in special.get(sched_kind, []):
        return "verified"
    return "nonspecial"

@app.get("/trades/active")
def get_active_trades(request: Request):
    """Returns active trades, filtered by the requesting viewer's tier —
    same rule as bot.py's /status: admin sees everything tagged by category,
    VIP sees only verified trades, Free sees only trades it actually got
    (share_free) plus a locked VIP tag (no numbers) for anything VIP-only.
    initData is optional (kept back-compat with older public callers), but
    an unauthenticated/unrecognized caller is treated as Free — the most
    restrictive default — never as admin/VIP."""
    state = read_state()

    viewer_tier = "free"
    is_admin_view = False
    my_symbols = set()   # symbols the caller personally has an open copytrade position on
    my_qty = {}; my_lev = {}   # symbol -> caller's own qty/leverage, since the shared feed doesn't know per-user size
    init_data = request.headers.get("X-Telegram-Init-Data", "")
    if init_data:
        try:
            u = verify_init_data(init_data)
            uid = str(u.get("id", ""))
            if uid:
                is_admin_view = bool(ADMIN_CHAT_ID) and uid == str(ADMIN_CHAT_ID)
                ct_users = _kv_dict("ct_users")
                urec = ct_users.get(uid) or {}
                viewer_tier = urec.get("tier", "free")
                for p in _ALL_SLOT_PREFIXES:
                    sym = urec.get(f"{p}symbol")
                    if sym:
                        my_symbols.add(sym)
                        my_qty[sym] = urec.get(f"{p}qty")
                        # Per-slot leverage (the ACTUAL leverage used for this specific
                        # trade, which can differ from the account's manual-mode default
                        # e.g. for auto-risk users where leverage is computed per-trade) —
                        # fall back to the account default only for older positions opened
                        # before this field existed.
                        my_lev[sym] = urec.get(f"{p}lev") or urec.get("leverage")
        except Exception:
            pass

    slot_state = _kv_dict("slot_auto_state")
    special  = slot_state.get("special", {})
    no_copy  = slot_state.get("no_copy", {})

    # bot.py saves key "trade" (not "active_trade")
    active = state.get("trade") or state.get("active_trade")
    # bot.py stores scan/demo trades as lists
    scan1  = state.get("scan1_trades", [])
    scan2  = state.get("scan2_trades", [])
    demo1  = state.get("demo1_trades", [])
    demo2  = state.get("demo2_trades", [])

    positions = []

    def _add(t: dict, source: str, filterable: bool):
        if not t or not t.get("signal"):
            return
        raw_side = t.get("signal", t.get("direction", "LONG"))
        side = "LONG" if raw_side in ("LONG", "BUY") else "SHORT"
        entry_hit = bool(t.get("entry_hit") or t.get("entry_type") == "MARKET")

        # BTC ("main") is intentionally never filtered — shown to everyone,
        # same as bot.py's /status. Scan/demo trades are gated by tier.
        if filterable:
            cat = _trade_category(source, t.get("created_at"), special, no_copy)
            share_free  = t.get("share_free", True)
            tier_routed = t.get("tier_routed", True)
            if is_admin_view:
                reveal, tag = True, cat
            elif viewer_tier == "vip":
                reveal, tag = (cat == "verified"), None
            else:
                reveal, tag = share_free, None
            if not reveal and t.get("symbol") in my_symbols:
                # Caller's own real copytrade position — always visible to
                # them regardless of the shared-feed tier filter, same as
                # bot.py's /mytrade never filters a user's own BingX position.
                reveal = True
            if not reveal:
                # is_admin_view always sets reveal=True above, so this path
                # only runs for Free (locked VIP tag) or VIP-hidden trades.
                if viewer_tier != "vip" and tier_routed:
                    positions.append({
                        "symbol": None, "side": None, "status": "LOCKED",
                        "entry": None, "tp1": None, "tp2": None, "sl": None,
                        "qty": None, "leverage": None, "tp1_hit": False,
                        "source": source, "opened_at": t.get("opened_at") or t.get("created_at"),
                        "locked": True, "tag": "vip",
                    })
                return
        else:
            tag = None

        _sym = t.get("symbol", "BTCUSDT" if source == "main" else "")
        positions.append({
            "symbol":  _sym,
            "side":    side,
            "status":  "RUNNING" if entry_hit else "PENDING",
            "entry":   t.get("entry"),
            "tp1":     t.get("tp1"),
            "tp2":     t.get("tp2"),
            "sl":      t.get("sl"),
            "qty":     t.get("qty") or my_qty.get(_sym),
            "leverage":t.get("leverage") or my_lev.get(_sym) or 10,
            "tp1_hit": bool(t.get("tp1_hit", False)),
            "source":  source,
            "opened_at": t.get("opened_at") or t.get("created_at"),
            "locked":  False,
            **({"category": tag} if tag else {}),
        })

    if active:
        _add(active, "main", filterable=False)
    # scan lists: each element is a trade dict
    for t in (scan1 if isinstance(scan1, list) else scan1.values()):
        _add(t, "scan1", filterable=True)
    for t in (scan2 if isinstance(scan2, list) else scan2.values()):
        _add(t, "scan2", filterable=True)
    for t in (demo1 if isinstance(demo1, list) else demo1.values()):
        _add(t, "demo1", filterable=True)
    for t in (demo2 if isinstance(demo2, list) else demo2.values()):
        _add(t, "demo2", filterable=True)

    return {"positions": positions, "count": len(positions)}


@app.get("/trades/history")
def get_trade_history(user: dict = Depends(get_current_user)):
    """Per-user closed-trade history — previously returned the bot's GLOBAL
    scan_history/outcomes (same list for every viewer, regardless of
    whether they even have copytrade on). Now reads each user's own
    trade_log from ct_users, populated by copytrade.py's _record_pnl() —
    covers users who set up copytrade via the bot's own commands, not just
    the Mini App's connect flow, since both write to the same ct_users record."""
    ct_users = _kv_dict("ct_users")
    urec = ct_users.get(str(user.get("id", "")), {})
    log = list(reversed(urec.get("trade_log", [])[-50:]))
    history = [{
        "symbol": t.get("symbol"), "direction": t.get("side"),
        "pnl": t.get("pnl"), "result": t.get("result"),
        "closed_at": t.get("closed_at"),
    } for t in log]
    return {"history": history, "total": len(history)}


@app.get("/public/results")
def get_public_results(limit: int = 10):
    """Public, unauthenticated closed-result feed for the marketing website.

    Reads the bot's GLOBAL scan_history (Scan1/Scan2 closed trades) out of the
    pushed state blob. SL outcomes are excluded by design — the public page
    shows reached-target and break-even outcomes only. BE is its own category
    and is never merged into SL or counted as a loss.

    Deliberately carries no price levels (entry/sl/tp), no user data and no
    win-rate — a rate computed over an SL-filtered set would be misleading."""
    state = read_state() or {}
    hist = state.get("scan_history") or []
    if not isinstance(hist, list):
        hist = []

    limit = max(1, min(int(limit or 10), 30))
    out = []
    # scan_history is append-ordered, so iterate newest-first.
    for h in reversed(hist):
        if not isinstance(h, dict):
            continue
        result = str(h.get("result", "")).strip().upper()
        if not result or result.startswith("SL"):
            continue
        if result.startswith("TIMEOUT"):
            continue
        raw_side = str(h.get("signal", "")).upper()
        out.append({
            "symbol": h.get("symbol") or "",
            "side":   "LONG" if raw_side in ("LONG", "BUY") else "SHORT",
            "result": result,                       # TP1 | TP2 | BE
            "source": "S2" if h.get("ver") == 2 else "S1",
            "time":   h.get("time") or "",
            # Historical exit price only. Entry/SL/TP levels stay private —
            # those are the live product, this is a settled outcome.
            "close_price": h.get("close_price"),
        })
        if len(out) >= limit:
            break

    counts = {"tp2": 0, "tp1": 0, "be": 0}
    for r in out:
        if r["result"].startswith("TP2"):  counts["tp2"] += 1
        elif r["result"].startswith("TP1"): counts["tp1"] += 1
        elif r["result"].startswith("BE"):  counts["be"]  += 1

    return {"results": out, "count": len(out), "counts": counts}


class _AnonRequest:
    """Stands in for a Request with no Telegram initData, so a delegated call
    is always resolved at the most restrictive (Free) tier."""
    headers: dict = {}


@app.get("/public/signals")
def get_public_signals():
    """Free-tier view of the live positions, for the public website.

    Delegates to /trades/active with the auth header deliberately stripped, so
    the same server-side tier filtering runs and VIP-only setups come back as
    locked placeholders with no numbers. Separate route only so the public site
    stays reachable while the mini app is in maintenance."""
    return get_active_trades(_AnonRequest())


_IST = timezone(timedelta(hours=5, minutes=30))

def _parse_ist(s: str) -> float:
    """bot.py stamps history with ist_str(): '11 Aug 2026  02:15 PM IST'."""
    try:
        return datetime.strptime(str(s).replace(" IST", "").strip(),
                                 "%d %b %Y  %I:%M %p").replace(tzinfo=_IST).timestamp()
    except Exception:
        return 0.0


_STATUS_STALE_AFTER = 180   # no heartbeat for 3 min => we do not know, and must not claim ONLINE

@app.get("/public/status")
def get_public_status():
    """Real operational state of the bot, published by bot.py's heartbeat
    thread (see _push_public_status_if_due). Read-only, no ids, no server
    names, no credentials.

    An absent or stale blob resolves to OFFLINE rather than defaulting to
    anything operational — API reachability is not bot liveness."""
    st = _kv_dict("public_status")
    ts = float(st.get("ts") or 0)
    age = time.time() - ts if ts else None
    live = bool(ts) and age is not None and age < _STATUS_STALE_AFTER

    if not live:
        unknown = "OFFLINE"
        return {
            "live": False, "last_seen": ts or None, "age": age,
            "system": unknown, "scanner": unknown,
            "signal_engine": unknown, "market_monitor": unknown,
        }

    paused  = bool(st.get("paused"))
    stopped = bool(st.get("stopped"))
    scan_on = bool(st.get("scan1_auto")) or bool(st.get("scan2_auto"))

    # PAUSE freezes everything; STOP blocks new scans but monitoring continues.
    system = "PAUSED" if paused else ("LIMITED" if stopped else "ONLINE")
    if paused or stopped:
        scanner = "PAUSED"
        engine  = "PAUSED"
    elif not scan_on:
        scanner = "IDLE"
        engine  = "IDLE"
    else:
        scanner = "ACTIVE"
        engine  = "ACTIVE"
    monitor = "PAUSED" if paused else "ACTIVE"

    return {
        "live": True,
        "last_seen": ts,
        "age": age,
        "system": system,
        "scanner": scanner,
        "signal_engine": engine,
        "market_monitor": monitor,
        # raw flags so the UI never has to re-derive meaning
        "paused": paused,
        "stopped": stopped,
        "scan1_auto": bool(st.get("scan1_auto")),
        "scan2_auto": bool(st.get("scan2_auto")),
        "scan_interval": st.get("scan_interval"),
        "last_scan": st.get("last_scan") or None,
        "last_scan_ts": st.get("last_scan_ts") or None,
    }


@app.get("/public/activity")
def get_public_activity(limit: int = 12):
    """Public activity feed, derived strictly from records the bot actually
    wrote. Two real event kinds exist today:

      opened  — a position present in the live state, timestamped by opened_at
      closed  — an entry in scan_history, timestamped by its own close time

    There is no scanner-event log in the system, so nothing here reports scans
    started, pairs evaluated or setups rejected. SL closes are excluded, same
    rule as /public/results. No user data, no ids, no price levels."""
    state = read_state() or {}
    limit = max(1, min(int(limit or 12), 40))
    events = []

    for key, src in (("scan1_trades", "S1"), ("scan2_trades", "S2")):
        lst = state.get(key) or []
        if not isinstance(lst, list):
            lst = list(lst.values()) if hasattr(lst, "values") else []
        for t in lst:
            if not isinstance(t, dict) or not t.get("symbol"):
                continue
            events.append({
                "kind": "opened",
                "symbol": t.get("symbol"),
                "side": "SHORT" if str(t.get("signal", "")).upper() in ("SHORT", "SELL") else "LONG",
                "source": src,
                "result": None,
                # bot.py stamps scan trades with created_at; opened_at only
                # exists on the BTC/main trade
                "ts": float(t.get("opened_at") or t.get("created_at") or 0),
            })

    for h in (state.get("scan_history") or []):
        if not isinstance(h, dict):
            continue
        result = str(h.get("result", "")).strip().upper()
        if not result or result.startswith("SL") or result.startswith("TIMEOUT"):
            continue
        events.append({
            "kind": "closed",
            "symbol": h.get("symbol"),
            "side": "SHORT" if str(h.get("signal", "")).upper() in ("SHORT", "SELL") else "LONG",
            "source": "S2" if h.get("ver") == 2 else "S1",
            "result": result,
            "ts": _parse_ist(h.get("time")),
        })

    events = [e for e in events if e["ts"] > 0]
    events.sort(key=lambda e: e["ts"], reverse=True)
    return {"events": events[:limit], "count": len(events[:limit])}


@app.get("/public/prices")
def get_public_prices(syms: str = "BTC-USDT"):
    """Batch quotes for the website's watchlist and ticker.

    One request instead of one per symbol — the page polls this on a short
    interval, so fanning it out per symbol multiplied backend load by the
    number of rows for no benefit. Capped at 16 symbols per call. A symbol
    that fails is simply absent from the response; it is never filled in."""
    wanted = [s.strip().upper() for s in str(syms).split(",") if s.strip()][:16]
    out = {}
    for s in wanted:
        try:
            d = get_price(s)
            if d and d.get("price"):
                out[s] = {"price": d["price"], "change": d.get("change", 0.0)}
        except Exception:
            continue
    return {"prices": out, "count": len(out), "ts": int(time.time())}


_KLINE_TF = {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
             "1h": "1h", "4h": "4h", "1d": "1d"}

@app.get("/public/klines")
def get_public_klines(sym: str = "BTC-USDT", tf: str = "15m", limit: int = 200):
    """Real OHLCV from BingX, fetched server-side for the same reason /price
    is — the browser is CORS-blocked from calling the exchange directly.
    No key required; this is BingX's public market data."""
    interval = _KLINE_TF.get(str(tf).lower())
    if not interval:
        raise HTTPException(400, "bad timeframe")
    limit = max(10, min(int(limit or 200), 500))
    try:
        r = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/klines",
                         params={"symbol": sym, "interval": interval, "limit": limit},
                         timeout=10)
        rows = (r.json() or {}).get("data") or []
    except Exception as e:
        raise HTTPException(502, f"market data unavailable: {e}")

    out = []
    for k in rows:
        try:
            out.append({
                "t": int(k["time"]), "o": float(k["open"]), "h": float(k["high"]),
                "l": float(k["low"]),  "c": float(k["close"]), "v": float(k.get("volume") or 0),
            })
        except Exception:
            continue
    out.sort(key=lambda c: c["t"])
    if not out:
        raise HTTPException(502, "market data unavailable")
    return {"symbol": sym, "tf": tf, "candles": out, "count": len(out)}


@app.get("/virtual/state")
def get_virtual_state(user: dict = Depends(get_current_user)):
    """Paper-trading state for the Mini App's Virtual tab — settings, open
    positions and closed history, all driven server-side by bot.py's
    ct.virtual_on_signal/tp1/close hooks (see copytrade.py), keyed under each
    user's own ct_users record same as real trade_log."""
    ct_users = _kv_dict("ct_users")
    cid = str(user.get("id", ""))
    urec = ct_users.get(cid, {}) or {}
    v = urec.get("virtual") or {}
    log = list(reversed((v.get("trade_log") or [])[-50:]))
    history = [{
        "symbol": t.get("symbol"), "side": t.get("side"),
        "pnl": t.get("pnl"), "result": t.get("result"),
        "closed_at": t.get("closed_at"),
    } for t in log]
    open_positions = [{"symbol": sym, **pos} for sym, pos in (v.get("open") or {}).items()]
    enabled = bool(v.get("enabled", False))
    # A toggle tap is only APPLIED once bot.py's payment-events poller picks it
    # up (up to ~30s later) — without this check, re-opening the Virtual tab
    # inside that window read the still-stale ct_users value and visibly
    # flipped the switch back off/on right after the user had just tapped it.
    # An unprocessed toggle event is the user's own most recent intent, so it
    # wins over the possibly-stale ct_users snapshot.
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT meta FROM payment_events
                    WHERE cid = %s AND event_type = 'virtual_toggle' AND processed = FALSE
                    ORDER BY id DESC LIMIT 1
                """, (cid,))
                row = cur.fetchone()
        if row:
            enabled = bool((row["meta"] or {}).get("enabled", enabled))
    except Exception as e:
        print(f"[VIRTUAL STATE] pending-toggle check error: {e}")
    return {
        "enabled":  enabled,
        "balance":  v.get("balance", 1000.0),
        "leverage": v.get("leverage", 10.0),
        "open":     open_positions,
        "history":  history,
        "tier":     urec.get("tier", "free"),
    }


class VirtualToggle(BaseModel):
    enabled: bool

@app.post("/virtual/toggle")
def virtual_toggle(body: VirtualToggle, user: dict = Depends(get_current_user)):
    """Queues an event for bot.py's existing payment-events poller to apply —
    same race-free pattern as CryptoBot payments/VIP grants (see payment_events
    above): api.py never writes ct_users directly since bot.py's copytrade.py
    is the one long-running process that owns it in memory. Takes effect within
    the poller's ~30s cycle."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO payment_events (cid, event_type, amount, meta)
                VALUES (%s, 'virtual_toggle', 0, %s)
            """, (str(user["id"]), json.dumps({"enabled": body.enabled})))
        conn.commit()
    return {"queued": True, "enabled": body.enabled}


class VirtualSettings(BaseModel):
    balance:  Optional[float] = None
    leverage: Optional[float] = None

@app.post("/virtual/settings")
def virtual_settings(body: VirtualSettings, user: dict = Depends(get_current_user)):
    """Syncs the Mini App's Virtual Calculator (balance/leverage) into the
    backend so new server-driven virtual positions size off it. Same queued
    pattern as virtual_toggle above."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO payment_events (cid, event_type, amount, meta)
                VALUES (%s, 'virtual_settings', 0, %s)
            """, (str(user["id"]), json.dumps({"balance": body.balance, "leverage": body.leverage})))
        conn.commit()
    return {"queued": True}


def _session_for_hm(hour: int, minute: int) -> str:
    """Mirrors bot.py's get_session() IST hour ranges exactly, so 'best
    session' means the same LONDON/NEW_YORK/ASIA windows the bot itself
    schedules signals around."""
    mins = hour * 60 + minute
    if 450 <= mins < 990: return "LONDON"
    if mins >= 1110 or mins < 60: return "NEW_YORK"
    return "ASIA"

# Approximate R-multiple per result — trade_log doesn't store entry/SL (only
# symbol/side/pnl/result/closed_at), so a true per-trade R can't be computed.
# These mirror the bot's own documented target design (TP1 = 1.5x SL
# distance, TP2 = 3x) — a reasonable historical average, not a fabricated
# number, but still an approximation since real position sizing/partial
# closes aren't reflected. Generic WIN/LOSS results (no specific TP/SL/BE
# tag) are skipped rather than guessed.
_R_BY_RESULT = {"TP2": 3.0, "TP1": 1.5, "BE": 0.0, "SL": -1.0}

@app.get("/trades/stats")
def get_trade_stats(user: dict = Depends(get_current_user)):
    """Per-user win/loss/P&L stats — see get_trade_history() for why this no
    longer reads the bot's global trade_stats. avg_r and best_session are now
    computed from the same per-user trade_log get_trade_history() reads."""
    ct_users = _kv_dict("ct_users")
    urec = ct_users.get(str(user.get("id", "")), {})
    h = urec.get("history", {}) or {}
    won  = h.get("won_usdt", 0.0)
    lost = h.get("lost_usdt", 0.0)

    log = urec.get("trade_log", [])
    r_values = [_R_BY_RESULT[t["result"]] for t in log if t.get("result") in _R_BY_RESULT]
    avg_r = round(sum(r_values) / len(r_values), 2) if r_values else 0

    session_pnl = {}
    for t in log:
        try:
            dt = datetime.strptime(t.get("closed_at", ""), "%Y-%m-%d %H:%M")
        except ValueError:
            continue
        sess = _session_for_hm(dt.hour, dt.minute)
        session_pnl[sess] = session_pnl.get(sess, 0.0) + (t.get("pnl") or 0.0)
    best_session = "—"
    if session_pnl:
        _SESSION_LABEL = {"LONDON": "London", "NEW_YORK": "New York", "ASIA": "Asia"}
        best_key = max(session_pnl, key=session_pnl.get)
        best_session = _SESSION_LABEL.get(best_key, best_key)

    return {
        "wins": h.get("profit", 0),
        "losses": h.get("loss", 0),
        "total_pnl": h.get("total_pnl", 0.0),
        "profit_factor": round(won / lost, 2) if lost else (won and "∞" or 0),
        "avg_r": avg_r,
        "best_session": best_session,
    }

# ═════════════════════════════════════════════════════════════════════════════
# BINGX credentials endpoints
# ═════════════════════════════════════════════════════════════════════════════

class BingXKeys(BaseModel):
    api_key: str
    api_secret: str

@app.post("/bingx/connect")
def connect_bingx(body: BingXKeys, user: dict = Depends(get_current_user)):
    upsert_user(user)
    api_key_enc    = fernet.encrypt(body.api_key.encode())
    api_secret_enc = fernet.encrypt(body.api_secret.encode())
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bingx_credentials (tg_id, api_key_enc, api_secret_enc)
                VALUES (%s, %s, %s)
                ON CONFLICT (tg_id) DO UPDATE
                  SET api_key_enc    = EXCLUDED.api_key_enc,
                      api_secret_enc = EXCLUDED.api_secret_enc,
                      connected_at   = NOW()
            """, (user["id"], api_key_enc, api_secret_enc))
        conn.commit()
    return {"connected": True, "key_hint": "••••" + body.api_key[-4:]}


@app.delete("/bingx/connect")
def disconnect_bingx(user: dict = Depends(get_current_user)):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bingx_credentials WHERE tg_id = %s", (user["id"],))
        conn.commit()
    return {"connected": False}


@app.get("/copy/balance")
def get_copy_balance(user: dict = Depends(get_current_user)):
    """Real BingX balance for the Mini App Portfolio tab — served from the
    cache bot.py refreshes every ~60s (copytrade.py's start_balance_sync_loop),
    since api.py can't decrypt ct_users' API keys to fetch this live itself."""
    ct_users = _kv_dict("ct_users")
    urec = ct_users.get(str(user["id"]), {}) or {}
    if not urec.get("connected") or "balance_usdt" not in urec:
        return {"available": False}
    return {
        "available":   True,
        "balance":     urec.get("balance_usdt", 0.0),
        "avail":       urec.get("avail_usdt", 0.0),
        "in_trade":    urec.get("intrade_usdt", 0.0),
        "unrealized":  urec.get("unrealized_usdt", 0.0),
        "updated_at":  urec.get("balance_updated_at"),
    }


@app.get("/bingx/status")
def bingx_status(user: dict = Depends(get_current_user)):
    upsert_user(user)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT api_key_enc, connected_at FROM bingx_credentials WHERE tg_id = %s", (user["id"],))
            row = cur.fetchone()
    if row:
        api_key = fernet.decrypt(bytes(row["api_key_enc"])).decode()
        return {
            "connected":    True,
            "key_hint":     "••••" + api_key[-4:],
            "connected_at": row["connected_at"].isoformat() if row["connected_at"] else None,
        }
    # Not in this table — but the user may have connected via the bot's own
    # /connect command instead, which writes to the separate ct_users blob
    # (a different encryption key from this table's, so it can't be decrypted
    # here for a key_hint) — check that too so the Mini App doesn't wrongly
    # ask them to reconnect when they're already connected via the bot.
    ct_users = _kv_dict("ct_users")
    urec = ct_users.get(str(user["id"]), {})
    if urec.get("connected") and urec.get("api_key_enc"):
        return {"connected": True, "key_hint": "•••• (via bot)", "connected_at": None}
    return {"connected": False}

# ═════════════════════════════════════════════════════════════════════════════
# COPY SETTINGS endpoints
# ═════════════════════════════════════════════════════════════════════════════

class CopySettings(BaseModel):
    auto_copy:  bool            = False
    size_mode:  str             = "fixed"
    size_val:   float           = 50.0
    lev_mode:   str             = "manual"
    leverage:   Optional[int]   = 10
    risk_usdt:  Optional[float] = None

@app.get("/copy/settings")
def get_copy_settings(user: dict = Depends(get_current_user)):
    upsert_user(user)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM copy_settings WHERE tg_id = %s", (user["id"],))
            row = cur.fetchone()
    if not row:
        return CopySettings().dict()
    return dict(row)


@app.post("/copy/settings")
def save_copy_settings(body: CopySettings, user: dict = Depends(get_current_user)):
    upsert_user(user)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO copy_settings (tg_id, auto_copy, size_mode, size_val, lev_mode, leverage, risk_usdt)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tg_id) DO UPDATE
                  SET auto_copy  = EXCLUDED.auto_copy,
                      size_mode  = EXCLUDED.size_mode,
                      size_val   = EXCLUDED.size_val,
                      lev_mode   = EXCLUDED.lev_mode,
                      leverage   = EXCLUDED.leverage,
                      risk_usdt  = EXCLUDED.risk_usdt,
                      updated_at = NOW()
            """, (user["id"], body.auto_copy, body.size_mode, body.size_val,
                  body.lev_mode, body.leverage, body.risk_usdt))
        conn.commit()
    return {"saved": True}

# ═════════════════════════════════════════════════════════════════════════════
# COPY TRADES log (what this user has mirrored)
# ═════════════════════════════════════════════════════════════════════════════

class MirrorRequest(BaseModel):
    symbol:    str
    side:      str
    entry:     float
    tp:        float
    sl:        float
    size_usdt: float
    lev_mode:  str             = "manual"
    leverage:  Optional[int]   = 10
    risk_usdt: Optional[float] = None

@app.post("/copy/mirror")
def mirror_trade(body: MirrorRequest, user: dict = Depends(get_current_user)):
    """Log that user mirrored a trade. Actual BingX order placed here later."""
    upsert_user(user)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO copy_trades (tg_id, symbol, side, entry, tp, sl, size_usdt, leverage)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (user["id"], body.symbol.upper(), body.side.upper(),
                  body.entry, body.tp, body.sl, body.size_usdt, body.leverage))
            trade_id = cur.fetchone()["id"]
        conn.commit()
    return {"ok": True, "trade_id": trade_id}


@app.get("/copy/trades")
def get_copy_trades(user: dict = Depends(get_current_user)):
    upsert_user(user)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM copy_trades
                WHERE tg_id = %s
                ORDER BY opened_at DESC
                LIMIT 50
            """, (user["id"],))
            rows = cur.fetchall()
    return {"trades": [dict(r) for r in rows]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
