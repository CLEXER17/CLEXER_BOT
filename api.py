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
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
from cryptography.fernet import Fernet

# ── env ──────────────────────────────────────────────────────────────────────
DATABASE_URL    = os.environ["DATABASE_URL"]
BOT_TOKEN       = os.environ["TELEGRAM_BOT_TOKEN"]
ENCRYPTION_KEY  = os.environ["ENCRYPTION_KEY"].encode()
DATA_DIR        = os.environ.get("DATA_DIR", "/data")
STATE_FILE      = os.path.join(DATA_DIR, "clexer_state.json")

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
                    leverage    INT DEFAULT 10,
                    risk_mode   TEXT DEFAULT 'pct',
                    risk_val    NUMERIC DEFAULT 2,
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
            """)
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
    if time.time() - auth_date > 3600:
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

# ── startup ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    init_db()

# ── health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

# ── price (server-side BingX fetch — avoids browser CORS block) ───────────────
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
            return {
                "price":  price,
                "change": float(d.get("priceChangePercent", 0)),
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

@app.get("/trades/active")
def get_active_trades(user: dict = Depends(get_current_user)):
    """Return all active trades from bot state file."""
    upsert_user(user)
    state = read_state()

    # bot.py saves key "trade" (not "active_trade")
    active = state.get("trade") or state.get("active_trade")
    # bot.py stores scan trades as lists
    scan1  = state.get("scan1_trades", [])
    scan2  = state.get("scan2_trades", [])

    positions = []

    def _add(t: dict, source: str):
        if not t or not t.get("signal"):
            return
        raw_side = t.get("signal", t.get("direction", "LONG"))
        side = "LONG" if raw_side in ("LONG", "BUY") else "SHORT"
        positions.append({
            "symbol":  t.get("symbol", "BTC-USDT"),
            "side":    side,
            "status":  t.get("status", "RUNNING"),
            "entry":   t.get("entry"),
            "tp1":     t.get("tp1"),
            "tp2":     t.get("tp2"),
            "sl":      t.get("sl"),
            "qty":     t.get("qty"),
            "leverage":t.get("leverage", 10),
            "source":  source,
            "opened_at": t.get("opened_at"),
        })

    if active:
        _add(active, "main")
    # scan lists: each element is a trade dict
    for t in (scan1 if isinstance(scan1, list) else scan1.values()):
        _add(t, "scan1")
    for t in (scan2 if isinstance(scan2, list) else scan2.values()):
        _add(t, "scan2")

    return {"positions": positions, "count": len(positions)}


@app.get("/trades/history")
def get_trade_history(user: dict = Depends(get_current_user)):
    """Return closed trade outcomes from bot state file."""
    upsert_user(user)
    state  = read_state()
    # bot.py saves as "outcomes"; also check scan_history
    closed = state.get("outcomes", state.get("trade_outcomes", []))
    scan_h = state.get("scan_history", [])
    all_history = list(reversed((closed + scan_h)[-50:]))
    return {"history": all_history, "total": len(closed) + len(scan_h)}


@app.get("/trades/stats")
def get_trade_stats(user: dict = Depends(get_current_user)):
    """Return summary stats from bot state file."""
    upsert_user(user)
    state = read_state()
    stats = state.get("stats", state.get("trade_stats", {}))
    return stats

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


@app.get("/bingx/status")
def bingx_status(user: dict = Depends(get_current_user)):
    upsert_user(user)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT api_key_enc, connected_at FROM bingx_credentials WHERE tg_id = %s", (user["id"],))
            row = cur.fetchone()
    if not row:
        return {"connected": False}
    api_key = fernet.decrypt(bytes(row["api_key_enc"])).decode()
    return {
        "connected":    True,
        "key_hint":     "••••" + api_key[-4:],
        "connected_at": row["connected_at"].isoformat() if row["connected_at"] else None,
    }

# ═════════════════════════════════════════════════════════════════════════════
# COPY SETTINGS endpoints
# ═════════════════════════════════════════════════════════════════════════════

class CopySettings(BaseModel):
    auto_copy:  bool    = False
    size_mode:  str     = "fixed"
    size_val:   float   = 50.0
    leverage:   int     = 10
    risk_mode:  str     = "pct"
    risk_val:   float   = 2.0

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
                INSERT INTO copy_settings (tg_id, auto_copy, size_mode, size_val, leverage, risk_mode, risk_val)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tg_id) DO UPDATE
                  SET auto_copy  = EXCLUDED.auto_copy,
                      size_mode  = EXCLUDED.size_mode,
                      size_val   = EXCLUDED.size_val,
                      leverage   = EXCLUDED.leverage,
                      risk_mode  = EXCLUDED.risk_mode,
                      risk_val   = EXCLUDED.risk_val,
                      updated_at = NOW()
            """, (user["id"], body.auto_copy, body.size_mode, body.size_val,
                  body.leverage, body.risk_mode, body.risk_val))
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
    leverage:  int

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
