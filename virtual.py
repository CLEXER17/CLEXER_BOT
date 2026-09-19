#!/usr/bin/env python3
"""virtual.py - paper trading, version 2.

A user sets a total capital, picks Auto or Manual leverage, a margin per
trade and (for Auto) how much a stop-loss may cost, then presses Start.
From then on every signal of the user's tier is mirrored as a paper trade:

    Auto  : leverage = risk / (margin x stop distance)   capped at 100x.
            Over the cap the MARGIN is raised so the stop still costs the
            risk; under 1x the trade is taken at 1x anyway.
    Manual: the user's leverage, margin in $ or % of the current balance.
    Fees  : 0.05 % of notional on open, on the TP1 partial and on close.
    No free margin -> the signal is skipped. Loss can never exceed the
    trade's margin (liquidation). When the balance cannot fund one more
    trade the run stops by itself (status "wiped").

Books: every closed trade is filed under the month it closed in, in a
per-user blob (kv key vbook_<cid>, local file fallback) - separate from
ct_users so that the big users blob does not grow with every trade. The
Mini App pages through months; a PDF report of any month can be exported
(5 a month Free, 17 VIP); on the 1st of each month the previous month's
report is DM'd to everyone who traded; Reset sends a report of the whole
run first, then wipes it.

No AI anywhere in here. copytrade.py binds `ct` (its own module) at import
so this file can read/write the users dict without a circular import.
"""

import io
import json
import os
import threading
import time
import base64
from datetime import datetime, timezone, timedelta

import requests

ct = None                                   # bound by copytrade.py
on_wiped = None                             # bot.py sets a (cid) -> None notifier
IST = timedelta(hours=5, minutes=30)
FEE = 0.0005                                # taker fee, each side
MAX_LEV = 100.0
MIN_LEV = 1.0
EXPORT_LIMIT = {"free": 5, "vip": 17}
_DATA_DIR = os.getenv("DATA_DIR", ".")
BOOK_FILE = os.path.join(_DATA_DIR, "virtual_books.json")
_books: dict = {}
_book_lock = threading.Lock()
_book_loaded = False


# ── time helpers ───────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc) + IST


def _stamp(dt=None):
    return (dt or _now()).strftime("%Y-%m-%d %H:%M")


def month_key(dt=None):
    return (dt or _now()).strftime("%Y-%m")


def month_label(key):
    try:
        return datetime.strptime(str(key).split("#")[0], "%Y-%m").strftime("%B %Y")
    except Exception:
        return key


# A page is at most this many closed trades. A busy month splits into parts:
# page keys are "2026-09" (its newest part) or "2026-09#2" (the next older
# one), so Prev / Next always step back or forward in time, across months.
PAGE_ROWS = 30


def _page_list(b: dict):
    """Every page in time order: [(month_key, part, trades_newest_first)]."""
    out = []
    for mk in sorted(b["months"]):
        tr = b["months"][mk].get("trades", [])
        parts = max(1, -(-len(tr) // PAGE_ROWS))
        for part in range(parts, 0, -1):              # oldest chunk first
            hi = len(tr) - (part - 1) * PAGE_ROWS
            lo = max(0, hi - PAGE_ROWS)
            out.append((mk, part, list(reversed(tr[lo:hi]))))
    return out


def page_key(mk: str, part: int) -> str:
    return mk if part <= 1 else f"{mk}#{part}"


def split_key(key):
    """'2026-09#2' -> ('2026-09', 2); '2026-09' -> ('2026-09', 1)."""
    if not key:
        return None, 1
    mk, _, part = str(key).partition("#")
    try:
        return mk, max(1, int(part or 1))
    except Exception:
        return mk, 1


def prev_month_key(dt=None):
    d = (dt or _now()).replace(day=1) - timedelta(days=1)
    return d.strftime("%Y-%m")


# ── per-user config ────────────────────────────────────────────────────────

def default() -> dict:
    return {"v": 2, "status": "setup", "capital": 0.0, "balance": 0.0,
            "lev_mode": "auto", "lev": 10.0, "risk_mode": "usd", "risk": 0.5,
            "margin_mode": "usd", "margin": 2.0, "open": {}, "started_at": "",
            "run": 0, "closed": 0, "skipped": 0}


def get(user: dict) -> dict:
    """The user's virtual block, migrated from the old on/off calculator if
    needed. Old users are stopped and asked to set up again - it is a new
    system with different sizing, their old balance means nothing here."""
    v = user.get("virtual")
    if not isinstance(v, dict) or v.get("v") != 2:
        nv = default()
        if isinstance(v, dict) and v.get("trade_log"):
            nv["legacy_log"] = v["trade_log"][-50:]
        user["virtual"] = nv
        v = nv
    return v


def _tier(user):
    return "vip" if user.get("tier") == "vip" else "free"


# ── books (closed trades, by month) ────────────────────────────────────────

def _load_books_file():
    global _book_loaded
    if _book_loaded:
        return
    _book_loaded = True
    try:
        if os.path.exists(BOOK_FILE):
            with open(BOOK_FILE) as f:
                _books.update(json.load(f))
    except Exception as e:
        print(f"[VIRTUAL] books load error: {e}")


def _empty_book():
    return {"runs": [], "months": {}, "reports_sent": [], "exports": {}}


def book(cid: str) -> dict:
    """Central copy wins when it is there; the local file is the fallback."""
    cid = str(cid)
    _load_books_file()
    with _book_lock:
        if cid in _books:
            return _books[cid]
    b = None
    try:
        r = ct._central_get(f"/kv/vbook_{cid}", timeout=6, retries=1)
        if r is not None and r.ok:
            body = r.json()
            if isinstance(body, dict) and "found" in body:
                b = body.get("data") if body.get("found") else None
            else:
                b = body
    except Exception as e:
        print(f"[VIRTUAL] book pull {cid}: {e}")
    if not isinstance(b, dict) or "months" not in b:
        b = _empty_book()
    with _book_lock:
        _books[cid] = b
    return b


def _save_book(cid: str):
    cid = str(cid)
    with _book_lock:
        try:
            with open(BOOK_FILE, "w") as f:
                json.dump(_books, f)
        except Exception as e:
            print(f"[VIRTUAL] books save error: {e}")
        data = _books.get(cid)
    if ct and ct._API_URL and data is not None:
        def push():
            try:
                hdrs = {"X-Push-Secret": ct._API_SECRET} if ct._API_SECRET else {}
                requests.post(f"{ct._API_URL}/kv/vbook_{cid}", json=data, headers=hdrs, timeout=15)
            except Exception as e:
                print(f"[VIRTUAL] book push {cid}: {e}")
        threading.Thread(target=push, daemon=True).start()


# ── setup / control ────────────────────────────────────────────────────────

def validate(capital, lev_mode, lev, risk_mode, risk, margin_mode, margin):
    """Returns an error string, or None when the numbers make sense."""
    try:
        capital = float(capital); lev = float(lev or 10); risk = float(risk or 0); margin = float(margin)
    except Exception:
        return "Numbers only."
    if capital < 1:
        return "Total capital must be at least $1."
    if lev_mode not in ("auto", "manual"):
        return "Leverage must be Auto or Manual."
    if lev_mode == "manual" and not (1 <= lev <= MAX_LEV):
        return f"Leverage must be 1-{int(MAX_LEV)}x."
    if margin_mode not in ("usd", "pct"):
        return "Margin must be in $ or %."
    if margin_mode == "usd" and not (0.1 <= margin <= capital):
        return "Margin per trade must be between $0.10 and your capital."
    if margin_mode == "pct" and not (1 <= margin <= 100):
        return "Margin % must be 1-100."
    if lev_mode == "auto":
        if risk_mode not in ("usd", "pct"):
            return "Risk must be in $ or %."
        if risk_mode == "usd" and risk <= 0:
            return "Risk per trade must be more than $0."
        if risk_mode == "pct" and not (0.1 <= risk <= 100):
            return "Risk % must be 0.1-100."
        m = margin if margin_mode == "usd" else capital * margin / 100
        r = risk if risk_mode == "usd" else m * risk / 100
        if r > m:
            return "Risk per trade can't be more than the margin per trade."
    return None


def setup(cid: str, capital, lev_mode, lev, risk_mode, risk, margin_mode, margin) -> str:
    """Start (or restart from setup) a run. Returns an error string or ''."""
    err = validate(capital, lev_mode, lev, risk_mode, risk, margin_mode, margin)
    if err:
        return err
    user = ct._get(cid) or ct._default_user()
    v = get(user)
    if v["status"] == "running":
        return "Already running - Stop or Reset first."
    v.update({"status": "running", "capital": round(float(capital), 4), "balance": round(float(capital), 4),
              "lev_mode": lev_mode, "lev": float(lev or 10), "risk_mode": risk_mode, "risk": float(risk or 0),
              "margin_mode": margin_mode, "margin": float(margin), "open": {}, "started_at": _stamp(),
              "run": int(v.get("run", 0)) + 1, "closed": 0, "skipped": 0})
    ct._set(cid, user)
    b = book(cid)
    b["runs"].append({"run": v["run"], "started": v["started_at"], "capital": v["capital"], "ended": "", "final": None})
    _save_book(cid)
    return ""


def stop(cid: str) -> str:
    user = ct._get(cid) or ct._default_user()
    v = get(user)
    if v["status"] != "running":
        return "Not running."
    v["status"] = "stopped"
    ct._set(cid, user)
    return ""


def resume(cid: str) -> str:
    user = ct._get(cid) or ct._default_user()
    v = get(user)
    if v["status"] != "stopped":
        return "Nothing to resume." if v["status"] != "wiped" else "Capital is gone - Reset to start again."
    v["status"] = "running"
    ct._set(cid, user)
    return ""


def reset(cid: str):
    """Wipe the run. Returns (pdf_bytes_or_None, filename) for the caller to
    DM BEFORE anything is lost - the report is built from the books first."""
    user = ct._get(cid) or ct._default_user()
    v = get(user)
    b = book(cid)
    pdf = None
    fname = ""
    has_trades = any(m.get("trades") for m in b["months"].values())
    if has_trades:
        try:
            pdf = build_pdf(cid, user, v, b, month=None)
            fname = f"CLEXER_virtual_run{v.get('run', 0)}.pdf"
        except Exception as e:
            print(f"[VIRTUAL] reset pdf {cid}: {e}")
    if b["runs"]:
        b["runs"][-1]["ended"] = _stamp()
        b["runs"][-1]["final"] = v.get("balance")
    b["months"] = {}
    b["reports_sent"] = []
    _save_book(cid)
    keep_run = int(v.get("run", 0))
    user["virtual"] = default()
    user["virtual"]["run"] = keep_run
    ct._set(cid, user)
    return pdf, fname


# ── sizing ─────────────────────────────────────────────────────────────────

def _margin_for(v: dict, bal: float) -> float:
    return float(v["margin"]) if v["margin_mode"] == "usd" else bal * float(v["margin"]) / 100.0


def _min_needed(v: dict) -> float:
    """Balance below this cannot fund a trade - the run is over."""
    return float(v["margin"]) if v["margin_mode"] == "usd" else 1.0


def size(v: dict, entry: float, sl: float):
    """(plan, note) or (None, reason). plan = {margin, lev, risk}."""
    bal = float(v["balance"])
    used = sum(float(p.get("margin", 0)) for p in v["open"].values())
    free = bal - used
    margin = _margin_for(v, bal)
    if margin <= 0 or free < margin:
        return None, "no free margin"
    sl_pct = abs(entry - sl) / entry if sl and entry else 0.0
    note = ""
    risk = None
    if v["lev_mode"] == "auto":
        risk = float(v["risk"]) if v["risk_mode"] == "usd" else margin * float(v["risk"]) / 100.0
        if sl_pct <= 0:
            lev = min(MAX_LEV, float(v.get("lev") or 10))
            note = "no stop on signal"
        else:
            lev = risk / (margin * sl_pct)
            if lev > MAX_LEV:
                lev = MAX_LEV
                raised = risk / (lev * sl_pct)
                if raised <= free:
                    margin = raised
                    note = "margin raised"
                else:
                    note = "capped"
            elif lev < MIN_LEV:
                lev = MIN_LEV
                note = "wide stop, 1x"
        lev = round(lev, 1)
    else:
        lev = float(v["lev"])
    return {"margin": round(margin, 4), "lev": lev, "risk": risk}, note


def preview(v: dict, entry: float, sl: float, tp1: float, tp2: float, side: str = "BUY") -> dict:
    """What a signal would look like with these settings - for the setup
    screen. Never touches state."""
    plan, note = size(v, entry, sl)
    if not plan:
        return {"skip": note}
    notional = plan["margin"] * plan["lev"]
    qty = notional / entry
    sign = 1 if side == "BUY" else -1
    fee = notional * FEE * 2
    out = {"lev": plan["lev"], "margin": plan["margin"], "position": round(notional, 2), "note": note,
           "sl": round(-abs(entry - sl) * qty - fee, 2) if sl else None,
           "tp1": round(sign * (tp1 - entry) * qty - fee, 2) if tp1 else None,
           "tp2": round(sign * (tp2 - entry) * qty - fee, 2) if tp2 else None}
    return out


# ── signal hooks (called from copytrade's virtual_* wrappers) ──────────────

def _eligible(tier_routed: bool, share_free: bool):
    if not tier_routed:
        return []
    out = []
    for cid, user in list(ct._db.items()):
        v = user.get("virtual")
        if not isinstance(v, dict) or v.get("v") != 2 or v.get("status") != "running":
            continue
        tier = _tier(user)
        if tier == "vip" or (tier == "free" and share_free):
            out.append((cid, user))
    return out


def on_signal(symbol, side, entry, sl, tp1, tp2, tier_routed=True, share_free=True):
    entry = float(entry or 0)
    if entry <= 0:
        return
    for cid, user in _eligible(tier_routed, share_free):
        v = get(user)
        if symbol in v["open"]:
            continue
        plan, note = size(v, entry, float(sl or 0))
        if not plan:
            v["skipped"] = int(v.get("skipped", 0)) + 1
            ct._set(cid, user)
            continue
        notional = plan["margin"] * plan["lev"]
        fee = round(notional * FEE, 6)
        v["balance"] = round(float(v["balance"]) - fee, 6)
        v["open"][symbol] = {"side": side, "entry": entry, "sl": float(sl or 0), "tp1": float(tp1 or 0),
                             "tp2": float(tp2 or 0), "qty": notional / entry, "qty0": notional / entry,
                             "margin": plan["margin"], "lev": plan["lev"], "risk": plan["risk"],
                             "fee": fee, "fee0": fee, "realized": 0.0, "tp1_hit": False, "opened_at": _stamp(), "note": note,
                             "mk": month_key()}          # the month this trade belongs to, whenever it closes
        ct._set(cid, user)


def _entry_fee(pos) -> float:
    """The fee paid when this position opened. It left the balance there and
    then, so a trade's P&L has to carry it - positions opened before this was
    stored fall back to the same sum it was calculated from."""
    f = pos.get("fee0")
    if f is None:
        f = float(pos.get("margin", 0)) * float(pos.get("lev", 0)) * FEE
    return float(f)


def _pnl(pos, price, qty):
    if pos["side"] == "BUY":
        return (price - pos["entry"]) * qty
    return (pos["entry"] - price) * qty


def on_tp1(symbol, price):
    price = float(price or 0)
    if price <= 0:
        return
    portion = float(getattr(ct, "TP1_CLOSE_PCT", 50)) / 100.0
    for cid, user in list(ct._db.items()):
        v = user.get("virtual")
        if not isinstance(v, dict) or v.get("v") != 2:
            continue
        pos = v.get("open", {}).get(symbol)
        if not pos or pos.get("tp1_hit"):
            continue
        q = pos["qty"] * portion
        pnl = _pnl(pos, price, q)
        fee = q * price * FEE
        pos["realized"] = round(pos.get("realized", 0) + pnl - fee, 6)
        pos["fee"] = round(pos.get("fee", 0) + fee, 6)
        pos["qty"] -= q
        pos["tp1_hit"] = True
        v["balance"] = round(float(v["balance"]) + pnl - fee, 6)
        ct._set(cid, user)


def on_close(symbol, price, result):
    price = float(price or 0)
    if price <= 0:
        return
    for cid, user in list(ct._db.items()):
        v = user.get("virtual")
        if not isinstance(v, dict) or v.get("v") != 2:
            continue
        pos = v.get("open", {}).pop(symbol, None)
        if not pos:
            continue
        pnl = _pnl(pos, price, pos["qty"])
        fee = pos["qty"] * price * FEE
        total = pos.get("realized", 0) + pnl - fee
        note = pos.get("note", "")
        if total < -pos["margin"]:                    # a loss beyond the margin is a liquidation
            total = -pos["margin"]
            note = (note + " · " if note else "") + "liquidated"
            result = "LIQ"
        delta = total - pos.get("realized", 0)
        v["balance"] = round(max(0.0, float(v["balance"]) + delta), 6)
        v["closed"] = int(v.get("closed", 0)) + 1
        res = str(result)
        if pos.get("tp1_hit") and res in ("BE", "SL"):
            res = "TP1+BE" if res == "BE" else "TP1+SL"
        rec = {"o": pos.get("opened_at", ""), "t": _stamp(), "sym": symbol, "side": pos["side"],
               "lev": pos["lev"], "margin": round(pos["margin"], 4), "entry": pos["entry"], "exit": price,
               "res": res, "pnl": round(total - _entry_fee(pos), 4),
               "fee": round(pos.get("fee", 0) + fee, 6),
               "bal": round(float(v["balance"]), 4), "note": note, "run": v.get("run", 0)}
        b = book(cid)
        m = b["months"].setdefault(pos.get("mk") or month_key(), {"run": v.get("run", 0), "trades": [], "start_bal": None})
        if m.get("start_bal") is None:
            m["start_bal"] = round(rec["bal"] - rec["pnl"], 4)
        m["trades"].append(rec)
        _save_book(cid)
        wiped = v["status"] == "running" and v["balance"] < _min_needed(v)
        if wiped:
            v["status"] = "wiped"
            v["wiped_at"] = _stamp()
        ct._set(cid, user)
        if wiped and on_wiped:
            try:
                on_wiped(cid)
            except Exception as e:
                print(f"[VIRTUAL] wiped notify {cid}: {e}")


# ── reading ────────────────────────────────────────────────────────────────

def stats(trades: list, start_bal=None) -> dict:
    n = len(trades)
    if not n:
        return {"trades": 0, "wins": 0, "losses": 0, "wr": 0.0, "net": 0.0, "best": 0.0, "worst": 0.0,
                "fees": 0.0, "dd": 0.0, "by_res": {}}
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] < 0]
    peak = None
    dd = 0.0
    for t in trades:
        b = t.get("bal", 0)
        peak = b if peak is None else max(peak, b)
        dd = min(dd, b - peak)
    by = {}
    for t in trades:
        by[t["res"]] = by.get(t["res"], 0) + 1
    return {"trades": n, "wins": len(wins), "losses": len(losses), "wr": round(100.0 * len(wins) / n, 1),
            "net": round((trades[-1].get("bal", 0) - float(start_bal)) if start_bal is not None
                         else sum(t["pnl"] for t in trades), 2), "best": round(max(t["pnl"] for t in trades), 2),
            "worst": round(min(t["pnl"] for t in trades), 2), "fees": round(sum(t.get("fee", 0) for t in trades), 2),
            "dd": round(dd, 2), "by_res": by}


def state(cid: str, month: str = None) -> dict:
    """Everything the Mini App / DM card needs, one call (bot side)."""
    user = ct._get(cid) or {}
    return state_from(user, book(cid), month)


def state_from(user: dict, b: dict, month: str = None) -> dict:
    """Pure version - api.py feeds it the kv blobs it read itself."""
    v = user.get("virtual") if isinstance(user.get("virtual"), dict) and user["virtual"].get("v") == 2 else default()
    b = b if isinstance(b, dict) and "months" in b else _empty_book()
    keys = sorted(b["months"])
    pages = _page_list(b)
    mk, part = split_key(month)
    idx = next((i for i, pg in enumerate(pages) if pg[0] == mk and pg[1] == part), None)
    if idx is None:
        idx = next((i for i in range(len(pages) - 1, -1, -1) if pages[i][0] == mk), None)   # part out of range -> that month's newest
    if idx is None:
        idx = len(pages) - 1                                                  # unknown month -> newest page
    if idx >= 0:
        month, part, page_trades = pages[idx][0], pages[idx][1], pages[idx][2]
        parts = sum(1 for pg in pages if pg[0] == month)
    else:
        month, part, page_trades, parts = month_key(), 1, [], 1
    m = b["months"].get(month, {"trades": []})
    trades = m.get("trades", [])
    used = sum(float(p.get("margin", 0)) for p in v["open"].values())
    open_list = [{"symbol": s, **p} for s, p in v["open"].items()]
    lim = EXPORT_LIMIT[_tier(user)]
    return {
        "legacy": not (isinstance(user.get("virtual"), dict) and user["virtual"].get("v") == 2),
        "status": v["status"], "capital": v["capital"], "balance": round(v["balance"], 4),
        "free_margin": round(v["balance"] - used, 4),
        "pnl_pct": round((v["balance"] - v["capital"]) / v["capital"] * 100, 2) if v["capital"] else 0.0,
        "lev_mode": v["lev_mode"], "lev": v["lev"], "risk_mode": v["risk_mode"], "risk": v["risk"],
        "margin_mode": v["margin_mode"], "margin": v["margin"], "started_at": v.get("started_at", ""),
        "run": v.get("run", 0), "closed": v.get("closed", 0), "skipped": v.get("skipped", 0),
        "open": open_list, "tier": _tier(user),
        "months": keys, "month": month, "month_label": month_label(month),
        "key": page_key(month, part), "part": part, "parts": parts,
        "page": idx + 1 if idx >= 0 else 1, "pages": max(1, len(pages)),
        "prev": page_key(*pages[idx - 1][:2]) if idx > 0 else None,
        "next": page_key(*pages[idx + 1][:2]) if 0 <= idx < len(pages) - 1 else None,
        "stats": stats(trades, m.get("start_bal")), "trades": page_trades,
        "exports_used": int(b["exports"].get(month_key(), 0)), "exports_limit": lim,
    }


def export_allowed(cid: str, tier: str) -> str:
    """'' if another export is allowed this month (and counts it), else why not."""
    b = book(cid)
    mk = month_key()
    used = int(b["exports"].get(mk, 0))
    lim = EXPORT_LIMIT.get(tier, 5)
    if used >= lim:
        return f"PDF limit reached - {lim} exports this month ({'VIP' if tier == 'vip' else 'Free'} plan)."
    b["exports"][mk] = used + 1
    _save_book(cid)
    return ""


# ── PDF ────────────────────────────────────────────────────────────────────

def _money(x) -> str:
    x = float(x or 0)
    return f"{'-' if x < 0 else ''}${abs(x):,.2f}"


def _tx(s) -> str:
    """matplotlib reads $...$ as maths - every dollar sign must be escaped."""
    return str(s).replace("$", "\\$")


def px(x) -> str:
    """Price text that survives 8-decimal coins (0.00001234) - never the
    1.2e-05 that :g would print, never 0.00 for something that isn't zero."""
    try:
        x = float(x)
    except Exception:
        return str(x)
    if x == 0:
        return "0"
    a = abs(x)
    dec = 2 if a >= 1000 else 4 if a >= 1 else 6 if a >= 0.01 else 8
    s = f"{x:.{dec}f}".rstrip("0").rstrip(".")
    return s if s not in ("", "-", "-0") else f"{x:.8f}"


def build_pdf(cid: str, user: dict, v: dict, b: dict, month: str = None) -> bytes:
    """Month report (month='YYYY-MM') or the whole run (month=None)."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    if month:
        trades = list(b["months"].get(month, {}).get("trades", []))
        title = f"Virtual Trading Report - {month_label(month)}"
        start_bal = b["months"].get(month, {}).get("start_bal")
    else:
        trades = [t for k in sorted(b["months"]) for t in b["months"][k].get("trades", [])]
        title = f"Virtual Trading Report - Run #{v.get('run', 0)}"
        start_bal = v.get("capital")
    s = stats(trades, start_bal)
    name = user.get("username") or str(cid)
    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        fig = plt.figure(figsize=(8.27, 11.69))
        fig.patch.set_facecolor("white")
        fig.text(0.07, 0.95, "CLEXER", fontsize=22, fontweight="bold", color="#1f2430")
        fig.text(0.07, 0.92, title, fontsize=13, color="#333")
        fig.text(0.07, 0.895, f"@{name}  ·  generated {_stamp()} IST  ·  practice account, no real money", fontsize=8, color="#777")
        mode = ("Auto leverage  ·  risk " + (f"${v['risk']:.2f}" if v["risk_mode"] == "usd" else f"{v['risk']:g}% of margin")
                if v["lev_mode"] == "auto" else f"Manual leverage {v['lev']:g}x")
        marg = f"${v['margin']:.2f}" if v["margin_mode"] == "usd" else f"{v['margin']:g}% of balance"
        rows = [("Starting balance", _money(start_bal if start_bal is not None else v.get('capital', 0))),
                ("Ending balance", _money(trades[-1]['bal'] if trades else v.get('balance', 0))),
                ("Net result", ("+" if s["net"] >= 0 else "") + _money(s["net"])),
                ("Trades", f"{s['trades']}  (wins {s['wins']}, losses {s['losses']}, win rate {s['wr']}%)"),
                ("Best / worst", f"+{_money(s['best'])}  /  {_money(s['worst'])}"),
                ("Max drawdown", _money(s["dd"])),
                ("Fees paid", _money(s["fees"])),
                ("Settings", f"{mode}  ·  margin {marg}"),
                ("Results", "  ".join(f"{k} {n}" for k, n in sorted(s["by_res"].items())) or "-")]
        y = 0.86
        for k, val in rows:
            fig.text(0.07, y, k, fontsize=9.5, color="#666")
            fig.text(0.30, y, _tx(val), fontsize=9.5, color="#111")
            y -= 0.024
        ax = fig.add_axes([0.09, 0.30, 0.84, 0.30])
        curve = [start_bal if start_bal is not None else (trades[0]["bal"] - trades[0]["pnl"] if trades else 0)]
        for t in trades:
            curve.append(t["bal"])
        ax.plot(range(len(curve)), curve, color="#1f77b4", linewidth=1.6)
        ax.fill_between(range(len(curve)), curve, min(curve) if curve else 0, color="#1f77b4", alpha=0.08)
        ax.set_title("Balance after each trade", fontsize=10, loc="left")
        ax.grid(alpha=0.25)
        ax.tick_params(labelsize=8)
        ax.set_xlabel("trade #", fontsize=8)
        by_sym = {}
        for t in trades:
            by_sym[t["sym"]] = by_sym.get(t["sym"], 0) + t["pnl"]
        if by_sym:
            ax2 = fig.add_axes([0.09, 0.06, 0.84, 0.17])
            items = sorted(by_sym.items(), key=lambda kv: kv[1])[-12:]
            ax2.barh([k for k, _ in items], [val for _, val in items], color=["#2ca02c" if val >= 0 else "#d62728" for _, val in items])
            ax2.set_title("Result by coin", fontsize=10, loc="left")
            ax2.tick_params(labelsize=7)
            ax2.grid(axis="x", alpha=0.25)
        pdf.savefig(fig)
        plt.close(fig)
        # trade pages
        cols = ["Opened", "Closed", "Coin", "Side", "Lev", "Margin", "Entry", "Exit", "Result", "P&L", "Balance"]
        per = 34
        for i in range(0, len(trades), per):
            chunk = trades[i:i + per]
            fig = plt.figure(figsize=(8.27, 11.69))
            fig.text(0.07, 0.95, f"Trades {i + 1}-{i + len(chunk)} of {len(trades)}", fontsize=12, color="#333")
            ax = fig.add_axes([0.04, 0.05, 0.92, 0.87])
            ax.axis("off")
            cells = [[t["o"][5:], t["t"][5:], t["sym"].replace("-USDT", "").replace("USDT", ""), "LONG" if t["side"] == "BUY" else "SHORT",
                      f"{t['lev']:g}x", _tx(_money(t['margin'])), px(t['entry']), px(t['exit']), t["res"],
                      f"{'+' if t['pnl'] >= 0 else ''}{t['pnl']:.2f}", f"{t['bal']:.2f}"] for t in chunk]
            tb = ax.table(cellText=cells, colLabels=cols, loc="upper center", cellLoc="center")
            tb.auto_set_font_size(False)
            tb.set_fontsize(7)
            tb.scale(1, 1.25)
            for (r, c), cell in tb.get_celld().items():
                cell.set_edgecolor("#dddddd")
                if r == 0:
                    cell.set_facecolor("#1f2430")
                    cell.set_text_props(color="white", fontweight="bold")
                elif c == 9:
                    cell.set_text_props(color="#2ca02c" if chunk[r - 1]["pnl"] >= 0 else "#d62728")
            pdf.savefig(fig)
            plt.close(fig)
    return buf.getvalue()


def pdf_for(cid: str, month: str = None):
    user = ct._get(cid) or {}
    v = get(user) if user else default()
    b = book(cid)
    month = split_key(month)[0] if month else None      # a page key names its month
    if month and month not in b["months"]:
        return None, "No trades in that month."
    if not month and not any(m.get("trades") for m in b["months"].values()):
        return None, "No trades yet."
    data = build_pdf(cid, user, v, b, month)
    return data, f"CLEXER_virtual_{month or 'run' + str(v.get('run', 0))}.pdf"


# ── month-end job ──────────────────────────────────────────────────────────

def month_rollover(send_doc):
    """Call once an hour. Closes out every finished month (admin 2026-09-16):

    - waits until every trade opened in that month has closed;
    - builds the month's PDF and DMs it once - if the DM fails (blocked bot,
      closed chat) the PDF is parked in the book and handed over the next
      time the user opens /virtual, with no further retries here;
    - wipes the month's trades and export counter from the book;
    - the new month starts from the carried balance: capital := balance, so
      the Result % is the new month's own.

    send_doc is bot.py's (cid, bytes, filename, caption) sender, returning
    truthy on delivery."""
    cur = month_key()
    done = 0
    for cid, user in list(ct._db.items()):
        try:
            v = user.get("virtual")
            if not isinstance(v, dict) or v.get("v") != 2:
                continue
            b = book(cid)
            old = [mk for mk in sorted(b["months"]) if mk < cur]
            if not old:
                continue
            changed = False
            for mk in old:
                if any((p.get("mk") or cur) == mk for p in v.get("open", {}).values()):
                    continue                                   # a trade from that month is still running
                trades = b["months"].get(mk, {}).get("trades", [])
                if trades:
                    data = build_pdf(cid, user, v, b, mk)
                    ok = False
                    try:
                        ok = bool(send_doc(cid, data, f"CLEXER_virtual_{mk}.pdf",
                                           f"📄 Your virtual trading report for {month_label(mk)}."))
                    except Exception as e:
                        print(f"[VIRTUAL] month report DM {cid} {mk}: {e}")
                    if not ok:
                        b.setdefault("pending", {})[mk] = base64.b64encode(data).decode("ascii")
                    time.sleep(1.2)
                b["months"].pop(mk, None)
                b.get("exports", {}).pop(mk, None)
                b.setdefault("reports_sent", []).append(mk)
                changed = True
                done += 1
            if changed:
                # the next month starts from what is actually in the account
                v["capital"] = round(float(v["balance"]), 4) if float(v["balance"]) > 0 else v["capital"]
                _save_book(cid)
                ct._set(cid, user)
        except Exception as e:
            print(f"[VIRTUAL] month rollover {cid}: {e}")
    return done


monthly_reports = month_rollover      # bot.py's hourly loop calls this name


def pending_reports(cid: str):
    """[(month_key, pdf_bytes)] parked because the month-end DM failed."""
    b = book(cid)
    out = []
    for mk, b64 in sorted((b.get("pending") or {}).items()):
        try:
            out.append((mk, base64.b64decode(b64)))
        except Exception:
            pass
    return out


def clear_pending(cid: str, mk: str):
    b = book(cid)
    if (b.get("pending") or {}).pop(mk, None) is not None:
        _save_book(cid)
