#!/usr/bin/env python3
"""sysvirtual.py - admin-only virtual accounts for the TEST system and ELITE.

Each admin (main admin and co-admin) can run one paper account per system.
Same money rules as the users' /virtual - it borrows virtual.py's sizing,
fees, stats, month pages and PDF, and never changes that file:

    capital, Auto or Manual leverage, margin per trade, risk at stop,
    0.05 % fee on open / TP1 partial / close, liquidation cap, wipe-out.

Where it differs:
    - the trades come from the Test system (/test) or Elite (/el), not from
      the VIP/Free signals;
    - a position is keyed by the trade's signal id, not its coin - Elite may
      hold one coin LONG and SHORT at once;
    - account block user["virtual_<sys>"], book vbook_<sys>_<cid>;
    - admins only, no PDF limit, no month-end auto report.

bot.py binds `ct` (copytrade) and `is_allowed` (cid -> admin?) at start.
api.py imports only the pure state_from().
"""

import virtual as V

ct = None                 # bound by bot.py
is_allowed = None         # bound by bot.py: (cid) -> bool

SYSTEMS = {"test": "Test System", "elite": "Elite"}


def ukey(sysk: str) -> str:
    return f"virtual_{sysk}"


def bid(cid, sysk: str) -> str:
    """The book id - vbook_<sys>_<cid> in the central store."""
    return f"{sysk}_{cid}"


def get(user: dict, sysk: str) -> dict:
    v = user.get(ukey(sysk))
    if not isinstance(v, dict) or v.get("v") != 2:
        v = V.default()
        user[ukey(sysk)] = v
    return v


def _user(cid):
    return ct._get(str(cid)) or ct._default_user()


# ── control ────────────────────────────────────────────────────────────────

def setup(cid, sysk, capital, lev_mode, lev, risk_mode, risk, margin_mode, margin) -> str:
    err = V.validate(capital, lev_mode, lev, risk_mode, risk, margin_mode, margin)
    if err:
        return err
    cid = str(cid)
    user = _user(cid)
    v = get(user, sysk)
    if v["status"] == "running":
        return "Already running - Stop or Reset first."
    v.update({"status": "running", "capital": round(float(capital), 4), "balance": round(float(capital), 4),
              "lev_mode": lev_mode, "lev": float(lev or 10), "risk_mode": risk_mode, "risk": float(risk or 0),
              "margin_mode": margin_mode, "margin": float(margin), "open": {}, "started_at": V._stamp(),
              "run": int(v.get("run", 0)) + 1, "closed": 0, "skipped": 0})
    ct._set(cid, user)
    b = V.book(bid(cid, sysk))
    b["runs"].append({"run": v["run"], "started": v["started_at"], "capital": v["capital"], "ended": "", "final": None})
    V._save_book(bid(cid, sysk))
    return ""


def stop(cid, sysk) -> str:
    user = _user(cid)
    v = get(user, sysk)
    if v["status"] != "running":
        return "Not running."
    v["status"] = "stopped"
    ct._set(str(cid), user)
    return ""


def resume(cid, sysk) -> str:
    user = _user(cid)
    v = get(user, sysk)
    if v["status"] != "stopped":
        return "Nothing to resume." if v["status"] != "wiped" else "Capital is gone - Reset to start again."
    v["status"] = "running"
    ct._set(str(cid), user)
    return ""


def _pdf_user(user: dict, sysk: str) -> dict:
    """virtual.build_pdf prints '@name' in its header - carry the system's
    name there so a Test report and an Elite report can't be mixed up."""
    return {**user, "username": f"{user.get('username') or 'admin'} · {SYSTEMS[sysk]} virtual"}


def reset(cid, sysk):
    """(pdf_bytes_or_None, filename) of the whole run, built before the wipe."""
    cid = str(cid)
    user = _user(cid)
    v = get(user, sysk)
    b = V.book(bid(cid, sysk))
    pdf, fname = None, ""
    if any(m.get("trades") for m in b["months"].values()):
        try:
            pdf = V.build_pdf(cid, _pdf_user(user, sysk), v, b, month=None)
            fname = f"CLEXER_{sysk}_virtual_run{v.get('run', 0)}.pdf"
        except Exception as e:
            print(f"[SYSVIRTUAL] reset pdf {sysk} {cid}: {e}")
    if b["runs"]:
        b["runs"][-1]["ended"] = V._stamp()
        b["runs"][-1]["final"] = v.get("balance")
    b["months"] = {}
    b["reports_sent"] = []
    V._save_book(bid(cid, sysk))
    keep_run = int(v.get("run", 0))
    user[ukey(sysk)] = V.default()
    user[ukey(sysk)]["run"] = keep_run
    ct._set(cid, user)
    return pdf, fname


def pdf_for(cid, sysk, month=None):
    cid = str(cid)
    user = ct._get(cid) or {}
    v = get(user, sysk) if user else V.default()
    b = V.book(bid(cid, sysk))
    month = V.split_key(month)[0] if month else None
    if month and month not in b["months"]:
        return None, "No trades in that month."
    if not month and not any(m.get("trades") for m in b["months"].values()):
        return None, "No trades yet."
    data = V.build_pdf(cid, _pdf_user(user, sysk), v, b, month)
    return data, f"CLEXER_{sysk}_virtual_{month or 'run' + str(v.get('run', 0))}.pdf"


# ── trade hooks (bot.py: Test system and Elite open / TP1 / close) ─────────

def _accounts(sysk, running_only):
    out = []
    for cid, user in list(ct._db.items()):
        v = user.get(ukey(sysk))
        if not isinstance(v, dict) or v.get("v") != 2:
            continue
        if running_only and (v.get("status") != "running" or (is_allowed and not is_allowed(cid))):
            continue
        out.append((cid, user, v))
    return out


def on_open(sysk, tkey, symbol, side, entry, sl, tp1, tp2):
    entry = float(entry or 0)
    if entry <= 0 or not tkey:
        return
    for cid, user, v in _accounts(sysk, running_only=True):
        if tkey in v["open"]:
            continue
        plan, note = V.size(v, entry, float(sl or 0))
        if not plan:
            v["skipped"] = int(v.get("skipped", 0)) + 1
            ct._set(cid, user)
            continue
        notional = plan["margin"] * plan["lev"]
        fee = round(notional * V.FEE, 6)
        v["balance"] = round(float(v["balance"]) - fee, 6)
        v["open"][tkey] = {"symbol": symbol, "side": side, "entry": entry, "sl": float(sl or 0),
                           "tp1": float(tp1 or 0), "tp2": float(tp2 or 0),
                           "qty": notional / entry, "qty0": notional / entry,
                           "margin": plan["margin"], "lev": plan["lev"], "risk": plan["risk"],
                           "fee": fee, "fee0": fee, "realized": 0.0, "tp1_hit": False,
                           "opened_at": V._stamp(), "note": note, "mk": V.month_key()}
        ct._set(cid, user)


def on_tp1(sysk, tkey, price):
    """The partial close at TP1 - on stopped accounts too: a stop only means
    no NEW trades, open ones still run to their end."""
    price = float(price or 0)
    if price <= 0:
        return
    portion = float(getattr(ct, "TP1_CLOSE_PCT", 50)) / 100.0
    for cid, user, v in _accounts(sysk, running_only=False):
        pos = v.get("open", {}).get(tkey)
        if not pos or pos.get("tp1_hit"):
            continue
        q = pos["qty"] * portion
        pnl = V._pnl(pos, price, q)
        fee = q * price * V.FEE
        pos["realized"] = round(pos.get("realized", 0) + pnl - fee, 6)
        pos["fee"] = round(pos.get("fee", 0) + fee, 6)
        pos["qty"] -= q
        pos["tp1_hit"] = True
        v["balance"] = round(float(v["balance"]) + pnl - fee, 6)
        ct._set(cid, user)


def on_close(sysk, tkey, price, result):
    price = float(price or 0)
    if price <= 0:
        return
    for cid, user, v in _accounts(sysk, running_only=False):
        pos = v.get("open", {}).pop(tkey, None)
        if not pos:
            continue
        symbol = pos.get("symbol", "")
        pnl = V._pnl(pos, price, pos["qty"])
        fee = pos["qty"] * price * V.FEE
        total = pos.get("realized", 0) + pnl - fee
        note = pos.get("note", "")
        res = str(result)
        if total < -pos["margin"]:                    # a loss beyond the margin is a liquidation
            total = -pos["margin"]
            note = (note + " · " if note else "") + "liquidated"
            res = "LIQ"
        delta = total - pos.get("realized", 0)
        v["balance"] = round(max(0.0, float(v["balance"]) + delta), 6)
        v["closed"] = int(v.get("closed", 0)) + 1
        if pos.get("tp1_hit") and res in ("BE", "SL"):
            res = "TP1+BE" if res == "BE" else "TP1+SL"
        rec = {"o": pos.get("opened_at", ""), "t": V._stamp(), "sym": symbol, "side": pos["side"],
               "lev": pos["lev"], "margin": round(pos["margin"], 4), "entry": pos["entry"], "exit": price,
               "res": res, "pnl": round(total - V._entry_fee(pos), 4),
               "fee": round(pos.get("fee", 0) + fee, 6),
               "bal": round(float(v["balance"]), 4), "note": note, "run": v.get("run", 0)}
        b = V.book(bid(cid, sysk))
        m = b["months"].setdefault(pos.get("mk") or V.month_key(),
                                   {"run": v.get("run", 0), "trades": [], "start_bal": None})
        if m.get("start_bal") is None:
            m["start_bal"] = round(rec["bal"] - rec["pnl"], 4)
        m["trades"].append(rec)
        V._save_book(bid(cid, sysk))
        if v["status"] == "running" and v["balance"] < V._min_needed(v):
            v["status"] = "wiped"
            v["wiped_at"] = V._stamp()
        ct._set(cid, user)


# ── reading ────────────────────────────────────────────────────────────────

def state_from(urec: dict, b: dict, sysk: str, month: str = None) -> dict:
    """Pure - api.py feeds it the kv blobs. virtual.state_from does the month
    pages and stats; the open list comes out with each position's own coin,
    since every position here stores it."""
    v = (urec or {}).get(ukey(sysk))
    st = V.state_from({"virtual": v, "tier": "vip"}, b, month)
    st["system"] = sysk
    st["system_label"] = SYSTEMS.get(sysk, sysk)
    st["exports_limit"] = None                      # admins: no PDF limit
    st.pop("legacy", None)
    return st


def state(cid, sysk, month=None) -> dict:
    cid = str(cid)
    return state_from(ct._get(cid) or {}, V.book(bid(cid, sysk)), sysk, month)
