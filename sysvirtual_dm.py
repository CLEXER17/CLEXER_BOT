#!/usr/bin/env python3
"""sysvirtual_dm.py - the bot screens for the admin-only Test / Elite virtual
accounts (sysvirtual.py). Same layout as the users' /virtual screens
(virtual_dm.py, which this file does not touch), one account per system.

bot.py forwards:
    /test v  |  /el v              -> show(cid, chat_id, sysk)
    callback data "vs:<sys>:..."   -> on_callback(...)   (returns popup text or None)
    typed text while a form asks   -> wants_text(cid) / on_text(cid, chat_id, text)
    Mini App events "vsys_*"       -> apply_event(cid, etype, meta)
"""

import html as _html
import threading
import time

import requests

import copytrade as ct
import virtual as V
import sysvirtual as SV

_TOKEN = None
_forms: dict = {}          # (cid, sys) -> {"field", "msg_id", "draft"}
_confirm_reset: dict = {}  # (cid, sys) -> ts
_active_form: dict = {}    # cid -> sys whose form is waiting for typed text
_admin_ids = None          # () -> list of admin cids, bound by init()


def init(token, admin_ids):
    """admin_ids() lists who may use these accounts. It is also published to
    the central store as vsys_admins, so the Mini App's api.py can tell a
    co-admin from a user without knowing bot.py's settings."""
    global _TOKEN, _admin_ids
    _TOKEN = token
    _admin_ids = admin_ids
    SV.is_allowed = lambda cid: str(cid) in {str(x) for x in (_admin_ids() or []) if x}
    threading.Thread(target=_publish_admins_loop, daemon=True).start()


def _publish_admins_loop():
    last = None
    while True:
        try:
            ids = sorted({str(x) for x in (_admin_ids() or []) if x})
            if ids != last and ct._API_URL:
                hdrs = {"X-Push-Secret": ct._API_SECRET} if ct._API_SECRET else {}
                r = requests.post(f"{ct._API_URL}/kv/vsys_admins", json={"ids": ids}, headers=hdrs, timeout=15)
                if r.ok:
                    last = ids
        except Exception as e:
            print(f"  [SYSVIRTUAL] admin list push: {e}")
        time.sleep(600)


def allowed(cid) -> bool:
    return bool(SV.is_allowed and SV.is_allowed(cid))


def _api(method, payload=None, files=None, timeout=15):
    if not _TOKEN:
        return {}
    url = f"https://api.telegram.org/bot{_TOKEN}/{method}"
    try:
        if files:
            r = requests.post(url, data=payload, files=files, timeout=max(timeout, 30))
        else:
            r = requests.post(url, json=payload, timeout=timeout)
        return r.json()
    except Exception as e:
        print(f"  [SYSVIRTUAL DM] {method} failed: {e}")
        return {}


def _send_doc(cid, data: bytes, filename: str, caption: str = ""):
    j = _api("sendDocument", {"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
             files={"document": (filename, data, "application/pdf")})
    return bool(isinstance(j, dict) and j.get("ok"))


def _esc(s):
    return _html.escape(str(s or ""))


def _money(x):
    x = float(x or 0)
    return f"{'-' if x < 0 else ''}${abs(x):,.2f}"


def _name(sysk):
    return "ELITE" if sysk == "elite" else "TEST SYSTEM"


def _draft(cid, sysk):
    k = (cid, sysk)
    if k in _forms and _forms[k].get("draft"):
        return _forms[k]["draft"]
    user = ct._get(cid) or {}
    v = SV.get(user, sysk) if user else V.default()
    d = {"capital": v.get("capital") or 20, "lev_mode": v.get("lev_mode", "auto"), "lev": v.get("lev", 10),
         "risk_mode": v.get("risk_mode", "usd"), "risk": v.get("risk", 0.5),
         "margin_mode": v.get("margin_mode", "usd"), "margin": v.get("margin", 2)}
    _forms[k] = {"field": None, "msg_id": None, "draft": d}
    return d


def _fmt_risk(d):
    return f"${float(d['risk']):g}" if d["risk_mode"] == "usd" else f"{float(d['risk']):g}% of margin"


def _fmt_margin(d):
    return f"${float(d['margin']):g}" if d["margin_mode"] == "usd" else f"{float(d['margin']):g}% of balance"


# ── screens ────────────────────────────────────────────────────────────────

def _setup_text(cid, sysk, err=None):
    d = _draft(cid, sysk)
    v = V.default()
    v.update({"balance": float(d["capital"]), "capital": float(d["capital"]), "lev_mode": d["lev_mode"],
              "lev": float(d["lev"]), "risk_mode": d["risk_mode"], "risk": float(d["risk"]),
              "margin_mode": d["margin_mode"], "margin": float(d["margin"])})
    out = [f"🎮 <b>{_name(sysk)} Virtual - setup</b>", ""]
    out.append(f"💰 Total capital: <b>{_money(d['capital'])}</b>")
    if d["lev_mode"] == "auto":
        out.append("⚖️ Leverage: <b>Auto</b> - set per trade so a stop-loss costs your risk")
        out.append(f"🎯 Risk per trade at SL: <b>{_fmt_risk(d)}</b>")
    else:
        out.append(f"⚖️ Leverage: <b>Manual {float(d['lev']):g}x</b>")
    out.append(f"💵 Margin per trade: <b>{_fmt_margin(d)}</b>")
    pv = V.preview(v, 100, 98.8, 101.5, 103)
    if "skip" not in pv:
        out += ["", f"Example trade (1.2% stop): <b>{pv['lev']:g}x</b> · position {_money(pv['position'])} · "
                    f"loss at SL {_money(pv['sl'])} · TP1 {'+' if pv['tp1'] >= 0 else ''}{_money(pv['tp1'])} · "
                    f"TP2 +{_money(pv['tp2'])}"]
    if err:
        out += ["", f"⚠️ {_esc(err)}"]
    out += ["", f"Admin practice account - no real money. Every {_name(sysk).title()} trade is copied "
                f"here once you press Start."]
    return "\n".join(out)


def _setup_kb(cid, sysk):
    d = _draft(cid, sysk)
    p = f"vs:{sysk}"
    rows = [[{"text": "💰 Capital", "callback_data": f"{p}:f:capital"},
             {"text": f"⚖️ {'Auto' if d['lev_mode'] == 'auto' else 'Manual'} leverage", "callback_data": f"{p}:lev"}]]
    if d["lev_mode"] == "auto":
        rows.append([{"text": "🎯 Risk per trade", "callback_data": f"{p}:f:risk"},
                     {"text": "💵 Margin per trade", "callback_data": f"{p}:f:margin"}])
    else:
        rows.append([{"text": "🔢 Leverage value", "callback_data": f"{p}:f:lev"},
                     {"text": "💵 Margin per trade", "callback_data": f"{p}:f:margin"}])
    rows.append([{"text": "▶ START", "callback_data": f"{p}:start", "style": "success"}])
    rows.append([{"text": "📄 Past reports", "callback_data": f"{p}:page:"}])
    return {"inline_keyboard": rows}


def _status_text(cid, sysk, month=None):
    st = SV.state(cid, sysk, month)
    s = st["status"]
    head = {"running": "🟢 RUNNING", "stopped": "⏸ STOPPED", "wiped": "💀 CAPITAL EXHAUSTED"}.get(s, s.upper())
    out = [f"🎮 <b>{_name(sysk)} Virtual</b> · {head}"]
    if st.get("started_at"):
        out.append(f"since {st['started_at']} · run #{st['run']}")
    out.append("")
    sign = "+" if st["pnl_pct"] >= 0 else ""
    out.append(f"Capital {_money(st['capital'])} → Balance <b>{_money(st['balance'])}</b> ({sign}{st['pnl_pct']}%)")
    if st["lev_mode"] == "auto":
        out.append(f"Auto leverage · risk {_fmt_risk(st)} · margin {_fmt_margin(st)}")
    else:
        out.append(f"Manual {float(st['lev']):g}x · margin {_fmt_margin(st)}")
    out.append(f"Closed {st['closed']} · skipped {st['skipped']} (no free margin)")
    if st["open"]:
        out += ["", "<b>Open now</b>"]
        for p in st["open"][:8]:
            sym = str(p.get("symbol", "")).replace("-USDT", "").replace("USDT", "")
            side = "LONG" if p["side"] == "BUY" else "SHORT"
            out.append(f"• {sym} {side} {p['lev']:g}x · entry {V.px(p['entry'])} · SL {V.px(p['sl'])} · "
                       f"TP1 {V.px(p['tp1'])}" + (" · TP1 ✅" if p.get("tp1_hit") else "")
                       + (f" · {p['note']}" if p.get("note") else ""))
    if s == "wiped":
        out += ["", "The balance can no longer fund a trade, so trading stopped. Reset to start a new run."]
    stt = st["stats"]
    out += ["", f"📅 <b>{st['month_label']}</b> · page {st['page']}/{st['pages']}"
                + (f" · part {st['part']}/{st['parts']}" if st.get("parts", 1) > 1 else "")]
    if stt["trades"]:
        out.append(f"Trades {stt['trades']} · wins {stt['wins']} · losses {stt['losses']} · win rate {stt['wr']}%")
        out.append(f"Net {'+' if stt['net'] >= 0 else ''}{_money(stt['net'])} · best +{_money(stt['best'])} · "
                   f"worst {_money(stt['worst'])} · max drawdown {_money(stt['dd'])} · fees {_money(stt.get('fees', 0))}")
        for t in st["trades"][:8]:
            sym = t["sym"].replace("-USDT", "").replace("USDT", "")
            side = "LONG" if t["side"] == "BUY" else "SHORT"
            out.append(f"<code>{t['t'][5:16]}</code> {sym} {side} {t['lev']:g}x · {t['res']} · "
                       f"<b>{'+' if t['pnl'] >= 0 else ''}{_money(t['pnl'])}</b>")
        if stt["trades"] > 8:
            out.append(f"… {stt['trades'] - 8} more in the PDF")
    else:
        out.append("No closed trades this month yet.")
    return "\n".join(out)[:4000]


def _status_kb(cid, sysk, month=None):
    st = SV.state(cid, sysk, month)
    p = f"vs:{sysk}"
    rows = []
    if st["status"] == "running":
        rows.append([{"text": "⏸ Stop", "callback_data": f"{p}:stop"},
                     {"text": "↺ Reset", "callback_data": f"{p}:reset", "style": "danger"}])
    elif st["status"] == "stopped":
        rows.append([{"text": "▶ Resume", "callback_data": f"{p}:resume", "style": "success"},
                     {"text": "↺ Reset", "callback_data": f"{p}:reset", "style": "danger"}])
    else:
        rows.append([{"text": "↺ Reset & set up again", "callback_data": f"{p}:reset", "style": "danger"}])
    nav = []
    if st["prev"]:
        nav.append({"text": "◀ Prev", "callback_data": f"{p}:page:{st['prev']}"})
    nav.append({"text": f"📄 PDF {st['month_label'][:3]}", "callback_data": f"{p}:pdf:{st['month']}"})
    if st["next"]:
        nav.append({"text": "Next ▶", "callback_data": f"{p}:page:{st['next']}"})
    rows.append(nav)
    rows.append([{"text": "🔄 Refresh", "callback_data": f"{p}:page:{st.get('key') or st['month']}"}])
    return {"inline_keyboard": rows}


def _show(cid, chat_id, sysk, msg_id=None, month=None, err=None):
    user = ct._get(cid) or {}
    v = SV.get(user, sysk) if user else V.default()
    if v["status"] == "setup":
        text, kb = _setup_text(cid, sysk, err), _setup_kb(cid, sysk)
    else:
        text, kb = _status_text(cid, sysk, month), _status_kb(cid, sysk, month)
    if msg_id:
        j = _api("editMessageText", {"chat_id": chat_id, "message_id": msg_id, "text": text,
                                     "parse_mode": "HTML", "reply_markup": kb})
        if j.get("ok") or "not modified" in str(j.get("description", "")):
            return msg_id
    j = _api("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "reply_markup": kb})
    return (j.get("result") or {}).get("message_id")


def show(cid, chat_id, sysk):
    cid = str(cid)
    if not allowed(cid):
        _api("sendMessage", {"chat_id": chat_id, "text": "This virtual account is for admins only."})
        return
    _forms.pop((cid, sysk), None)
    _show(cid, chat_id, sysk)


# ── form input ─────────────────────────────────────────────────────────────

_PROMPT = {
    "capital": "💰 Send the <b>total capital</b> in USDT (for example <code>20</code>).",
    "lev": "🔢 Send the <b>leverage</b> for every trade, 1-100 (for example <code>10</code>).",
    "risk": "🎯 How much may one trade lose at stop-loss? Send an amount like <code>0.5</code> (dollars) or a percent of the margin like <code>25%</code>.",
    "margin": "💵 Margin per trade - send dollars like <code>2</code> or a percent of the balance like <code>20%</code>.",
}


def wants_text(cid) -> bool:
    sysk = _active_form.get(str(cid))
    f = _forms.get((str(cid), sysk)) if sysk else None
    return bool(f and f.get("field"))


def on_text(cid, chat_id, text) -> bool:
    cid = str(cid)
    sysk = _active_form.get(cid)
    f = _forms.get((cid, sysk)) if sysk else None
    if not f or not f.get("field"):
        return False
    field, d = f["field"], f["draft"]
    raw = text.strip().replace("$", "").replace(",", "")
    pct = raw.endswith("%")
    raw = raw.rstrip("%").strip()
    try:
        val = float(raw)
    except Exception:
        _api("sendMessage", {"chat_id": chat_id, "text": "Numbers only - try again.", "parse_mode": "HTML"})
        return True
    err = None
    if field == "capital":
        if val < 1:
            err = "Capital must be at least $1."
        else:
            d["capital"] = val
    elif field == "lev":
        if not 1 <= val <= V.MAX_LEV:
            err = f"Leverage must be 1-{int(V.MAX_LEV)}."
        else:
            d["lev"] = val
    elif field == "risk":
        d["risk_mode"] = "pct" if pct else "usd"
        d["risk"] = val
    elif field == "margin":
        d["margin_mode"] = "pct" if pct else "usd"
        d["margin"] = val
    if not err:
        err = V.validate(d["capital"], d["lev_mode"], d["lev"], d["risk_mode"], d["risk"], d["margin_mode"], d["margin"])
    f["field"] = None
    _active_form.pop(cid, None)
    _show(cid, chat_id, sysk, f.get("msg_id"), err=err)
    return True


# ── callbacks ──────────────────────────────────────────────────────────────

def on_callback(data, cid, chat_id, msg_id):
    """data = vs:<sys>:<act>[:<arg>]"""
    cid = str(cid)
    parts = data.split(":")
    sysk = parts[1] if len(parts) > 1 else ""
    act = parts[2] if len(parts) > 2 else ""
    arg = parts[3] if len(parts) > 3 else ""
    if sysk not in SV.SYSTEMS:
        return None
    if not allowed(cid):
        return "Admins only."
    k = (cid, sysk)
    p = f"vs:{sysk}"
    if act == "show":
        _forms.pop(k, None)
        _show(cid, chat_id, sysk)
        return None
    if act == "f":
        _draft(cid, sysk)
        _forms[k].update({"field": arg, "msg_id": msg_id})
        _active_form[cid] = sysk
        _api("editMessageText", {"chat_id": chat_id, "message_id": msg_id, "text": _PROMPT[arg], "parse_mode": "HTML",
                                 "reply_markup": {"inline_keyboard": [[{"text": "↩ Back", "callback_data": f"{p}:back"}]]}})
        return None
    if act == "back":
        f = _forms.get(k)
        if f:
            f["field"] = None
        _active_form.pop(cid, None)
        _show(cid, chat_id, sysk, msg_id)
        return None
    if act == "lev":
        d = _draft(cid, sysk)
        d["lev_mode"] = "manual" if d["lev_mode"] == "auto" else "auto"
        _show(cid, chat_id, sysk, msg_id)
        return None
    if act == "start":
        d = _draft(cid, sysk)
        err = SV.setup(cid, sysk, d["capital"], d["lev_mode"], d["lev"], d["risk_mode"], d["risk"],
                       d["margin_mode"], d["margin"])
        if err:
            _show(cid, chat_id, sysk, msg_id, err=err)
            return err
        _forms.pop(k, None)
        _show(cid, chat_id, sysk, msg_id)
        return f"{_name(sysk).title()} virtual started."
    if act == "stop":
        err = SV.stop(cid, sysk)
        _show(cid, chat_id, sysk, msg_id)
        return err or "Paused - open trades still close normally, no new ones open."
    if act == "resume":
        err = SV.resume(cid, sysk)
        _show(cid, chat_id, sysk, msg_id)
        return err or "Running again."
    if act == "reset":
        if time.time() - _confirm_reset.get(k, 0) > 60:
            _confirm_reset[k] = time.time()
            _api("editMessageReplyMarkup", {"chat_id": chat_id, "message_id": msg_id, "reply_markup": {"inline_keyboard": [
                [{"text": "Yes, wipe it", "callback_data": f"{p}:reset", "style": "danger"},
                 {"text": "No", "callback_data": f"{p}:page:"}]]}})
            return "Reset wipes the balance and history. The report is sent to you first. Tap Yes to confirm."
        _confirm_reset.pop(k, None)
        do_reset(cid, chat_id, sysk)
        _forms.pop(k, None)
        _show(cid, chat_id, sysk, msg_id)
        return "Run wiped."
    if act == "page":
        _show(cid, chat_id, sysk, msg_id, month=arg or None)
        return None
    if act == "pdf":
        return do_pdf(cid, chat_id, sysk, arg or None)
    return None


def do_reset(cid, chat_id, sysk):
    pdf, fname = SV.reset(cid, sysk)
    if pdf:
        _send_doc(chat_id, pdf, fname, f"📄 {_name(sysk).title()} virtual run, before the reset. It is now wiped.")


def do_pdf(cid, chat_id, sysk, month):
    data, fname = SV.pdf_for(cid, sysk, month)
    if not data:
        return fname
    _send_doc(chat_id, data, fname, f"📄 {_name(sysk).title()} virtual report · "
                                    f"{V.month_label(V.split_key(month)[0]) if month else 'whole run'}")
    return "Report sent below."


# ── Mini App events (queued by api.py, applied by bot.py's poller) ─────────

def apply_event(cid, etype, meta):
    cid = str(cid)
    meta = meta or {}
    sysk = meta.get("sys")
    if sysk not in SV.SYSTEMS or not allowed(cid):
        return
    if etype == "vsys_setup":
        err = SV.setup(cid, sysk, meta.get("capital"), meta.get("lev_mode", "auto"), meta.get("lev") or 10,
                       meta.get("risk_mode", "usd"), meta.get("risk") or 0, meta.get("margin_mode", "usd"),
                       meta.get("margin"))
        if err:
            _api("sendMessage", {"chat_id": cid, "text": f"⚠️ {_name(sysk).title()} virtual could not start: {_esc(err)}",
                                 "parse_mode": "HTML"})
    elif etype == "vsys_stop":
        SV.stop(cid, sysk)
    elif etype == "vsys_resume":
        SV.resume(cid, sysk)
    elif etype == "vsys_reset":
        do_reset(cid, cid, sysk)
    elif etype == "vsys_pdf":
        msg = do_pdf(cid, cid, sysk, meta.get("month"))
        if msg and "sent" not in msg:
            _api("sendMessage", {"chat_id": cid, "text": f"⚠️ {_esc(msg)}", "parse_mode": "HTML"})
