#!/usr/bin/env python3
"""virtual_dm.py - the /virtual screens in the bot DM (the Mini App has the
same thing with a nicer face). Same engine underneath: virtual.py.

bot.py forwards:
    /virtual                      -> cmd_virtual(chat_id, cid)
    callback data "vt:..."        -> on_callback(...)  (returns popup text or None)
    typed text while a form asks  -> wants_text(cid) / on_text(cid, text)
    poller events from the Mini App -> apply_event(cid, etype, meta)
"""

import html as _html
import time

import requests

import copytrade as ct
import virtual as V

_TOKEN = None
_forms: dict = {}          # cid -> {"field": ..., "msg_id": ..., "draft": {...}}
_confirm_reset: dict = {}  # cid -> ts


def init(token):
    global _TOKEN
    _TOKEN = token


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
        print(f"  [VIRTUAL DM] {method} failed: {e}")
        return {}


def send_doc(cid, data: bytes, filename: str, caption: str = ""):
    return _api("sendDocument", {"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
                files={"document": (filename, data, "application/pdf")})


def _esc(s):
    return _html.escape(str(s or ""))


def _dyn(s):
    """Invisible markers around a dynamic value (see games._dyn / i18n)."""
    return f"⁣{s}⁣"


def _money(x):
    x = float(x or 0)
    return f"{'-' if x < 0 else ''}${abs(x):,.2f}"


def _draft(cid):
    """The setup form - starts from whatever the user last used."""
    if cid in _forms and _forms[cid].get("draft"):
        return _forms[cid]["draft"]
    user = ct._get(cid) or {}
    v = V.get(user) if user else V.default()
    d = {"capital": v.get("capital") or 20, "lev_mode": v.get("lev_mode", "auto"), "lev": v.get("lev", 10),
         "risk_mode": v.get("risk_mode", "usd"), "risk": v.get("risk", 0.5),
         "margin_mode": v.get("margin_mode", "usd"), "margin": v.get("margin", 2)}
    _forms[cid] = {"field": None, "msg_id": None, "draft": d}
    return d


def _fmt_risk(d):
    return f"${float(d['risk']):g}" if d["risk_mode"] == "usd" else f"{float(d['risk']):g}% of margin"


def _fmt_margin(d):
    return f"${float(d['margin']):g}" if d["margin_mode"] == "usd" else f"{float(d['margin']):g}% of balance"


# ── screens ────────────────────────────────────────────────────────────────

def _setup_text(cid, err=None):
    d = _draft(cid)
    v = V.default()
    v.update({"balance": float(d["capital"]), "capital": float(d["capital"]), "lev_mode": d["lev_mode"], "lev": float(d["lev"]),
              "risk_mode": d["risk_mode"], "risk": float(d["risk"]), "margin_mode": d["margin_mode"], "margin": float(d["margin"])})
    out = ["📊 <b>Virtual Trading - setup</b>", ""]
    out.append(f"💰 Total capital: <b>{_money(d['capital'])}</b>")
    if d["lev_mode"] == "auto":
        out.append("⚖️ Leverage: <b>Auto</b> - set per trade so a stop-loss costs your risk")
        out.append(f"🎯 Risk per trade at SL: <b>{_fmt_risk(d)}</b>")
    else:
        out.append(f"⚖️ Leverage: <b>Manual {float(d['lev']):g}x</b>")
    out.append(f"💵 Margin per trade: <b>{_fmt_margin(d)}</b>")
    pv = V.preview(v, 100, 98.8, 101.5, 103)
    if "skip" not in pv:
        out.append("")
        out.append(f"Example signal (1.2% stop): <b>{pv['lev']:g}x</b> · position {_money(pv['position'])} · "
                   f"loss at SL {_money(pv['sl'])} · TP1 {'+' if pv['tp1'] >= 0 else ''}{_money(pv['tp1'])} · TP2 +{_money(pv['tp2'])}")
    if err:
        out += ["", f"⚠️ {_esc(err)}"]
    out += ["", "Practice account - no real money. Signals of your tier are copied automatically once you press Start."]
    return "\n".join(out)


def _setup_kb(cid):
    d = _draft(cid)
    rows = [[{"text": "💰 Capital", "callback_data": "vt:f:capital"},
             {"text": f"⚖️ {'Auto' if d['lev_mode'] == 'auto' else 'Manual'} leverage", "callback_data": "vt:lev"}]]
    if d["lev_mode"] == "auto":
        rows.append([{"text": "🎯 Risk per trade", "callback_data": "vt:f:risk"},
                     {"text": "💵 Margin per trade", "callback_data": "vt:f:margin"}])
    else:
        rows.append([{"text": "🔢 Leverage value", "callback_data": "vt:f:lev"},
                     {"text": "💵 Margin per trade", "callback_data": "vt:f:margin"}])
    rows.append([{"text": "▶ START", "callback_data": "vt:start", "style": "success"}])
    rows.append([{"text": "📄 Past reports", "callback_data": "vt:page:"}])
    return {"inline_keyboard": rows}


def _status_text(cid, month=None):
    st = V.state(cid, month)
    s = st["status"]
    head = {"running": "🟢 RUNNING", "stopped": "⏸ STOPPED", "wiped": "💀 CAPITAL EXHAUSTED"}.get(s, s.upper())
    out = [f"📊 <b>Virtual Trading</b> · {head}"]
    if st.get("started_at"):
        out.append(f"since {_dyn(st['started_at'])} · run #{st['run']}")
    out.append("")
    sign = "+" if st["pnl_pct"] >= 0 else ""
    out.append(f"Capital {_money(st['capital'])} → Balance <b>{_money(st['balance'])}</b> ({sign}{st['pnl_pct']}%)")
    if st["lev_mode"] == "auto":
        out.append(f"Auto leverage · risk {_fmt_risk(st)} · margin {_fmt_margin(st)}")
    else:
        out.append(f"Manual {float(st['lev']):g}x · margin {_fmt_margin(st)}")
    out.append(f"Closed {st['closed']} · skipped {st['skipped']} (no free margin)")
    if st["open"]:
        out.append("")
        out.append("<b>Open now</b>")
        for p in st["open"][:6]:
            sym = p["symbol"].replace("-USDT", "").replace("USDT", "")
            side = "LONG" if p["side"] == "BUY" else "SHORT"
            out.append(f"• {_dyn(sym)} {_dyn(side)} {p['lev']:g}x · entry {V.px(p['entry'])} · SL {V.px(p['sl'])} · TP1 {V.px(p['tp1'])}"
                       + (" · TP1 ✅" if p.get("tp1_hit") else "") + (f" · {p['note']}" if p.get("note") else ""))
    if s == "wiped":
        out += ["", "The balance can no longer fund a trade, so trading stopped. Reset to start a new run."]
    # month page
    stt = st["stats"]
    out += ["", f"📅 <b>{_dyn(st['month_label'])}</b> · page {st['page']}/{st['pages']}"]
    if stt["trades"]:
        out.append(f"Trades {stt['trades']} · wins {stt['wins']} · losses {stt['losses']} · win rate {stt['wr']}%")
        out.append(f"Net {'+' if stt['net'] >= 0 else ''}{_money(stt['net'])} · best +{_money(stt['best'])} · worst {_money(stt['worst'])} · max drawdown {_money(stt['dd'])}")
        for t in st["trades"][:8]:
            sym = t["sym"].replace("-USDT", "").replace("USDT", "")
            side = "LONG" if t["side"] == "BUY" else "SHORT"
            out.append(f"<code>{t['t'][5:16]}</code> {_dyn(sym)} {_dyn(side)} {t['lev']:g}x · {_dyn(t['res'])} · <b>{'+' if t['pnl'] >= 0 else ''}{_money(t['pnl'])}</b>")
        if stt["trades"] > 8:
            out.append(f"… {stt['trades'] - 8} more in the PDF")
    else:
        out.append("No closed trades this month yet.")
    out.append(f"PDF exports this month: {st['exports_used']}/{st['exports_limit']}")
    return "\n".join(out)[:4000]


def _status_kb(cid, month=None):
    st = V.state(cid, month)
    rows = []
    if st["status"] == "running":
        rows.append([{"text": "⏸ Stop", "callback_data": "vt:stop"}, {"text": "↺ Reset", "callback_data": "vt:reset", "style": "danger"}])
    elif st["status"] == "stopped":
        rows.append([{"text": "▶ Resume", "callback_data": "vt:resume", "style": "success"}, {"text": "↺ Reset", "callback_data": "vt:reset", "style": "danger"}])
    else:
        rows.append([{"text": "↺ Reset & set up again", "callback_data": "vt:reset", "style": "danger"}])
    nav = []
    if st["prev"]:
        nav.append({"text": "◀ Prev", "callback_data": f"vt:page:{st['prev']}"})
    nav.append({"text": f"📄 PDF {_dyn(st['month_label'][:3])}", "callback_data": f"vt:pdf:{st['month']}"})
    if st["next"]:
        nav.append({"text": "Next ▶", "callback_data": f"vt:page:{st['next']}"})
    rows.append(nav)
    rows.append([{"text": "🔄 Refresh", "callback_data": f"vt:page:{st['month']}"}])
    return {"inline_keyboard": rows}


def _show(cid, chat_id, msg_id=None, month=None, err=None):
    user = ct._get(cid) or {}
    v = V.get(user) if user else V.default()
    if v["status"] == "setup":
        text, kb = _setup_text(cid, err), _setup_kb(cid)
    else:
        text, kb = _status_text(cid, month), _status_kb(cid, month)
    if msg_id:
        j = _api("editMessageText", {"chat_id": chat_id, "message_id": msg_id, "text": text,
                                     "parse_mode": "HTML", "reply_markup": kb})
        if j.get("ok") or "not modified" in str(j.get("description", "")):
            return msg_id
    j = _api("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "reply_markup": kb})
    return (j.get("result") or {}).get("message_id")


def cmd_virtual(chat_id, cid):
    cid = str(cid)
    _forms.pop(cid, None)
    _show(cid, chat_id)


# ── form input ─────────────────────────────────────────────────────────────

_PROMPT = {
    "capital": "💰 Send your <b>total capital</b> in USDT (for example <code>20</code>).",
    "lev": "🔢 Send the <b>leverage</b> to use on every trade, 1-100 (for example <code>10</code>).",
    "risk": "🎯 How much may one trade lose at stop-loss? Send an amount like <code>0.5</code> (dollars) or a percent of the margin like <code>25%</code>.",
    "margin": "💵 Margin per trade - send dollars like <code>2</code> or a percent of your balance like <code>20%</code>.",
}


def wants_text(cid) -> bool:
    f = _forms.get(str(cid))
    return bool(f and f.get("field"))


def on_text(cid, chat_id, text) -> bool:
    cid = str(cid)
    f = _forms.get(cid)
    if not f or not f.get("field"):
        return False
    field = f["field"]
    d = f["draft"]
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
    _show(cid, chat_id, f.get("msg_id"), err=err)
    return True


# ── callbacks ──────────────────────────────────────────────────────────────

def on_callback(data, cid, chat_id, msg_id):
    cid = str(cid)
    parts = data.split(":")
    act = parts[1] if len(parts) > 1 else ""
    user = ct._get(cid) or {}
    tier = "vip" if user.get("tier") == "vip" else "free"

    if act == "f":
        field = parts[2]
        d = _draft(cid)
        _forms[cid].update({"field": field, "msg_id": msg_id})
        _api("editMessageText", {"chat_id": chat_id, "message_id": msg_id, "text": _PROMPT[field], "parse_mode": "HTML",
                                 "reply_markup": {"inline_keyboard": [[{"text": "↩ Back", "callback_data": "vt:back"}]]}})
        return None
    if act == "back":
        f = _forms.get(cid)
        if f:
            f["field"] = None
        _show(cid, chat_id, msg_id)
        return None
    if act == "lev":
        d = _draft(cid)
        d["lev_mode"] = "manual" if d["lev_mode"] == "auto" else "auto"
        _show(cid, chat_id, msg_id)
        return None
    if act == "start":
        d = _draft(cid)
        err = V.setup(cid, d["capital"], d["lev_mode"], d["lev"], d["risk_mode"], d["risk"], d["margin_mode"], d["margin"])
        if err:
            _show(cid, chat_id, msg_id, err=err)
            return err
        _forms.pop(cid, None)
        _show(cid, chat_id, msg_id)
        return "Virtual trading started."
    if act == "stop":
        err = V.stop(cid)
        _show(cid, chat_id, msg_id)
        return err or "Paused - open trades still close normally, no new ones open."
    if act == "resume":
        err = V.resume(cid)
        _show(cid, chat_id, msg_id)
        return err or "Running again."
    if act == "reset":
        if time.time() - _confirm_reset.get(cid, 0) > 60:
            _confirm_reset[cid] = time.time()
            _api("editMessageReplyMarkup", {"chat_id": chat_id, "message_id": msg_id, "reply_markup": {"inline_keyboard": [
                [{"text": "Yes, wipe it", "callback_data": "vt:reset", "style": "danger"}, {"text": "No", "callback_data": "vt:page:"}]]}})
            return "Reset wipes the balance and history. Your report is sent to you first. Tap Yes to confirm."
        _confirm_reset.pop(cid, None)
        do_reset(cid, chat_id)
        _forms.pop(cid, None)
        _show(cid, chat_id, msg_id)
        return "Run wiped."
    if act == "page":
        month = parts[2] if len(parts) > 2 and parts[2] else None
        _show(cid, chat_id, msg_id, month=month)
        return None
    if act == "pdf":
        month = parts[2] if len(parts) > 2 else None
        return do_pdf(cid, chat_id, month, tier)
    return None


def do_reset(cid, chat_id):
    pdf, fname = V.reset(cid)
    if pdf:
        send_doc(chat_id, pdf, fname, "📄 Your virtual trading run, before the reset. Everything below is now wiped.")


def do_pdf(cid, chat_id, month, tier):
    err = V.export_allowed(cid, tier)
    if err:
        return err
    data, fname = V.pdf_for(cid, month)
    if not data:
        return fname
    send_doc(chat_id, data, fname, f"📄 Virtual trading report · {V.month_label(month) if month else 'whole run'}")
    return "Report sent below."


# ── Mini App events (queued through api.py, applied by bot.py's poller) ────

def apply_event(cid, etype, meta):
    cid = str(cid)
    meta = meta or {}
    if etype == "virtual_setup":
        err = V.setup(cid, meta.get("capital"), meta.get("lev_mode", "auto"), meta.get("lev") or 10,
                      meta.get("risk_mode", "usd"), meta.get("risk") or 0, meta.get("margin_mode", "usd"), meta.get("margin"))
        if err:
            _api("sendMessage", {"chat_id": cid, "text": f"⚠️ Virtual trading could not start: {_esc(err)}", "parse_mode": "HTML"})
    elif etype == "virtual_stop":
        V.stop(cid)
    elif etype == "virtual_resume":
        V.resume(cid)
    elif etype == "virtual_reset":
        do_reset(cid, cid)
    elif etype == "virtual_pdf":
        user = ct._get(cid) or {}
        tier = "vip" if user.get("tier") == "vip" else "free"
        msg = do_pdf(cid, cid, meta.get("month"), tier)
        if msg and "sent" not in msg:
            _api("sendMessage", {"chat_id": cid, "text": f"⚠️ {_esc(msg)}", "parse_mode": "HTML"})
