"""
jev_gate.py - Jev (TypeSafe AI) decision gate for CLEXER BOT.

ONLY Railway env var needed:   TYPESAFE_API_KEY
Everything else is controlled from Telegram with /jev (admin) and saved via save_settings().

  /jev                 status + counters
  /jev off|shadow|on   off = nothing, shadow = log only, on = reorder coins + veto trades
  /jev coin 0.35       min P(clean setup) to keep a coin
  /jev trade 0.55      min P(trade agrees with context) to allow a trade
  /jev timeout 2.5     seconds per Jev call
  /jev test            one live ping, shows latency + answer

Fail-open: any error / timeout / missing key => bot behaves exactly as before.
"""

import os, time, requests, html as _html
from concurrent.futures import ThreadPoolExecutor

TYPESAFE_API_KEY = os.getenv("TYPESAFE_API_KEY", "")
JEV_URL   = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-latest"

_cfg = {"mode": "off", "min_coin": 0.35, "min_trade": 0.55, "timeout": 2.5}
_stat = {"calls": 0, "fails": 0, "coin_drops": 0, "vetoes": 0, "last": "-"}
_fail_streak = 0
_pause_until = 0.0


# ---- config persistence hooks (used by save_settings / load) ----------------
def jev_get_cfg() -> dict:
    return dict(_cfg)


def jev_set_cfg(d):
    if not isinstance(d, dict):
        return
    if d.get("mode") in ("off", "shadow", "on"):
        _cfg["mode"] = d["mode"]
    for k, lo, hi in (("min_coin", 0.0, 1.0), ("min_trade", 0.0, 1.0), ("timeout", 0.5, 10.0)):
        try:
            v = float(d.get(k, _cfg[k]))
            if lo <= v <= hi:
                _cfg[k] = v
        except (TypeError, ValueError):
            pass


# ---- core call --------------------------------------------------------------
def _enabled() -> bool:
    return _cfg["mode"] in ("shadow", "on") and bool(TYPESAFE_API_KEY) and time.time() >= _pause_until


def _call(state: dict, questions: dict, force: bool = False):
    """Returns the 'answers' dict or None on any failure (fail-open)."""
    global _fail_streak, _pause_until
    if not (force and TYPESAFE_API_KEY) and not _enabled():
        return None
    _stat["calls"] += 1
    try:
        r = requests.post(
            JEV_URL,
            headers={"Authorization": f"Bearer {TYPESAFE_API_KEY}", "Content-Type": "application/json"},
            json={"model": JEV_MODEL, "state": state, "questions": questions},
            timeout=_cfg["timeout"],
        )
        r.raise_for_status()
        ans = r.json().get("answers")
        _fail_streak = 0
        return ans
    except Exception as e:
        _stat["fails"] += 1
        _fail_streak += 1
        _stat["last"] = f"error: {str(e)[:80]}"
        print(f"  [JEV] call failed ({_fail_streak}): {e}")
        if _fail_streak >= 5:
            _pause_until = time.time() + 300
            _fail_streak = 0
            print("  [JEV] paused 5 min after repeated failures")
        return None


_COIN_Q = {
    "clean_setup": {
        "type": "noul",
        "instructions": ("Is this coin currently a clean, tradeable futures setup for a short-term "
                         "scalp (healthy volume, an orderly move, not an exhausted or erratic spike)?"),
    },
}

_TRADE_Q = {
    "agrees": {
        "type": "noul",
        "instructions": ("Does the proposed trade direction agree with the market context "
                         "(4H structure, 24h move, momentum) and carry a sane stop distance?"),
    },
    "risk": {
        "type": "score",
        "instructions": "How risky is entering this trade right now?",
        "criteria": [
            "Low risk: direction matches structure and momentum, stop is reasonable",
            "Medium risk: mixed signals or counter-trend with some support",
            "High risk: fights structure, chasing an exhausted move, or stop is too tight/wide",
        ],
    },
}


# ---- gate 1: coin selection -------------------------------------------------
def jev_rank_coins(candidates: list, kind: str = "") -> list:
    """Reorders best-first and drops weak coins (mode=on). Otherwise returns the list untouched."""
    if not candidates or not _enabled():
        return candidates

    def one(m):
        state = {"coin": m["base"], "scan": kind, "change_24h_pct": round(m["chg"], 2),
                 "volume_usd_m": m["vol_m"], "momentum_score": round(m["score"], 1)}
        ans = _call(state, _COIN_Q)
        try:
            return float(ans["clean_setup"]["noul"]) if ans else None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=6) as ex:
        probs = list(ex.map(one, candidates))
    if all(p is None for p in probs):
        return candidates
    scored = [(m, p if p is not None else 0.5) for m, p in zip(candidates, probs)]
    line = ", ".join(f"{m['base']}={p:.2f}" for m, p in scored)
    _stat["last"] = f"coins[{kind}] {line}"[:200]
    print(f"  [JEV] coins[{kind}]: {line}")
    if _cfg["mode"] != "on":
        return candidates
    kept = [(m, p) for m, p in scored if p >= _cfg["min_coin"]]
    _stat["coin_drops"] += len(scored) - len(kept)
    kept.sort(key=lambda x: -x[1])
    return [m for m, _ in kept]


# ---- gate 2: trade veto -----------------------------------------------------
def jev_veto_trade(side: str, m: dict, entry: float, sl_pct: float, struct_4h: str = "NEUTRAL"):
    """Returns (allowed, note). Always (True, "") when off / shadow / failure."""
    if not _enabled():
        return True, ""
    state = {
        "coin": m["base"],
        "proposed_direction": "LONG" if side == "BUY" else "SHORT",
        "change_24h_pct": round(m["chg"], 2),
        "volume_usd_m": m["vol_m"],
        "structure_4h": struct_4h,
        "stop_distance_pct": round(sl_pct, 2),
        "counter_trend_vs_24h": (side == "BUY" and m["chg"] < 0) or (side == "SELL" and m["chg"] > 0),
    }
    ans = _call(state, _TRADE_Q)
    if not ans:
        return True, ""
    try:
        p = float(ans["agrees"]["noul"])
        risk = float(ans["risk"]["score"])
    except Exception:
        return True, ""
    note = f"agree={p:.2f} risk={risk:.1f}/2"
    _stat["last"] = f"{m['base']} {side} {note}"
    print(f"  [JEV] {m['base']} {side}: {note}")
    if _cfg["mode"] != "on":
        return True, note
    if p < _cfg["min_trade"]:
        _stat["vetoes"] += 1
        return False, f"Jev veto ({note})"
    return True, note


# ---- /jev command -----------------------------------------------------------
def jev_command(args: list):
    """args = words after '/jev'. Returns (html_text, changed_bool). Call save_settings() if changed."""
    changed = False
    msg = ""
    a = [x.lower() for x in args]
    if a and a[0] in ("off", "shadow", "on"):
        _cfg["mode"] = a[0]; changed = True
        msg = f"Mode set to <b>{a[0].upper()}</b>.\n\n"
    elif len(a) >= 2 and a[0] in ("coin", "trade", "timeout"):
        key = {"coin": "min_coin", "trade": "min_trade", "timeout": "timeout"}[a[0]]
        lo, hi = (0.5, 10.0) if a[0] == "timeout" else (0.0, 1.0)
        try:
            v = float(a[1])
            if lo <= v <= hi:
                _cfg[key] = v; changed = True
                msg = f"<b>{a[0]}</b> set to <b>{v}</b>.\n\n"
            else:
                msg = f"Value must be between {lo} and {hi}.\n\n"
        except ValueError:
            msg = "Value must be a number.\n\n"
    elif a and a[0] == "test":
        if not TYPESAFE_API_KEY:
            return "TYPESAFE_API_KEY is not set on Railway.", False
        t0 = time.time()
        ans = _call({"coin": "BTC", "change_24h_pct": 1.2, "volume_usd_m": 900, "momentum_score": 40.0},
                    _COIN_Q, force=True)
        ms = int((time.time() - t0) * 1000)
        if ans:
            try:
                return f"✅ Jev reachable — {ms} ms — clean_setup = {float(ans['clean_setup']['noul']):.2f}", False
            except Exception:
                return f"⚠️ Jev replied in {ms} ms but the answer shape was unexpected: {_html.escape(str(ans)[:200])}", False
        return f"❌ Jev call failed ({ms} ms): {_html.escape(_stat['last'])}", False

    paused = max(0, int(_pause_until - time.time()))
    txt = (
        msg +
        "<b>🧠 Jev Gate</b>\n\n"
        f"API key: <b>{'set' if TYPESAFE_API_KEY else 'MISSING (Railway env TYPESAFE_API_KEY)'}</b>\n"
        f"Mode: <b>{_cfg['mode'].upper()}</b>"
        + (f"  (paused {paused}s after failures)" if paused else "") + "\n"
        f"Min coin prob: <b>{_cfg['min_coin']}</b>\n"
        f"Min trade prob: <b>{_cfg['min_trade']}</b>\n"
        f"Timeout: <b>{_cfg['timeout']}s</b>\n\n"
        f"Calls: {_stat['calls']} | Fails: {_stat['fails']} | Coins dropped: {_stat['coin_drops']} | Vetoes: {_stat['vetoes']}\n"
        f"Last: <code>{_html.escape(_stat['last'])}</code>\n\n"
        "<b>Commands</b>\n"
        "/jev off | shadow | on\n"
        "/jev coin 0.35\n"
        "/jev trade 0.55\n"
        "/jev timeout 2.5\n"
        "/jev test\n\n"
        "<i>off = nothing · shadow = log only, trades unchanged · on = reorders coins + vetoes trades</i>"
    )
    return txt, changed
