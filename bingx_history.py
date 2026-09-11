#!/usr/bin/env python3
"""Pull BingX perpetual klines as far back as the exchange holds them, to CSV.

    python bingx_history.py                       # BTC-USDT, all six intervals
    python bingx_history.py --symbol SOL-USDT
    python bingx_history.py --intervals 1m,5m
    python bingx_history.py --out data/klines

One file per interval: <out>/<symbol>_<interval>.csv with columns
    time_utc,time_ms,open,high,low,close,volume
oldest first, no duplicates.

How BingX serves history (measured 11 Sep 2026, BTC-USDT):
  - at most 1000 candles per call (it reports 999 on the newest page; the
    documented cap of 1440 is only the parameter validator)
  - startTime is ignored for anything old; endTime pages backwards
  - depth differs wildly by interval: 4h reached Aug 2022, 1h Apr 2024,
    30m Dec 2024, while 1m ran past 400 pages (Dec 2025) without ending.
    15m and 5m returned a SHORT page after 100 / 45 days - that looked like
    the end, but a short page can also be a gap in the exchange's data, so
    this walker keeps going and only stops when a page is genuinely empty
    or adds nothing new three times in a row.
Re-running is safe: an existing file is read first and only the missing
older pages are fetched.
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

import requests

URL = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"
PAGE = 1000
BINANCE_PAGE = 1500
PAUSE = 0.12                # seconds between calls; 400 in a row at this pace was fine
STALL_LIMIT = 3             # consecutive pages that add nothing before giving up


def _page(symbol, interval, end_ms):
    for _try in range(4):
        try:
            r = requests.get(URL, params={"symbol": symbol, "interval": interval,
                                          "limit": PAGE, "endTime": end_ms}, timeout=20)
            j = r.json()
            if j.get("code") not in (0, None):
                raise RuntimeError(j.get("msg"))
            return j.get("data") or []
        except Exception as e:
            wait = 2 * (_try + 1)
            print(f"    retry in {wait}s: {e}", file=sys.stderr)
            time.sleep(wait)
    return []


def _binance_page(symbol, interval, start_ms):
    """Binance USDT-M perpetual. Pages FORWARD from startTime, 1500 a call,
    and holds every interval back to Sep 2019 - the exchange the admin's
    trade plan was researched on. BingX keeps 5m only 45 days and 15m 100."""
    for _try in range(4):
        try:
            r = requests.get(BINANCE_URL, params={"symbol": symbol, "interval": interval,
                                                  "startTime": start_ms, "limit": BINANCE_PAGE},
                             timeout=20)
            j = r.json()
            if isinstance(j, dict):
                raise RuntimeError(j.get("msg"))
            return j
        except Exception as e:
            wait = 2 * (_try + 1)
            print(f"    retry in {wait}s: {e}", file=sys.stderr)
            time.sleep(wait)
    return []


def pull_binance(symbol, interval, out_dir, since_ms):
    path = os.path.join(out_dir, f"{symbol}_{interval}.csv")
    rows = _load_existing(path)
    have = len(rows)
    start = max(rows) + 1 if rows else since_ms
    calls = 0
    print(f"{symbol} {interval} (binance): {'resuming after ' + _ts(start) if rows else 'from ' + _ts(start)}")
    while True:
        data = _binance_page(symbol, interval, start)
        calls += 1
        if not data:
            break
        newest = None
        for r in data:
            t, o, h, l, c, v = _norm(r)
            rows[t] = (o, h, l, c, v)
            newest = t if newest is None else max(newest, t)
        if len(data) < BINANCE_PAGE:
            break
        start = newest + 1
        if calls % 50 == 0:
            print(f"    {calls} calls · {len(rows):,} candles · up to {_ts(newest)}")
        time.sleep(0.08)
    times = sorted(rows)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["time_utc", "time_ms", "open", "high", "low", "close", "volume"])
        for t in times:
            w.writerow([_ts(t), t, *rows[t]])
    days = (times[-1] - times[0]) / 86400000 if times else 0
    print(f"  -> {len(rows):,} candles (+{len(rows) - have:,} new) · {_ts(times[0])} to "
          f"{_ts(times[-1])} · {days:.0f} days · {calls} calls · {path}")
    return path


def _norm(row):
    if isinstance(row, (list, tuple)):
        t, o, h, l, c, v = row[:6]
    else:
        t = row.get("time") or row.get("openTime")
        o, h, l, c, v = (row.get(k) for k in ("open", "high", "low", "close", "volume"))
    return int(float(t)), float(o), float(h), float(l), float(c), float(v or 0)


def _load_existing(path):
    if not os.path.exists(path):
        return {}
    out = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            out[int(r["time_ms"])] = (float(r["open"]), float(r["high"]), float(r["low"]),
                                       float(r["close"]), float(r["volume"]))
    return out


def pull(symbol, interval, out_dir):
    path = os.path.join(out_dir, f"{symbol}_{interval}.csv")
    rows = _load_existing(path)
    have = len(rows)
    end = min(rows) - 1 if rows else int(time.time() * 1000)
    stall = calls = 0
    print(f"{symbol} {interval}: {'resuming below ' + _ts(end) if rows else 'from now'}")
    while True:
        data = _page(symbol, interval, end)
        calls += 1
        if not data:
            break
        added = 0
        oldest = None
        for r in data:
            t, o, h, l, c, v = _norm(r)
            oldest = t if oldest is None else min(oldest, t)
            if t not in rows:
                rows[t] = (o, h, l, c, v)
                added += 1
        stall = 0 if added else stall + 1
        if stall >= STALL_LIMIT:
            break
        end = oldest - 1
        if calls % 25 == 0:
            print(f"    {calls} calls · {len(rows):,} candles · back to {_ts(oldest)}")
        time.sleep(PAUSE)
    times = sorted(rows)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["time_utc", "time_ms", "open", "high", "low", "close", "volume"])
        for t in times:
            w.writerow([_ts(t), t, *rows[t]])
    days = (times[-1] - times[0]) / 86400000 if times else 0
    print(f"  -> {len(rows):,} candles (+{len(rows) - have:,} new) · {_ts(times[0])} to "
          f"{_ts(times[-1])} · {days:.0f} days · {calls} calls · {path}")
    return path


def _ts(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTC-USDT")
    ap.add_argument("--intervals", default="4h,1h,30m,15m,5m,1m")
    ap.add_argument("--out", default="data/klines")
    ap.add_argument("--source", default="bingx", choices=["bingx", "binance"])
    ap.add_argument("--since", default="2022-08-03",
                    help="binance only: earliest date to pull, YYYY-MM-DD")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    for iv in [x.strip() for x in a.intervals.split(",") if x.strip()]:
        if a.source == "binance":
            _since = int(datetime.strptime(a.since, "%Y-%m-%d")
                         .replace(tzinfo=timezone.utc).timestamp() * 1000)
            pull_binance(a.symbol.replace("-", ""), iv, a.out, _since)
        else:
            pull(a.symbol, iv, a.out)
