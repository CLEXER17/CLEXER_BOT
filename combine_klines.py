#!/usr/bin/env python3
"""Stitch the per-interval kline CSVs into one file.

    python combine_klines.py data/klines_binance BTCUSDT
    python combine_klines.py data/klines BTC-USDT --out data/BTC_all_bingx.csv

Output columns: interval,time_utc,time_ms,open,high,low,close,volume - sorted
by interval (largest first: 4h, 1h, 30m, 15m, 5m, 1m) then time. Filter on the
interval column to get any one timeframe back out.
"""

import csv
import os
import sys

ORDER = ["4h", "1h", "30m", "15m", "5m", "1m"]


def main():
    if len(sys.argv) < 3:
        sys.exit("usage: combine_klines.py <folder> <symbol> [--out file.csv]")
    folder, symbol = sys.argv[1], sys.argv[2]
    out = sys.argv[sys.argv.index("--out") + 1] if "--out" in sys.argv \
        else os.path.join(folder, f"{symbol}_ALL.csv")
    total = 0
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["interval", "time_utc", "time_ms", "open", "high", "low", "close", "volume"])
        for iv in ORDER:
            p = os.path.join(folder, f"{symbol}_{iv}.csv")
            if not os.path.exists(p):
                print(f"  {iv:<4} missing - skipped")
                continue
            n = 0
            with open(p, newline="") as src:
                for r in csv.DictReader(src):
                    w.writerow([iv, r["time_utc"], r["time_ms"], r["open"], r["high"],
                                r["low"], r["close"], r["volume"]])
                    n += 1
            total += n
            print(f"  {iv:<4} {n:>10,} candles")
    print(f"\n{total:,} rows -> {out}  ({os.path.getsize(out) / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
