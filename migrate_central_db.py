#!/usr/bin/env python3
"""Copy the CLEXER central store from one Postgres to another.

Used when the bot moves back to a different Railway project and the
central-api (api.py) + Postgres has to come with it. api.py's own schema is
created by init_db() on ITS startup, so deploy api.py on the destination
FIRST and let it boot once - this script only moves rows, it never creates
tables.

    python migrate_central_db.py                 # dry run: counts only
    python migrate_central_db.py --go            # actually copy
    python migrate_central_db.py --go --only kv_store,bot_state

Both URLs come from the environment so no connection string is ever typed
into a shell history or a chat window:

    SRC_DATABASE_URL   the Postgres being left behind
    DST_DATABASE_URL   the Postgres being moved to

Nothing is deleted at either end. Every row is an upsert on the destination's
primary key, so the script is safe to run twice, and safe to run again after
fixing a partial failure.
"""

import os
import sys

import psycopg2
import psycopg2.extras

# FK order matters: users owns three child tables by tg_id, so it has to land
# before them or every child row is rejected. The rest are independent.
TABLES = [
    ("users",              ["tg_id"]),
    ("bingx_credentials",  ["tg_id"]),
    ("copy_settings",      ["tg_id"]),
    ("copy_trades",        ["id"]),
    ("bot_state",          ["id"]),
    ("kv_store",           ["key"]),
    ("payment_events",     ["id"]),
]

# Blobs worth naming in the report - if one of these is missing on the far
# side the migration silently lost something the admin would notice days
# later (a wiped weekday grid, an empty user list, everyone's copy settings).
KEY_BLOBS = [
    "ct_users", "registered_users", "bot_settings", "slot_auto_state",
    "trade_history_csv", "sig_snapshots", "free_signal_tracker",
    "daily_buckets", "scheduled_broadcasts", "test_system", "autoclear",
    "notify", "secretary", "time_panel", "server_registry", "active_server",
    "free_sl_log", "vip_sl_log", "ct_last_signal", "api_cost_log_csv",
    "bench_state", "public_status", "userbot_owner",
]


def _cols(cur, table):
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    return [r[0] for r in cur.fetchall()]


def _count(cur, table):
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        return cur.fetchone()[0]
    except Exception:
        return None          # table does not exist on this side


def main():
    go   = "--go" in sys.argv
    only = None
    if "--only" in sys.argv:
        only = {t.strip() for t in sys.argv[sys.argv.index("--only") + 1].split(",")}

    src_url = os.getenv("SRC_DATABASE_URL")
    dst_url = os.getenv("DST_DATABASE_URL")
    if not src_url or not dst_url:
        sys.exit("Set SRC_DATABASE_URL and DST_DATABASE_URL first (see the docstring).")
    if src_url == dst_url:
        sys.exit("SRC and DST are the same database - refusing.")

    src = psycopg2.connect(src_url)
    dst = psycopg2.connect(dst_url)
    sc, dc = src.cursor(), dst.cursor()

    print(f"{'table':<20} {'source':>8} {'dest before':>12} {'copied':>8}")
    print("-" * 52)

    total = 0
    for table, pk in TABLES:
        if only and table not in only:
            continue
        n_src = _count(sc, table)
        n_dst = _count(dc, table)
        if n_src is None:
            print(f"{table:<20} {'MISSING':>8}  (not in source - skipped)")
            continue
        if n_dst is None:
            print(f"{table:<20} {n_src:>8}  DESTINATION HAS NO SUCH TABLE - "
                  f"deploy api.py there and let it boot once, then re-run.")
            continue

        copied = 0
        if go and n_src:
            # Only columns BOTH sides have, so a schema that drifted between
            # the two deployments migrates what it can instead of failing.
            cols = [c for c in _cols(sc, table) if c in set(_cols(dc, table))]
            collist = ", ".join(f'"{c}"' for c in cols)
            updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in pk)
            conflict = (f'ON CONFLICT ({", ".join(chr(34) + c + chr(34) for c in pk)}) '
                        + (f"DO UPDATE SET {updates}" if updates else "DO NOTHING"))
            sc.execute(f'SELECT {collist} FROM "{table}"')
            while True:
                rows = sc.fetchmany(500)
                if not rows:
                    break
                psycopg2.extras.execute_values(
                    dc, f'INSERT INTO "{table}" ({collist}) VALUES %s {conflict}', rows)
                copied += len(rows)
            dst.commit()
            # SERIAL columns do not follow their rows. Without this the next
            # insert on the destination reuses id 1 and collides.
            if "id" in pk:
                try:
                    dc.execute(f"""SELECT setval(pg_get_serial_sequence('{table}', 'id'),
                                   COALESCE((SELECT MAX(id) FROM "{table}"), 1))""")
                    dst.commit()
                except Exception as e:
                    print(f"    (sequence reset skipped for {table}: {e})")
        total += copied
        print(f"{table:<20} {n_src:>8} {n_dst:>12} {copied:>8}")

    # The part that actually matters: the kv blobs the bot reloads on boot.
    print("\nkv_store blobs")
    print("-" * 52)
    missing = []
    for key in KEY_BLOBS:
        sc.execute("SELECT updated_at FROM kv_store WHERE key = %s", (key,))
        s_row = sc.fetchone()
        dc.execute("SELECT updated_at FROM kv_store WHERE key = %s", (key,))
        d_row = dc.fetchone()
        if not s_row:
            continue                      # never existed here either; not a loss
        mark = "ok" if d_row else "MISSING ON DEST"
        if not d_row:
            missing.append(key)
        print(f"  {key:<24} src {str(s_row[0])[:19]}   {mark}")

    sc.execute("SELECT COUNT(*) FROM kv_store")
    print(f"\n{sc.fetchone()[0]} keys in source kv_store, "
          f"{_count(dc, 'kv_store')} on destination.")
    if missing:
        print(f"STILL MISSING: {', '.join(missing)}")
    if not go:
        print("\nDRY RUN - nothing was written. Re-run with --go to copy.")
    else:
        print(f"\nCopied {total} rows.")

    src.close()
    dst.close()


if __name__ == "__main__":
    main()
