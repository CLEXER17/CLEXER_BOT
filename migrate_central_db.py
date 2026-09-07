#!/usr/bin/env python3
"""Copy the CLEXER central store from one Postgres to another.

Used when the bot moves back to a different Railway project and the
central-api (api.py) + Postgres has to come with it. api.py's own schema is
created by init_db() on ITS startup, so deploy api.py on the destination
FIRST and let it boot once - this script only moves rows, it never creates
tables.

    python migrate_central_db.py                 # dry run: counts only
    python migrate_central_db.py --go            # actually copy
    python migrate_central_db.py --go --wipe     # empty the destination first
    python migrate_central_db.py --go --only kv_store,bot_state

--wipe empties the destination tables before copying. Use it when the
destination is a REUSED database rather than a fresh one: copy_trades and
payment_events have SERIAL primary keys that both databases numbered from 1
independently, so an upsert leaves the destination's surplus old rows behind,
silently mixed into live data. It only ever touches the destination.

Both URLs come from the environment so no connection string is ever typed
into a shell history or a chat window:

    SRC_DATABASE_URL   the Postgres being left behind
    DST_DATABASE_URL   the Postgres being moved to

Nothing is deleted at either end. Every row is an upsert on the destination's
primary key, so the script is safe to run twice, and safe to run again after
fixing a partial failure.
"""

import base64
import datetime
import decimal
import json
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


def _json_cols(cur, table):
    """Names of this table's json/jsonb columns.

    psycopg2 decodes a jsonb column into a plain dict on the way out but
    cannot adapt one on the way back in ("can't adapt type 'dict'"), so every
    such value has to be re-wrapped in Json() before the insert. The types are
    looked up rather than guessed from the value: a genuine Postgres array
    column also arrives as a Python list, and wrapping that would quietly
    turn it into json."""
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
          AND data_type IN ('json', 'jsonb')
    """, (table,))
    return {r[0] for r in cur.fetchall()}


def _count(cur, table):
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        return cur.fetchone()[0]
    except Exception:
        return None          # table does not exist on this side


def _arg(flag):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else None


# ── dump / load ──────────────────────────────────────────────────────────────
# Same rows, via a file on disk instead of a live connection. Worth having
# separately from the direct copy: the file is a real backup, so the data
# survives the source project being deleted or running out of credit, and the
# load half can be re-run as often as needed without the source existing.
#
# json/jsonb values already are dicts and go through untouched. Everything
# else Postgres hands back that JSON has no notion of - timestamps, NUMERIC,
# BYTEA - is tagged on the way out and rebuilt on the way in, per COLUMN
# rather than per value, so a jsonb blob that happens to contain a key like
# "__ts" is never mistaken for a tag.

def _encode(v):
    if isinstance(v, (bytes, memoryview)):
        return {"__b64": base64.b64encode(bytes(v)).decode()}
    if isinstance(v, decimal.Decimal):
        return {"__dec": str(v)}
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return {"__ts": v.isoformat()}
    return v


def _decode(v):
    if isinstance(v, dict):
        if "__b64" in v:
            return psycopg2.Binary(base64.b64decode(v["__b64"]))
        if "__dec" in v:
            return decimal.Decimal(v["__dec"])
        if "__ts" in v:
            return v["__ts"]          # Postgres parses the ISO string itself
    return v


def do_dump(src_url, path, only):
    src = psycopg2.connect(src_url)
    sc = src.cursor()
    out = {"created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "source_host": src_url.split("@")[-1], "tables": {}}
    print(f"{'table':<20} {'rows':>8}")
    print("-" * 30)
    for table, _pk in TABLES:
        if only and table not in only:
            continue
        if _count(sc, table) is None:
            print(f"{table:<20} {'MISSING':>8}")
            continue
        cols = _cols(sc, table)
        jcols = _json_cols(sc, table)
        sc.execute(f'SELECT {", ".join(chr(34) + c + chr(34) for c in cols)} FROM "{table}"')
        rows = [[v if cols[i] in jcols else _encode(v) for i, v in enumerate(r)]
                for r in sc.fetchall()]
        out["tables"][table] = {"cols": cols, "json_cols": sorted(jcols), "rows": rows}
        print(f"{table:<20} {len(rows):>8}")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f)
    src.close()
    size = os.path.getsize(path)
    print(f"\nWrote {path}  ({size:,} bytes)")
    print("This file is a full copy of the source store. Keep it until the "
          "migration is verified.")


def do_load(dst_url, path, go, wipe, only):
    with open(path, encoding="utf-8") as f:
        blob = json.load(f)
    print(f"Dump taken {blob.get('created_at', '?')} from {blob.get('source_host', '?')}\n")
    dst = psycopg2.connect(dst_url)
    dc = dst.cursor()

    if wipe and go:
        targets = [t for t, _ in TABLES
                   if t in blob["tables"] and (not only or t in only)
                   and _count(dc, t) is not None]
        print(f"WIPING destination tables: {', '.join(targets)}")
        dc.execute("TRUNCATE TABLE " + ", ".join(f'"{t}"' for t in targets)
                   + " RESTART IDENTITY")
        dst.commit()
        print("Destination emptied.\n")

    print(f"{'table':<20} {'in file':>8} {'loaded':>8}")
    print("-" * 40)
    total = 0
    for table, pk in TABLES:
        if table not in blob["tables"] or (only and table not in only):
            continue
        spec = blob["tables"][table]
        n = len(spec["rows"])
        loaded = 0
        if go and n:
            if _count(dc, table) is None:
                print(f"{table:<20} {n:>8}  DESTINATION HAS NO SUCH TABLE - skipped")
                continue
            dcols = set(_cols(dc, table))
            keep = [i for i, c in enumerate(spec["cols"]) if c in dcols]
            cols = [spec["cols"][i] for i in keep]
            jset = set(spec["json_cols"])
            collist = ", ".join(f'"{c}"' for c in cols)
            updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in pk)
            conflict = (f'ON CONFLICT ({", ".join(chr(34) + c + chr(34) for c in pk)}) '
                        + (f"DO UPDATE SET {updates}" if updates else "DO NOTHING"))
            rows = [tuple(psycopg2.extras.Json(r[i]) if (spec["cols"][i] in jset
                                                         and r[i] is not None)
                          else _decode(r[i]) for i in keep)
                    for r in spec["rows"]]
            for i in range(0, len(rows), 500):
                psycopg2.extras.execute_values(
                    dc, f'INSERT INTO "{table}" ({collist}) VALUES %s {conflict}',
                    rows[i:i + 500])
            dst.commit()
            loaded = len(rows)
            if "id" in pk:
                try:
                    dc.execute(f"""SELECT setval(pg_get_serial_sequence('{table}', 'id'),
                                   COALESCE((SELECT MAX(id) FROM "{table}"), 1))""")
                    dst.commit()
                except Exception as e:
                    print(f"    (sequence reset skipped for {table}: {e})")
        total += loaded
        print(f"{table:<20} {n:>8} {loaded:>8}")
    dc.execute("SELECT COUNT(*) FROM kv_store")
    print(f"\n{dc.fetchone()[0]} keys now in destination kv_store.")
    print(f"Loaded {total} rows." if go else
          "\nDRY RUN - nothing was written. Add --go to load.")
    dst.close()


def main():
    go   = "--go" in sys.argv
    only = None
    if "--only" in sys.argv:
        only = {t.strip() for t in sys.argv[sys.argv.index("--only") + 1].split(",")}

    src_url = os.getenv("SRC_DATABASE_URL")
    dst_url = os.getenv("DST_DATABASE_URL")
    wipe = "--wipe" in sys.argv

    # --dump reads the source only, --load writes the destination only. Each
    # half works with just its own URL set, so the file can be taken now and
    # loaded later, from a machine that can no longer reach the source at all.
    dump_path, load_path = _arg("--dump"), _arg("--load")
    if dump_path:
        if not src_url:
            sys.exit("Set SRC_DATABASE_URL to dump from.")
        return do_dump(src_url, dump_path, only)
    if load_path:
        if not dst_url:
            sys.exit("Set DST_DATABASE_URL to load into.")
        return do_load(dst_url, load_path, go, wipe, only)

    if not src_url or not dst_url:
        sys.exit("Set SRC_DATABASE_URL and DST_DATABASE_URL first (see the docstring).")
    if src_url == dst_url:
        sys.exit("SRC and DST are the same database - refusing.")

    if wipe and not go:
        print("--wipe has no effect on a dry run; add --go to actually do it.\n")

    src = psycopg2.connect(src_url)
    dst = psycopg2.connect(dst_url)
    sc, dc = src.cursor(), dst.cursor()

    if wipe and go:
        # ONE statement listing every table. Emptying children first is not
        # enough - Postgres refuses TRUNCATE on a table another table
        # references unless that table is named in the same command, even
        # when the child is already empty. CASCADE would satisfy it too, but
        # is deliberately NOT used: it silently widens the blast radius to
        # tables this script never listed.
        targets = [t for t, _ in TABLES
                   if (not only or t in only) and _count(dc, t) is not None]
        print(f"WIPING destination tables: {', '.join(targets)}")
        dc.execute("TRUNCATE TABLE "
                   + ", ".join(f'"{t}"' for t in targets)
                   + " RESTART IDENTITY")
        dst.commit()
        print("Destination emptied.\n")

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
            jsonat = [i for i, c in enumerate(cols) if c in _json_cols(sc, table)]
            sc.execute(f'SELECT {collist} FROM "{table}"')
            while True:
                rows = sc.fetchmany(500)
                if not rows:
                    break
                if jsonat:
                    rows = [tuple(psycopg2.extras.Json(v)
                                  if (i in jsonat and v is not None) else v
                                  for i, v in enumerate(r)) for r in rows]
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
