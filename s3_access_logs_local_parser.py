#!/usr/bin/env python3
"""
S3 Access Logs -> Local CSV + DuckDB/SQLite (Athena-compatible parsing)

- Parses S3 access logs using the SAME RegexSerDe pattern as the AWS Athena DDL you provided.
- Streams parse to CSV (no need to load entire file into memory).
- Optionally loads the CSV into DuckDB (preferred) or SQLite (fallback).
- Includes a few demo queries.

USAGE EXAMPLES
--------------
# Parse a single file and create CSV + DuckDB (auto-detects DuckDB; falls back to SQLite)
python s3_access_logs_local_parser.py \
  --input /path/to/your/logfile \
  --csv-out ./s3_logs_parsed.csv \
  --db auto \
  --db-path ./s3_logs.duckdb \
  --bad-out ./s3_logs_bad_lines.txt \
  --demo

# Parse a directory (recursively) of logs (.log and .gz supported)
python s3_access_logs_local_parser.py \
  --input /path/to/dir \
  --csv-out ./s3_logs_parsed.csv \
  --db auto \
  --demo

# If DuckDB is not installed, force SQLite:
python s3_access_logs_local_parser.py \
  --input /path/to/logs \
  --db sqlite \
  --db-path ./s3_logs.sqlite

NOTES
-----
- The CSV has all Athena DDL columns plus an extra column "request_ts_utc" (ISO-8601) parsed from requestdatetime.
- Numeric fields are converted; "-" becomes empty (NULL in DB).
- DuckDB import uses: CREATE OR REPLACE TABLE s3_access_logs AS SELECT * FROM read_csv_auto(...).
- SQLite import uses pandas.to_sql (chunked).
"""

import os
import re
import io
import csv
import gzip
import sys
import argparse
from datetime import datetime, timezone

# Optional imports (only loaded if needed)
try:
    import duckdb  # type: ignore
    _HAS_DUCKDB = True
except Exception:
    _HAS_DUCKDB = False

try:
    import sqlite3  # stdlib
    _HAS_SQLITE = True
except Exception:
    _HAS_SQLITE = False

try:
    import pandas as pd  # for SQLite import and previews
    _HAS_PANDAS = True
except Exception:
    _HAS_PANDAS = False


# ---------- Athena-compatible schema/regex ----------

COLS = [
    "bucketowner",
    "bucket_name",
    "requestdatetime",   # e.g., 04/Nov/2025:10:12:44 +0000   (without brackets)
    "remoteip",
    "requester",
    "requestid",
    "operation",
    "key",
    "request_uri",       # quoted string
    "httpstatus",
    "errorcode",
    "bytessent",
    "objectsize",
    "totaltime",
    "turnaroundtime",
    "referrer",          # quoted string
    "useragent",         # quoted string
    "versionid",
    "hostid",
    "sigv",
    "ciphersuite",
    "authtype",
    "endpoint",
    "tlsversion",
    "accesspointarn",
    "aclrequired",
]

# Directly adapted from your Athena RegexSerDe
PATTERN = re.compile(
    r'([^ ]*) ([^ ]*) \[(.*?)\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) '
    r'(\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) '
    r'(\"[^\"]*\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$'
)

NUMERIC_COLS = {"httpstatus", "bytessent", "objectsize", "totaltime", "turnaroundtime"}
QUOTED_COLS  = {"request_uri", "referrer", "useragent"}
DASH_TO_NONE = {"errorcode", "versionid", "hostid", "sigv", "ciphersuite", "authtype",
                "endpoint", "tlsversion", "accesspointarn", "aclrequired"}

EXTRA_COLS = ["request_ts_utc"]  # derived ISO timestamp


# ---------- Helpers ----------

def strip_quotes(val):
    if val is None:
        return None
    val = val.strip()
    if len(val) >= 2 and val[0] == '"' and val[-1] == '"':
        return val[1:-1]
    return val

def dash_to_empty(val):
    if val == "-" or val is None:
        return ""
    return val

def to_int_or_empty(val):
    if val is None or val == "-":
        return ""
    try:
        return int(val)
    except Exception:
        return ""

def parse_datetime_iso_utc(requestdatetime):
    # requestdatetime like "04/Nov/2025:10:12:44 +0000"
    if not requestdatetime:
        return ""
    try:
        dt = datetime.strptime(requestdatetime, "%d/%b/%Y:%H:%M:%S %z")
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ""

def open_text_any(path):
    """
    Try plain text; if that fails, try gzip.
    """
    try:
        return open(path, "rt", encoding="utf-8", errors="replace")
    except Exception:
        try:
            return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="replace")
        except Exception:
            raise

def iter_input_files(input_path):
    """
    Yield file paths. If a directory is given, walk recursively.
    """
    if os.path.isfile(input_path):
        yield input_path
    else:
        for root, _, files in os.walk(input_path):
            for name in files:
                fp = os.path.join(root, name)
                # optionally skip hidden or non-log files; here we take all
                yield fp

# ---------- Core parsing ----------

def parse_to_csv(input_path, csv_out, bad_out=None):
    total_rows = 0
    bad_lines = 0
    wrote_header = False

    # Ensure output dir
    os.makedirs(os.path.dirname(os.path.abspath(csv_out)), exist_ok=True)
    bad_writer = None

    if bad_out:
        os.makedirs(os.path.dirname(os.path.abspath(bad_out)), exist_ok=True)
        bad_writer = open(bad_out, "w", encoding="utf-8")

    with open(csv_out, "w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)
        header = COLS + EXTRA_COLS
        writer.writerow(header)
        wrote_header = True

        for fp in iter_input_files(input_path):
            try:
                with open_text_any(fp) as fin:
                    for ln, line in enumerate(fin, start=1):
                        line = line.strip()
                        if not line:
                            continue
                        m = PATTERN.match(line)
                        if not m:
                            bad_lines += 1
                            if bad_writer:
                                bad_writer.write(f"{fp}:{ln}:{line}\n")
                            continue
                        groups = list(m.groups())
                        # groups may be shorter than COLS if optional tail absent
                        if len(groups) < len(COLS):
                            groups += [None] * (len(COLS) - len(groups))

                        row = []
                        for col, val in zip(COLS, groups[:len(COLS)]):
                            if col in QUOTED_COLS:
                                val = strip_quotes(val)
                            if col in DASH_TO_NONE:
                                val = dash_to_empty(val)
                            if col in NUMERIC_COLS:
                                val = to_int_or_empty(val)
                            row.append(val if val is not None else "")

                        # Append derived ISO timestamp (UTC)
                        iso_ts = parse_datetime_iso_utc(row[2])  # requestdatetime is index 2
                        row.append(iso_ts)

                        writer.writerow(row)
                        total_rows += 1
            except Exception as e:
                if bad_writer:
                    bad_writer.write(f"{fp}:0:__FILE_ERROR__ {repr(e)}\n")

    if bad_writer:
        bad_writer.close()

    return {"rows": total_rows, "bad_lines": bad_lines, "csv_out": csv_out, "bad_out": bad_out}

# ---------- DB loaders ----------

def load_duckdb(csv_out, db_path):
    import duckdb  # ensure import inside
    con = duckdb.connect(db_path)
    con.execute(f"""
        CREATE OR REPLACE TABLE s3_access_logs AS
        SELECT * FROM read_csv_auto('{csv_out}', header=True);
    """)
    # Demo queries
    demo = {}
    demo["total_rows"] = con.execute("SELECT COUNT(*) AS total_rows FROM s3_access_logs;").fetchdf()
    demo["top_ops"] = con.execute("""
        SELECT operation, COUNT(*) AS c
        FROM s3_access_logs
        GROUP BY 1 ORDER BY c DESC LIMIT 10;
    """).fetchdf()
    demo["status"] = con.execute("""
        SELECT httpstatus, COUNT(*) AS c
        FROM s3_access_logs
        GROUP BY 1 ORDER BY c DESC LIMIT 10;
    """).fetchdf()
    demo["per_day"] = con.execute("""
        SELECT CAST(strptime(requestdatetime, '%d/%b/%Y:%H:%M:%S %z') AS DATE) AS day, COUNT(*) AS c
        FROM s3_access_logs GROUP BY 1 ORDER BY day;
    """).fetchdf()
    con.close()
    return demo

def load_sqlite(csv_out, db_path, chunksize=200000):
    if not _HAS_PANDAS:
        raise RuntimeError("pandas is required to load into SQLite (not installed).")
    import sqlite3
    # Create table (replace) and append chunks
    conn = sqlite3.connect(db_path)
    first = True
    for chunk in pd.read_csv(csv_out, chunksize=chunksize):
        chunk.to_sql("s3_access_logs", conn, if_exists="replace" if first else "append", index=False)
        first = False
    # Demo queries
    demo = {}
    demo["total_rows"] = pd.read_sql_query("SELECT COUNT(*) AS total_rows FROM s3_access_logs;", conn)
    demo["top_ops"] = pd.read_sql_query("""
        SELECT operation, COUNT(*) AS c
        FROM s3_access_logs
        GROUP BY operation ORDER BY c DESC LIMIT 10;
    """, conn)
    demo["status"] = pd.read_sql_query("""
        SELECT httpstatus, COUNT(*) AS c
        FROM s3_access_logs
        GROUP BY httpstatus ORDER BY c DESC LIMIT 10;
    """, conn)
    conn.close()
    return demo

# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Parse S3 access logs locally (Athena-compatible), then load into DuckDB/SQLite.")
    ap.add_argument("--input", "-i", required=True, help="Path to a log FILE or DIRECTORY (recursively processed). .gz supported.")
    ap.add_argument("--csv-out", default="./s3_logs_parsed.csv", help="Output CSV path.")
    ap.add_argument("--bad-out", default="./s3_logs_bad_lines.txt", help="Write malformed lines here (optional).")
    ap.add_argument("--db", choices=["auto", "duckdb", "sqlite", "none"], default="auto",
                    help="DB target: 'auto' (try duckdb then sqlite), 'duckdb', 'sqlite', or 'none'.")
    ap.add_argument("--db-path", default=None, help="DB file path (e.g., ./s3_logs.duckdb or ./s3_logs.sqlite).")
    ap.add_argument("--demo", action="store_true", help="Run a few demo queries after loading into DB and print results.")
    args = ap.parse_args()

    # 1) Parse to CSV
    res = parse_to_csv(args.input, args.csv_out, bad_out=args.bad_out)
    print(f"[parse] rows={res['rows']} bad_lines={res['bad_lines']} -> {res['csv_out']}")
    if res["bad_out"] and os.path.exists(res["bad_out"]) and os.path.getsize(res["bad_out"]) > 0:
        print(f"[warn] Some lines didn't match the regex. See: {res['bad_out']}")

    # 2) DB selection
    db_choice = args.db
    db_path = args.db_path

    if db_choice == "auto":
        if _HAS_DUCKDB:
            db_choice = "duckdb"
        elif _HAS_SQLITE and _HAS_PANDAS:
            db_choice = "sqlite"
        else:
            db_choice = "none"

    # Default DB path if not provided
    if db_choice == "duckdb" and db_path is None:
        db_path = "./s3_logs.duckdb"
    if db_choice == "sqlite" and db_path is None:
        db_path = "./s3_logs.sqlite"

    # 3) Load into DB + demo
    if db_choice == "duckdb":
        if not _HAS_DUCKDB:
            print("[error] duckdb package not installed. Try: pip install duckdb")
            sys.exit(2)
        print(f"[duckdb] loading into {db_path} ...")
        demo = load_duckdb(args.csv_out, db_path)
        print(f"[duckdb] table=s3_access_logs rows={int(demo['total_rows']['total_rows'].iloc[0])}")
        if args.demo:
            print("\n-- top operations --")
            print(demo["top_ops"].to_string(index=False))
            print("\n-- status counts --")
            print(demo["status"].to_string(index=False))
            print("\n-- per day --")
            print(demo["per_day"].to_string(index=False))

    elif db_choice == "sqlite":
        if not _HAS_SQLITE:
            print("[error] sqlite3 not available in this Python. Unusual.")
            sys.exit(2)
        if not _HAS_PANDAS:
            print("[error] pandas not installed; required for SQLite load. Try: pip install pandas")
            sys.exit(2)
        print(f"[sqlite] loading into {db_path} ...")
        demo = load_sqlite(args.csv_out, db_path)
        print(f"[sqlite] table=s3_access_logs rows={int(demo['total_rows']['total_rows'].iloc[0])}")
        if args.demo:
            print("\n-- top operations --")
            print(demo["top_ops"].to_string(index=False))
            print("\n-- status counts --")
            print(demo["status"].to_string(index=False))

    else:
        print("[info] DB load skipped (db=none). CSV is ready.")

if __name__ == "__main__":
    main()
