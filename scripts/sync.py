#!/usr/bin/env python3
"""
MySQL dump -> Supabase ETL pipeline.

Reads .sql dump files directly from the repo (placed in data/ folder).
GitHub Actions checks out the repo on every run, so the files are
always available at their local path.

Strategy: truncate + full reload on every run.
"""

import os
import re
import sys
import logging
import psycopg2
from psycopg2.extras import execute_values

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

# Folder inside the repo where .sql files live
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Maps filename -> primary key column(s)
PRIMARY_KEYS = {
    "auct_auctions_xml.sql":  ["id"],
    "auct_lots_xml_jp.sql":   ["id"],
    "auct_models_xml.sql":    ["model_id"],
    "auct_colors_xml.sql":    ["color_id"],
    "auct_companies_xml.sql": ["company_id"],
    "auct_results_xml.sql":   ["result_id"],
}


def _safe_db_url(url: str) -> str:
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)


# ── SQL dump parser ───────────────────────────────────────────────────────────

_INSERT_RE = re.compile(
    r"INSERT INTO\s+`?[\w]+`?\s+VALUES\s+(.*?);",
    re.DOTALL | re.IGNORECASE,
)


def _parse_value(raw: str):
    raw = raw.strip()
    if raw.upper() == "NULL":
        return None
    if raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1].replace("''", "'").replace("\\'", "'")
    try:
        return int(raw) if "." not in raw else float(raw)
    except ValueError:
        return raw


def _split_row(row_str: str) -> list:
    tokens, current, in_quote, escape = [], [], False, False
    for ch in row_str:
        if escape:
            current.append(ch); escape = False
        elif ch == "\\" and in_quote:
            current.append(ch); escape = True
        elif ch == "'" and not in_quote:
            in_quote = True; current.append(ch)
        elif ch == "'" and in_quote:
            in_quote = False; current.append(ch)
        elif ch == "," and not in_quote:
            tokens.append("".join(current).strip()); current = []
        else:
            current.append(ch)
    if current:
        tokens.append("".join(current).strip())
    return [_parse_value(t) for t in tokens]


def parse_columns(sql_text: str) -> list:
    m = re.search(
        r"CREATE TABLE\s+`?[\w]+`?\s*\((.*?)\)\s*ENGINE",
        sql_text, re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return []
    cols = []
    for line in m.group(1).split("\n"):
        line = line.strip().rstrip(",")
        cm = re.match(r"`(\w+)`\s+", line)
        if cm:
            cols.append(cm.group(1))
    return cols


def parse_dump(path: str) -> tuple:
    with open(path, "rb") as f:
        sql_bytes = f.read()
    sql_text = sql_bytes.decode("utf-8", errors="replace")
    columns = parse_columns(sql_text)
    rows = []
    for m in _INSERT_RE.finditer(sql_text):
        for rm in re.finditer(r"\(([^()]*(?:'[^']*'[^()]*)*)\)", m.group(1)):
            rows.append(tuple(_split_row(rm.group(1))))
    log.info(f"  Parsed {len(rows)} rows, {len(columns)} columns")
    return columns, rows


# ── Supabase helpers ──────────────────────────────────────────────────────────

def infer_pg_type(val) -> str:
    if isinstance(val, int):   return "BIGINT"
    if isinstance(val, float): return "DOUBLE PRECISION"
    return "TEXT"


def truncate_and_load(conn, table: str, columns: list, pk: list, rows: list):
    if not rows:
        log.warning(f"  No rows to load into '{table}' — skipping.")
        return

    with conn.cursor() as cur:
        col_defs  = [f'"{c}" {infer_pg_type(v)}' for c, v in zip(columns, rows[0])]
        pk_clause = ", ".join(f'"{c}"' for c in pk)
        cur.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
            + ",\n".join(f"  {d}" for d in col_defs)
            + f",\n  PRIMARY KEY ({pk_clause})\n);"
        )
        cur.execute(f'TRUNCATE TABLE "{table}";')
        col_list = ", ".join(f'"{c}"' for c in columns)
        execute_values(
            cur,
            f'INSERT INTO "{table}" ({col_list}) VALUES %s;',
            rows,
            page_size=500,
        )
        conn.commit()
    log.info(f"  {len(rows)} rows loaded into '{table}'")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== MySQL -> Supabase sync (truncate + reload) ===")
    errors = []

    log.info(f"Connecting to Supabase at {_safe_db_url(SUPABASE_DB_URL)} ...")
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
    except psycopg2.OperationalError as e:
        safe_err = re.sub(r"password=[^\s]+", "password=***", str(e))
        log.error(f"Could not connect to Supabase: {safe_err}")
        sys.exit(1)
    log.info("Connected.")

    for filename, pk in PRIMARY_KEYS.items():
        path = os.path.join(DATA_DIR, filename)
        table = filename.replace(".sql", "")
        log.info(f"--- {filename} -> {table} ---")

        if not os.path.exists(path):
            log.error(f"File not found: {path}")
            errors.append(filename)
            continue

        try:
            columns, rows = parse_dump(path)
            if not columns:
                raise ValueError("Could not parse column names — check CREATE TABLE is present")
            truncate_and_load(conn, table, columns, pk, rows)
        except Exception as e:
            log.error(f"Failed on {filename}: {e}", exc_info=True)
            errors.append(filename)
            conn.rollback()

    conn.close()

    if errors:
        log.error(f"Finished WITH ERRORS: {errors}")
        sys.exit(1)

    log.info("=== All done ===")


if __name__ == "__main__":
    main()
