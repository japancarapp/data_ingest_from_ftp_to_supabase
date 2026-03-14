#!/usr/bin/env python3
"""
MySQL dump → Supabase ETL pipeline.

Authentication: NONE — uses a single OneDrive "Anyone with link" shared
folder URL. Microsoft's Graph API accepts a base64-encoded share URL as
a driveItem identifier, no Azure app registration required.

Strategy: truncate + full reload on every run.
"""

import os
import re
import sys
import base64
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ONEDRIVE_FOLDER_URL = os.environ["ONEDRIVE_FOLDER_URL"]  # The single shared folder link
SUPABASE_DB_URL     = os.environ["SUPABASE_DB_URL"]


def _safe_db_url(url: str) -> str:
    """Return the DB URL with the password replaced by *** for safe logging."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)

# Only process files matching this pattern (safeguard against unrelated files)
SQL_FILE_PATTERN = re.compile(r"^auct_.*\.sql$", re.IGNORECASE)

# Maps filename → primary key column(s)
# Add entries here if new dump files are added to the folder later
PRIMARY_KEYS = {
    "auct_auctions_xml.sql":  ["id"],
    "auct_lots_xml_jp.sql":   ["id"],
    "auct_models_xml.sql":    ["model_id"],
    "auct_colors_xml.sql":    ["color_id"],
    "auct_companies_xml.sql": ["company_id"],
    "auct_results_xml.sql":   ["result_id"],
}

# ── OneDrive shared-folder helpers (no auth needed) ───────────────────────────

def _resolve_share_url(share_url: str) -> str:
    """
    Resolve a short OneDrive URL (1drv.ms) to its full URL by following
    redirects without downloading the page. This is needed because newer
    1drv.ms/f/c/... links must be resolved before base64-encoding.
    """
    if "1drv.ms" in share_url:
        resp = requests.head(share_url, allow_redirects=True, timeout=15)
        resolved = resp.url
        log.debug(f"Resolved short URL to: {resolved}")
        return resolved
    return share_url


def _share_url_to_encoded_id(share_url: str) -> str:
    """
    Convert a OneDrive/SharePoint share URL to the base64 token format
    that Microsoft Graph accepts as a sharing token.

    Documented at:
    https://learn.microsoft.com/en-us/graph/api/shares-get
    """
    resolved = _resolve_share_url(share_url)
    b64 = base64.b64encode(resolved.encode("utf-8")).decode("utf-8")
    b64 = b64.rstrip("=").replace("/", "_").replace("+", "-")
    return f"u!{b64}"


def list_files_in_shared_folder(share_url: str) -> list[dict]:
    """
    List all items inside a shared OneDrive folder using the
    anonymous Graph sharing API — no token required.

    Returns list of dicts with keys: name, downloadUrl
    """
    token = _share_url_to_encoded_id(share_url)
    # This endpoint is publicly accessible for "Anyone with link" shares
    url = f"https://graph.microsoft.com/v1.0/shares/{token}/driveItem/children"

    resp = requests.get(url, timeout=30)

    if resp.status_code == 401:
        raise PermissionError(
            "OneDrive returned 401. Make sure the folder is shared as "
            "'Anyone with the link can view' (not restricted to specific people). "
            "Check the ONEDRIVE_FOLDER_URL secret is correct."
        )
    if resp.status_code == 404:
        raise FileNotFoundError(
            "OneDrive folder not found (404). Double-check the ONEDRIVE_FOLDER_URL secret."
        )
    resp.raise_for_status()

    items = resp.json().get("value", [])
    files = []
    for item in items:
        if "file" in item:  # skip subfolders
            files.append({
                "name": item["name"],
                "downloadUrl": item.get("@microsoft.graph.downloadUrl", ""),
            })

    log.info(f"Found {len(files)} file(s) in shared folder: {[f['name'] for f in files]}")
    return files


def download_file(download_url: str, name: str) -> bytes:
    resp = requests.get(download_url, timeout=60)
    resp.raise_for_status()
    log.info(f"  Downloaded {name} ({len(resp.content):,} bytes)")
    return resp.content

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
    """Split a VALUES row into Python values, respecting quoted strings."""
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


def parse_columns(sql_text: str) -> list[str]:
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


def parse_dump(sql_bytes: bytes) -> tuple[list[str], list[tuple]]:
    sql_text = sql_bytes.decode("utf-8", errors="replace")
    columns = parse_columns(sql_text)
    rows = []
    for m in _INSERT_RE.finditer(sql_text):
        # Match individual row tuples: (v1, v2, ...)
        for rm in re.finditer(r"\(([^()]*(?:'[^']*'[^()]*)*)\)", m.group(1)):
            rows.append(tuple(_split_row(rm.group(1))))
    log.info(f"  Parsed {len(rows)} rows, {len(columns)} columns: {columns}")
    return columns, rows

# ── Supabase helpers ──────────────────────────────────────────────────────────

def infer_pg_type(val) -> str:
    if isinstance(val, int):   return "BIGINT"
    if isinstance(val, float): return "DOUBLE PRECISION"
    return "TEXT"


def truncate_and_load(conn, table: str, columns: list[str], pk: list[str], rows: list[tuple]):
    if not rows:
        log.warning(f"  No rows to load into '{table}' — skipping truncate.")
        return

    with conn.cursor() as cur:
        # Auto-create table if it doesn't exist yet
        col_defs  = [f'"{c}" {infer_pg_type(v)}' for c, v in zip(columns, rows[0])]
        pk_clause = ", ".join(f'"{c}"' for c in pk)
        cur.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
            + ",\n".join(f"  {d}" for d in col_defs)
            + f",\n  PRIMARY KEY ({pk_clause})\n);"
        )

        # Full reload
        cur.execute(f'TRUNCATE TABLE "{table}";')
        col_list = ", ".join(f'"{c}"' for c in columns)
        execute_values(
            cur,
            f'INSERT INTO "{table}" ({col_list}) VALUES %s;',
            rows,
            page_size=500,
        )
        conn.commit()
    log.info(f"  ✓ {len(rows)} rows loaded into '{table}'")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== MySQL → Supabase sync (truncate + reload) ===")
    errors = []

    # 1. Discover files in the shared OneDrive folder
    all_files = list_files_in_shared_folder(ONEDRIVE_FOLDER_URL)
    sql_files = [f for f in all_files if SQL_FILE_PATTERN.match(f["name"])]

    if not sql_files:
        log.error("No matching .sql files found in the shared folder. Aborting.")
        sys.exit(1)

    # Warn about any expected files that are missing
    found_names = {f["name"] for f in sql_files}
    for expected in PRIMARY_KEYS:
        if expected not in found_names:
            log.warning(f"Expected file not found in folder: {expected}")

    # 2. Connect to Supabase
    log.info(f"Connecting to Supabase at {_safe_db_url(SUPABASE_DB_URL)} …")
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
    except psycopg2.OperationalError as e:
        # psycopg2 sometimes includes the DSN (with password) in the error message.
        # Sanitize it before logging.
        safe_err = re.sub(r"password=[^\s]+", "password=***", str(e))
        log.error(f"Could not connect to Supabase: {safe_err}")
        sys.exit(1)
    log.info("Connected.")

    # 3. Process each file
    for file in sql_files:
        name  = file["name"]
        table = name.replace(".sql", "")  # filename minus extension = table name
        pk    = PRIMARY_KEYS.get(name)

        if not pk:
            log.warning(f"No primary key configured for '{name}' — skipping. "
                        f"Add it to PRIMARY_KEYS in sync.py to include it.")
            continue

        log.info(f"--- {name} → {table} ---")
        try:
            raw              = download_file(file["downloadUrl"], name)
            columns, rows    = parse_dump(raw)
            if not columns:
                raise ValueError("Could not parse column names — check CREATE TABLE is present in dump")
            truncate_and_load(conn, table, columns, pk, rows)
        except Exception as e:
            log.error(f"Failed on {name}: {e}", exc_info=True)
            errors.append(name)
            conn.rollback()

    conn.close()

    if errors:
        log.error(f"Finished WITH ERRORS: {errors}")
        sys.exit(1)

    log.info("=== All done ✓ ===")


if __name__ == "__main__":
    main()
