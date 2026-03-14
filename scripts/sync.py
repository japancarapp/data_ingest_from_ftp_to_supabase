#!/usr/bin/env python3
"""
MySQL dump → Supabase ETL pipeline.

Authentication: NONE — uses a single OneDrive "Anyone with link" shared
folder URL. Files are discovered by fetching the folder page and extracting
the embedded JSON manifest OneDrive injects into every shared folder page.
No Graph API, no Microsoft account needed.

Strategy: truncate + full reload on every run.
"""

import os
import re
import sys
import json
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
ONEDRIVE_FOLDER_URL = os.environ["ONEDRIVE_FOLDER_URL"]
SUPABASE_DB_URL     = os.environ["SUPABASE_DB_URL"]


def _safe_db_url(url: str) -> str:
    """Return the DB URL with the password replaced by *** for safe logging."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)


# Only process files matching this pattern
SQL_FILE_PATTERN = re.compile(r"^auct_.*\.sql$", re.IGNORECASE)

# Maps filename -> primary key column(s)
PRIMARY_KEYS = {
    "auct_auctions_xml.sql":  ["id"],
    "auct_lots_xml_jp.sql":   ["id"],
    "auct_models_xml.sql":    ["model_id"],
    "auct_colors_xml.sql":    ["color_id"],
    "auct_companies_xml.sql": ["company_id"],
    "auct_results_xml.sql":   ["result_id"],
}

# ── OneDrive shared-folder helpers ────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def list_files_in_shared_folder(share_url: str) -> list:
    """
    List files in a public OneDrive shared folder.

    Fetches the folder page and extracts the JSON manifest that OneDrive
    embeds in every shared folder page. Contains file names and pre-signed
    download URLs. No Graph API or Microsoft login needed.
    """
    log.info("Fetching OneDrive shared folder page ...")
    resp = requests.get(share_url, headers=HEADERS, timeout=30, allow_redirects=True)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Could not open shared folder (HTTP {resp.status_code}). "
            "Check ONEDRIVE_FOLDER_URL is correct and shared as 'Anyone with link'."
        )

    html = resp.text

    # OneDrive embeds file data as a JSON blob in a <script> tag.
    # Try several known patterns across different OneDrive versions.

    # Pattern A: items array directly in page JS
    m = re.search(r'"items"\s*:\s*(\[.+?\])\s*[,}]', html, re.DOTALL)
    if m:
        try:
            items = json.loads(m.group(1))
            files = _extract_files(items)
            if files:
                log.info(f"Found {len(files)} file(s) via pattern A")
                return files
        except Exception:
            pass

    # Pattern B: __odSpPageContextInfo or similar JSON object containing items
    m = re.search(r'({[^<]*"items"\s*:\s*\[.+?\]})\s*[;,<]', html, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(1))
            items = data.get("items", [])
            files = _extract_files(items)
            if files:
                log.info(f"Found {len(files)} file(s) via pattern B")
                return files
        except Exception:
            pass

    # Pattern C: data-manifiest attribute
    m = re.search(r'data-manifest="([^"]+)"', html)
    if m:
        try:
            items = json.loads(m.group(1).replace("&quot;", '"'))
            files = _extract_files(items if isinstance(items, list) else items.get("items", []))
            if files:
                log.info(f"Found {len(files)} file(s) via pattern C")
                return files
        except Exception:
            pass

    # Pattern D: large JSON blob — find all objects with a "name" and download URL
    files = _extract_from_json_blobs(html)
    if files:
        log.info(f"Found {len(files)} file(s) via pattern D")
        return files

    raise RuntimeError(
        "Could not find file list in the OneDrive page. "
        "The folder share link may have expired — try regenerating it."
    )


def _extract_files(items: list) -> list:
    """Pull name + downloadUrl from a list of OneDrive item dicts."""
    files = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = item.get("name", "")
        url = (
            item.get("@content.downloadUrl")
            or item.get("downloadUrl")
            or item.get("url")
            or ""
        )
        # Skip folders (they have no download URL or have a childCount key)
        if name and url and "folder" not in item:
            files.append({"name": name, "downloadUrl": url})
    return files


def _extract_from_json_blobs(html: str) -> list:
    """
    Last-resort: scan the page for any JSON object that has both
    a 'name' ending in .sql and a download URL field.
    """
    files = []
    seen = set()
    for m in re.finditer(r'\{[^{}]{20,}\}', html):
        try:
            obj = json.loads(m.group(0))
            name = obj.get("name", "")
            url = obj.get("@content.downloadUrl") or obj.get("downloadUrl") or ""
            if name and url and name not in seen and SQL_FILE_PATTERN.match(name):
                files.append({"name": name, "downloadUrl": url})
                seen.add(name)
        except Exception:
            pass
    return files


def download_file(download_url: str, name: str) -> bytes:
    resp = requests.get(download_url, headers=HEADERS, timeout=60, allow_redirects=True)
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


def parse_dump(sql_bytes: bytes) -> tuple:
    sql_text = sql_bytes.decode("utf-8", errors="replace")
    columns = parse_columns(sql_text)
    rows = []
    for m in _INSERT_RE.finditer(sql_text):
        for rm in re.finditer(r"\(([^()]*(?:'[^']*'[^()]*)*)\)", m.group(1)):
            rows.append(tuple(_split_row(rm.group(1))))
    log.info(f"  Parsed {len(rows)} rows, {len(columns)} columns: {columns}")
    return columns, rows


# ── Supabase helpers ──────────────────────────────────────────────────────────

def infer_pg_type(val) -> str:
    if isinstance(val, int):   return "BIGINT"
    if isinstance(val, float): return "DOUBLE PRECISION"
    return "TEXT"


def truncate_and_load(conn, table: str, columns: list, pk: list, rows: list):
    if not rows:
        log.warning(f"  No rows to load into '{table}' — skipping truncate.")
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

    # 1. Discover files in the shared OneDrive folder
    all_files = list_files_in_shared_folder(ONEDRIVE_FOLDER_URL)
    sql_files = [f for f in all_files if SQL_FILE_PATTERN.match(f["name"])]

    if not sql_files:
        log.error("No matching auct_*.sql files found in the shared folder. Aborting.")
        sys.exit(1)

    found_names = {f["name"] for f in sql_files}
    for expected in PRIMARY_KEYS:
        if expected not in found_names:
            log.warning(f"Expected file not present this run: {expected}")

    # 2. Connect to Supabase
    log.info(f"Connecting to Supabase at {_safe_db_url(SUPABASE_DB_URL)} ...")
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
    except psycopg2.OperationalError as e:
        safe_err = re.sub(r"password=[^\s]+", "password=***", str(e))
        log.error(f"Could not connect to Supabase: {safe_err}")
        sys.exit(1)
    log.info("Connected.")

    # 3. Process each file
    for file in sql_files:
        name  = file["name"]
        table = name.replace(".sql", "")
        pk    = PRIMARY_KEYS.get(name)

        if not pk:
            log.warning(f"No primary key configured for '{name}' — skipping. "
                        "Add it to PRIMARY_KEYS in sync.py to include it.")
            continue

        log.info(f"--- {name} -> {table} ---")
        try:
            raw           = download_file(file["downloadUrl"], name)
            columns, rows = parse_dump(raw)
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

    log.info("=== All done ===")


if __name__ == "__main__":
    main()
