#!/usr/bin/env python3
"""
Temporary debug script — prints what OneDrive actually returns
so we can see what pattern to extract files from.
Run once, then delete.
"""
import os, re, json, requests

ONEDRIVE_FOLDER_URL = os.environ["ONEDRIVE_FOLDER_URL"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

resp = requests.get(ONEDRIVE_FOLDER_URL, headers=HEADERS, timeout=30, allow_redirects=True)
print(f"Status: {resp.status_code}")
print(f"Final URL: {resp.url}")
print(f"Content-Type: {resp.headers.get('Content-Type')}")
print(f"Page length: {len(resp.text)} chars")
print()

html = resp.text

# Print every line that contains "sql" (case insensitive)
print("=== Lines containing 'sql' ===")
for i, line in enumerate(html.splitlines()):
    if "sql" in line.lower():
        print(f"  Line {i}: {line[:300]}")

print()

# Print every line containing "downloadUrl" or "download"
print("=== Lines containing 'download' ===")
for i, line in enumerate(html.splitlines()):
    if "download" in line.lower():
        print(f"  Line {i}: {line[:300]}")

print()

# Find all script tags and show their first 200 chars
print("=== Script tag openings ===")
for m in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL):
    content = m.group(1).strip()
    if content:
        print(f"  {content[:200]}")
        print("  ---")

# Show first 3000 chars of raw HTML for context
print()
print("=== First 3000 chars of page ===")
print(html[:3000])
