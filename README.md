# MySQL Dumps → Supabase Sync

Hourly GitHub Actions pipeline. Downloads 6 MySQL `.sql` dumps from a single
shared OneDrive folder link and does a full truncate + reload into Supabase.

**No Azure account. No Microsoft login. Just two secrets.**

---

## How it works

```
OneDrive shared folder  (files refreshed hourly by someone else)
        │
        │  Microsoft Graph anonymous sharing API
        │  (works with any "Anyone with link" OneDrive URL — no auth needed)
        ▼
GitHub Actions  (cron: every hour at :10)
        │
        │  1. List all files in the shared folder
        │  2. Filter to auct_*.sql files
        │  3. Download each file
        │  4. Parse CREATE TABLE → column names + types
        │  5. Parse INSERT VALUES → rows
        │  6. TRUNCATE + bulk INSERT into Supabase
        ▼
Supabase (PostgreSQL)
```

---

## Setup — 3 steps

### Step 1 — Get a shared folder link from OneDrive

Ask the person managing the OneDrive folder to:

1. Right-click the folder containing the `.sql` files
2. Click **Share**
3. Under "Link settings", choose **"Anyone with the link can view"**
   (this is the key setting — must be "Anyone", not "Specific people")
4. Click **Copy link** and send it to you

The link will look like one of these:
```
https://1drv.ms/f/s!AbCdEfGhIjKl...          ← short form
https://mycompany.sharepoint.com/:f:/s/...     ← SharePoint form
```

Either format works.

> ⚠️ If it says "Specific people" or requires a Microsoft login to open,
> the anonymous Graph API call will return 401 and the sync will fail.
> It must be "Anyone with the link".

---

### Step 2 — Add two GitHub Secrets

In your GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**

| Secret name           | Value                                                   |
|-----------------------|---------------------------------------------------------|
| `ONEDRIVE_FOLDER_URL` | The shared folder link from Step 1                      |
| `SUPABASE_DB_URL`     | Supabase → Settings → Database → Connection string → URI |

The Supabase URI looks like:
```
postgresql://postgres:[PASSWORD]@db.xxxxxxxxxxxx.supabase.co:5432/postgres
```

That's it. Two secrets, done.

---

### Step 3 — Push and test

```bash
git add .
git commit -m "Add hourly MySQL→Supabase sync"
git push
```

Go to **Actions tab → MySQL Dumps → Supabase Sync → Run workflow** to trigger
a manual test run. Logs will show each file being found, downloaded, and loaded.

---

## Tables created in Supabase

Auto-created on first run. Column types are inferred from the data.
Every run is a full truncate + reload, so data always matches the latest dump.

| Supabase table         | Primary key   |
|------------------------|---------------|
| `auct_auctions_xml`    | `id`          |
| `auct_lots_xml_jp`     | `id`          |
| `auct_models_xml`      | `model_id`    |
| `auct_colors_xml`      | `color_id`    |
| `auct_companies_xml`   | `company_id`  |
| `auct_results_xml`     | `result_id`   |

---

## Adding a new dump file later

If a 7th file gets added to the OneDrive folder:

1. Open `scripts/sync.py`
2. Add an entry to `PRIMARY_KEYS`:
   ```python
   PRIMARY_KEYS = {
       ...
       "auct_new_table.sql": ["id"],   # ← add this
   }
   ```
3. Commit and push — the next run will pick it up automatically.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `401 PermissionError` | Folder shared to "Specific people", not "Anyone" | Ask the sharer to change to "Anyone with link" |
| `404 FileNotFoundError` | Wrong URL in secret | Re-copy the share link and update `ONEDRIVE_FOLDER_URL` |
| `No matching .sql files found` | Files not yet written this hour, or wrong folder | Check timing; verify the right folder is shared |
| `Could not parse column names` | Dump missing `CREATE TABLE` | Check the `.sql` file is a full mysqldump, not just data |
| Supabase `connection refused` | Project paused (free tier) or wrong URL | Wake up project in Supabase dashboard; check `SUPABASE_DB_URL` |

---

## Timing note

The cron runs at **:10 past every hour**. If you know roughly when the dumps
are written (e.g. always at :00), this 10-minute buffer is conservative and safe.
You can change `"10 * * * *"` in `.github/workflows/sync.yml` if needed —
e.g. `"30 * * * *"` to run at half past every hour.
