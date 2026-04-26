# Deployment Runbook

End-to-end steps to deploy this project to Render and wire up the Airtable manual address feed.

---

## 1. Push to GitHub

The target repo is `seboyer/owner-research-tool`. From the project root:

```bash
# First-time init (skip if already a git repo)
git init -b main

# Stage everything except what's in .gitignore (which excludes .env, venv/, etc.)
git add .
git commit -m "Initial commit — pre-Render deploy"

# Add the remote and push. Create the empty repo on GitHub first.
git remote add origin git@github.com:seboyer/owner-research-tool.git
git push -u origin main
```

**Before pushing, verify no secrets are staged:**

```bash
git status --short             # should show ?? .env if it exists
git ls-files | grep -i env     # should NOT show .env
```

If `.env` is staged, abort and check `.gitignore`.

---

## 2. Create the Render Web Service

1. Sign up at [render.com](https://render.com) (no credit card required for the Starter plan).
2. Connect your GitHub account.
3. **New → Blueprint** → select `seboyer/owner-research-tool` → Render reads `render.yaml` and proposes one Web Service named `nyc-landlord-finder`.
4. Click **Apply**. Render begins building the Docker image.

The build will take 3–5 minutes. The container will fail to fully start until you set the env vars in step 3 — that's expected.

---

## 3. Set environment variables

In the Render dashboard → your service → **Environment**, add the following.

### Required — service won't function without these

| Key | Value |
|-----|-------|
| `SUPABASE_URL` | from Supabase → Project Settings → API |
| `SUPABASE_KEY` | the anon key (preferred) or service role key |
| `ANTHROPIC_API_KEY` | from console.anthropic.com |

### Auto-search toggle (default OFF)

| Key | Value | Notes |
|-----|-------|-------|
| `AUTO_SEARCH_ENABLED` | `false` | Already set in `render.yaml`. Flip to `"true"` only when you're ready for daily/weekly cron jobs to fire automatically. While `false`, the Airtable manual feed still works. |

### Airtable webhook integration

| Key | Value |
|-----|-------|
| `AIRTABLE_WEBHOOK_SECRET` | generate with `openssl rand -hex 32` — **save this**, you'll paste it into the Airtable automation in step 5 |
| `AIRTABLE_API_KEY` | personal access token from [airtable.com/create/tokens](https://airtable.com/create/tokens) with `data.records:write` scope on the LL Pipeline base |
| `AIRTABLE_BASE_ID` | `appstQVl7JeMfr7d0` (already in `render.yaml`) |
| `AIRTABLE_ADDRESS_TABLE_ID` | `tblVOwshwfY0F3gSS` (already in `render.yaml`) |
| `AIRTABLE_BBL_FIELD_ID` | `fldYiP7RhhoYx6QpS` (already in `render.yaml`) |
| `AIRTABLE_HPD_FIELD_ID` | `fldqyrQmuJlglgXqu` (already in `render.yaml`) |

### Optional — enrichment APIs (paid tiers activate when their key is set)

`NYC_OPENDATA_APP_TOKEN`, `BATCHDATA_API_KEY`, `APOLLO_API_KEY`, `HUNTER_API_KEY`, `GOOGLE_PLACES_API_KEY`, `PROXYCURL_API_KEY`, `WHITEPAGES_API_KEY`, `BROWSERLESS_API_KEY`, `SCRAPERAPI_KEY`, `ZOOMINFO_CLIENT_ID`, `ZOOMINFO_PRIVATE_KEY`, `ZOOMINFO_USERNAME`, `OPENAI_API_KEY`.

After setting the env vars, click **Save Changes**. Render redeploys automatically.

---

## 4. Verify the service is up

Once Render shows "Live", grab your service URL — something like `https://nyc-landlord-finder.onrender.com`.

```bash
curl https://nyc-landlord-finder.onrender.com/health
```

Expected response:

```json
{
  "status": "ok",
  "auto_search_enabled": false,
  "missing_config": [],
  "database_reachable": true,
  "database_error": null
}
```

If `missing_config` is non-empty or `database_reachable` is false, fix the env vars and redeploy.

---

## 5. Configure the Airtable automation

In the **LL Pipeline** base:

1. **Automations → Create automation**.
2. **Trigger: When record enters view**.
   - Table: `Addresses`
   - View: `Research (Robot)`
3. **Action: Run a script** OR **Send HTTP request** (HTTP request is simpler).

### HTTP request action configuration

- **Method:** POST
- **URL:** `https://nyc-landlord-finder.onrender.com/webhook/airtable`
- **Headers:**
  - `Authorization: Bearer <the AIRTABLE_WEBHOOK_SECRET you generated>`
  - `Content-Type: application/json`
- **Body (JSON):**

  ```json
  {
    "record_id": "{{record_id_from_trigger}}",
    "house_number": "{{Street Number from trigger record}}",
    "street_name": "{{Street Name from trigger record}}",
    "borough": "{{Borough from trigger record}}"
  }
  ```

  In the Airtable automation editor, replace each `{{...}}` placeholder by inserting the corresponding field from the trigger record.

4. **Test the automation** — Airtable will send a request using a sample record. The webhook responds 202 immediately and processes the address in the background. Within 1–3 minutes the row should populate `BBL Number` and `HPD Building ID`.

### Auth on the webhook

The service rejects any request without a matching `Authorization: Bearer <AIRTABLE_WEBHOOK_SECRET>` header. Both values must match exactly.

---

## 6. Toggling the auto-search worker

The cron-based daily/weekly pipelines are **off by default**. To enable them:

1. Render dashboard → service → **Environment**.
2. Edit `AUTO_SEARCH_ENABLED` to `true`.
3. Click **Save**. Render redeploys.

To disable, flip back to `false` and save. The Airtable manual feed continues to work in either state.

---

## 7. Logs

Render dashboard → service → **Logs** tab. Real-time tail. Key log events to look for:

- `webhook.startup` — service booted
- `scheduler.auto_search_enabled` / `scheduler.auto_search_disabled` — confirms toggle state
- `webhook.received` — Airtable triggered an address run
- `webhook.pipeline_done` — pipeline finished
- `airtable.writeback_ok` — BBL/HPD ID written back to the row
- `scheduler.health` — hourly stats (entities, contacts, queue depth)

---

## 8. One-off CLI runs

You can still run the CLI commands inside the Render container (Shell tab):

```bash
python main.py stats
python main.py daily
python main.py pierce --entity "WEST 72 STREET OWNERS CORP"
```

---

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `401 Missing Bearer token` | Airtable automation missing the `Authorization` header |
| `403 Invalid webhook token` | The secret in Airtable doesn't match `AIRTABLE_WEBHOOK_SECRET` in Render |
| Pipeline runs but Airtable row doesn't update | `AIRTABLE_API_KEY` missing or lacks `data.records:write` scope on LL Pipeline base |
| `/health` returns `database_reachable: false` | `SUPABASE_URL` / `SUPABASE_KEY` wrong, or Supabase project paused |
| `auto_search_enabled: true` but no daily run | Cron fires at 3 AM ET; check tomorrow's logs. Or run `python main.py daily` manually from Render Shell |
| Service sleeps and first request is slow | Render Starter plan spins down after 15 min of inactivity. Upgrade to Standard for always-on, or set up a cron-job.org keepalive ping every 10 min on `/health` |
