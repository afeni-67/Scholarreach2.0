# Scholarreach 2.0

**Continuous academic paper discovery & extraction** (title + author emails) powered entirely by free tiers:

- **GitHub Actions** – the long-running workers (max 6 h, started every 3 h → overlapping coverage)
- **Render free Web Service** – the always-available front door that accepts new jobs
- **MongoDB Atlas** (free M0) – the persistent queue + results store

Named after the original concept: the runner can only process while it is running; the water (jobs) is laid in the tracks (MongoDB) so any runner can pick it up.

## Architecture

```
You / scripts / cron
        │
        ▼
┌─────────────────────┐
│  Render (FastAPI)   │  POST /enqueue
│  free web service   │  (spins down after 15 min idle – fine)
└─────────┬───────────┘
          │ writes jobs
          ▼
┌─────────────────────┐
│   MongoDB Atlas     │  collections: jobs, results
│   (free M0 cluster) │
└─────────┬───────────┘
          │ claimed by
          ▼
┌─────────────────────┐
│  GitHub Actions     │  schedule every 3 h + workflow_dispatch
│  (public repo)      │  + repository_dispatch from Render
│  max 6 h runtime    │
└─────────────────────┘
          │
          ▼
   PDF download → title + emails extracted → results collection
```

## Supported journals (out of the box)

- **IJETRM** (`ijetrm.com`) – full issue pages via `citation_pdf_url` meta tags
- Generic PDF-link pages (works for many other open-access journals)
- Easy to extend for IJDDT and others in `src/scraper.py`

## Quick start

### 1. MongoDB Atlas (free)

1. Create a free M0 cluster at https://cloud.mongodb.com
2. Create a database user + allow network access `0.0.0.0/0` (or GitHub + Render IPs)
3. Copy the connection string (`mongodb+srv://...`)

### 2. GitHub Secrets

In the repo → Settings → Secrets and variables → Actions:

| Secret         | Value                          |
|----------------|--------------------------------|
| `MONGODB_URI`  | your Atlas connection string   |
| `MONGODB_DB`   | `scholarreach` (optional)      |
| `GH_PAT`       | the PAT you already provided   |

### 3. Deploy the Render producer

1. Create a new **Web Service** on https://render.com (free plan)
2. Connect this GitHub repo
3. Root directory: `render`
4. Build command: `pip install -r requirements.txt`
5. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
6. Environment variables on Render:

| Key            | Value                                      |
|----------------|--------------------------------------------|
| `MONGODB_URI`  | same Atlas URI                             |
| `MONGODB_DB`   | `scholarreach`                             |
| `GITHUB_TOKEN` | same PAT (for /trigger-actions)            |
| `GITHUB_REPO`  | `afeni-67/Scholarreach2.0`   |
| `API_KEY`      | any random string (optional protection)    |

### 4. Seed the first jobs

```bash
# Discover an entire issue
curl -X POST https://YOUR-RENDER-URL/enqueue \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_API_KEY" \
  -d '{"type":"discover","url":"https://ijetrm.com/issue/?volume=February~2026"}'

# Or a single PDF
curl -X POST https://YOUR-RENDER-URL/enqueue \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_API_KEY" \
  -d '{"type":"extract","url":"https://ijetrm.com/issues/files/....pdf"}'
```

Then either wait for the next scheduled Actions run (every 3 h) or call:

```bash
curl -X POST https://YOUR-RENDER-URL/trigger-actions \
  -H "X-API-Key: YOUR_API_KEY"
```

## Job model (MongoDB `jobs`)

```json
{
  "type": "discover" | "extract",
  "url": "https://...",
  "status": "pending" | "processing" | "done" | "failed",
  "attempts": 0,
  "priority": 0,
  "meta": {},
  "created_at": "...",
  "claimed_by": "gha-123-abc",
  "claimed_at": "...",
  "result": {},
  "error": null
}
```

Stale `processing` jobs older than 120 min are automatically reclaimed.

## Results (`results` collection)

```json
{
  "pdf_url": "https://...",
  "title": "...",
  "emails": ["author@university.edu", ...],
  "email_count": 2,
  "source_domain": "ijetrm.com",
  "source_page": "https://ijetrm.com/issue/...",
  "doi": "...",
  "extracted_at": "..."
}
```

## Local development

```bash
export MONGODB_URI="mongodb+srv://..."
pip install -r requirements.txt
python -c "from src.processor import run_loop; run_loop(max_runtime_seconds=300)"
```

## Design notes

- Public repository → unlimited GitHub Actions minutes.
- Overlapping 3-hour starts + 6-hour max runtime ≈ continuous processing.
- Render free tier sleeps after 15 min idle – that is intentional; it only needs to be awake when you enqueue.
- All heavy work (scraping + PDF parsing) happens inside Actions, never on the free Render instance.

## License

MIT
