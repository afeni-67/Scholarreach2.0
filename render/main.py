"""
Scholarreach 2.0 – Render free-tier producer service.

Endpoints:
  GET  /health
  POST /enqueue          {type, url, meta?}
  GET  /queue/stats
  POST /trigger-actions   (optional: fire repository_dispatch so Actions wakes up sooner)
"""
import os
import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel, HttpUrl, Field
from dotenv import load_dotenv

load_dotenv()

# Re-use the same db module by adding parent to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import db
from src.config import GITHUB_TOKEN, GITHUB_REPO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scholarreach.render")

app = FastAPI(
    title="Scholarreach 2.0 API",
    description="Enqueue paper discovery / extraction jobs. Backed by MongoDB + GitHub Actions.",
    version="2.0.0",
)

API_KEY = os.getenv("API_KEY")  # optional simple protection


class EnqueueRequest(BaseModel):
    type: str = Field(..., pattern="^(discover|extract)$")
    url: str
    meta: Optional[Dict[str, Any]] = None
    priority: int = 0


class EnqueueResponse(BaseModel):
    ok: bool
    job_id: str
    status: str
    message: str


def check_api_key(x_api_key: Optional[str] = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")
    return True


@app.on_event("startup")
def startup():
    try:
        db.ensure_indexes()
        logger.info("MongoDB connected and indexes ensured")
    except Exception as e:
        logger.error("MongoDB connection failed on startup: %s", e)


@app.get("/health")
def health():
    try:
        db.get_client().admin.command("ping")
        stats = db.count_by_status()
        return {"status": "ok", "mongo": "up", "queue": stats}
    except Exception as e:
        return {"status": "degraded", "error": str(e)}


@app.post("/enqueue", response_model=EnqueueResponse, dependencies=[Depends(check_api_key)])
def enqueue(body: EnqueueRequest):
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "url must be absolute http(s)")
    job = db.enqueue_job(
        job_type=body.type,
        url=body.url,
        meta=body.meta,
        priority=body.priority,
    )
    return EnqueueResponse(
        ok=True,
        job_id=str(job["_id"]),
        status=job["status"],
        message=f"Job {job['status']} – Actions will pick it up on next run (or trigger manually)",
    )


@app.get("/queue/stats", dependencies=[Depends(check_api_key)])
def queue_stats():
    return db.count_by_status()


@app.post("/trigger-actions", dependencies=[Depends(check_api_key)])
def trigger_actions():
    """Fire a repository_dispatch so a new Actions run starts immediately."""
    if not GITHUB_TOKEN:
        raise HTTPException(500, "GITHUB_TOKEN not configured on Render")
    import requests
    url = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload = {"event_type": "process", "client_payload": {"source": "render"}}
    r = requests.post(url, json=payload, headers=headers, timeout=15)
    if r.status_code not in (200, 204):
        raise HTTPException(r.status_code, r.text)
    return {"ok": True, "message": "repository_dispatch sent"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
