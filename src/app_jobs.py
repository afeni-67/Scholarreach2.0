"""
Bridge to the existing ScholarReach web app's ExtractionJob collection.

The Render app creates documents in the `test.extractionjobs` collection
(Mongoose model ExtractionJob). GitHub Actions workers claim those jobs,
run discovery + PDF extraction, and continuously update progress so the
UI can poll live results for the exact user.
"""
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from bson import ObjectId
from pymongo import ReturnDocument, ASCENDING, DESCENDING

from src.config import MONGODB_URI, CLAIM_TIMEOUT_MINUTES
from src.db import get_client

# The web app uses the "test" database by default (URI has no db name).
APP_DB_NAME = "test"
EXTRACTION_JOBS = "extractionjobs"
JOB_EMAILS = "jobemails"
EXTRACTED_EMAILS = "extractedemails"
USER_JOURNALS = "userjournals"


def app_db():
    return get_client()[APP_DB_NAME]


def extraction_jobs():
    return app_db()[EXTRACTION_JOBS]


def job_emails():
    return app_db()[JOB_EMAILS]


def extracted_emails():
    return app_db()[EXTRACTED_EMAILS]


def claim_next_app_job(worker_id: str) -> Optional[Dict[str, Any]]:
    """
    Atomically claim the next ExtractionJob from the UI.

    Accepts:
    - status=queued (any runner, including missing runner after partial deploys)
    - status=running + runner=github-actions that went stale
    Never claims old browser-only jobs that are actively heartbeating.
    """
    now = datetime.now(timezone.utc)
    stale_before = now - timedelta(minutes=CLAIM_TIMEOUT_MINUTES)

    # 1) Any queued job (github-actions or unset runner after migration)
    job = extraction_jobs().find_one_and_update(
        {
            "status": "queued",
            "$or": [
                {"runner": "github-actions"},
                {"runner": None},
                {"runner": {"$exists": False}},
            ],
        },
        {
            "$set": {
                "status": "running",
                "stage": "Claimed by GitHub Actions worker…",
                "runner": "github-actions",
                "lastHeartbeatAt": now,
                "currentUrl": "",
                "claimedBy": worker_id,
                "claimedAt": now,
            },
            "$inc": {"attempts": 1},
        },
        sort=[("priority", DESCENDING), ("startedAt", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    if job:
        return job

    # 2) Reclaim stale running github-actions jobs (worker died / timed out)
    job = extraction_jobs().find_one_and_update(
        {
            "status": "running",
            "runner": "github-actions",
            "$or": [
                {"lastHeartbeatAt": {"$lt": stale_before}},
                {"lastHeartbeatAt": None},
                {"lastHeartbeatAt": {"$exists": False}},
            ],
        },
        {
            "$set": {
                "stage": "Reclaimed by GitHub Actions worker…",
                "lastHeartbeatAt": now,
                "claimedBy": worker_id,
                "claimedAt": now,
            },
            "$inc": {"attempts": 1},
        },
        sort=[("lastHeartbeatAt", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    return job


def update_job_progress(
    job_id,
    *,
    stage: Optional[str] = None,
    progress: Optional[float] = None,
    emails_collected: Optional[int] = None,
    papers_processed: Optional[int] = None,
    current_url: Optional[str] = None,
    batch_preview: Optional[str] = None,
    error_message: Optional[str] = None,
    status: Optional[str] = None,
):
    now = datetime.now(timezone.utc)
    update: Dict[str, Any] = {"lastHeartbeatAt": now}
    if stage is not None:
        update["stage"] = stage
    if progress is not None:
        update["progress"] = max(0, min(100, progress))
    if emails_collected is not None:
        update["emailsCollected"] = emails_collected
    if papers_processed is not None:
        update["papersProcessed"] = papers_processed
    if current_url is not None:
        update["currentUrl"] = current_url
    if batch_preview is not None:
        update["batchPreview"] = batch_preview[:500]
    if error_message is not None:
        update["errorMessage"] = error_message
    if status is not None:
        update["status"] = status
        if status in ("complete", "failed", "stopped"):
            update["finishedAt"] = now

    extraction_jobs().update_one({"_id": job_id}, {"$set": update})


def push_emails_for_job(job: Dict, emails: List[str], paper_url: str = "", title: str = "") -> int:
    """
    Insert new emails into jobemails + extractedemails (same collections the UI reads).
    Returns number of newly added emails for this job.
    """
    if not emails:
        return 0
    user_id = job.get("userId")
    job_id = job["_id"]
    added = 0
    now = datetime.now(timezone.utc)

    for email in emails:
        email = (email or "").strip().lower()
        if not email or "@" not in email:
            continue
        # Avoid duplicates on this job
        exists = job_emails().find_one({"jobId": job_id, "email": email})
        if exists:
            continue
        try:
            job_emails().insert_one(
                {
                    "jobId": job_id,
                    "userId": user_id,
                    "email": email,
                    "paperUrl": paper_url or "",
                    "title": title or "",
                    "createdAt": now,
                }
            )
            added += 1
        except Exception:
            continue

        # Also upsert into the global pool
        try:
            extracted_emails().update_one(
                {"email": email},
                {
                    "$set": {
                        "email": email,
                        "lastSeenAt": now,
                        "lastPaperUrl": paper_url or "",
                        "lastTitle": title or "",
                    },
                    "$setOnInsert": {"createdAt": now},
                    "$inc": {"seenCount": 1},
                },
                upsert=True,
            )
        except Exception:
            pass

    if added:
        # Refresh count from source of truth
        total = job_emails().count_documents({"jobId": job_id})
        update_job_progress(job_id, emails_collected=total)
    return added


def complete_app_job(job_id, *, success: bool = True, stage: str = "Done"):
    update_job_progress(
        job_id,
        status="complete" if success else "failed",
        stage=stage,
        progress=100 if success else None,
    )



def get_already_processed_pdfs(job_id) -> set:
    """Return set of paperUrl values already stored for this job (checkpoint)."""
    urls = set()
    for doc in job_emails().find({"jobId": job_id}, {"paperUrl": 1}):
        u = (doc.get("paperUrl") or "").strip()
        if u:
            urls.add(u)
    # Also read processedPdfUrls array if present on the job
    job = extraction_jobs().find_one({"_id": job_id}, {"processedPdfUrls": 1, "papersProcessed": 1})
    if job:
        for u in job.get("processedPdfUrls") or []:
            if u:
                urls.add(u)
    return urls


def mark_pdf_processed(job_id, pdf_url: str):
    """Append pdf_url to processedPdfUrls on the job (idempotent-ish)."""
    if not pdf_url:
        return
    extraction_jobs().update_one(
        {"_id": job_id},
        {
            "$addToSet": {"processedPdfUrls": pdf_url},
            "$set": {"lastHeartbeatAt": datetime.now(timezone.utc), "currentUrl": pdf_url},
        },
    )



def get_journal_seed_urls(journal: str, user_id) -> Dict[str, List[str]]:
    """
    Resolve listing / PDF seed URLs for a journal (built-in or custom).
    """
    journal = (journal or "").lower().strip()
    result = {"listingUrls": [], "pdfUrls": [], "samplePaperUrls": []}

    if journal in ("ijddt", "ijetrm"):
        # Built-in: return known listing pages; scraper will expand.
        if journal == "ijetrm":
            result["listingUrls"] = [
                "https://ijetrm.com/issue/?volume=current",
                "https://ijetrm.com/issue/?volume=February~2026",
                "https://ijetrm.com/issue/?volume=March~2026",
            ]
        else:
            # IJDDT — workers will use generic discovery / known seeds
            result["listingUrls"] = ["https://ijddt.com/"]
        return result

    # Custom user journal
    if user_id:
        uj = app_db()[USER_JOURNALS].find_one(
            {"userId": user_id, "slug": journal, "status": "ready"}
        )
        if uj:
            result["listingUrls"] = uj.get("listingUrls") or ([uj["seedUrl"]] if uj.get("seedUrl") else [])
            result["pdfUrls"] = uj.get("pdfUrls") or []
            result["samplePaperUrls"] = uj.get("samplePaperUrls") or []
    return result
