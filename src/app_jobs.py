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
                {"runner": ""},
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
        # Charge quota by REAL emails only (WeeklyUsage in app DB)
        try:
            from datetime import datetime as _dt
            week = _dt.utcnow().strftime("%Y-%m")
            app_db()["weeklyusages"].update_one(
                {"userId": user_id, "week": week},
                {"$inc": {"emailsUsed": added}, "$setOnInsert": {"userId": user_id, "week": week}},
                upsert=True,
            )
        except Exception as e:
            logger = __import__("logging").getLogger(__name__)
            logger.warning("quota increment failed: %s", e)
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
    raw = (journal or "").strip()
    journal = raw.lower()
    result = {"listingUrls": [], "pdfUrls": [], "samplePaperUrls": []}

    if journal in ("ijddt", "ijetrm"):
        if journal == "ijetrm":
            result["listingUrls"] = [
                "https://ijetrm.com/issue/?volume=current",
                "https://ijetrm.com/issue/?volume=February~2026",
                "https://ijetrm.com/issue/?volume=March~2026",
            ]
        else:
            result["listingUrls"] = ["https://ijddt.com/"]
        return result

    # Direct URL pasted as journal id
    if raw.startswith("http://") or raw.startswith("https://"):
        result["listingUrls"] = [raw]
        return result

    coll = app_db()[USER_JOURNALS]

    def _from_uj(uj):
        if not uj:
            return
        listings = uj.get("listingUrls") or []
        seed = uj.get("seedUrl")
        if seed and seed not in listings:
            listings = [seed] + list(listings)
        result["listingUrls"] = listings
        result["pdfUrls"] = uj.get("pdfUrls") or []
        result["samplePaperUrls"] = uj.get("samplePaperUrls") or []

    # Custom user journal — tolerate ObjectId / str userId and case variants
    if user_id is not None:
        uid_candidates = [user_id]
        try:
            from bson import ObjectId
            if isinstance(user_id, str) and ObjectId.is_valid(user_id):
                uid_candidates.append(ObjectId(user_id))
            elif isinstance(user_id, ObjectId):
                uid_candidates.append(str(user_id))
        except Exception:
            pass

        for uid in uid_candidates:
            uj = coll.find_one({"userId": uid, "slug": raw})
            if not uj:
                uj = coll.find_one({"userId": uid, "slug": journal})
            if not uj:
                # regex slug (ignore case)
                uj = coll.find_one({"userId": uid, "slug": {"$regex": f"^{re.escape(raw)}$", "$options": "i"}})
            if uj:
                _from_uj(uj)
                break

    # Fallback: any journal with this slug (ready or not)
    if not result["listingUrls"]:
        uj = coll.find_one({"slug": raw}) or coll.find_one({"slug": journal})
        if not uj:
            uj = coll.find_one({"slug": {"$regex": f"^{re.escape(raw)}$", "$options": "i"}})
        _from_uj(uj)

    # Reconstruct from custom-* slug if still empty (e.g. custom-https-isjem-com-past-issues-xxx)
    if not result["listingUrls"] and journal.startswith("custom-"):
        body = journal[len("custom-") :]
        # drop random suffix after last short token if present
        parts = body.split("-")
        # try https://host/path reconstruction
        if len(parts) >= 2 and parts[0] in ("https", "http"):
            scheme = parts[0]
            # last segment is often random id (qgct) — drop if short
            if parts[-1] and len(parts[-1]) <= 6 and parts[-1].isalnum():
                parts = parts[:-1]
            host = parts[1] if len(parts) > 1 else ""
            path_parts = parts[2:]
            path = "/" + "/".join(path_parts) if path_parts else "/"
            if not path.endswith("/") and "issue" in path:
                path += "/"
            guess = f"{scheme}://{host}{path}"
            if host:
                result["listingUrls"] = [guess]
                # common fix: isjem.com past-issues
                if "isjem" in host and "past" in path:
                    result["listingUrls"] = ["https://isjem.com/past-issues/"]

    return result
