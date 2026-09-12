from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from pymongo import MongoClient, ReturnDocument, ASCENDING
from pymongo.collection import Collection
from src.config import (
    MONGODB_URI,
    MONGODB_DB,
    JOBS_COLLECTION,
    RESULTS_COLLECTION,
    CLAIM_TIMEOUT_MINUTES,
    MAX_ATTEMPTS,
)

_client: Optional[MongoClient] = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
        # quick ping
        _client.admin.command("ping")
    return _client


def get_db():
    return get_client()[MONGODB_DB]


def jobs_col() -> Collection:
    return get_db()[JOBS_COLLECTION]


def results_col() -> Collection:
    return get_db()[RESULTS_COLLECTION]


def ensure_indexes():
    """Create indexes for efficient claiming and querying."""
    j = jobs_col()
    j.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
    j.create_index([("status", ASCENDING), ("claimed_at", ASCENDING)])
    j.create_index("url", unique=False)
    r = results_col()
    r.create_index("pdf_url", unique=True)
    r.create_index("extracted_at")


def enqueue_job(
    job_type: str,
    url: str,
    meta: Optional[Dict[str, Any]] = None,
    priority: int = 0,
) -> Dict[str, Any]:
    """Add a new job if it does not already exist as pending/processing."""
    now = datetime.now(timezone.utc)
    doc = {
        "type": job_type,  # "discover" | "extract"
        "url": url.strip(),
        "status": "pending",
        "attempts": 0,
        "priority": priority,
        "meta": meta or {},
        "created_at": now,
        "updated_at": now,
        "claimed_by": None,
        "claimed_at": None,
        "error": None,
        "result": None,
    }
    # Avoid exact duplicate pending jobs
    existing = jobs_col().find_one(
        {"url": doc["url"], "type": job_type, "status": {"$in": ["pending", "processing"]}}
    )
    if existing:
        return existing
    res = jobs_col().insert_one(doc)
    doc["_id"] = res.inserted_id
    return doc


def claim_next_job(worker_id: str) -> Optional[Dict[str, Any]]:
    """
    Atomically claim the next pending job (or a stale processing job).
    Returns the claimed document or None.
    """
    now = datetime.now(timezone.utc)
    stale_before = now - timedelta(minutes=CLAIM_TIMEOUT_MINUTES)

    # First try a fresh pending job
    job = jobs_col().find_one_and_update(
        {
            "status": "pending",
            "attempts": {"$lt": MAX_ATTEMPTS},
        },
        {
            "$set": {
                "status": "processing",
                "claimed_by": worker_id,
                "claimed_at": now,
                "updated_at": now,
            },
            "$inc": {"attempts": 1},
        },
        sort=[("priority", -1), ("created_at", 1)],
        return_document=ReturnDocument.AFTER,
    )
    if job:
        return job

    # Reclaim stale processing jobs
    job = jobs_col().find_one_and_update(
        {
            "status": "processing",
            "claimed_at": {"$lt": stale_before},
            "attempts": {"$lt": MAX_ATTEMPTS},
        },
        {
            "$set": {
                "status": "processing",
                "claimed_by": worker_id,
                "claimed_at": now,
                "updated_at": now,
            },
            "$inc": {"attempts": 1},
        },
        sort=[("claimed_at", 1)],
        return_document=ReturnDocument.AFTER,
    )
    return job


def complete_job(job_id, result: Optional[Dict] = None, error: Optional[str] = None):
    now = datetime.now(timezone.utc)
    update: Dict[str, Any] = {"updated_at": now}
    if error:
        update["status"] = "failed"
        update["error"] = error
    else:
        update["status"] = "done"
        update["result"] = result
        update["error"] = None
    jobs_col().update_one({"_id": job_id}, {"$set": update})


def save_result(doc: Dict[str, Any]) -> None:
    """Upsert a paper result by pdf_url."""
    now = datetime.now(timezone.utc)
    doc = {**doc, "extracted_at": now, "updated_at": now}
    results_col().update_one(
        {"pdf_url": doc["pdf_url"]},
        {"$set": doc},
        upsert=True,
    )


def count_by_status() -> Dict[str, int]:
    pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    return {r["_id"]: r["count"] for r in jobs_col().aggregate(pipeline)}
