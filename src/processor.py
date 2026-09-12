"""
Core job processor used by GitHub Actions (and optionally locally).
"""
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from src import db
from src.scraper import discover_papers
from src.extractor import process_pdf_url
from src.config import MAX_ATTEMPTS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("scholarreach.processor")


def process_discover(job: dict) -> dict:
    url = job["url"]
    papers = discover_papers(url)
    enqueued = 0
    for p in papers:
        pdf = p.get("pdf_url")
        if not pdf:
            continue
        db.enqueue_job(
            job_type="extract",
            url=pdf,
            meta={
                "title_hint": p.get("title"),
                "authors_hint": p.get("authors"),
                "doi": p.get("doi"),
                "source_page": url,
                "discovery_job_id": str(job["_id"]),
            },
            priority=1,
        )
        enqueued += 1
    return {
        "discovered": len(papers),
        "enqueued_extract": enqueued,
        "source_url": url,
    }


def process_extract(job: dict) -> dict:
    pdf_url = job["url"]
    result = process_pdf_url(pdf_url)
    # Enrich with discovery hints if present
    meta = job.get("meta") or {}
    if not result.get("title") and meta.get("title_hint"):
        result["title"] = meta["title_hint"]
    result["source_page"] = meta.get("source_page")
    result["doi"] = meta.get("doi")
    result["job_id"] = str(job["_id"])

    db.save_result(result)
    return result


def run_once(worker_id: str) -> bool:
    """Claim and process a single job. Returns True if a job was processed."""
    job = db.claim_next_job(worker_id)
    if not job:
        logger.info("No pending jobs")
        return False

    jid = job["_id"]
    jtype = job.get("type")
    logger.info("Claimed job %s type=%s url=%s (attempt %s)", jid, jtype, job["url"], job.get("attempts"))

    try:
        if jtype == "discover":
            result = process_discover(job)
        elif jtype == "extract":
            result = process_extract(job)
        else:
            raise ValueError(f"Unknown job type: {jtype}")
        db.complete_job(jid, result=result)
        logger.info("Completed job %s → %s", jid, {k: result.get(k) for k in ("title", "emails", "discovered", "enqueued_extract") if k in result})
        return True
    except Exception as e:
        logger.exception("Job %s failed: %s", jid, e)
        # If max attempts reached it will stay failed; otherwise next claim can retry
        if job.get("attempts", 1) >= MAX_ATTEMPTS:
            db.complete_job(jid, error=str(e))
        else:
            # put back to pending so another worker (or later run) can retry
            db.jobs_col().update_one(
                {"_id": jid},
                {
                    "$set": {
                        "status": "pending",
                        "error": str(e),
                        "updated_at": datetime.now(timezone.utc),
                        "claimed_by": None,
                        "claimed_at": None,
                    }
                },
            )
        return True  # we did work (even if failed)


def run_loop(max_runtime_seconds: int = 5 * 3600 + 1800, idle_sleep: int = 3):
    """
    Main loop for a GitHub Actions job.

    - Runs for the FULL max_runtime_seconds (never exits early just because queue is empty).
    - Polls every `idle_sleep` seconds (default 3s) when no work is available.
    - Multiple parallel workers can safely share the same queue thanks to atomic claiming.
    """
    worker_id = f"gha-{os.getenv('GITHUB_RUN_ID', 'local')}-{os.getenv('GITHUB_JOB', 'job')}-{uuid.uuid4().hex[:6]}"
    logger.info("Worker %s starting (max runtime %ss, idle poll %ss)", worker_id, max_runtime_seconds, idle_sleep)

    db.ensure_indexes()
    start = time.time()
    processed = 0
    last_status_log = 0

    while True:
        elapsed = time.time() - start
        if elapsed >= max_runtime_seconds:
            logger.info("Reached max runtime (%.0fs). Processed %d jobs. Exiting.", elapsed, processed)
            break

        had_work = run_once(worker_id)
        if had_work:
            processed += 1
        else:
            # No job available right now — wait a short time then check again.
            # We NEVER exit early; we stay alive for the full runtime.
            time.sleep(idle_sleep)

        # Log queue stats every ~5 minutes so we can see activity
        if elapsed - last_status_log > 300:
            try:
                stats = db.count_by_status()
                logger.info("Queue status (%.0fs elapsed, %d processed): %s", elapsed, processed, stats)
            except Exception:
                pass
            last_status_log = elapsed

    stats = db.count_by_status()
    logger.info("Final queue stats: %s | Total processed this worker: %d", stats, processed)
    return processed
