"""
Core job processor used by GitHub Actions.

Priority:
1. Claim and process ExtractionJob documents created by the ScholarReach web UI
   (live progress written back so the user sees results in real time).
2. Fall back to the internal jobs collection (legacy / seed jobs).
"""
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from src import db
from src import app_jobs
from src.scraper import discover_papers
from src.extractor import process_pdf_url
from src.config import MAX_ATTEMPTS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("scholarreach.processor")


def process_app_extraction_job(job: dict, worker_id: str) -> None:
    """
    Full pipeline for a UI-created ExtractionJob:
    - Resolve seed / listing URLs for the journal
    - Discover PDF links
    - Download + extract title/emails
    - Push emails into jobemails / extractedemails
    - Continuously update progress/stage so the UI polls live data
    """
    job_id = job["_id"]
    journal = job.get("journal") or "ijddt"
    target = int(job.get("target") or 100)
    user_id = job.get("userId")

    app_jobs.update_job_progress(
        job_id,
        stage=f"Discovering papers for {journal}…",
        progress=2,
        status="running",
    )

    seeds = app_jobs.get_journal_seed_urls(journal, user_id)
    listing_urls = seeds.get("listingUrls") or []
    seed_pdfs = list(seeds.get("pdfUrls") or [])
    sample_urls = seeds.get("samplePaperUrls") or []

    # Fallback built-in listing pages when no custom seeds exist
    if not listing_urls and not seed_pdfs and not sample_urls:
        if journal == "ijetrm":
            listing_urls = [
                "https://ijetrm.com/issue/?volume=current",
                "https://ijetrm.com/issue/?volume=February~2026",
                "https://ijetrm.com/issue/?volume=March~2026",
            ]
        elif journal == "ijddt":
            listing_urls = ["https://ijddt.com/"]

    app_jobs.update_job_progress(
        job_id,
        stage=f"Discovering papers for {journal} (generic crawler)…",
        progress=5,
        current_url=(listing_urls[0] if listing_urls else ""),
    )

    from src.scraper import discover_from_seeds

    try:
        papers = discover_from_seeds(
            listing_urls=listing_urls,
            pdf_urls=seed_pdfs,
            sample_paper_urls=sample_urls,
        )
    except Exception as e:
        logger.exception("Discovery failed for job %s: %s", job_id, e)
        app_jobs.complete_app_job(
            job_id,
            success=False,
            stage=f"Discovery failed: {str(e)[:100]}",
        )
        return

    pdf_urls = []
    seen = set()
    for p in papers:
        u = p.get("pdf_url")
        if u and u not in seen:
            seen.add(u)
            pdf_urls.append(u)

    if not pdf_urls:
        app_jobs.complete_app_job(
            job_id,
            success=False,
            stage="No PDF links found for this journal",
        )
        return

    app_jobs.update_job_progress(
        job_id,
        stage=f"Found {len(pdf_urls)} PDFs — extracting emails…",
        progress=20,
        papers_processed=0,
    )

    emails_total = 0
    papers_done = 0
    errors = 0

    for idx, pdf_url in enumerate(pdf_urls):
        # Stop if we already hit the user's target
        if emails_total >= target:
            break

        try:
            app_jobs.update_job_progress(
                job_id,
                stage=f"Extracting paper {idx+1}/{len(pdf_urls)}…",
                current_url=pdf_url,
                progress=min(95, 20 + int(70 * (idx + 1) / max(len(pdf_urls), 1))),
                papers_processed=papers_done,
                emails_collected=emails_total,
            )

            result = process_pdf_url(pdf_url)
            title = result.get("title") or ""
            emails = result.get("emails") or []

            added = app_jobs.push_emails_for_job(job, emails, paper_url=pdf_url, title=title)
            emails_total += added
            papers_done += 1

            preview = ", ".join(emails[:3]) if emails else "(no emails)"
            app_jobs.update_job_progress(
                job_id,
                batch_preview=preview,
                emails_collected=emails_total,
                papers_processed=papers_done,
            )
            logger.info(
                "Job %s paper %s → +%d emails (total %d) title=%s",
                job_id, pdf_url[-40:], added, emails_total, (title or "")[:60],
            )
        except Exception as e:
            errors += 1
            logger.warning("Extract failed %s: %s", pdf_url, e)
            app_jobs.update_job_progress(job_id, error_message=str(e)[:200])
            continue

    # Final status
    final_stage = f"Done — {emails_total} emails from {papers_done} papers"
    if emails_total == 0:
        final_stage = f"Finished — no emails found ({papers_done} papers, {errors} errors)"
    app_jobs.complete_app_job(job_id, success=True, stage=final_stage)
    app_jobs.update_job_progress(
        job_id,
        progress=100,
        emails_collected=emails_total,
        papers_processed=papers_done,
    )
    logger.info("Completed app job %s → %s", job_id, final_stage)


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
    meta = job.get("meta") or {}
    if not result.get("title") and meta.get("title_hint"):
        result["title"] = meta["title_hint"]
    result["source_page"] = meta.get("source_page")
    result["doi"] = meta.get("doi")
    result["job_id"] = str(job["_id"])
    db.save_result(result)
    return result


def run_once(worker_id: str) -> bool:
    """
    ONLY process UI ExtractionJobs (from ScholarReach web app).
    Internal ijetrm/ijddt seed jobs are disabled — they were confusing the queue.
    """
    app_job = app_jobs.claim_next_app_job(worker_id)
    if not app_job:
        return False

    logger.info(
        "Claimed UI ExtractionJob %s journal=%s target=%s status=%s user=%s",
        app_job["_id"],
        app_job.get("journal"),
        app_job.get("target"),
        app_job.get("status"),
        app_job.get("userId"),
    )
    try:
        process_app_extraction_job(app_job, worker_id)
    except Exception as e:
        logger.exception("App job %s failed: %s", app_job["_id"], e)
        app_jobs.complete_app_job(
            app_job["_id"],
            success=False,
            stage=f"Failed: {str(e)[:120]}",
        )
    return True


def run_loop(max_runtime_seconds: int = 5 * 3600 + 1800, idle_sleep: int = 3):
    """
    Runs for the FULL max_runtime_seconds.
    Polls every idle_sleep seconds when the queue is empty.
    Never exits early just because there are no jobs.
    """
    worker_id = f"gha-{os.getenv('GITHUB_RUN_ID', 'local')}-{os.getenv('GITHUB_JOB', 'job')}-{uuid.uuid4().hex[:6]}"
    logger.info(
        "Worker %s starting (max runtime %ss, idle poll %ss)",
        worker_id,
        max_runtime_seconds,
        idle_sleep,
    )

    db.ensure_indexes()
    start = time.time()
    processed = 0
    last_status_log = 0

    while True:
        elapsed = time.time() - start
        if elapsed >= max_runtime_seconds:
            logger.info(
                "Reached max runtime (%.0fs). Processed %d jobs. Exiting.",
                elapsed,
                processed,
            )
            break

        had_work = run_once(worker_id)
        if had_work:
            processed += 1
        else:
            time.sleep(idle_sleep)

        if elapsed - last_status_log > 300:
            try:
                stats = db.count_by_status()
                logger.info(
                    "Internal queue (%.0fs, %d done): %s",
                    elapsed,
                    processed,
                    stats,
                )
            except Exception:
                pass
            last_status_log = elapsed

    try:
        stats = db.count_by_status()
        logger.info("Final internal queue: %s | processed=%d", stats, processed)
    except Exception:
        pass
    return processed
