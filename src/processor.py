"""
Core job processor used by GitHub Actions.

Priority:
1. Claim and process ExtractionJob documents created by the ScholarReach web UI
   (live progress written back so the user sees results in real time).
2. Fall back to the internal jobs collection (legacy / seed jobs).
"""
import logging
import threading
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    Stream pipeline for UI ExtractionJob:
    - Resolve seeds (archive/current fallbacks)
    - BFS discover → extract each PDF immediately (do not wait for full journal)
    - Hard stop when emails_collected >= target (plan limit)
    - Checkpoint last paper + processed URLs for resume on next claim
    - Fail fast if no PDFs after a short probe
    """
    job_id = job["_id"]
    journal = job.get("journal") or "ijddt"
    target = int(job.get("target") or 100)
    user_id = job.get("userId")

    app_jobs.update_job_progress(
        job_id,
        stage=f"Streaming extract for {journal}…",
        progress=2,
        status="running",
    )

    seeds = app_jobs.get_journal_seed_urls(journal, user_id)
    listing_urls = list(seeds.get("listingUrls") or [])
    seed_pdfs = list(seeds.get("pdfUrls") or [])
    sample_urls = list(seeds.get("samplePaperUrls") or [])
    logger.info(
        "Job %s seeds listing=%s pdfs=%d samples=%d target=%d",
        job_id, listing_urls[:3], len(seed_pdfs), len(sample_urls), target,
    )

    if not listing_urls and not seed_pdfs and not sample_urls:
        if journal == "ijetrm":
            listing_urls = [
                "https://ijetrm.com/issue/?volume=current",
                "https://ijetrm.com/issue/?volume=February~2026",
            ]
        elif journal == "ijddt":
            listing_urls = ["https://ijddt.com/"]
        elif "isjem" in (journal or "").lower():
            listing_urls = ["https://isjem.com/past-issues/"]

    expanded = list(listing_urls)
    for base in list(listing_urls):
        b = (base or "").rstrip("/")
        if not b or "/issue/" in b:
            continue
        for suffix in (
            "/issue/archive",
            "/issue/current",
            "/index.php/issue/archive",
            "/index.php/issue/current",
        ):
            cand = b + suffix
            if cand not in expanded:
                expanded.append(cand)
    listing_urls = expanded

    already = app_jobs.get_user_journal_processed_pdfs(user_id, journal, job_id=job_id)
    existing_emails = int(job.get("emailsCollected") or 0)
    existing_papers = int(job.get("papersProcessed") or 0)
    emails_total = existing_emails
    papers_done = existing_papers
    errors = 0
    found_any_pdf = False
    checkpoint_urls = list(already)[:3000]
    last_pdf = job.get("lastPaperUrl") or job.get("currentUrl") or ""

    pending_seed_pdfs = []
    for u in seed_pdfs + sample_urls:
        if u and u not in already:
            pending_seed_pdfs.append(u)

    from src.scraper import iter_generic_discover

    def _extract_one(pdf_url: str):
        try:
            result = process_pdf_url(pdf_url)
            return pdf_url, result, None
        except Exception as e:
            return pdf_url, None, e

    def _checkpoint(pdf_url: str, stage: str, progress: float):
        nonlocal checkpoint_urls, last_pdf
        last_pdf = pdf_url
        if pdf_url not in checkpoint_urls:
            checkpoint_urls.append(pdf_url)
        app_jobs.mark_pdf_processed(job_id, pdf_url, user_id=user_id, journal=journal)
        app_jobs.update_job_progress(
            job_id,
            stage=stage,
            current_url=pdf_url,
            progress=min(95, progress),
            papers_processed=papers_done,
            emails_collected=emails_total,
        )
        try:
            app_jobs.extraction_jobs().update_one(
                {"_id": job_id},
                {"$set": {
                    "lastPaperUrl": pdf_url,
                    "lastProcessedPdfUrl": pdf_url,
                    "updatedAt": datetime.now(timezone.utc),
                }},
            )
        except Exception:
            pass
        if papers_done % 5 == 0:
            app_jobs.save_journal_progress(
                user_id,
                journal,
                discovered_pdfs=checkpoint_urls[-500:],
                processed_pdf=pdf_url,
                listing_urls=listing_urls[:50] if listing_urls else None,
            )

    stop_hb = {"v": False}

    def _hb_loop():
        while not stop_hb["v"]:
            try:
                app_jobs.update_job_progress(
                    job_id,
                    stage=f"Streaming extract… {emails_total}/{target} emails, {papers_done} papers",
                    emails_collected=emails_total,
                    papers_processed=papers_done,
                    current_url=last_pdf or "",
                )
            except Exception:
                pass
            time.sleep(25)

    hb_thread = threading.Thread(target=_hb_loop, daemon=True)
    hb_thread.start()

    try:
        for pdf_url in pending_seed_pdfs:
            if emails_total >= target:
                break
            found_any_pdf = True
            pdf_url, result, err = _extract_one(pdf_url)
            if err is not None:
                errors += 1
                app_jobs.mark_pdf_processed(job_id, pdf_url, user_id=user_id, journal=journal)
                continue
            title = (result or {}).get("title") or ""
            emails = (result or {}).get("emails") or []
            added = app_jobs.push_emails_for_job(job, emails, paper_url=pdf_url, title=title)
            emails_total += added
            papers_done += 1
            _checkpoint(
                pdf_url,
                stage=f"Extracting… {emails_total}/{target} emails ({papers_done} papers)",
                progress=5 + 90 * min(emails_total, target) / max(target, 1),
            )

        if emails_total < target and listing_urls:
            pdf_iter = iter_generic_discover(
                listing_urls,
                max_pages=400,
                max_depth=6,
                max_pdfs=max(target * 10, 500),
                skip_pdfs=set(already) | set(pending_seed_pdfs),
            )
            for paper in pdf_iter:
                if emails_total >= target:
                    break
                pdf_url = (paper or {}).get("pdf_url") or ""
                if not pdf_url or pdf_url in already:
                    continue
                found_any_pdf = True
                pdf_url, result, err = _extract_one(pdf_url)
                already.add(pdf_url)
                if err is not None:
                    errors += 1
                    app_jobs.mark_pdf_processed(job_id, pdf_url, user_id=user_id, journal=journal)
                    if errors <= 5 or errors % 25 == 0:
                        logger.warning("Extract failed %s: %s", pdf_url, err)
                    continue
                title = (result or {}).get("title") or ""
                emails = (result or {}).get("emails") or []
                added = app_jobs.push_emails_for_job(job, emails, paper_url=pdf_url, title=title)
                emails_total += added
                papers_done += 1
                preview = ", ".join(emails[:3]) if emails else "(no emails)"
                _checkpoint(
                    pdf_url,
                    stage=f"Extracting… {emails_total}/{target} emails ({papers_done} papers) · {preview[:60]}",
                    progress=5 + 90 * min(emails_total, target) / max(target, 1),
                )
                if added:
                    logger.info(
                        "Job %s …%s → +%d emails (total %d/%d)",
                        job_id, pdf_url[-40:], added, emails_total, target,
                    )

        if not found_any_pdf and papers_done == 0 and emails_total == 0:
            app_jobs.complete_app_job(
                job_id,
                success=False,
                stage="No PDF links found after short probe (retryable — check seed / archive URL)",
            )
            app_jobs.update_job_progress(
                job_id,
                progress=100,
                emails_collected=0,
                papers_processed=0,
                status="failed",
            )
            logger.warning("Job %s fail-fast: no PDFs for %s", job_id, journal)
            return

        if emails_total >= target:
            final_stage = f"Done — {emails_total} emails (hit plan limit {target}) from {papers_done} papers"
        elif emails_total == 0:
            final_stage = f"Finished — no emails found ({papers_done} papers, {errors} errors)"
        else:
            final_stage = f"Done — {emails_total} emails from {papers_done} papers (stream exhausted)"

        app_jobs.save_journal_progress(
            user_id,
            journal,
            discovered_pdfs=checkpoint_urls[-500:],
            listing_urls=listing_urls[:50] if listing_urls else None,
        )
        app_jobs.complete_app_job(job_id, success=True, stage=final_stage)
        app_jobs.update_job_progress(
            job_id,
            progress=100,
            emails_collected=emails_total,
            papers_processed=papers_done,
            current_url=last_pdf or "",
        )
        logger.info("Completed app job %s → %s", job_id, final_stage)
    finally:
        stop_hb["v"] = True



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

        if elapsed - last_status_log > 60:
            remaining = max_runtime_seconds - elapsed
            logger.info(
                "Listening for jobs… elapsed=%.0fs remaining=%.0fs ui_jobs_done=%d (poll every %ss)",
                elapsed,
                remaining,
                processed,
                idle_sleep,
            )
            last_status_log = elapsed

    try:
        stats = db.count_by_status()
        logger.info("Final internal queue: %s | processed=%d", stats, processed)
    except Exception:
        pass
    return processed
