"""
Hunt fleet loop (topic + author email). Killswitch: HUNT_ENABLED.

OpenAlex discovery is DISABLED. Primary source = PKP Beacon journals in Mongo
(huntedjournals.source == pkp_beacon).

Role split by worker number:
  worker_num % 3 == 1 → hunter (claim pending PKP journal, crawlability sample)
  worker_num % 3 == 2 → crawler (homepage/OJS issue crawl → PDF queue; no OpenAlex)
  worker_num % 3 == 0 → topicker (PDF → title/topic + emails)
"""
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("scholarreach.hunt")

# Hard off — do not discover/call OpenAlex for hunt
OPENALEX_HUNT_ENABLED = False


def role_for(worker_num: int) -> str:
    m = int(worker_num or 1) % 3
    if m == 1:
        return "hunter"
    if m == 2:
        return "crawler"
    return "topicker"


def _claim_pending_pkp(worker_id: str):
    """Atomically claim one pending PKP Beacon journal for crawlability check."""
    from src import hunt_db as hdb

    now = datetime.now(timezone.utc)
    return hdb.db()["huntedjournals"].find_one_and_update(
        {
            "source": "pkp_beacon",
            "status": "pending",
            "$or": [
                {"claimedBy": {"$exists": False}},
                {"claimedBy": None},
                {"claimExpires": {"$lt": now}},
            ],
        },
        {
            "$set": {
                "status": "sampling",
                "claimedBy": worker_id,
                "claimExpires": datetime.fromtimestamp(now.timestamp() + 1800, tz=timezone.utc),
                "updatedAt": now,
            }
        },
        sort=[("totalRecordCount", -1)],  # richer journals first
        return_document=True,
    )


def hunter_loop(worker_id: str, max_runtime: float, idle_sleep: int):
    """Validate PKP journals (no OpenAlex). Mark ready or rejected."""
    from src import validate as v
    from src import hunt_db as hdb

    sess = requests.Session()
    sess.headers.update({"User-Agent": "Scholarreach-Hunt/2.0 (PKP beacon hunter)"})
    start = time.time()
    done = 0
    while time.time() - start < max_runtime:
        job = _claim_pending_pkp(worker_id)
        if not job:
            time.sleep(idle_sleep)
            continue
        key = job.get("key")
        home = (job.get("homepageUrl") or "").strip()
        oai = (job.get("oaiUrl") or "").strip()
        name = (job.get("displayName") or key or "")[:80]
        alive = 0
        checked = 0
        try:
            # 1) homepage reachable?
            if home:
                try:
                    r = sess.get(home, timeout=25, allow_redirects=True)
                    if r.status_code < 400 and len(r.text or "") > 200:
                        alive += 1
                    checked += 1
                except Exception:
                    checked += 1
            # 2) OAI Identify
            if oai:
                try:
                    probe = oai if "verb=" in oai.lower() else (oai.rstrip("/") + "?verb=Identify")
                    r = sess.get(probe, timeout=25)
                    if r.status_code < 400 and ("OAI-PMH" in (r.text or "") or "Identify" in (r.text or "")):
                        alive += 1
                    checked += 1
                except Exception:
                    checked += 1
            # 3) light seed discover — any pdf-ish links?
            pdf_hits = 0
            if home:
                try:
                    from src.scraper import discover_from_seeds

                    papers = discover_from_seeds(
                        listing_urls=[home], pdf_urls=[], sample_paper_urls=[]
                    )
                    for p in (papers or [])[:15]:
                        checked += 1
                        u = p.get("pdf_url") or ""
                        if not u:
                            continue
                        try:
                            pr = v.probe_pdf(u, session=sess)
                        except Exception:
                            pr = {"alive": False}
                        if pr.get("alive"):
                            alive += 1
                            pdf_hits += 1
                        time.sleep(0.3)
                except Exception as e:
                    logger.debug("hunter %s discover %s: %s", worker_id, key, e)

            # Gate: homepage or OAI ok, or at least 2 live PDFs
            home_ok = checked > 0 and alive >= 1
            if home_ok or pdf_hits >= 2:
                status = "ready"
                reason = None
            else:
                status = "rejected"
                reason = f"pkp_sample alive={alive} checked={checked} pdfs={pdf_hits}"

            hdb.db()["huntedjournals"].update_one(
                {"key": key},
                {
                    "$set": {
                        "status": status,
                        "sampleChecked": checked,
                        "sampleAlive": alive,
                        "samplePdfHits": pdf_hits,
                        "rejectReason": reason,
                        "claimedBy": None,
                        "claimExpires": None,
                        "lastCheckedAt": datetime.now(timezone.utc),
                        "updatedAt": datetime.now(timezone.utc),
                    }
                },
            )
            done += 1
            logger.info("hunter %s %s → %s (%s)", worker_id, name, status, reason or "ok")
            time.sleep(0.8)
        except Exception as e:
            logger.exception("hunter %s %s failed: %s", worker_id, key, e)
            try:
                hdb.db()["huntedjournals"].update_one(
                    {"key": key},
                    {"$set": {"status": "pending", "claimedBy": None, "claimExpires": None}},
                )
            except Exception:
                pass
            time.sleep(2)
    return done


def crawler_loop(worker_id: str, max_runtime: float, idle_sleep: int):
    """Queue PDFs from OJS homepage/issues only — OpenAlex disabled."""
    from src import hunt_db as hdb

    assert not OPENALEX_HUNT_ENABLED, "OpenAlex must stay disabled for hunt"
    start = time.time()
    done = 0
    while time.time() - start < max_runtime:
        job = hdb.claim_hunting_journal(worker_id)
        if not job:
            time.sleep(idle_sleep)
            continue
        # Prefer PKP / skip openalex-sourced if any linger
        if (job.get("source") or "") == "openalex" and not job.get("forceCrawl"):
            logger.info("crawler %s skip openalex journal %s", worker_id, job.get("key"))
            time.sleep(0.5)
            continue
        key = job["key"]
        try:
            pdfs = []
            home = job.get("homepageUrl") or ""
            if home:
                try:
                    from src.scraper import discover_from_seeds

                    papers = discover_from_seeds(
                        listing_urls=[home], pdf_urls=[], sample_paper_urls=[]
                    )
                    for p in papers[:500]:
                        u = p.get("pdf_url")
                        if u and all(x.get("pdf_url") != u for x in pdfs):
                            pdfs.append(
                                {
                                    "pdf_url": u,
                                    "title": p.get("title") or "",
                                    "doi": p.get("doi") or "",
                                }
                            )
                except Exception as e:
                    logger.debug("crawler %s homepage discover failed %s: %s", worker_id, key, e)
            if pdfs:
                hdb.db()["huntedjournals"].update_one(
                    {"key": key},
                    {
                        "$set": {
                            "pdfQueue": [p.get("pdf_url") for p in pdfs[:500]],
                            "pdfQueueMeta": pdfs[:500],
                            "updatedAt": datetime.now(timezone.utc),
                        }
                    },
                )
                hdb.bump_counts(key, pdf_delta=len(pdfs[:500]))
                done += 1
                logger.info("crawler %s %s queued %d pdfs (PKP/OJS only)", worker_id, key, len(pdfs[:500]))
            else:
                time.sleep(2)
        except Exception as e:
            logger.exception("crawler %s %s failed: %s", worker_id, key, e)
            time.sleep(3)
    return done


def topicker_loop(worker_id: str, max_runtime: float, idle_sleep: int):
    from src import hunt_db as hdb
    from src.topic_extract import process_pdf_topic

    start = time.time()
    done = 0
    while time.time() - start < max_runtime:
        job = hdb.claim_journal_with_pdfs(worker_id) if hasattr(hdb, "claim_journal_with_pdfs") else None
        if not job:
            # fallback: find journal with non-empty pdfQueue
            now = datetime.now(timezone.utc)
            job = hdb.db()["huntedjournals"].find_one_and_update(
                {"pdfQueue.0": {"$exists": True}, "status": {"$in": ["ready", "crawling", "harvesting"]}},
                {"$set": {"topickerId": worker_id, "updatedAt": now}},
                return_document=True,
            )
        if not job:
            time.sleep(idle_sleep)
            continue
        key = job["key"]
        queue = list(job.get("pdfQueue") or [])
        meta = {m.get("pdf_url"): m for m in (job.get("pdfQueueMeta") or []) if m.get("pdf_url")}
        batch = queue[:20]
        added = 0
        for pdf_url in batch:
            try:
                m = meta.get(pdf_url) or {}
                out = process_pdf_topic(pdf_url)
                if not out:
                    continue
                emails = out.get("emails") or []
                if not emails and not out.get("topic"):
                    continue
                # require email for pool rows (product rule)
                if not emails:
                    continue
                ok = hdb.save_topic(
                    journal_key=key,
                    topic=out.get("topic") or out.get("title") or "untitled paper",
                    pdf_url=pdf_url,
                    doi=m.get("doi") or "",
                    source_page=job.get("homepageUrl") or "",
                    author_name=out.get("authorName"),
                    emails=emails,
                )
                if ok:
                    added += 1
            except Exception as e:
                logger.debug("topicker %s %s skip: %s", worker_id, pdf_url[-50:], str(e)[:80])
            time.sleep(0.5)
        try:
            hdb.db()["huntedjournals"].update_one(
                {"key": key}, {"$set": {"pdfQueue": queue[len(batch) :]}}
            )
            if added:
                hdb.bump_counts(key, topic_delta=added, email_delta=added)
                done += added
                logger.info("topicker %s %s +%d topic+email rows", worker_id, key, added)
            # mark dry when queue empty
            remaining = queue[len(batch) :]
            if not remaining:
                hdb.db()["huntedjournals"].update_one(
                    {"key": key},
                    {"$set": {"status": "dry", "dry": True, "updatedAt": datetime.now(timezone.utc)}},
                )
                logger.info("topicker %s %s marked dry", worker_id, key)
        except Exception:
            pass
    return done


def run_loop(max_runtime_seconds: int = 5 * 3600 + 1800, idle_sleep: int = 5):
    from src.config import HUNT_ENABLED

    if not HUNT_ENABLED:
        logger.warning(
            "HUNT_ENABLED is FALSE — journal hunt fleet idle. "
            "Set HUNT_ENABLED=true to resume PKP Beacon hunt."
        )
        return 0
    if OPENALEX_HUNT_ENABLED:
        logger.error("Refusing to run: OPENALEX_HUNT_ENABLED must be False")
        return 0

    from src import hunt_db as hdb

    try:
        hdb.ensure_hunt_indexes()
    except Exception as e:
        logger.warning("hunt indexes failed: %s", e)

    # Sync kill flags into Mongo for operators
    try:
        hdb.db()["systemsettings"].update_one(
            {"key": "hunt"},
            {
                "$set": {
                    "openalexEnabled": False,
                    "primarySource": "pkp_beacon",
                    "preferSourceOrder": ["pkp_beacon"],
                }
            },
            upsert=True,
        )
    except Exception:
        pass

    num = int(os.getenv("HUNT_WORKER_NUM") or "1")
    role = role_for(num)
    worker_id = (
        f"hunt-{os.getenv('GITHUB_RUN_ID', 'local')}-"
        f"{os.getenv('GITHUB_JOB', 'job')}-{uuid.uuid4().hex[:6]}-{role}"
    )
    logger.info("Hunt worker %s starting role=%s (num=%s) source=pkp_beacon openalex=OFF", worker_id, role, num)
    start = time.time()
    if role == "hunter":
        n = hunter_loop(worker_id, max_runtime_seconds - (time.time() - start), idle_sleep)
    elif role == "crawler":
        n = crawler_loop(worker_id, max_runtime_seconds - (time.time() - start), idle_sleep)
    else:
        n = topicker_loop(worker_id, max_runtime_seconds - (time.time() - start), idle_sleep)
    logger.info("Hunt worker %s role=%s done n=%d", worker_id, role, n)
    try:
        logger.info("Hunt stats: %s", hdb.hunt_stats())
    except Exception:
        pass
    return n
