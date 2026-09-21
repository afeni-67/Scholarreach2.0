"""
Hunt fleet loop (topic + author email). Killswitch: HUNT_ENABLED. processor.py untouched.

Role split by worker number (scales to any fleet size):
  worker_num % 3 == 1 → hunter (OpenAlex discover + validate)
  worker_num % 3 == 2 → crawler (direct PDF links per journal)
  worker_num % 3 == 0 → topicker (PDF → title/topic + emails; skip if no email)

20 workers/run × 2 overlapping waves ≈ 40 hunters/crawlers/topickers.
With 30 workers the split is exactly 10/10/10.
"""
import logging
import os
import time
import uuid
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("scholarreach.hunt")


def role_for(worker_num: int) -> str:
    m = int(worker_num or 1) % 3
    if m == 1:
        return "hunter"
    if m == 2:
        return "crawler"
    return "topicker"


def hunter_loop(worker_id: str, max_runtime: float, idle_sleep: int):
    from src import openalex as oa
    from src import validate as v
    from src import hunt_db as hdb
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Scholarreach-Hunt/1.0 (topic+email hunter)"})
    start = time.time()
    done = 0
    try:
        for src in oa.iter_oa_sources(per_page=30, max_pages=40):
            if time.time() - start >= max_runtime:
                break
            name = src.get("display_name") or "Untitled journal"
            homepage = src.get("homepage_url") or ""
            openalex_id = src.get("id") or ""
            key = "openalex:" + (openalex_id.split("/")[-1] if "/" in openalex_id else openalex_id or name[:40])
            try:
                works = oa.works_with_pdfs(openalex_id, per_page=10)
            except Exception as e:
                logger.debug("hunter %s openalex works failed %s: %s", worker_id, name[:40], e)
                continue
            if len(works) < 5:
                continue
            alive = 0
            with_topic = 0
            for w in works[:10]:
                try:
                    p = v.probe_pdf(w["pdf_url"], session=sess)
                except Exception:
                    p = {"alive": False, "reason": "probe_error"}
                if p.get("alive"):
                    alive += 1
                    if (w.get("title") or "").strip():
                        with_topic += 1
                time.sleep(0.4)
            # hunter rule: ≥7/10 alive AND ≥5/10 with OpenAlex title signal
            # (full topic proof happens in topickers; hunters only gate obvious duds)
            if alive >= 7 and with_topic >= 5:
                status = "ready"
                reason = None
            else:
                status = "rejected"
                reason = f"sample alive={alive}/10 topics={with_topic}/10"
            hdb.upsert_journal({
                "key": key,
                "displayName": str(name)[:160],
                "homepageUrl": str(homepage)[:500],
                "openalexId": str(openalex_id)[:120],
                "issn": src.get("issn") or [],
                "status": status,
                "sampleChecked": min(10, len(works)),
                "sampleAlive": alive,
                "sampleWithTopic": with_topic,
                "rejectReason": reason,
                "authorName": None,  # EDIT LATER
                "lastCheckedAt": __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            })
            done += 1
            logger.info("hunter %s %s → %s (%s)", worker_id, name[:50], status, reason or f"alive={alive}")
            time.sleep(1.0)
    except Exception as e:
        logger.exception("hunter %s failed: %s", worker_id, e)
    return done


def crawler_loop(worker_id: str, max_runtime: float, idle_sleep: int):
    from src import openalex as oa
    from src import hunt_db as hdb
    start = time.time()
    done = 0
    while time.time() - start < max_runtime:
        job = hdb.claim_hunting_journal(worker_id)
        if not job:
            time.sleep(idle_sleep)
            continue
        key = job["key"]
        try:
            pdfs = []
            # 1) OpenAlex direct pdf links (bot-friendly, no listing crawl)
            try:
                for w in oa.works_with_pdfs(job.get("openalexId") or "", per_page=50):
                    if w.get("pdf_url"):
                        pdfs.append(w)
            except Exception as e:
                logger.debug("crawler %s openalex failed %s: %s", worker_id, key, e)
            # 2) Homepage discovery for journals with crawlable listing pages
            try:
                from src.scraper import discover_from_seeds
                home = job.get("homepageUrl") or ""
                if home:
                    papers = discover_from_seeds(listing_urls=[home], pdf_urls=[], sample_paper_urls=[])
                    for p in papers[:200]:
                        u = p.get("pdf_url")
                        if u and all(x.get("pdf_url") != u for x in pdfs):
                            pdfs.append({"pdf_url": u, "title": p.get("title") or "", "doi": p.get("doi") or ""})
            except Exception as e:
                logger.debug("crawler %s homepage discover failed %s: %s", worker_id, key, e)
            # store queue inside journal doc (cap 300)
            if pdfs:
                hdb.db()["huntedjournals"].update_one(
                    {"key": key},
                    {"$set": {"pdfQueue": [p.get("pdf_url") for p in pdfs[:300]],
                              "pdfQueueMeta": pdfs[:300]}},
                )
                hdb.bump_counts(key, pdf_delta=len(pdfs[:300]))
                done += 1
                logger.info("crawler %s %s queued %d pdfs", worker_id, key, len(pdfs[:300]))
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
        job = hdb.claim_topic_journal()
        if not job:
            time.sleep(idle_sleep)
            continue
        key = job["key"]
        queue = job.get("pdfQueue") or []
        meta = {m.get("pdf_url"): m for m in (job.get("pdfQueueMeta") or []) if m.get("pdf_url")}
        if not queue:
            time.sleep(idle_sleep * 2)
            continue
        batch = queue[:10]
        added = 0
        for pdf_url in batch:
            if time.time() - start >= max_runtime:
                break
            try:
                out = process_pdf_topic(pdf_url)
                m = meta.get(pdf_url) or {}
                ok = hdb.save_topic(
                    key,
                    out.get("title") or m.get("title") or "Untitled paper",
                    out.get("topic") or m.get("title") or "Untitled paper",
                    pdf_url,
                    doi=m.get("doi") or "",
                    source_page=job.get("homepageUrl") or "",
                    author_name=out.get("authorName"),
                    emails=out.get("emails") or [],
                )
                if ok:
                    added += 1
            except Exception as e:
                logger.debug("topicker %s %s skip: %s", worker_id, pdf_url[-50:], str(e)[:80])
            time.sleep(0.5)
        # pop processed batch
        try:
            hdb.db()["huntedjournals"].update_one(
                {"key": key}, {"$set": {"pdfQueue": queue[len(batch):]}})
            if added:
                hdb.bump_counts(key, topic_delta=added, email_delta=added)
                done += added
                logger.info("topicker %s %s +%d topic+email rows", worker_id, key, added)
        except Exception:
            pass
    return done


def run_loop(max_runtime_seconds: int = 5 * 3600 + 1800, idle_sleep: int = 5):
    """Killswitch: HUNT_ENABLED=false stops the entire journal hunt fleet."""
    from src.config import HUNT_ENABLED
    if not HUNT_ENABLED:
        logger.warning(
            "HUNT_ENABLED is FALSE — journal hunt fleet idle. "
            "Set HUNT_ENABLED=true on Render/Actions to resume automated hunt (Pro catalog supply)."
        )
        return 0
    from src import hunt_db as hdb
    try:
        hdb.ensure_hunt_indexes()
    except Exception as e:
        logger.warning("hunt indexes failed: %s", e)
    num = int(os.getenv("HUNT_WORKER_NUM") or "1")
    role = role_for(num)
    worker_id = f"hunt-{os.getenv('GITHUB_RUN_ID', 'local')}-{os.getenv('GITHUB_JOB', 'job')}-{uuid.uuid4().hex[:6]}-{role}"
    logger.info("Hunt worker %s starting role=%s (num=%s)", worker_id, role, num)
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
