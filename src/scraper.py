"""
Journal page scrapers — generic discovery for any open-access journal.

Supports:
- IJETRM (citation_pdf_url meta)
- OJS (article/view, article/download)
- DSpace / EPrints / handle-based repos
- Direct PDF links
- Article pages that embed or link to PDFs
- Paginated archive / issue listings
- Custom user journals (listingUrls + samplePaperUrls from UserJournal)
"""
import logging
import time
import re
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, urlparse, urldefrag
from collections import deque

import requests
from bs4 import BeautifulSoup

from src.config import USER_AGENT, REQUEST_TIMEOUT, REQUEST_SLEEP

logger = logging.getLogger(__name__)

MAX_PAGES = 80
MAX_DEPTH = 3
MAX_PDFS = 400


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "close",  # avoid sticky broken keep-alives on flaky hosts
        }
    )
    # Limit pool size so 20 workers don't stampede one origin
    adapter = requests.adapters.HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _get(url: str, session: Optional[requests.Session] = None, retries: int = 3) -> str:
    """Fetch with retries + exponential backoff. Raises on final failure."""
    sess = session or _session()
    last_err = None
    for attempt in range(retries):
        try:
            resp = sess.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
            time.sleep(REQUEST_SLEEP)
            return resp.text
        except Exception as e:
            last_err = e
            wait = min(20, (2 ** attempt) + 0.5)
            logger.warning("Fetch attempt %d/%d failed %s: %s — retry in %.1fs", attempt + 1, retries, url, e, wait)
            time.sleep(wait)
    raise last_err  # type: ignore


def ojs_download_candidates(article_view_url: str) -> List[str]:
    """
    OJS: /article/view/1234  →  try /article/download/1234 and /article/download/1234/XXXX
    Many journals (including cspub-ijcisim) serve PDFs this way without needing the HTML page.
    """
    m = re.search(r"(article/view/)(\d+)(?:/(\d+))?", article_view_url, re.I)
    if not m:
        return []
    base = article_view_url[: m.start(1)]
    article_id = m.group(2)
    galley_id = m.group(3)
    out = [
        f"{base}article/download/{article_id}",
        f"{base}article/view/{article_id}",
    ]
    if galley_id:
        out.insert(0, f"{base}article/download/{article_id}/{galley_id}")
        out.insert(1, f"{base}article/download/{article_id}/{galley_id}/pdf")
    else:
        # common second-path pattern when galley id is unknown
        out.append(f"{base}article/download/{article_id}/pdf")
    return out


def _origin(url: str) -> str:
    try:
        return urlparse(url).scheme + "://" + urlparse(url).netloc
    except Exception:
        return ""


def _resolve(href: str, base: str) -> Optional[str]:
    if not href:
        return None
    href = href.replace("&amp;", "&").strip()
    if href.startswith(("mailto:", "javascript:", "#")):
        return None
    try:
        full = urljoin(base, href)
        full, _ = urldefrag(full)
        return full
    except Exception:
        return None


def _same_site(url: str, origin: str) -> bool:
    try:
        return urlparse(url).netloc == urlparse(origin).netloc
    except Exception:
        return False


def classify_link(url: str, text_hint: str = "") -> Optional[str]:
    """
    Returns 'pdf' | 'article' | 'listing' | None
    Covers OJS, DSpace, EPrints, handle repos, generic patterns.
    """
    u = (url or "").lower()
    t = (text_hint or "").lower()

    # Direct PDF
    if re.search(r"\.pdf(\?|$)", u, re.I):
        if re.search(r"/cert/|certificate", u, re.I):
            return None
        return "pdf"

    # OJS / download endpoints
    if re.search(r"article/download/|/download/\d+", u, re.I):
        return "pdf"
    if re.search(r"/pdf/", u, re.I) and not re.search(r"journal", u, re.I):
        return "pdf"

    # DSpace / EPrints bitstream
    if re.search(r"/bitstream/", u, re.I) and not re.search(r"/cert/", u, re.I):
        return "pdf"
    if re.search(r"/xmlui/bitstream/|/jspui/bitstream/", u, re.I):
        return "pdf"

    # Fulltext / files that look like PDFs
    if re.search(r"/fulltext/", u, re.I) and re.search(r"\.(pdf|doc)", u, re.I):
        return "pdf"
    if re.search(r"/files?/\d+", u, re.I) and re.search(r"\.(pdf|doc)", u, re.I):
        return "pdf"

    # Article HTML pages
    if re.search(r"/show-\d+-\d+", u, re.I):
        return "article"
    if re.search(r"article/view/", u, re.I):
        return "article"
    if re.search(r"/articles?/", u, re.I) and not re.search(r"archive|issue|volume|list-", u, re.I):
        return "article"
    if "abstract" in u and re.search(r"view|full", u, re.I):
        return "article"

    # DSpace / EPrints handles
    if re.search(r"/handle/\d+/\d+", u, re.I):
        return "article"
    if re.search(r"/xmlui/handle/|/jspui/handle/", u, re.I):
        return "article"
    if re.search(r"/eprints?/\d+|/id/eprint/\d+", u, re.I):
        return "article"

    # Generic record / paper patterns
    if re.search(r"/record/\d+", u, re.I) and not re.search(r"archive|issue", u, re.I):
        return "article"
    if re.search(r"/pub/\d+|/paper/\d+", u, re.I):
        return "article"
    if re.search(r"/publication/\d+", u, re.I) and not re.search(r"list|archive", u, re.I):
        return "article"
    if re.search(r"/items?/", u, re.I) and re.search(r"\d+", u) and not re.search(r"archive|listing", u, re.I):
        return "article"
    if re.search(r"/works?/\d+|/node/\d+", u, re.I) and not re.search(r"archive|issue|volume", u, re.I):
        return "article"

    # Listing / archive / issue pages
    if re.search(r"issue/(view|archive|current)", u, re.I):
        return "listing"
    if re.search(r"/archive", u, re.I):
        return "listing"
    if re.search(r"list-\d+", u, re.I):
        return "listing"
    if re.search(r"/issue[s]?/", u, re.I) or re.search(r"/volume[s]?/", u, re.I):
        return "listing"
    if re.search(r"browse|viewall|all-issues|past-issues|current-issue", u, re.I):
        return "listing"
    if "issue" in t and ("view" in t or "archive" in t or "list" in t):
        return "listing"

    return None


def extract_links_from_html(html: str, base_url: str) -> List[Dict[str, str]]:
    """Return list of {url, text, kind} from a page."""
    soup = BeautifulSoup(html, "lxml")
    origin = _origin(base_url)
    out = []
    seen: Set[str] = set()

    # Meta citation_pdf_url (IJETRM and many OA journals)
    for meta in soup.find_all("meta", attrs={"name": re.compile(r"citation_pdf_url", re.I)}):
        pdf = (meta.get("content") or "").strip()
        if pdf and pdf not in seen:
            seen.add(pdf)
            out.append({"url": pdf, "text": "", "kind": "pdf"})

    for a in soup.find_all("a", href=True):
        full = _resolve(a["href"], base_url)
        if not full or full in seen:
            continue
        # Prefer same-site, but allow absolute PDF on CDNs
        kind = classify_link(full, a.get_text(" ", strip=True))
        if not kind:
            continue
        if kind != "pdf" and not _same_site(full, origin):
            continue
        seen.add(full)
        out.append(
            {
                "url": full,
                "text": a.get_text(" ", strip=True)[:200],
                "kind": kind,
            }
        )
    return out


def extract_pdfs_from_article_page(html: str, base_url: str) -> List[str]:
    """From an article HTML page, find the actual PDF link(s)."""
    soup = BeautifulSoup(html, "lxml")
    pdfs = []
    seen: Set[str] = set()

    for meta in soup.find_all("meta", attrs={"name": re.compile(r"citation_pdf_url", re.I)}):
        pdf = (meta.get("content") or "").strip()
        if pdf and pdf not in seen:
            seen.add(pdf)
            pdfs.append(pdf)

    for a in soup.find_all("a", href=True):
        full = _resolve(a["href"], base_url)
        if not full or full in seen:
            continue
        if classify_link(full, a.get_text(" ", strip=True)) == "pdf":
            seen.add(full)
            pdfs.append(full)

    # iframe / embed src that look like PDFs
    for tag in soup.find_all(["iframe", "embed", "object"]):
        src = tag.get("src") or tag.get("data")
        full = _resolve(src, base_url) if src else None
        if full and full not in seen and re.search(r"\.pdf|download|bitstream", full, re.I):
            seen.add(full)
            pdfs.append(full)

    return pdfs


def scrape_ijetrm_issue(issue_url: str) -> List[Dict[str, Any]]:
    html = _get(issue_url)
    soup = BeautifulSoup(html, "lxml")
    papers: List[Dict[str, Any]] = []

    for meta in soup.find_all("meta", attrs={"name": "citation_pdf_url"}):
        pdf = (meta.get("content") or "").strip()
        if pdf and pdf.lower().endswith(".pdf"):
            papers.append(
                {
                    "title": None,
                    "authors": None,
                    "pdf_url": pdf,
                    "doi": None,
                    "source": "meta",
                    "page_url": issue_url,
                }
            )

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower():
            full = urljoin(issue_url, href)
            if not any(p["pdf_url"] == full for p in papers):
                papers.append(
                    {
                        "title": a.get_text(strip=True) or None,
                        "authors": None,
                        "pdf_url": full,
                        "doi": None,
                        "source": "anchor",
                        "page_url": issue_url,
                    }
                )

    # Deduplicate
    seen = set()
    unique = []
    for p in papers:
        if p["pdf_url"] not in seen:
            seen.add(p["pdf_url"])
            unique.append(p)
    logger.info("IJETRM issue %s → %d PDF links", issue_url, len(unique))
    return unique


def generic_discover(
    seed_urls: List[str],
    *,
    max_pages: int = MAX_PAGES,
    max_depth: int = MAX_DEPTH,
    max_pdfs: int = MAX_PDFS,
) -> List[Dict[str, Any]]:
    """
    BFS crawl from seed URLs. Collects PDF links and follows listing/article
    pages to discover more. Works for OJS, DSpace, EPrints, generic OA journals.
    """
    if not seed_urls:
        return []

    session = _session()
    queue: deque = deque()  # (url, depth)
    visited: Set[str] = set()
    pdfs: List[Dict[str, Any]] = []
    seen_pdfs: Set[str] = set()
    pages_fetched = 0

    for s in seed_urls:
        if s:
            queue.append((s, 0))

    while queue and pages_fetched < max_pages and len(pdfs) < max_pdfs:
        url, depth = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        try:
            html = _get(url, session)
            pages_fetched += 1
        except Exception as e:
            logger.warning("Fetch failed %s: %s", url, e)
            continue

        links = extract_links_from_html(html, url)

        # If this looks like an article page, also dig for embedded PDFs
        page_kind = classify_link(url)
        if page_kind == "article" or any(l["kind"] == "pdf" for l in links) is False:
            for pdf in extract_pdfs_from_article_page(html, url):
                if pdf not in seen_pdfs:
                    seen_pdfs.add(pdf)
                    pdfs.append(
                        {
                            "title": None,
                            "authors": None,
                            "pdf_url": pdf,
                            "doi": None,
                            "source": "article_page",
                            "page_url": url,
                        }
                    )

        for link in links:
            kind = link["kind"]
            href = link["url"]
            if kind == "pdf":
                if href not in seen_pdfs:
                    seen_pdfs.add(href)
                    pdfs.append(
                        {
                            "title": link.get("text") or None,
                            "authors": None,
                            "pdf_url": href,
                            "doi": None,
                            "source": "link",
                            "page_url": url,
                        }
                    )
            elif kind == "article":
                # OJS: synthesize download URLs so we don't need to open every article HTML page
                for cand in ojs_download_candidates(href):
                    if "download" in cand and cand not in seen_pdfs:
                        seen_pdfs.add(cand)
                        pdfs.append(
                            {
                                "title": link.get("text") or None,
                                "authors": None,
                                "pdf_url": cand,
                                "doi": None,
                                "source": "ojs_synth",
                                "page_url": href,
                            }
                        )
                if depth < max_depth and href not in visited:
                    queue.append((href, depth + 1))
            elif kind == "listing" and depth < max_depth:
                if href not in visited:
                    queue.append((href, depth + 1))

        if pages_fetched % 10 == 0:
            logger.info(
                "Generic discover: pages=%d pdfs=%d queue=%d",
                pages_fetched,
                len(pdfs),
                len(queue),
            )

    logger.info(
        "Generic discover done: pages=%d pdfs=%d from %d seeds",
        pages_fetched,
        len(pdfs),
        len(seed_urls),
    )
    return pdfs[:max_pdfs]


def discover_papers(url: str) -> List[Dict[str, Any]]:
    """
    Dispatch: IJETRM-specific fast path, otherwise generic crawler.
    """
    domain = urlparse(url).netloc.lower()
    if "ijetrm.com" in domain:
        return scrape_ijetrm_issue(url)
    return generic_discover([url])


def discover_from_seeds(
    listing_urls: List[str],
    pdf_urls: Optional[List[str]] = None,
    sample_paper_urls: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    High-level entry for custom journals and multi-seed discovery.
    Merges direct PDF seeds + crawl results.
    """
    results: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    for u in pdf_urls or []:
        if u and u not in seen:
            seen.add(u)
            results.append(
                {
                    "title": None,
                    "authors": None,
                    "pdf_url": u,
                    "doi": None,
                    "source": "seed_pdf",
                    "page_url": u,
                }
            )

    for u in sample_paper_urls or []:
        if not u or u in seen:
            continue
        if re.search(r"\.pdf(\?|$)", u, re.I):
            seen.add(u)
            results.append(
                {
                    "title": None,
                    "authors": None,
                    "pdf_url": u,
                    "doi": None,
                    "source": "sample_pdf",
                    "page_url": u,
                }
            )
            continue

        # OJS article/view → try direct download URLs first (avoids flaky HTML fetches)
        ojs_urls = ojs_download_candidates(u)
        if ojs_urls:
            for cand in ojs_urls:
                if cand not in seen and "download" in cand:
                    seen.add(cand)
                    results.append(
                        {
                            "title": None,
                            "authors": None,
                            "pdf_url": cand,
                            "doi": None,
                            "source": "ojs_download",
                            "page_url": u,
                        }
                    )
            continue

        # Non-OJS article page: light crawl
        try:
            found = generic_discover([u], max_pages=3, max_depth=1, max_pdfs=10)
            for p in found:
                if p["pdf_url"] not in seen:
                    seen.add(p["pdf_url"])
                    results.append(p)
        except Exception as e:
            logger.warning("Sample paper crawl failed %s: %s", u, e)

    if listing_urls:
        crawled = generic_discover(listing_urls)
        for p in crawled:
            if p["pdf_url"] not in seen:
                seen.add(p["pdf_url"])
                results.append(p)

    return results
