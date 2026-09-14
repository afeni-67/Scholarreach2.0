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

MAX_PAGES = 200
MAX_DEPTH = 4
MAX_PDFS = 800


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


def _is_cloudflare_block(resp: requests.Response) -> bool:
    if resp.status_code in (403, 503):
        body = (resp.text or "")[:2000].lower()
        if "just a moment" in body or "cf-mitigated" in (resp.headers.get("cf-mitigated") or "").lower():
            return True
        if "cloudflare" in body and ("challenge" in body or "enable javascript" in body):
            return True
    return False


def _wayback_url(url: str) -> str:
    """Internet Archive snapshot proxy — bypasses Cloudflare for blocked OA journals."""
    # Prefer most recent available capture
    return f"https://web.archive.org/web/2/{url}"


def _unwrap_wayback(url: str) -> str:
    """Turn https://web.archive.org/web/2025id_/https://isjem.com/... into the original URL."""
    if not url or "web.archive.org" not in url:
        return url
    m = re.search(r"web\.archive\.org/web/\d+(?:id_|if_|[a-z]*_)?/(https?://.+)$", url, re.I)
    if m:
        return m.group(1)
    m = re.search(r"web\.archive\.org/web/\d+/(https?://.+)$", url, re.I)
    if m:
        return m.group(1)
    return url


def _get(url: str, session: Optional[requests.Session] = None, retries: int = 3) -> str:
    """Fetch with retries, Cloudflare detection, and Wayback Machine fallback."""
    sess = session or _session()
    last_err = None
    candidates = [url]
    # If already a wayback URL, don't nest
    if "web.archive.org" not in url:
        candidates.append(_wayback_url(url))

    for target in candidates:
        for attempt in range(retries):
            try:
                resp = sess.get(target, timeout=REQUEST_TIMEOUT + (15 if "web.archive.org" in target else 0), allow_redirects=True)
                if _is_cloudflare_block(resp):
                    logger.warning("Cloudflare block on %s — will try fallback", target)
                    last_err = RuntimeError(f"Cloudflare blocked {target}")
                    break  # try next candidate
                resp.raise_for_status()
                html = resp.text or ""
                if "just a moment" in html[:1500].lower() and "enable javascript" in html[:2000].lower():
                    logger.warning("CF challenge HTML on %s", target)
                    last_err = RuntimeError(f"Cloudflare challenge {target}")
                    break
                # Strip wayback toolbar noise slightly
                if "web.archive.org" in target:
                    html = re.sub(r"<!--\s*BEGIN WAYBACK TOOLBAR INSERT[\s\S]*?END WAYBACK TOOLBAR INSERT\s*-->", "", html, flags=re.I)
                time.sleep(REQUEST_SLEEP)
                if target != url:
                    logger.info("Fetched via Wayback: %s", url)
                return html
            except Exception as e:
                last_err = e
                wait = min(15, (2 ** attempt) + 0.5)
                logger.warning("Fetch attempt %d/%d failed %s: %s — retry in %.1fs", attempt + 1, retries, target, e, wait)
                time.sleep(wait)
    raise last_err  # type: ignore



def paper_download_candidates(article_url: str) -> List[str]:
    """
    Synthesize direct download URLs from paper view pages.
    IJIRCT: viewPaper.php?paperId=2606017 → download.php?a_pid=2606017
    """
    out = []
    m = re.search(r"(viewpaper\.php)\?([^#]*)", article_url, re.I)
    if m:
        qs = m.group(2)
        pid = None
        for part in qs.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                if k.lower() in ("paperid", "paper_id", "id", "pid", "a_pid"):
                    pid = v.strip()
                    break
        if pid:
            base = article_url[: m.start(1)]  # scheme+host+path up to viewPaper.php
            out.append(f"{base}download.php?a_pid={pid}")
            out.append(f"{base}download.php?paperId={pid}")
    return out


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


def _registrable(host: str) -> str:
    """Rough eTLD+1: arpnjournals.com / arpnjournals.org → arpnjournals"""
    host = (host or "").lower().split(":")[0]
    parts = host.split(".")
    if len(parts) >= 2:
        return parts[-2]  # good enough for most journal hosts
    return host


def _same_site(url: str, origin: str) -> bool:
    try:
        a = urlparse(url).netloc.lower()
        b = urlparse(origin).netloc.lower()
        if a == b:
            return True
        # Allow sibling TLDs of the same journal (e.g. .com archive → .org PDFs)
        return _registrable(a) == _registrable(b) and _registrable(a) != ""
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
    # WordPress Download Manager (ISJEM and similar)
    if re.search(r"/download/[^?#]+", u, re.I) and re.search(r"wpdmdl=\d+", u, re.I):
        return "pdf"
    if re.search(r"wpdmdl=\d+", u, re.I):
        return "pdf"
    if re.search(r"/wp-content/uploads/.*\.pdf", u, re.I):
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
    # IJIRCT / custom PHP journals
    if re.search(r"viewpaper\.php\?.*paperid=", u, re.I):
        return "article"
    if re.search(r"view[_-]?paper\.php", u, re.I) and re.search(r"id=", u, re.I):
        return "article"
    if re.search(r"download\.php\?.*a_pid=", u, re.I):
        return "pdf"
    if re.search(r"download\.php\?.*paper", u, re.I):
        return "pdf"

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

    # Listing / archive / issue / volume TOC pages (ARPN, many society journals)
    if re.search(r"issue/(view|archive|current)", u, re.I):
        return "listing"
    if re.search(r"/archive(\.htm|/|$|\?)", u, re.I) or u.rstrip("/").endswith("archive.htm"):
        return "listing"
    if re.search(r"list-\d+", u, re.I):
        return "listing"
    if re.search(r"/issue[s]?/", u, re.I) or re.search(r"/volume[s]?/", u, re.I):
        return "listing"
    # ARPN-style: volume_01_2026.htm, volume_12_2019.html
    if re.search(r"volume[_-]?\d+", u, re.I):
        return "listing"
    if re.search(r"vol[_-]?\d+.*\.(htm|html|php)", u, re.I):
        return "listing"
    if re.search(r"browse|viewall|all-issues|past-issues|current-issue|back.?issues", u, re.I):
        return "listing"
    if re.search(r"publications\.php", u, re.I) and re.search(r"volume=|issue=", u, re.I):
        return "listing"
    if re.search(r"publications\.php", u, re.I):
        return "listing"
    # ISJEM / WordPress issue archives
    if re.search(r"/past-issues/?", u, re.I):
        return "listing"
    if re.search(r"/volume[-_]?\d+", u, re.I) or re.search(r"volume\d+issue\d+", u, re.I):
        return "listing"
    if re.search(r"/current-issue/?", u, re.I):
        return "listing"
    if re.search(r"/special-edition", u, re.I):
        return "listing"
    if re.search(r"contents?\.htm", u, re.I) or re.search(r"toc\.htm", u, re.I):
        return "listing"
    if "issue" in t and ("view" in t or "archive" in t or "list" in t or re.search(r"\d", t)):
        return "listing"
    if re.search(r"^issue\s*\d+", t, re.I) or re.search(r"volume\s*\d+", t, re.I):
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
        if not full:
            continue
        full = _unwrap_wayback(full)
        if not full or full in seen:
            continue
        # Prefer same-site, but allow absolute PDF on CDNs
        kind = classify_link(full, a.get_text(" ", strip=True))
        if not kind:
            continue
        if kind != "pdf" and not _same_site(full, origin) and not _same_site(full, _unwrap_wayback(base_url)):
            # still allow volume/issue listings for known journal hosts
            if kind == "listing" and re.search(r"volume|issue|past-issues|archive", full, re.I):
                pass
            else:
                continue
        # Skip template / certificate noise downloads
        if kind == "pdf" and re.search(r"manuscript-template|sample-certificate|copyright-form|author.guideline", full, re.I):
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
                # OJS + IJIRCT-style: synthesize download URLs without opening every HTML page
                for cand in ojs_download_candidates(href) + paper_download_candidates(href):
                    if cand not in seen_pdfs:
                        seen_pdfs.add(cand)
                        pdfs.append(
                            {
                                "title": link.get("text") or None,
                                "authors": None,
                                "pdf_url": cand,
                                "doi": None,
                                "source": "synth_download",
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
