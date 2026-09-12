"""
Journal page scrapers.
Currently focused on IJETRM (ijetrm.com) which exposes citation_pdf_url meta tags.
Easy to extend for IJDDT and others.
"""
import logging
import time
import re
from typing import List, Dict, Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from src.config import USER_AGENT, REQUEST_TIMEOUT, REQUEST_SLEEP

logger = logging.getLogger(__name__)


def _get(url: str) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    time.sleep(REQUEST_SLEEP)
    return resp.text


def scrape_ijetrm_issue(issue_url: str) -> List[Dict[str, Any]]:
    """
    Parse an IJETRM issue page and return list of papers:
    [{title, authors, pdf_url, doi, page_url}, ...]
    """
    html = _get(issue_url)
    soup = BeautifulSoup(html, "lxml")

    papers: List[Dict[str, Any]] = []

    # Primary: citation_pdf_url meta tags (most reliable)
    pdf_metas = soup.find_all("meta", attrs={"name": "citation_pdf_url"})
    title_metas = soup.find_all("meta", attrs={"name": "citation_title"})
    author_metas = soup.find_all("meta", attrs={"name": "citation_author"})

    # Group by proximity is hard; instead collect all PDFs and pair with nearby titles if possible
    # For robustness we also walk the visible list structure.

    # Method 1 – meta tags (best when present)
    for meta in pdf_metas:
        pdf = meta.get("content", "").strip()
        if not pdf or not pdf.lower().endswith(".pdf"):
            continue
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

    # Enrich titles from citation_title if counts match roughly
    if title_metas and len(title_metas) == len(papers):
        for i, tm in enumerate(title_metas):
            papers[i]["title"] = tm.get("content", "").strip() or None

    # Method 2 – visible table / list (fallback & enrichment)
    # The page has repeating blocks of title + Author: ... + optional DOI + PDF
    # We also look for direct <a href="...pdf">
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

    # Deduplicate by pdf_url
    seen = set()
    unique = []
    for p in papers:
        if p["pdf_url"] not in seen:
            seen.add(p["pdf_url"])
            unique.append(p)

    # Try to pull plain-text titles from the page body for items missing title
    text = soup.get_text("\n", strip=True)
    # Simple heuristic: lines that look like titles (long, uppercase-ish)
    # We leave title=None if not confidently found; extractor will get it from PDF.

    logger.info("IJETRM issue %s → %d PDF links", issue_url, len(unique))
    return unique


def scrape_generic_pdf_links(page_url: str) -> List[Dict[str, Any]]:
    """Fallback scraper: any page that contains direct PDF links."""
    html = _get(page_url)
    soup = BeautifulSoup(html, "lxml")
    papers = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"\.pdf(\?|$)", href, re.I):
            full = urljoin(page_url, href)
            papers.append(
                {
                    "title": a.get_text(strip=True) or None,
                    "authors": None,
                    "pdf_url": full,
                    "doi": None,
                    "source": "generic",
                    "page_url": page_url,
                }
            )
    # also meta citation_pdf_url
    for meta in soup.find_all("meta", attrs={"name": "citation_pdf_url"}):
        pdf = meta.get("content", "").strip()
        if pdf and not any(p["pdf_url"] == pdf for p in papers):
            papers.append(
                {
                    "title": None,
                    "authors": None,
                    "pdf_url": pdf,
                    "doi": None,
                    "source": "meta",
                    "page_url": page_url,
                }
            )
    return papers


def discover_papers(url: str) -> List[Dict[str, Any]]:
    """
    Dispatch to the right scraper based on domain / path.
    """
    domain = urlparse(url).netloc.lower()
    if "ijetrm.com" in domain:
        return scrape_ijetrm_issue(url)
    # Future: elif "ijddt.com" in domain: ...
    return scrape_generic_pdf_links(url)
