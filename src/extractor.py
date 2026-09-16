"""
PDF title + email extraction.
Uses pypdf for metadata + text, pdfplumber as fallback for better layout text.
"""
import re
import io
import logging
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests
from pypdf import PdfReader
import pdfplumber

from src.config import USER_AGENT, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(
    r"""
    (?<![\w.+-])                          # not preceded by word char
    ([a-zA-Z0-9][a-zA-Z0-9._%+-]{0,63}    # local part
    @
    [a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?
    (?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+)
    (?![\w.+-])                           # not followed by word char
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Common noise / false positives to drop
EMAIL_BLACKLIST = {
    "example@example.com",
    "email@example.com",
    "name@domain.com",
    "user@domain.com",
    "info@ijetrm.com",
    "editor@ijetrm.com",
    "ijetrm@gmail.com",
    "support@ijetrm.com",
    "admin@ijetrm.com",
}


def _clean_email(e: str) -> Optional[str]:
    e = e.strip().lower().rstrip(".,;:)")
    if e in EMAIL_BLACKLIST:
        return None
    if len(e) > 80 or len(e) < 6:
        return None
    # drop obvious non-personal journal / system addresses if desired
    if any(x in e for x in ("noreply", "no-reply", "donotreply")):
        return None
    return e


def extract_emails_from_text(text: str) -> List[str]:
    found = set()
    for m in EMAIL_RE.finditer(text or ""):
        cleaned = _clean_email(m.group(1))
        if cleaned:
            found.add(cleaned)
    return sorted(found)


def download_pdf(url: str) -> bytes:
    """
    Download a PDF from a page-sourced URL.
    - Fail fast on origin 404/410 (no point hammering retries).
    - Wayback only as a single fallback for previously-valid hrefs, not for guessed paths.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/pdf,application/octet-stream,*/*",
        "Connection": "close",
    }

    def _try_once(target: str, timeout: int):
        resp = requests.get(
            target,
            headers=headers,
            timeout=timeout,
            stream=True,
            allow_redirects=True,
        )
        if resp.status_code in (404, 410):
            raise FileNotFoundError(f"404 Client Error: NOT FOUND for url: {target}")
        if resp.status_code in (403, 503) and (
            "cloudflare" in (resp.headers.get("server") or "").lower()
            or "just a moment" in (resp.text or "")[:1500].lower()
        ):
            raise RuntimeError(f"Cloudflare blocked PDF {target}")
        resp.raise_for_status()
        data = resp.content
        if data.startswith(b"%PDF"):
            return data
        if b"%PDF" in data[:4000]:
            return data[data.find(b"%PDF"):]
        raise ValueError(
            f"Downloaded content is not a PDF (url={target}, ctype={resp.headers.get('Content-Type')}, len={len(data)})"
        )

    last_err = None
    # Origin: up to 2 attempts
    for attempt in range(2):
        try:
            return _try_once(url, REQUEST_TIMEOUT)
        except FileNotFoundError as e:
            last_err = e
            break  # dead link — don't retry origin
        except Exception as e:
            last_err = e
            import time
            time.sleep(min(6, 2 ** attempt))

    # Wayback once only (skip if URL already archive, or if origin was hard 404 on a synth-looking path)
    if "web.archive.org" not in url:
        looks_guessed = bool(
            __import__("re").search(r"article/download/\d+(/\d+)?(/pdf)?/?$", url, __import__("re").I)
        )
        # Still allow wayback for page-sourced download hrefs; cheap single try
        try:
            return _try_once(f"https://web.archive.org/web/2/{url}", REQUEST_TIMEOUT + 15)
        except Exception as e:
            last_err = e

    raise last_err  # type: ignore


def extract_title_and_emails(pdf_bytes: bytes) -> Tuple[Optional[str], List[str]]:
    """
    Returns (title, emails).
    Title preference: PDF metadata /Title → first non-empty line on page 1 that looks like a title.
    """
    title: Optional[str] = None
    full_text_parts: List[str] = []

    # --- pypdf path ---
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        meta = reader.metadata
        if meta and meta.title:
            t = str(meta.title).strip()
            if t and t.lower() not in ("untitled", "unknown", ""):
                title = t

        # first 3 pages text is usually enough for title + corresponding author
        for i, page in enumerate(reader.pages[:2]):
            try:
                txt = page.extract_text() or ""
                full_text_parts.append(txt)
            except Exception:
                continue
    except Exception as e:
        logger.warning("pypdf failed: %s", e)

    # --- pdfplumber fallback / enrichment ---
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not title and pdf.pages:
                # try first page top text
                first = pdf.pages[0]
                words = first.extract_words(keep_blank_chars=False) or []
                # crude title heuristic: largest font size near top
                if words:
                    # sort by size desc then top position
                    candidates = sorted(
                        words,
                        key=lambda w: (-float(w.get("size", 0) or 0), float(w.get("top", 9999))),
                    )
                    # take a few top large words and join nearby
                    top_line = " ".join(c["text"] for c in candidates[:12])
                    if len(top_line) > 15:
                        title = top_line[:300].strip()

            for page in pdf.pages[:2]:
                try:
                    txt = page.extract_text() or ""
                    if txt:
                        full_text_parts.append(txt)
                except Exception:
                    continue
    except Exception as e:
        logger.warning("pdfplumber failed: %s", e)

    text = "\n".join(full_text_parts)

    # Clean title
    if title:
        title = re.sub(r"\s+", " ", title).strip()
        # remove common journal header noise
        for noise in ("International Journal of", "IJETRM", "ISSN:", "Impact Factor"):
            if title.upper().startswith(noise.upper()):
                # try next line heuristic later if needed
                pass
        if len(title) < 8:
            title = None

    # Fallback title from first substantial line
    if not title:
        for line in text.splitlines():
            line = line.strip()
            if len(line) > 20 and not line.lower().startswith(("abstract", "keywords", "volume", "issn", "doi")):
                title = line[:300]
                break

    emails = extract_emails_from_text(text)
    return title, emails


def process_pdf_url(pdf_url: str) -> dict:
    """High-level: download + extract. Returns structured result.

    For OJS article/view URLs, tries article/download variants automatically.
    """
    candidates = [pdf_url]
    # OJS view → download variants
    import re
    m = re.search(r"(article/view/)(\d+)(?:/(\d+))?", pdf_url, re.I)
    if m:
        base = pdf_url[: m.start(1)]
        aid = m.group(2)
        gid = m.group(3)
        candidates = []
        if gid:
            candidates.append(f"{base}article/download/{aid}/{gid}")
        candidates.append(f"{base}article/download/{aid}")
        candidates.append(f"{base}article/download/{aid}/pdf")
        if pdf_url not in candidates:
            candidates.append(pdf_url)
    elif "article/download/" in pdf_url and not pdf_url.rstrip("/").endswith("pdf"):
        candidates = [pdf_url, pdf_url.rstrip("/") + "/pdf"]

    last_err = None
    for cand in candidates:
        try:
            data = download_pdf(cand)
            title, emails = extract_title_and_emails(data)
            return {
                "pdf_url": cand,
                "title": title,
                "emails": emails,
                "email_count": len(emails),
                "source_domain": urlparse(cand).netloc,
            }
        except Exception as e:
            last_err = e
            continue
    raise last_err  # type: ignore
