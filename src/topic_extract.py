"""
Topic-only PDF extraction for the hunt fleet.

Reuses extractor.download_pdf (already Cloudflare-aware) but NEVER
extracts or stores emails. Title/topic comes from PDF metadata +
first-page text heuristics.

EDIT LATER: author name lives here as an explicit placeholder.
Fill extract_author_name() when ready — callers already store authorName.
"""
import io
import logging
import re

logger = logging.getLogger(__name__)


def extract_author_name(_text: str = "", _pdf_bytes: bytes = b""):
    # EDIT LATER: implement author parsing (e.g. citation_author meta,
    # first-page author block). Return None until then.
    return None


def _title_from_text(text: str) -> str:
    for line in (text or "").splitlines():
        s = line.strip()
        if len(s) >= 12 and "@" not in s and not s.lower().startswith("http"):
            return s[:300]
    return "Untitled paper"


def extract_topic(pdf_bytes: bytes) -> dict:
    title = None
    text = ""
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        try:
            meta = reader.metadata
            raw = None
            if meta is not None:
                raw = getattr(meta, "title", None)
                if not raw and hasattr(meta, "get"):
                    try:
                        raw = meta.get("/Title")
                    except Exception:
                        raw = None
            if raw and str(raw).strip() and str(raw).strip().lower() not in ("untitled", "null"):
                title = str(raw).strip()[:300]
        except Exception:
            pass
        parts = []
        for page in reader.pages[:3]:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                pass
        text = "\n".join(parts)
    except Exception as e:
        logger.debug("pypdf topic extract failed: %s", e)

    if not text.strip():
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                parts = []
                for page in pdf.pages[:2]:
                    try:
                        parts.append(page.extract_text() or "")
                    except Exception:
                        pass
            text = "\n".join(parts)
        except Exception as e:
            logger.debug("pdfplumber topic extract failed: %s", e)

    if not title:
        title = _title_from_text(text)
    topic = re.sub(r"\s+", " ", title).strip()[:300] or "Untitled paper"
    return {"title": title, "topic": topic, "authorName": extract_author_name(text, pdf_bytes)}


def process_pdf_topic(pdf_url: str) -> dict:
    from src.extractor import download_pdf
    data = download_pdf(pdf_url)
    if not data[:4] == b"%PDF" and b"%PDF" not in data[:4000]:
        raise ValueError("not a PDF")
    out = extract_topic(data)
    out["pdf_url"] = pdf_url
    return out
