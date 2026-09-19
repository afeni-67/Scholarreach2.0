"""
Mongo helpers for the hunt fleet (topic-only).

Stores in the APP database (test) so the web app reads the same data:
- huntedjournals : validated journal catalog
- papertopics    : pre-extracted topic pool (no emails)

processor.py / app_jobs.py untouched.
"""
import os
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING

_client = None

APP_DB = "test"
JOURNALS_COL = "huntedjournals"
TOPICS_COL = "papertopics"


def _client_or_connect():
    global _client
    if _client is None:
        uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        _client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        _client.admin.command("ping")
    return _client


def db():
    return _client_or_connect()[APP_DB]


def ensure_hunt_indexes():
    db()[JOURNALS_COL].create_index([("status", ASCENDING), ("updatedAt", ASCENDING)])
    db()[TOPICS_COL].create_index([("journalKey", ASCENDING), ("extractedAt", ASCENDING)])
    try:
        db()[TOPICS_COL].create_index("pdfUrl", unique=True, sparse=True)
    except Exception:
        pass


def upsert_journal(doc: dict):
    now = datetime.now(timezone.utc)
    doc = {**doc, "updatedAt": now}
    db()[JOURNALS_COL].update_one(
        {"key": doc["key"]},
        {"$set": doc, "$setOnInsert": {"createdAt": now}},
        upsert=True,
    )


def claim_hunting_journal(worker_id: str):
    """Hunters create new journals from OpenAlex; crawlers/topickers claim ready ones."""
    # Journal needing PDF crawl
    j = db()[JOURNALS_COL].find_one({"status": "ready", "pdfQueued": {"$lt": 5}})
    return j


def claim_topic_journal():
    # Journal with queued PDFs but low topic count
    return db()[JOURNALS_COL].find_one(
        {"status": "ready"},
        sort=[("topicCount", ASCENDING)],
    )


def save_topic(journal_key: str, title: str, topic: str, pdf_url: str, doi: str = "", source_page: str = "", author_name=None):
    now = datetime.now(timezone.utc)
    try:
        db()[TOPICS_COL].update_one(
            {"pdfUrl": pdf_url},
            {"$set": {
                "journalKey": journal_key,
                "title": title[:300],
                "topic": (topic or title)[:300],
                "pdfUrl": pdf_url,
                "doi": (doi or "")[:200],
                "sourcePage": (source_page or "")[:500],
                "authorName": author_name,
                "extractedAt": now,
            }},
            upsert=True,
        )
        return True
    except Exception:
        return False


def bump_counts(key: str, pdf_delta: int = 0, topic_delta: int = 0):
    db()[JOURNALS_COL].update_one(
        {"key": key},
        {"$inc": {"pdfQueued": pdf_delta, "topicCount": topic_delta},
         "$set": {"updatedAt": datetime.now(timezone.utc)}},
    )


def hunt_stats() -> dict:
    pipe = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    journals = {r["_id"]: r["count"] for r in db()[JOURNALS_COL].aggregate(pipe)}
    topics = db()[TOPICS_COL].estimated_document_count()
    return {"journals": journals, "topics": topics}
