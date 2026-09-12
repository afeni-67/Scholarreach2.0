#!/usr/bin/env python3
"""Seed a few example discovery jobs (run once after MongoDB is ready)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import db

EXAMPLES = [
    ("discover", "https://ijetrm.com/issue/?volume=February~2026"),
    ("discover", "https://ijetrm.com/issue/?volume=March~2026"),
    ("discover", "https://ijetrm.com/issue/?volume=current"),
]

if __name__ == "__main__":
    db.ensure_indexes()
    for t, u in EXAMPLES:
        j = db.enqueue_job(t, u)
        print(f"Enqueued {t}: {u} → {j['_id']}")
    print("Queue stats:", db.count_by_status())
