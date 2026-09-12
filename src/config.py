import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "scholarreach")
JOBS_COLLECTION = "jobs"
RESULTS_COLLECTION = "results"

# Claim timeout: if a job stays in "processing" longer than this, it can be reclaimed
CLAIM_TIMEOUT_MINUTES = int(os.getenv("CLAIM_TIMEOUT_MINUTES", "120"))

# Max attempts before marking failed permanently
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))

# Polite scraping
USER_AGENT = "Scholarreach/2.0 (+https://github.com/afeni-67/Scholarreach2.0; research discovery bot)"
REQUEST_TIMEOUT = 45
REQUEST_SLEEP = 2.0  # seconds between requests (be polite to journal servers)

# GitHub (optional, for notifications / issues)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") or os.getenv("GH_PAT")
GITHUB_REPO = os.getenv("GITHUB_REPO", "afeni-67/Scholarreach2.0")
