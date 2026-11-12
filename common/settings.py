"""
Connection settings for every script in this repo.

Nothing secret lives in the code. Values come from, in order of priority:
  1. real environment variables (your shell, Docker Compose, GitHub Actions secrets)
  2. a local `.env` file in the repo root (copy `.env.example` to `.env`)

A missing password stops the script with a clear message instead of silently
falling back to a default.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]

# override=False: a variable that is already set (CI secret, Compose env) always wins.
load_dotenv(REPO_ROOT / ".env", override=False)


def require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(
            f"{name} is not set. Copy .env.example to .env and fill it in, "
            f"or export {name} in your shell."
        )
    return value


def db_params() -> dict:
    """psycopg2.connect(**db_params()) for the warehouse database."""
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "bank_dw"),
        user=os.environ.get("POSTGRES_USER", "bank_admin"),
        password=require("POSTGRES_PASSWORD"),
    )


def s3_location() -> tuple[str, str]:
    """(bucket, prefix) of the S3 landing zone, e.g. ("bank-dw-landing", "raw")."""
    return require("S3_BUCKET"), os.environ.get("S3_PREFIX", "raw").strip("/")
