"""Centralized configuration.

Reads values from Streamlit secrets (when running under Streamlit) and from
environment variables loaded from the project's .env file. Required values are
validated at import time so misconfiguration fails fast at startup.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if not os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
    load_dotenv(_PROJECT_ROOT / ".env")

def _get(key: str, default=None):
    """Read a config value, preferring Streamlit secrets when available."""
    try:
        import streamlit as st
        try:
            return st.secrets[key]
        except Exception:
            pass
    except ImportError:
        pass
    return os.getenv(key, default)


# AWS / S3
AWS_ACCESS_KEY_ID = _get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = _get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = _get("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = _get("S3_BUCKET_NAME")

# Database (legacy SQLite path — kept for backward compatibility during migration)
DB_PATH = _get("DB_PATH", "data/clearpath.db")

# Supabase Postgres
SUPABASE_HOST = _get("SUPABASE_HOST")
SUPABASE_PORT = _get("SUPABASE_PORT", "5432")
SUPABASE_USER = _get("SUPABASE_USER")
SUPABASE_PASSWORD = _get("SUPABASE_PASSWORD")
SUPABASE_DATABASE = _get("SUPABASE_DATABASE")

# Client
CLIENT_NAME = _get("CLIENT_NAME", "Juice Bar NYC")
REPORT_RECIPIENT_EMAIL = _get("REPORT_RECIPIENT_EMAIL")
BUSINESS_TYPE = _get("BUSINESS_TYPE", "Juice Bar")

# Email (Resend; SendGrid kept as fallback during migration)
RESEND_API_KEY = _get("RESEND_API_KEY")
SENDGRID_API_KEY = _get("SENDGRID_API_KEY")
FROM_EMAIL = _get("FROM_EMAIL")
REPLY_TO_EMAIL = _get("REPLY_TO_EMAIL", "insights@clearpathdata.org")


class ConfigError(RuntimeError):
    pass


def validate() -> None:
    """Raise ConfigError if any required value is missing."""
    required = {
        "S3_BUCKET_NAME": S3_BUCKET_NAME,
        "SUPABASE_HOST": SUPABASE_HOST,
        "SUPABASE_USER": SUPABASE_USER,
        "SUPABASE_PASSWORD": SUPABASE_PASSWORD,
        "SUPABASE_DATABASE": SUPABASE_DATABASE,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ConfigError(
            "Missing required configuration: "
            + ", ".join(missing)
            + ". Set these in your .env file or Streamlit secrets."
        )


validate()
