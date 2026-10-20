"""Minimal Superset settings for local development."""

import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me")

# No caching, so dashboards show live data on every refresh.
CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
