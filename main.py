"""Service entry point.

Run locally with:  uv run uvicorn main:app --reload
"""
from obs.app import app, create_app

__all__ = ["app", "create_app"]
