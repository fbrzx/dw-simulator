"""Shared configuration helpers for DW Simulator services."""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_TARGET_DB_URL = "sqlite:///./data/sqlite/dw_simulator.db"
DEFAULT_STAGE_BUCKET = "s3://local/dw-simulator/staging"


def get_target_db_url() -> str:
    """Resolve the warehouse connection string for the synthetic data generator."""

    url = os.environ.get("DW_SIMULATOR_TARGET_DB_URL", DEFAULT_TARGET_DB_URL)
    _ensure_sqlite_parent(url)
    return url


def get_stage_bucket() -> str:
    """Resolve the S3 staging bucket used for Parquet exchange."""

    return os.environ.get("DW_SIMULATOR_STAGE_BUCKET", DEFAULT_STAGE_BUCKET)


def _ensure_sqlite_parent(url: str) -> None:
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        return
    raw_path = url[len(prefix) :]
    if raw_path == ":memory:":
        return
    file_path = Path(raw_path)
    if not file_path.is_absolute():
        file_path = Path.cwd() / file_path
    file_path.parent.mkdir(parents=True, exist_ok=True)


__all__ = ["DEFAULT_TARGET_DB_URL", "DEFAULT_STAGE_BUCKET", "get_target_db_url", "get_stage_bucket"]
