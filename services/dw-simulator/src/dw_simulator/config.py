"""Shared configuration helpers for DW Simulator services."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

def _candidate_search_paths() -> Iterable[Path]:
    """Yield directories that might contain the repository sentinel."""

    cwd = Path.cwd().resolve()
    yield cwd
    yield from cwd.parents

    module_path = Path(__file__).resolve()
    yield from module_path.parents


def _locate_repo_root() -> Path | None:
    """Best-effort detection of the monorepo root (where docker-compose.yml lives)."""

    sentinel = "docker-compose.yml"
    seen: set[Path] = set()
    for candidate in _candidate_search_paths():
        if candidate in seen:
            continue
        seen.add(candidate)
        if (candidate / sentinel).exists():
            return candidate
    return None


def _resolve_data_root() -> Path:
    override = os.environ.get("DW_SIMULATOR_DATA_ROOT")
    if override:
        root = Path(override).expanduser()
    else:
        repo_root = _locate_repo_root()
        root = (repo_root / "data") if repo_root else (Path.cwd() / "data")
    root.mkdir(parents=True, exist_ok=True)
    return root


DATA_ROOT = _resolve_data_root()
DEFAULT_TARGET_DB_URL = f"sqlite:///{(DATA_ROOT / 'sqlite' / 'dw_simulator.db').resolve()}"
DEFAULT_STAGE_BUCKET = "s3://local/dw-simulator/staging"


def get_data_root() -> Path:
    """Return the resolved data directory for both SQLite and Parquet output."""

    return DATA_ROOT


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


__all__ = [
    "DATA_ROOT",
    "DEFAULT_TARGET_DB_URL",
    "DEFAULT_STAGE_BUCKET",
    "get_data_root",
    "get_target_db_url",
    "get_stage_bucket",
]
