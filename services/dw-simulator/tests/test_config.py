from __future__ import annotations

import os
from pathlib import Path

from dw_simulator import config


def test_locate_repo_root_walks_cwd_parents(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "docker-compose.yml").write_text("version: '3'")

    nested = repo_root / "services" / "dw-simulator"
    nested.mkdir(parents=True)

    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: nested))

    detected = config._locate_repo_root()
    assert detected == repo_root


def test_get_redshift_url_default_none():
    """Test that get_redshift_url returns None when not configured."""
    # Clear environment variable if set
    if "DW_SIMULATOR_REDSHIFT_URL" in os.environ:
        old_value = os.environ.pop("DW_SIMULATOR_REDSHIFT_URL")
        try:
            result = config.get_redshift_url()
            assert result is None
        finally:
            os.environ["DW_SIMULATOR_REDSHIFT_URL"] = old_value
    else:
        result = config.get_redshift_url()
        assert result is None


def test_get_redshift_url_from_env(monkeypatch):
    """Test that get_redshift_url returns environment variable value."""
    test_url = "postgresql://user:pass@localhost:5432/testdb"
    monkeypatch.setenv("DW_SIMULATOR_REDSHIFT_URL", test_url)

    result = config.get_redshift_url()
    assert result == test_url


def test_get_snowflake_url_default_none():
    """Test that get_snowflake_url returns None when not configured."""
    # Clear environment variable if set
    if "DW_SIMULATOR_SNOWFLAKE_URL" in os.environ:
        old_value = os.environ.pop("DW_SIMULATOR_SNOWFLAKE_URL")
        try:
            result = config.get_snowflake_url()
            assert result is None
        finally:
            os.environ["DW_SIMULATOR_SNOWFLAKE_URL"] = old_value
    else:
        result = config.get_snowflake_url()
        assert result is None


def test_get_snowflake_url_from_env(monkeypatch):
    """Test that get_snowflake_url returns environment variable value."""
    test_url = "snowflake://test:test@localhost:4566/test?account=test&warehouse=test"
    monkeypatch.setenv("DW_SIMULATOR_SNOWFLAKE_URL", test_url)

    result = config.get_snowflake_url()
    assert result == test_url
