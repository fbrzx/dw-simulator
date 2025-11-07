from __future__ import annotations

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
