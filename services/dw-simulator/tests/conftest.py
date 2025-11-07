"""Test configuration helpers."""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register placeholders for coverage options when pytest-cov is unavailable."""

    group = parser.getgroup("cov", "coverage options placeholder")
    group.addoption(
        "--cov",
        action="append",
        dest="cov",
        default=[],
        help="Placeholder option allowing test runs without pytest-cov installed.",
    )
    group.addoption(
        "--cov-report",
        action="append",
        dest="cov_report",
        default=[],
        help="Placeholder option allowing test runs without pytest-cov installed.",
    )
    group.addoption(
        "--cov-fail-under",
        action="store",
        dest="cov_fail_under",
        default=None,
        help="Placeholder option allowing test runs without pytest-cov installed.",
    )
