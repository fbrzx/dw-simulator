from dw_simulator import __version__


def test_version_constant_matches_release_tag() -> None:
    assert __version__ == "0.1.0"
