import pathlib

from datahub.testing.check_session_auth_bypass import ensure_no_session_header_copies


def test_no_session_header_copies() -> None:
    # Copying session.headers into another request only carries a static token;
    # OAuth token providers live in session.auth and get silently bypassed
    # (this exact idiom made the cloud events consumer 401 under OAuth).
    root = pathlib.Path(__file__).parents[2]
    ensure_no_session_header_copies([root / "src", root / "tests"])
