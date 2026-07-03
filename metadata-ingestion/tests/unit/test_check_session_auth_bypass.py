import pathlib

import pytest

from datahub.testing.check_session_auth_bypass import (
    ensure_no_session_header_copies,
    find_session_header_copies,
)

_BAD_SNIPPETS = [
    "headers = dict(self.graph._session.headers)",
    "headers = {**session.headers, 'X-Extra': 'v'}",
    "headers = self._session.headers.copy()",
    # Passing the session's headers directly (no copy) bypasses session.auth
    # identically.
    "requests.get(url, headers=self.graph._session.headers)",
]

_OK_SNIPPETS = [
    # Requests made through the session pick up session.auth per request.
    "response = self.graph._session.get(endpoint, params=params)",
    # Mutating session headers (e.g. adding a custom header) is fine.
    "session.headers['X-Custom'] = 'v'",
    # Copies of unrelated header dicts are fine.
    "headers = dict(response.headers)",
]


@pytest.mark.parametrize("snippet", _BAD_SNIPPETS)
def test_flags_session_header_copies(tmp_path: pathlib.Path, snippet: str) -> None:
    f = tmp_path / "code.py"
    f.write_text(snippet + "\n")
    findings = find_session_header_copies([f])
    assert len(findings) == 1
    with pytest.raises(ValueError, match="session.auth"):
        ensure_no_session_header_copies([tmp_path])


@pytest.mark.parametrize("snippet", _OK_SNIPPETS)
def test_allows_session_usage(tmp_path: pathlib.Path, snippet: str) -> None:
    f = tmp_path / "code.py"
    f.write_text(snippet + "\n")
    assert find_session_header_copies([f]) == []
