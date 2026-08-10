import re
from typing import Iterable, List, Pattern
from urllib.parse import unquote

# Only reads. Unlike the SQL gate's "is this a SELECT", which needed CTE and
# subquery analysis to mean anything, this one is exact.
_READ_METHOD = "GET"

# A placeholder stands for exactly one path segment. Allowing it to span "/"
# would let "/spaces/{token}/reports" match "/spaces/a/b/reports" and reach an
# endpoint nobody listed.
_PLACEHOLDER = re.compile(r"\{[^/}]+\}")
_SEGMENT = "[^/]+"


class ApiScopeError(ValueError):
    """A request was refused because it is not a listed read endpoint.

    A ValueError so recipe_cli maps it to the user-error exit code.

    Weaker in kind than the SQL gate, and worth being honest about: there is no
    parser here. sqlglot lets sql_gate reason about what a query *touches*; a
    path is opaque, so all this can do is match an allowlist. Whether a listed
    endpoint returns metadata or user data is the judgement of whoever listed
    it, and nothing here can check their work.
    """


def _compile(entry: str) -> Pattern[str]:
    method, _, template = entry.partition(" ")
    parts = [re.escape(p) for p in _PLACEHOLDER.split(template.strip())]
    return re.compile(f"^{_SEGMENT.join(parts)}$")


def _allowed_paths(allowlist: Iterable[str]) -> List[Pattern[str]]:
    return [
        _compile(e) for e in allowlist if e.split(" ", 1)[0].upper() == _READ_METHOD
    ]


def check_api_request(method: str, path: str, allowlist: Iterable[str]) -> None:
    """Raise ApiScopeError unless `path` is a listed read endpoint.

    Fail-closed: an empty allowlist permits nothing, so a connector that has
    not opted in exposes no endpoints at all.
    """
    if method.upper() != _READ_METHOD:
        raise ApiScopeError(
            f"the probe is read-only; {method.upper()} is not permitted"
        )

    # Decoded before inspection: "%2e%2e" is "..", and the connector's HTTP
    # client would decode it too.
    decoded = unquote(path)
    if not decoded.startswith("/") or decoded.startswith("//"):
        # "//host/x" is a protocol-relative URL, not a path on this host.
        raise ApiScopeError(
            f"'{path}' must start with '/' and name a path on the connector's "
            f"own host, not an absolute or protocol-relative URL"
        )
    if "://" in decoded:
        raise ApiScopeError(f"'{path}' must be a path, not a full URL")
    if any(segment == ".." for segment in decoded.split("?")[0].split("/")):
        # The path is concatenated onto the connector's base URI, so traversal
        # would aim its credentials somewhere it never meant to call.
        raise ApiScopeError(f"'{path}' may not traverse outside its base path")

    bare = decoded.split("?")[0]
    if not any(pattern.match(bare) for pattern in _allowed_paths(allowlist)):
        raise ApiScopeError(
            f"'{bare}' is not in this connector's allowlist of read endpoints"
        )
