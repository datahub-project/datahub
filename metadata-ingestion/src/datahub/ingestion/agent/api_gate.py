import re
from typing import Iterable, List, Optional, Pattern
from urllib.parse import unquote, urlsplit

# Only reads. Unlike the SQL gate's "is this a SELECT", which needed CTE and
# subquery analysis to mean anything, this one is exact.
#
# Public because the framework passes it in: it was private, so probe_methods
# spelled "GET" as a literal at the call site and _effective_path spelled it a
# third time. One name, so read-only cannot be relaxed in one place only.
READ_METHOD = "GET"

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
    return [_compile(e) for e in allowlist if e.split(" ", 1)[0].upper() == READ_METHOD]


def _effective_path(base_url: str, path: str) -> str:
    """The path the client will actually request, resolved as the client resolves it.

    RestApiPassthrough.api sends f"{api_base_url}{path}", and requests then
    normalises dot segments and drops any fragment. Reproducing that here means
    the gate inspects the wire path rather than the caller's string, and the
    result is returned relative to the base so it can be matched against an
    allowlist written in the connector's own terms ("GET /spaces").

    A path that resolves outside the base is refused outright: the base is what
    scopes these credentials to one workspace, and escaping it aims them
    somewhere the allowlist never described.
    """
    import requests

    prepared = requests.Request(READ_METHOD, f"{base_url}{path}").prepare()
    resolved = urlsplit(prepared.url or "").path
    base_path = urlsplit(base_url).path.rstrip("/")
    if not base_path:
        return resolved
    if resolved == base_path:
        return "/"
    if not resolved.startswith(base_path + "/"):
        raise ApiScopeError(
            f"'{path}' resolves to '{resolved}', outside this connector's API "
            f"base '{base_path}' -- the base is what scopes these credentials "
            f"to one workspace"
        )
    return resolved[len(base_path) :]


def check_api_request(
    method: str,
    path: str,
    allowlist: Iterable[str],
    base_url: Optional[str] = None,
) -> None:
    """Raise ApiScopeError unless `path` is a listed read endpoint.

    Fail-closed: an empty allowlist permits nothing, so a connector that has
    not opted in exposes no endpoints at all.
    """
    if method.upper() != READ_METHOD:
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
    # Checked on the RAW path, not the decoded one, and this is the one check
    # here where that distinction matters. A literal "#" starts the fragment, so
    # the client sends only what precedes it -- "/spaces/a#b/reports" satisfies
    # "/spaces/{token}/reports" (the placeholder spans one segment and "a#b"
    # holds no "/") and then fetches "/spaces/a", which nobody listed. That is
    # the bypass: one path validated, another issued. "%23" is the *encoded*
    # character and is sent through intact, so it truncates nothing and is
    # ordinary data -- rejecting it would refuse a legitimate "?filter=%23tag".
    if "#" in path:
        raise ApiScopeError(
            f"'{path}' may not contain a fragment: the client would drop it "
            f"and request a different path than the one checked"
        )
    if any(segment == ".." for segment in decoded.split("?")[0].split("/")):
        # The path is concatenated onto the connector's base URI, so traversal
        # would aim its credentials somewhere it never meant to call.
        raise ApiScopeError(f"'{path}' may not traverse outside its base path")

    # Match on the path the client will REALLY request, whenever the caller can
    # tell us the base. Checking the caller's own string is what let two
    # bypasses through, both of the same shape -- one path validated, another
    # issued:
    #
    #   "#"    truncated the request client-side, so the gate matched a listed
    #          template and the client fetched a shorter path.
    #   "%3F"  truncated only the gate's view. It decodes to "?" here, so
    #          `decoded.split("?")[0]` stopped there -- while on the wire it
    #          stays an ordinary path character, leaving the "../" segments
    #          after it live. requests normalises dot segments itself, so
    #          "/reports/x%3F/../../../api/other_ws/spaces" reached
    #          "/api/api/other_ws/spaces": outside the workspace base the
    #          allowlist scopes these credentials to, with no cooperating
    #          server needed.
    #
    # Resolving the URL the way the client resolves it ends the class rather
    # than adding a third special case.
    bare = _effective_path(base_url, path) if base_url else decoded.split("?")[0]
    if not any(pattern.match(bare) for pattern in _allowed_paths(allowlist)):
        raise ApiScopeError(
            f"'{bare}' is not in this connector's allowlist of read endpoints"
        )
