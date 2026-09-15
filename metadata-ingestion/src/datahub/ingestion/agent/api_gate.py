import re
from dataclasses import dataclass
from typing import FrozenSet, Iterable, List, Optional, Pattern, Tuple
from urllib.parse import parse_qsl, unquote, urlsplit

import requests

# Only reads. Unlike the SQL gate's "is this a SELECT", which needed CTE and
# subquery analysis to mean anything, this one is exact.
#
# Public because the framework passes it in: it was private, so probe_methods
# spelled "GET" as a literal at the call site and _effective_path spelled it a
# third time. One name, so read-only cannot be relaxed in one place only.
READ_METHOD = "GET"

# Stands in when a provider declares no api_base_url, so path resolution has
# one implementation instead of a weaker fallback. Never contacted: only its
# path component is read back out.
_SYNTHETIC_BASE = "https://probe.invalid"

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


@dataclass(frozen=True)
class _AllowedEndpoint:
    """One allowlist entry, split into the two things a request must satisfy.

    Kept together rather than as parallel lists because a parameter is only
    permitted on the endpoint that declared it: hoisting the names into one
    shared set would let a parameter listed on a harmless endpoint widen a
    different one.
    """

    path: Pattern[str]
    params: FrozenSet[str]


def _compile(entry: str) -> _AllowedEndpoint:
    _method, _, template = entry.partition(" ")
    path_template, _, query_template = template.strip().partition("?")
    parts = [re.escape(p) for p in _PLACEHOLDER.split(path_template)]
    return _AllowedEndpoint(
        path=re.compile(f"^{_SEGMENT.join(parts)}$"),
        # Names only, and deliberately: a value is opaque to this gate, so
        # "GET /projects?include" permits include=anything. Naming the
        # parameter is the connector author asserting the endpoint is safe with
        # it -- the same judgement the path allowlist already rests on, and the
        # ApiScopeError docstring is explicit that nothing here can check their
        # work.
        params=frozenset(name for name in query_template.split("&") if name),
    )


def _allowed_paths(allowlist: Iterable[str]) -> List[_AllowedEndpoint]:
    return [_compile(e) for e in allowlist if e.split(" ", 1)[0].upper() == READ_METHOD]


def probe_api_url(base_url: str, path: str) -> str:
    """The URL a probe API call actually requests. Used by BOTH the gate and the
    passthrough that sends it, so the two cannot disagree.

    Exactly one separator. `f"{base}{path}"` gives "//projects" when the base
    ends in "/" and the path begins with one, and Hex's base is user-supplied
    and unnormalised. Normalising in the gate alone was worse than not
    normalising at all: the gate then approved "/projects" while the passthrough
    still sent the double-slash URL -- one path validated, another issued, which
    is the whole class this check exists to close.
    """
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _effective_path(base_url: str, path: str) -> Tuple[str, FrozenSet[str]]:
    """What the client will actually request, resolved as the client resolves it.

    Returns the path relative to the base AND the names of the query parameters
    riding along with it. The query is part of the request and was being
    dropped here: `urlsplit(...).path` discards it, so the gate matched
    "/projects" and the client then sent "/projects?include=cells". A parameter
    is how a REST endpoint is asked to return more than its default, so an
    unexamined query is a hole in the very statement the allowlist makes --
    Hex lists /projects and deliberately omits /cells, and the omission was
    reachable through the listed endpoint.

    RestApiPassthrough.api sends f"{api_base_url}{path}", and requests then
    normalises dot segments and drops any fragment. Reproducing that here means
    the gate inspects the wire path rather than the caller's string, and the
    result is returned relative to the base so it can be matched against an
    allowlist written in the connector's own terms ("GET /spaces").

    A path that resolves outside the base is refused outright: the base is what
    scopes these credentials to one workspace, and escaping it aims them
    somewhere the allowlist never described.
    """
    prepared = requests.Request(READ_METHOD, probe_api_url(base_url, path)).prepare()
    # Decoded before matching. An encoded separator is invisible to a
    # single-segment placeholder -- "/spaces/a%2Fb/reports" satisfies
    # "/spaces/{token}/reports" because "a%2Fb" holds no literal "/" -- while an
    # API router that decodes percent-escapes sees "/spaces/a/b/reports", an
    # endpoint nobody listed. Matching the decoded form is what the server will
    # actually route on.
    split = urlsplit(prepared.url or "")
    resolved = unquote(split.path)
    # keep_blank_values so "?include" counts as the parameter "include" rather
    # than vanishing, and so a bare "?/../x" is seen as the (undeclared)
    # parameter it is instead of passing as no query at all.
    params = frozenset(
        name for name, _ in parse_qsl(split.query, keep_blank_values=True)
    )
    base_path = unquote(urlsplit(base_url).path).rstrip("/")
    if not base_path:
        return resolved, params
    if resolved == base_path:
        return "/", params
    if not resolved.startswith(base_path + "/"):
        raise ApiScopeError(
            f"'{path}' resolves to '{resolved}', outside this connector's API "
            f"base '{base_path}' -- the base is what scopes these credentials "
            f"to one workspace"
        )
    return resolved[len(base_path) :], params


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
    # One code path. When a provider primes no api_base_url, a synthetic base
    # gives the same resolution rather than falling back to
    # `decoded.split("?")[0]` -- which was the original buggy form and left the
    # %3F and %2F holes open for exactly the providers that declare no base.
    bare, params = _effective_path(base_url or _SYNTHETIC_BASE, path)
    endpoints = _allowed_paths(allowlist)
    matched = [e for e in endpoints if e.path.match(bare)]
    if not matched:
        raise ApiScopeError(
            f"'{bare}' is not in this connector's allowlist of read endpoints"
        )
    # A parameter has to be permitted by the SAME entry whose path matched, and
    # any matching entry will do -- two templates can both cover a path
    # ("/projects" and "/{token}") while declaring different parameters, and
    # refusing because the first one checked did not declare it would deny a
    # request the allowlist does permit.
    if not any(params <= endpoint.params for endpoint in matched):
        undeclared = sorted(params - set().union(*(e.params for e in matched)))
        raise ApiScopeError(
            f"'{bare}' is listed, but the query parameter(s) "
            f"{undeclared or sorted(params)} are not. A parameter can change "
            f"what an endpoint returns, so the allowlist has to name the ones "
            f"it vouches for -- add them as 'GET {bare}?<name>' if this "
            f"endpoint is safe with them"
        )
