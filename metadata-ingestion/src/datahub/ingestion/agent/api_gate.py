"""The allowlist gate for `probe run api`: GET only, on the connector's own
host, and only paths (and query parameters) the provider's api_allowlist names.

The path is matched as the client will send it: joined to the base URL,
percent-decoded, dot segments resolved, fragment refused, so no path can be
validated while a different one is issued.
"""

import re
from dataclasses import dataclass
from typing import FrozenSet, Iterable, List, Optional, Pattern, Tuple
from urllib.parse import parse_qsl, unquote, urlsplit

import requests

from datahub.ingestion.agent.verdicts import ProbeArgumentError

# The only method permitted; one name, so read-only cannot be relaxed in one
# place only.
READ_METHOD = "GET"

# Stands in for an undeclared api_base_url, so path resolution has one
# implementation. Never contacted.
_SYNTHETIC_BASE = "https://probe.invalid"

# A placeholder is exactly one segment: spanning "/" would reach endpoints
# nobody listed.
_PLACEHOLDER = re.compile(r"\{[^/}]+\}")
_SEGMENT = "[^/]+"


class ApiScopeError(ProbeArgumentError):
    """A request refused as not a listed read endpoint: the caller's to fix
    (exit 2). Weaker in kind than the SQL gate: a path is opaque, so whether a
    listed endpoint returns metadata is the judgement of whoever listed it."""


@dataclass(frozen=True)
class _AllowedEndpoint:
    """One allowlist entry: its path and the query parameters permitted on it,
    kept together so a parameter listed on one endpoint widens no other."""

    path: Pattern[str]
    params: FrozenSet[str]


def _compile(entry: str) -> _AllowedEndpoint:
    _method, _, template = entry.partition(" ")
    path_template, _, query_template = template.strip().partition("?")
    parts = [re.escape(p) for p in _PLACEHOLDER.split(path_template)]
    return _AllowedEndpoint(
        path=re.compile(f"^{_SEGMENT.join(parts)}$"),
        # Names only: a value is opaque here, so "?include" permits any value.
        params=frozenset(name for name in query_template.split("&") if name),
    )


def _allowed_paths(allowlist: Iterable[str]) -> List[_AllowedEndpoint]:
    return [_compile(e) for e in allowlist if e.split(" ", 1)[0].upper() == READ_METHOD]


def probe_api_url(base_url: str, path: str) -> str:
    """The URL a probe API call requests, joined with exactly one separator.
    The gate and the passthrough both use it, so they cannot disagree."""
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _effective_path(base_url: str, path: str) -> Tuple[str, FrozenSet[str]]:
    """The path the client will request, relative to the base, and the names of
    the query parameters sent with it.

    Resolved as requests resolves it (dot segments normalised, fragment
    dropped), so the gate inspects the wire path, not the caller's string. A
    path resolving outside the base is refused: the base scopes the
    credentials to one workspace.
    """
    prepared = requests.Request(READ_METHOD, probe_api_url(base_url, path)).prepare()
    # Decoded: a router decoding "a%2Fb" sees two segments a placeholder
    # would not span.
    split = urlsplit(prepared.url or "")
    resolved = unquote(split.path)
    # keep_blank_values, so "?include" is the parameter "include". ';' counts
    # as a separator too, as some servers read it: surfacing more parameters
    # can only refuse more.
    params = frozenset(
        name
        for field in re.split(r"[&;]", split.query)
        for name, _ in parse_qsl(field, keep_blank_values=True)
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
    """Raise ApiScopeError unless `path` is a listed read endpoint. An empty
    allowlist permits nothing."""
    if method.upper() != READ_METHOD:
        raise ApiScopeError(
            f"the probe is read-only; {method.upper()} is not permitted"
        )

    # Decoded, as the client decodes it: "%2e%2e" is "..".
    decoded = unquote(path)
    if not decoded.startswith("/") or decoded.startswith("//"):
        # "//host/x" is a protocol-relative URL, not a path on this host.
        raise ApiScopeError(
            f"'{path}' must start with '/' and name a path on the connector's "
            f"own host, not an absolute or protocol-relative URL"
        )
    if "://" in decoded:
        raise ApiScopeError(f"'{path}' must be a path, not a full URL")
    # On the raw path: a literal "#" ends what the client sends, so
    # "/spaces/a#b/reports" would fetch "/spaces/a". "%23" is sent intact and
    # stays legal.
    if "#" in path:
        raise ApiScopeError(
            f"'{path}' may not contain a fragment: the client would drop it "
            f"and request a different path than the one checked"
        )
    if any(segment == ".." for segment in decoded.split("?")[0].split("/")):
        # Traversal would aim the credentials outside the base.
        raise ApiScopeError(f"'{path}' may not traverse outside its base path")

    # Match the path the client will really request ("%3F" is "?" only to a
    # decoder, so "x%3F/../.." still traverses on the wire). A synthetic base
    # keeps one resolution for providers that declare none.
    bare, params = _effective_path(base_url or _SYNTHETIC_BASE, path)
    endpoints = _allowed_paths(allowlist)
    matched = [e for e in endpoints if e.path.match(bare)]
    if not matched:
        raise ApiScopeError(
            f"'{bare}' is not in this connector's allowlist of read endpoints"
        )
    # The parameters must all be permitted by one entry whose path matched;
    # any such entry will do.
    if not any(params <= endpoint.params for endpoint in matched):
        undeclared = sorted(params - set().union(*(e.params for e in matched)))
        raise ApiScopeError(
            f"'{bare}' is listed, but the query parameter(s) "
            f"{undeclared or sorted(params)} are not. A parameter can change "
            f"what an endpoint returns, so the allowlist has to name the ones "
            f"it vouches for -- add them as 'GET {bare}?<name>' if this "
            f"endpoint is safe with them"
        )
