import pytest

from datahub.ingestion.agent.api_gate import ApiScopeError, check_api_request

ALLOWLIST = [
    "GET /spaces",
    "GET /spaces/{token}/reports",
    "GET /reports/{token}/queries",
]

# Same endpoints, but vouching for two query parameters. Several tests below
# are about something else entirely (an encoded "#", URL resolution) and merely
# use a query string as the vehicle; they run against this so the query rule
# does not become the thing they accidentally assert.
ALLOWLIST_WITH_PARAMS = [entry + "?filter&per_page" for entry in ALLOWLIST]


def test_permits_a_listed_literal_path():
    check_api_request("GET", "/spaces", ALLOWLIST)


def test_permits_a_path_matching_a_template():
    check_api_request("GET", "/spaces/sp1/reports", ALLOWLIST)


def test_a_template_placeholder_does_not_span_segments():
    # {token} must not swallow a "/" -- otherwise "/spaces/a/b/reports" would
    # match and reach an endpoint nobody listed.
    with pytest.raises(ApiScopeError):
        check_api_request("GET", "/spaces/a/b/reports", ALLOWLIST)


def test_rejects_an_unlisted_path():
    with pytest.raises(ApiScopeError, match="not in this connector's allowlist"):
        check_api_request("GET", "/spaces/sp1/members", ALLOWLIST)


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE", "HEAD"])
def test_rejects_every_method_but_get(method):
    with pytest.raises(ApiScopeError, match="read-only"):
        check_api_request(method, "/spaces", ALLOWLIST)


def test_method_matching_is_case_insensitive():
    check_api_request("get", "/spaces", ALLOWLIST)


@pytest.mark.parametrize(
    "path",
    [
        "https://evil.example.com/spaces",
        "//evil.example.com/spaces",
        "/spaces/../../admin",
        "/spaces/%2e%2e/admin",
    ],
)
def test_rejects_anything_that_could_leave_the_connector_host(path):
    # No SQL analogue: a path is concatenated onto the connector's base URI, so
    # traversal or an absolute URL would aim the connector's own credentials at
    # somewhere it never intended to call.
    with pytest.raises(ApiScopeError):
        check_api_request("GET", path, ALLOWLIST)


def test_rejects_a_relative_path():
    with pytest.raises(ApiScopeError, match="must start with"):
        check_api_request("GET", "spaces", ALLOWLIST)


def test_a_query_string_is_part_of_matching():
    """This asserted the opposite -- that a query is allowed and not matched on
    -- which is exactly the hole. urlsplit(...).path dropped it, so the gate
    approved "/projects" while the client sent "/projects?include=cells", and a
    parameter is how a REST endpoint is asked for more than its default. Hex
    lists /projects and deliberately omits /cells; the omission was reachable
    through the listed endpoint."""
    check_api_request("GET", "/spaces?filter=all&per_page=30", ALLOWLIST_WITH_PARAMS)
    with pytest.raises(ApiScopeError, match="query parameter"):
        check_api_request("GET", "/spaces?filter=all&per_page=30", ALLOWLIST)


def test_an_empty_allowlist_permits_nothing():
    # Fail closed: a connector that has not opted in exposes no endpoints.
    with pytest.raises(ApiScopeError):
        check_api_request("GET", "/spaces", [])


@pytest.mark.parametrize(
    "path",
    [
        # Matches "^/spaces/[^/]+/reports$" because "a#b" holds no "/", but the
        # client drops everything from "#" and fetches "/spaces/a" instead --
        # a listed template validating an unlisted request.
        "/spaces/a#b/reports",
        "/spaces#",
        # A literal "#" truncates wherever it sits, query string included.
        "/spaces?filter=#tag",
    ],
)
def test_rejects_a_fragment_because_the_client_would_request_a_shorter_path(path):
    with pytest.raises(ApiScopeError, match="fragment"):
        check_api_request("GET", path, ALLOWLIST)


@pytest.mark.parametrize(
    "path",
    [
        "/spaces?filter=%23tag",
        "/spaces/a%23b/reports",
    ],
)
def test_an_encoded_hash_is_data_and_stays_usable(path):
    """An encoded hash is data, sent through intact -- so it truncates nothing
    and the request issued is the one that was checked. Only a literal "#"
    starts a fragment, so the check reads the raw path rather than the decoded
    one, which is the single place here that distinction matters."""
    check_api_request("GET", path, ALLOWLIST_WITH_PARAMS)


_BASE = "https://app.example.com/api/myworkspace"


@pytest.mark.parametrize(
    "path",
    [
        # The %3F bypass. It decodes to "?" so the gate's own split stopped
        # there, while on the wire it stays an ordinary path character and the
        # "../" segments after it stay live. requests normalises dot segments
        # itself, so this reached /api/api/other_ws/spaces -- outside the
        # workspace base that scopes these credentials -- with no cooperating
        # server needed.
        "/reports/x%3F/../../../api/other_ws/spaces",
        "/reports/x%3F/../../../../etc",
        # Encoded separators, the same trick one level down.
        "/reports/x%2F..%2F..%2Fadmin",
        # And the shapes the earlier checks already caught, re-asserted through
        # the base-aware path so a refactor cannot lose them.
        "/spaces/a#b/reports",
        "/spaces/../../other",
    ],
)
def test_a_path_resolving_outside_the_api_base_is_refused(path):
    with pytest.raises(ApiScopeError):
        check_api_request("GET", path, ALLOWLIST, base_url=_BASE)


@pytest.mark.parametrize(
    "path",
    [
        "/spaces",
        "/spaces/abc/reports",
        "/spaces?filter=all&per_page=30",
        # An encoded "#" is data, not a fragment, and must survive.
        "/spaces?filter=%23tag",
    ],
)
def test_a_listed_path_still_matches_against_the_resolved_url(path):
    check_api_request("GET", path, ALLOWLIST_WITH_PARAMS, base_url=_BASE)


def test_the_gate_still_works_without_a_base_url():
    """Not every provider primes api_base_url, and the raw checks must still
    apply when it is absent -- the base-aware match is an upgrade, not the only
    line of defence."""
    check_api_request("GET", "/spaces", ALLOWLIST)
    with pytest.raises(ApiScopeError):
        check_api_request("GET", "/spaces/a#b/reports", ALLOWLIST)
    with pytest.raises(ApiScopeError):
        check_api_request("GET", "/admin", ALLOWLIST)


@pytest.mark.parametrize("base", [_BASE, _BASE + "/"])
def test_a_trailing_slash_on_the_base_does_not_reject_a_listed_endpoint(base):
    """Joining with f"{base}{path}" gave "//projects" when both carried a
    slash, so the stripped result never matched "/projects". Fail-closed, but a
    legitimate endpoint refused for a stray slash is still wrong -- and Hex's
    base is user-supplied and not normalised."""
    check_api_request("GET", "/projects", ["GET /projects"], base_url=base)


def test_an_encoded_separator_cannot_hide_inside_a_placeholder():
    """A single-segment placeholder sees no "/" in "a%2Fb", so it matched --
    while an API router that decodes percent-escapes routes
    "/spaces/a/b/reports", an endpoint nobody listed."""
    with pytest.raises(ApiScopeError):
        check_api_request("GET", "/spaces/a%2Fb/reports", ALLOWLIST, base_url=_BASE)


def test_a_provider_with_no_base_gets_the_same_resolution():
    """The fallback used to be `decoded.split("?")[0]` -- the original buggy
    form -- so a provider declaring no api_base_url kept both the %3F and %2F
    holes. There is one resolution path now, via a synthetic base."""
    with pytest.raises(ApiScopeError):
        check_api_request("GET", "/reports/x%3F/../../admin", ALLOWLIST)
    with pytest.raises(ApiScopeError):
        check_api_request("GET", "/spaces/a%2Fb/reports", ALLOWLIST)
    check_api_request("GET", "/spaces", ALLOWLIST)


def test_the_gate_and_the_sender_agree_on_the_url():
    """The gate normalised the join and the passthrough did not.

    With a base ending in "/" and a path beginning with one, the gate resolved
    and approved "/projects" while RestApiPassthrough.api still sent
    f"{base}{path}" -- "https://app.hex.tech/api/v1//projects". One path
    validated, a different one issued, which is the class of bug this gate
    exists to close. Both now build the URL through probe_api_url.
    """
    from datahub.ingestion.agent.api_gate import probe_api_url
    from datahub.ingestion.agent.rest_passthrough import RestApiPassthrough

    base = "https://api.example.com/v1/"
    sent = []

    class _Provider(RestApiPassthrough):
        api_allowlist = ["GET /projects"]
        api_base_url = base

        def api_fetch_json(self, url: str) -> object:
            sent.append(url)
            return {}

    _Provider().api("/projects")
    assert sent == [probe_api_url(base, "/projects")]
    assert "//projects" not in sent[0]


# --- the query string is part of the request, so it is part of the check ----
#
# It was dropped before matching: urlsplit(...).path discards it, so the gate
# approved "/projects" and the client sent "/projects?include=cells". A
# parameter is how a REST endpoint is asked for more than its default, which
# makes an unexamined query a hole in the statement the allowlist makes. Hex
# lists /projects and deliberately omits /cells, and the omission was reachable
# through the listed endpoint.

_QUERY_ALLOWLIST = ["GET /projects?include&limit", "GET /data-connections"]
_BASE = "https://app.example.com/api/ws"


def test_an_undeclared_query_parameter_is_refused_on_a_listed_path():
    with pytest.raises(ApiScopeError, match="query parameter"):
        check_api_request(
            "GET", "/data-connections?include=cells", _QUERY_ALLOWLIST, base_url=_BASE
        )


def test_a_declared_query_parameter_is_permitted():
    check_api_request(
        "GET", "/projects?include=cells&limit=10", _QUERY_ALLOWLIST, base_url=_BASE
    )


def test_one_undeclared_parameter_spoils_an_otherwise_declared_query():
    with pytest.raises(ApiScopeError, match="'secret'"):
        check_api_request(
            "GET", "/projects?include=cells&secret=1", _QUERY_ALLOWLIST, base_url=_BASE
        )


def test_a_parameter_declared_on_one_endpoint_does_not_widen_another():
    """The names live on the entry that declared them, not in a shared pool.
    `include` is vouched for on /projects; that says nothing about
    /data-connections."""
    check_api_request("GET", "/projects?include=x", _QUERY_ALLOWLIST, base_url=_BASE)
    with pytest.raises(ApiScopeError, match="query parameter"):
        check_api_request(
            "GET", "/data-connections?include=x", _QUERY_ALLOWLIST, base_url=_BASE
        )


def test_a_bare_question_mark_traversal_is_seen_as_the_parameter_it_is():
    """`/reports/x?/../../../api/other_ws/spaces` is a listed path plus a
    nonsense query. Harmless on the wire -- the server routes on the path --
    but it read as "no query" before, which is the same blind spot that let
    include=cells through."""
    with pytest.raises(ApiScopeError, match="query parameter"):
        check_api_request(
            "GET", "/projects?/../../../other", _QUERY_ALLOWLIST, base_url=_BASE
        )


def test_a_listed_path_with_no_query_is_unaffected():
    """The control. A gate that refuses everything proves nothing, and this
    suite's first run of the fix did exactly that -- the allowlist entries were
    written without the "GET " prefix, so _allowed_paths filtered them all out
    and every case 'passed' by being denied."""
    check_api_request("GET", "/projects", _QUERY_ALLOWLIST, base_url=_BASE)
    check_api_request("GET", "/data-connections", _QUERY_ALLOWLIST, base_url=_BASE)


def test_declaring_no_parameters_permits_no_query_at_all():
    """Default deny: an entry that names none vouches for none."""
    with pytest.raises(ApiScopeError, match="query parameter"):
        check_api_request("GET", "/spaces?limit=1", ALLOWLIST)
