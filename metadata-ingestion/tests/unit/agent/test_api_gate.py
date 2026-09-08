import pytest

from datahub.ingestion.agent.api_gate import ApiScopeError, check_api_request

ALLOWLIST = [
    "GET /spaces",
    "GET /spaces/{token}/reports",
    "GET /reports/{token}/queries",
]


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


def test_a_query_string_is_allowed_and_not_part_of_matching():
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
    check_api_request("GET", path, ALLOWLIST)


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
    check_api_request("GET", path, ALLOWLIST, base_url=_BASE)


def test_the_gate_still_works_without_a_base_url():
    """Not every provider primes api_base_url, and the raw checks must still
    apply when it is absent -- the base-aware match is an upgrade, not the only
    line of defence."""
    check_api_request("GET", "/spaces", ALLOWLIST)
    with pytest.raises(ApiScopeError):
        check_api_request("GET", "/spaces/a#b/reports", ALLOWLIST)
    with pytest.raises(ApiScopeError):
        check_api_request("GET", "/admin", ALLOWLIST)
