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
