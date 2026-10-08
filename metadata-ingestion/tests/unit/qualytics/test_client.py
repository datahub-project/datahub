"""Tests for the Qualytics HTTP client.

Covers the behaviour we wrote: pagination across pages, retry on transient failures,
auth-vs-other error classification, TLS wiring, and root-path detection. `requests`
itself is not under test.
"""

from typing import Any

import pytest
import requests
from requests.adapters import HTTPAdapter
from requests_mock import Mocker
from urllib3.util.retry import Retry

from datahub.ingestion.source.qualytics.client import (
    QualyticsApiError,
    QualyticsAuthError,
    QualyticsClient,
)
from datahub.ingestion.source.qualytics.config import QualyticsSourceConfig

BASE = "https://acme.qualytics.io/api"


def _client(**overrides: object) -> QualyticsClient:
    config = QualyticsSourceConfig.model_validate(
        {"base_url": BASE, "token": "t", **overrides}
    )
    return QualyticsClient(config)


def _last_request(m: Mocker) -> Any:
    assert m.last_request is not None
    return m.last_request


def _retry_policy(client: QualyticsClient) -> Retry:
    adapter = client.session.get_adapter("https://acme.qualytics.io")
    assert isinstance(adapter, HTTPAdapter)
    retries = adapter.max_retries
    assert isinstance(retries, Retry)
    return retries


def _page(
    items: list[dict[str, Any]], page: int, pages: int, size: int
) -> dict[str, Any]:
    return {
        "items": items,
        "page": page,
        "pages": pages,
        "size": size,
        "total": len(items) * pages,
    }


def test_paginate_walks_every_page_and_stops_at_the_last(requests_mock: Mocker) -> None:
    requests_mock.get(
        f"{BASE}/datastores",
        [
            {"json": _page([{"id": 1}, {"id": 2}], page=1, pages=3, size=2)},
            {"json": _page([{"id": 3}, {"id": 4}], page=2, pages=3, size=2)},
            {"json": _page([{"id": 5}], page=3, pages=3, size=2)},
        ],
    )

    got = list(_client(page_size=2).list_datastores())

    assert [d["id"] for d in got] == [1, 2, 3, 4, 5]
    # Exactly three requests: it must not keep asking for page 4.
    assert requests_mock.call_count == 3
    assert [r.qs["page"] for r in requests_mock.request_history] == [
        ["1"],
        ["2"],
        ["3"],
    ]


def test_a_malformed_page_envelope_fails_loudly_rather_than_yielding_nothing(
    requests_mock: Mocker,
) -> None:
    # The most dangerous failure this client can have. The previous
    # `payload.get("items") or []` turned any envelope change into a run that emitted
    # nothing, scanned nothing, warned about nothing and exited 0 -- indistinguishable
    # from an empty tenant. Validating the envelope makes contract drift visible.
    requests_mock.get(f"{BASE}/datastores", json={"data": [{"id": 1}]})

    with pytest.raises(QualyticsApiError) as exc:
        list(_client().list_datastores())

    assert "page envelope" in str(exc.value)


def test_paginate_is_lazy(requests_mock: Mocker) -> None:
    # The generator must not fetch anything until iterated, and must fetch page 2 only
    # when the caller asks past the end of page 1. Tens of thousands of quality checks
    # make eager fetching a memory problem.
    requests_mock.get(
        f"{BASE}/datastores",
        [
            {"json": _page([{"id": 1}], page=1, pages=2, size=1)},
            {"json": _page([{"id": 2}], page=2, pages=2, size=1)},
        ],
    )

    items = _client(page_size=1).list_datastores()
    assert requests_mock.call_count == 0

    next(items)
    assert requests_mock.call_count == 1

    next(items)
    assert requests_mock.call_count == 2


def test_401_raises_auth_error_not_a_generic_api_error(requests_mock: Mocker) -> None:
    # The distinction drives behaviour: an auth failure aborts the run instead of
    # producing one warning per item.
    requests_mock.get(f"{BASE}/datastores", status_code=401, json={"detail": "nope"})

    with pytest.raises(QualyticsAuthError) as exc:
        list(_client().list_datastores())

    assert "rejected the API token" in str(exc.value)


def test_403_is_also_an_auth_error(requests_mock: Mocker) -> None:
    requests_mock.get(
        f"{BASE}/datastores", status_code=403, json={"detail": "forbidden"}
    )

    with pytest.raises(QualyticsAuthError):
        list(_client().list_datastores())


def test_404_is_a_plain_api_error_carrying_the_body(requests_mock: Mocker) -> None:
    requests_mock.get(f"{BASE}/datastores", status_code=404, text="no such route")

    with pytest.raises(QualyticsApiError) as exc:
        list(_client().list_datastores())

    assert "404" in str(exc.value)
    assert "no such route" in str(exc.value)
    assert not isinstance(exc.value, QualyticsAuthError)


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_statuses_are_retried(status: int) -> None:
    # Asserted against the installed urllib3 policy rather than through requests_mock,
    # which swaps out the HTTPAdapter and so never exercises retries at all. This calls
    # urllib3's real decision logic, so it tests the policy we chose, not its fields.
    retry = _retry_policy(_client())
    assert retry.is_retry("GET", status) is True


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
def test_client_errors_are_not_retried(status: int) -> None:
    # Retrying a bad token or a missing route just multiplies the wait before the user
    # sees the real problem.
    retry = _retry_policy(_client())
    assert retry.is_retry("GET", status) is False


def test_retry_budget_comes_from_config() -> None:
    retry = _retry_policy(_client(max_retries=5))
    assert retry.total == 5


def test_connection_errors_are_wrapped_with_the_url(requests_mock: Mocker) -> None:
    requests_mock.get(f"{BASE}/datastores", exc=requests.exceptions.ConnectTimeout)

    with pytest.raises(QualyticsApiError) as exc:
        list(_client().list_datastores())

    assert f"{BASE}/datastores" in str(exc.value)


def test_tls_failures_point_at_the_ca_cert_option(requests_mock: Mocker) -> None:
    # Private-CA deployments are common; the generic SSLError text does not tell the
    # operator that there is a config field for exactly this.
    requests_mock.get(f"{BASE}/datastores", exc=requests.exceptions.SSLError)

    with pytest.raises(QualyticsApiError) as exc:
        list(_client().list_datastores())

    assert "ca_cert_path" in str(exc.value)


def test_non_json_body_is_reported_as_such(requests_mock: Mocker) -> None:
    # A proxy or login page returning HTML with a 200 is a real failure mode on
    # private deployments, and "Expecting value: line 1 column 1" helps nobody.
    requests_mock.get(f"{BASE}/datastores", text="<html>login</html>")

    with pytest.raises(QualyticsApiError) as exc:
        list(_client().list_datastores())

    assert "non-JSON" in str(exc.value)


def test_token_is_sent_as_a_bearer_header(requests_mock: Mocker) -> None:
    requests_mock.get(f"{BASE}/datastores", json=_page([], page=1, pages=1, size=100))

    list(_client(token="s3cret").list_datastores())

    assert _last_request(requests_mock).headers["Authorization"] == "Bearer s3cret"


def test_tls_and_timeout_settings_reach_the_actual_request(
    requests_mock: Mocker,
) -> None:
    # Asserted on the outgoing request rather than the client's private attribute:
    # what matters is that requests receives them. A timeout that never reaches the
    # call is exactly how a hung private deployment wedges an ingestion forever.
    requests_mock.get(f"{BASE}/datastores", json=_page([], page=1, pages=1, size=100))

    list(_client(timeout_sec=7, ca_cert_path="/etc/ssl/corp.pem").list_datastores())

    assert _last_request(requests_mock).timeout == 7
    assert _last_request(requests_mock).verify == "/etc/ssl/corp.pem"


def test_verify_ssl_false_disables_verification(requests_mock: Mocker) -> None:
    requests_mock.get(f"{BASE}/datastores", json=_page([], page=1, pages=1, size=100))

    list(_client(verify_ssl=False).list_datastores())

    assert _last_request(requests_mock).verify is False


def test_a_rejected_token_is_not_swallowed_by_the_version_probe(
    requests_mock: Mocker,
) -> None:
    # get_version() is the first call of every run. Swallowing the 401 here threw away
    # the actionable message and let the run limp on to fail later with less context.
    requests_mock.get(f"{BASE}/openapi.json", status_code=401, json={"detail": "nope"})

    with pytest.raises(QualyticsAuthError):
        _client().get_version()


def test_quality_checks_are_filtered_server_side(requests_mock: Mocker) -> None:
    # Filtering server-side is the mitigation for assertion-volume blowup: fetching
    # every check and discarding most of them is what makes large tenants slow.
    requests_mock.get(
        f"{BASE}/quality-checks", json=_page([], page=1, pages=1, size=100)
    )

    list(_client().list_quality_checks(container_id=42))

    qs = _last_request(requests_mock).qs
    assert qs["container"] == ["42"]
    # No `archived`: the spec types it Literal["include", "only"], so a bool 422s.
    # This assertion used to require archived=false and locked the bug in.
    assert "archived" not in qs


def test_anomaly_window_is_passed_through_as_query_params(
    requests_mock: Mocker,
) -> None:
    requests_mock.get(f"{BASE}/anomalies", json=_page([], page=1, pages=1, size=100))

    list(_client().list_anomalies(start_date="2026-08-10", end_date="2026-09-09"))

    qs = _last_request(requests_mock).qs
    assert qs["start_date"] == ["2026-08-10"]
    assert qs["end_date"] == ["2026-09-09"]


def test_detect_api_root_path_reads_it_from_the_deployment_spec(
    requests_mock: Mocker,
) -> None:
    # The root path is configured per deployment.
    requests_mock.get(
        f"{BASE}/openapi.json",
        json={"paths": {"/gateway/datastores": {}, "/gateway/containers": {}}},
    )

    assert _client().detect_api_root_path() == "/gateway"


def test_detect_api_root_path_handles_a_multi_segment_root(
    requests_mock: Mocker,
) -> None:
    requests_mock.get(
        f"{BASE}/openapi.json",
        json={"paths": {"/gateway/api/datastores": {}, "/gateway/api/containers": {}}},
    )

    assert _client().detect_api_root_path() == "/gateway/api"


def test_detect_api_root_path_is_not_thrown_by_a_stray_top_level_route(
    requests_mock: Mocker,
) -> None:
    requests_mock.get(
        f"{BASE}/openapi.json",
        json={"paths": {"/health": {}, "/api/datastores": {}, "/api/containers": {}}},
    )

    assert _client().detect_api_root_path() == "/api"


def test_detect_api_root_path_gives_up_without_a_datastores_path(
    requests_mock: Mocker,
) -> None:
    requests_mock.get(f"{BASE}/openapi.json", json={"paths": {"/api/other": {}}})

    assert _client().detect_api_root_path() is None


def test_missing_root_path_in_base_url_is_reported_with_the_fix() -> None:
    with Mocker() as m:
        origin = "https://acme.qualytics.io"
        m.get(f"{origin}/openapi.json", json={"paths": {"/api/datastores": {}}})

        problem = _client(base_url=origin).check_base_url_root_path()

    assert problem is not None
    assert f"{origin}/api" in problem


def test_matching_root_path_reports_no_problem(requests_mock: Mocker) -> None:
    requests_mock.get(f"{BASE}/openapi.json", json={"paths": {"/api/datastores": {}}})

    assert _client().check_base_url_root_path() is None


def test_get_version_returns_none_rather_than_raising_when_the_spec_is_unavailable(
    requests_mock: Mocker,
) -> None:
    # The version is a reporting nicety; failing to read it must not fail the run.
    requests_mock.get(f"{BASE}/openapi.json", status_code=404, text="nope")

    assert _client().get_version() is None
