from unittest.mock import MagicMock, patch

from lib.expectations import (
    PERFORMANCE_PROFILE_ALLOW_WITHOUT_VIEW_AUTH,
    PERFORMANCE_PROFILE_AUTHZ_DENY,
    adapt_benchmarks_for_view_auth,
    benchmark_metric_labels,
    metric_key,
)
from lib.personas import PersonaBenchmark, Scenario
from lib.system_info import (
    SystemInfoSnapshot,
    _view_authorization_from_properties,
    fetch_system_info,
    warn_authorization_settings_drift,
)


def test_view_authorization_from_properties_prefers_dot_key() -> None:
    assert (
        _view_authorization_from_properties(
            {
                "authorization.view.enabled": "true",
                "VIEW_AUTHORIZATION_ENABLED": "false",
            }
        )
        is True
    )


def test_adapt_benchmarks_keeps_fixture_when_view_auth_enabled() -> None:
    benchmark = PersonaBenchmark(
        persona="persona-zero-authz",
        scenarios=[
            Scenario(
                "getDomain",
                {"urn": "urn:li:domain:domain-0001"},
                expected_status_code=403,
            )
        ],
    )
    adapted = adapt_benchmarks_for_view_auth(
        {"persona-zero-authz": benchmark},
        view_authorization_enabled=True,
    )
    scenario = adapted["persona-zero-authz"].scenarios[0]
    assert scenario.expected_status_code == 403
    assert scenario.fixture_expected == 403


def test_adapt_benchmarks_relaxes_deny_when_view_auth_disabled() -> None:
    benchmark = PersonaBenchmark(
        persona="persona-zero-authz",
        scenarios=[
            Scenario(
                "getDomain",
                {"urn": "urn:li:domain:domain-0001"},
                expected_status_code=403,
            )
        ],
    )
    adapted = adapt_benchmarks_for_view_auth(
        {"persona-zero-authz": benchmark},
        view_authorization_enabled=False,
    )
    scenario = adapted["persona-zero-authz"].scenarios[0]
    assert scenario.expected_status_code == 200
    assert scenario.fixture_expected == 403


def test_metric_key_and_profile_for_adapted_deny() -> None:
    scenario = Scenario(
        "getDomain",
        {"urn": "urn:li:domain:domain-0001"},
        expected_status_code=200,
        fixture_expected_status_code=403,
    )
    labels = benchmark_metric_labels(
        scenario,
        view_authorization_enabled=False,
    )
    assert labels["metric_key"] == "getDomain@expect200"
    assert labels["performance_profile"] == PERFORMANCE_PROFILE_ALLOW_WITHOUT_VIEW_AUTH


def test_metric_key_for_authz_deny() -> None:
    scenario = Scenario(
        "getDomain",
        {"urn": "urn:li:domain:domain-0001"},
        expected_status_code=403,
    )
    labels = benchmark_metric_labels(
        scenario,
        view_authorization_enabled=True,
    )
    assert labels["metric_key"] == metric_key("getDomain", 403)
    assert labels["performance_profile"] == PERFORMANCE_PROFILE_AUTHZ_DENY


def test_warn_authorization_settings_drift() -> None:
    snapshots = {
        "local": SystemInfoSnapshot(view_authorization_enabled=True, properties={}),
        "dev06": SystemInfoSnapshot(view_authorization_enabled=False, properties={}),
    }
    with patch("lib.system_info.log") as mock_log:
        warn_authorization_settings_drift(snapshots)
    assert mock_log.call_count == 1
    assert "differs across targets" in mock_log.call_args.args[0]


@patch("lib.system_info.requests.get")
def test_fetch_system_info(mock_get: MagicMock) -> None:
    mock_get.return_value.json.return_value = {
        "authorization.view.enabled": "true",
        "authorization.defaultAuthorizer.enabled": "true",
    }
    mock_get.return_value.raise_for_status = MagicMock()
    snap = fetch_system_info("http://localhost:8080", "token")
    assert snap.view_authorization_enabled is True
    assert snap.properties["authorization.view.enabled"] == "true"
