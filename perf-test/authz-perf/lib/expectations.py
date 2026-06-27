from __future__ import annotations

from typing import Dict

from lib.personas import PersonaBenchmark, Scenario

# Operations whose deny semantics require view authorization on OSS.
VIEW_GATED_OPERATIONS = frozenset({"getDomain"})

# Human-readable performance buckets for JSONL / compare grouping.
PERFORMANCE_PROFILE_AUTHZ_ALLOW = "authz_allow"
PERFORMANCE_PROFILE_AUTHZ_DENY = "authz_deny"
PERFORMANCE_PROFILE_ALLOW_WITHOUT_VIEW_AUTH = "allow_without_view_auth"


def metric_key(operation: str, expected_status_code: int) -> str:
    """Stable metric name keyed by effective HTTP expectation."""
    return f"{operation}@expect{expected_status_code}"


def performance_profile_for_scenario(
    scenario: Scenario,
    *,
    view_authorization_enabled: bool | None,
) -> str:
    if (
        scenario.fixture_expected == 403
        and scenario.expected_status_code == 200
    ):
        return PERFORMANCE_PROFILE_ALLOW_WITHOUT_VIEW_AUTH
    if scenario.expected_status_code == 403:
        return PERFORMANCE_PROFILE_AUTHZ_DENY
    return PERFORMANCE_PROFILE_AUTHZ_ALLOW


def benchmark_metric_labels(
    scenario: Scenario,
    *,
    view_authorization_enabled: bool | None,
) -> dict[str, str]:
    return {
        "metric_key": metric_key(scenario.operation, scenario.expected_status_code),
        "performance_profile": performance_profile_for_scenario(
            scenario,
            view_authorization_enabled=view_authorization_enabled,
        ),
    }


def adapt_benchmarks_for_view_auth(
    benchmarks: Dict[str, PersonaBenchmark],
    *,
    view_authorization_enabled: bool | None,
) -> Dict[str, PersonaBenchmark]:
    """Return benchmarks with per-target effective status expectations.

    Fixture ``expected_status_code: 403`` on view-gated operations assumes
    ``authorization.view.enabled=true``. When view authorization is disabled on
    a host, those scenarios expect HTTP 200 instead so runs can continue and
    latency comparisons stay meaningful.
    """
    if view_authorization_enabled is not False:
        return benchmarks

    adapted: Dict[str, PersonaBenchmark] = {}
    for name, benchmark in benchmarks.items():
        scenarios: list[Scenario] = []
        for scenario in benchmark.scenarios:
            fixture_expected = scenario.expected_status_code
            effective_expected = fixture_expected
            if (
                fixture_expected == 403
                and scenario.operation in VIEW_GATED_OPERATIONS
            ):
                effective_expected = 200
            scenarios.append(
                Scenario(
                    operation=scenario.operation,
                    variables=scenario.variables,
                    assertions=scenario.assertions,
                    expected_status_code=effective_expected,
                    fixture_expected_status_code=(
                        fixture_expected
                        if effective_expected != fixture_expected
                        else None
                    ),
                )
            )
        adapted[name] = PersonaBenchmark(
            persona=benchmark.persona,
            scenarios=scenarios,
        )
    return adapted
