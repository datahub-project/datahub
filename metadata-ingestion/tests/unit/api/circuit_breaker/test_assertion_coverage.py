from unittest.mock import patch

import pytest

from datahub.api.circuit_breaker import (
    AssertionCircuitBreaker,
    AssertionCircuitBreakerConfig,
)

DATASET = "urn:li:dataset:(urn:li:dataPlatform:postgres,example.public.orders,PROD)"


def _assertion(urn: str, *results: tuple[int, str | None]) -> dict[str, object]:
    return {
        "runEvents": {
            "runEvents": [
                {
                    "assertionUrn": urn,
                    "timestampMillis": timestamp,
                    "result": {"type": state} if state is not None else None,
                }
                for timestamp, state in results
            ]
        }
    }


@pytest.mark.parametrize("state", ["ERROR", "INIT", None])
def test_success_does_not_cover_an_unestablished_assertion(state: str | None) -> None:
    with patch("datahub.api.circuit_breaker.assertion_circuit_breaker.Assertion"):
        breaker = AssertionCircuitBreaker(
            AssertionCircuitBreakerConfig(
                datahub_host="http://example.test", verify_after_last_update=False
            )
        )
        with patch.object(
            breaker.assertion_api,
            "query_assertion",
            return_value=[
                _assertion("urn:li:assertion:checked", (2000, "SUCCESS")),
                _assertion("urn:li:assertion:unestablished", (2000, state)),
            ],
        ):
            assert breaker.is_circuit_breaker_active(DATASET)


def test_success_does_not_cover_an_assertion_without_runs() -> None:
    with patch("datahub.api.circuit_breaker.assertion_circuit_breaker.Assertion"):
        breaker = AssertionCircuitBreaker(
            AssertionCircuitBreakerConfig(
                datahub_host="http://example.test", verify_after_last_update=False
            )
        )
        with patch.object(
            breaker.assertion_api,
            "query_assertion",
            return_value=[
                _assertion("urn:li:assertion:checked", (2000, "SUCCESS")),
                _assertion("urn:li:assertion:unexamined"),
            ],
        ):
            assert breaker.is_circuit_breaker_active(DATASET)


@pytest.mark.parametrize(
    "older, latest, active", [("ERROR", "SUCCESS", False), ("SUCCESS", "ERROR", True)]
)
def test_latest_result_controls_recovery(older: str, latest: str, active: bool) -> None:
    with patch("datahub.api.circuit_breaker.assertion_circuit_breaker.Assertion"):
        breaker = AssertionCircuitBreaker(
            AssertionCircuitBreakerConfig(
                datahub_host="http://example.test", verify_after_last_update=False
            )
        )
        with patch.object(
            breaker.assertion_api,
            "query_assertion",
            return_value=[
                _assertion("urn:li:assertion:checked", (2000, "SUCCESS")),
                _assertion("urn:li:assertion:retry", (2000, latest), (1000, older)),
            ],
        ):
            assert breaker.is_circuit_breaker_active(DATASET) == active


@pytest.mark.parametrize("states", [("SUCCESS", "ERROR"), ("ERROR", "SUCCESS")])
def test_conflicting_results_at_the_same_time_do_not_allow_continuation(
    states: tuple[str, str],
) -> None:
    with patch("datahub.api.circuit_breaker.assertion_circuit_breaker.Assertion"):
        breaker = AssertionCircuitBreaker(
            AssertionCircuitBreakerConfig(
                datahub_host="http://example.test", verify_after_last_update=False
            )
        )
        with patch.object(
            breaker.assertion_api,
            "query_assertion",
            return_value=[
                _assertion(
                    "urn:li:assertion:retry", (2000, states[0]), (2000, states[1])
                )
            ],
        ):
            assert breaker.is_circuit_breaker_active(DATASET)
