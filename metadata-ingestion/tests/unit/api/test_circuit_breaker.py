import json
from unittest.mock import patch

from datahub.api.circuit_breaker import (
    AssertionCircuitBreaker,
    AssertionCircuitBreakerConfig,
)

lastUpdatedResponseBeforeLastAssertion = {
    "dataset": {"operations": [{"lastUpdatedTimestamp": 1640685600000}]}
}

lastUpdatedResponseAfterLastAssertion = {
    "dataset": {"operations": [{"lastUpdatedTimestamp": 1652450039000}]}
}


def test_circuit_breaker_no_error(pytestconfig):
    with patch("gql.client.Client.execute") as mock_gql_client:
        test_resources_dir = pytestconfig.rootpath / "tests/unit/api"
        f = open(
            f"{test_resources_dir}/assertion_gql_response_with_no_error.json",
        )
        data = json.load(f)
        mock_gql_client.side_effect = [lastUpdatedResponseBeforeLastAssertion, data]

        config = AssertionCircuitBreakerConfig(datahub_host="dummy")
        cb = AssertionCircuitBreaker(config)

        result = cb.is_circuit_breaker_active(
            urn="urn:li:dataset:(urn:li:dataPlatform:postgres,postgres1.postgres.public.foo1,PROD)"
        )
        assert result is False


def test_circuit_breaker_updated_at_after_last_assertion(pytestconfig):
    with patch("gql.client.Client.execute") as mock_gql_client:
        test_resources_dir = pytestconfig.rootpath / "tests/unit/api"
        f = open(
            f"{test_resources_dir}/assertion_gql_response_with_no_error.json",
        )
        data = json.load(f)
        mock_gql_client.side_effect = [lastUpdatedResponseAfterLastAssertion, data]

        config = AssertionCircuitBreakerConfig(datahub_host="dummy")
        cb = AssertionCircuitBreaker(config)
        result = cb.is_circuit_breaker_active(
            urn="urn:li:dataset:(urn:li:dataPlatform:postgres,postgres1.postgres.public.foo1,PROD)"
        )
        assert result is True


def test_circuit_breaker_assertion_with_active_assertion(pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/api"
    with patch("gql.client.Client.execute") as mock_gql_client:
        f = open(
            f"{test_resources_dir}/assertion_gql_response.json",
        )
        data = json.load(f)
        mock_gql_client.side_effect = [lastUpdatedResponseBeforeLastAssertion, data]
        config = AssertionCircuitBreakerConfig(datahub_host="dummy")
        cb = AssertionCircuitBreaker(config)
        result = cb.is_circuit_breaker_active(
            urn="urn:li:dataset:(urn:li:dataPlatform:postgres,postgres1.postgres.public.foo1,PROD)"
        )
        assert result is True  # add assertion here
