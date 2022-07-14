import json
from unittest.mock import patch

import pytest
from freezegun import freeze_time

try:
    from datahub.api.circuit_breaker import (
        AssertionCircuitBreaker,
        AssertionCircuitBreakerConfig,
        OperationCircuitBreaker,
        OperationCircuitBreakerConfig,
    )
# Imports are only available if we are running integrations tests
except ImportError:
    pass
lastUpdatedResponseBeforeLastAssertion = {
    "dataset": {"operations": [{"lastUpdatedTimestamp": 1640685600000}]}
}

lastUpdatedResponseAfterLastAssertion = {
    "dataset": {"operations": [{"lastUpdatedTimestamp": 1652450039000}]}
}


@pytest.mark.integration
def test_operation_circuit_breaker_with_empty_response(pytestconfig):
    with patch("gql.client.Client.execute") as mock_gql_client:
        test_resources_dir = pytestconfig.rootpath / "tests/integration/circuit_breaker"
        f = open(
            f"{test_resources_dir}/operation_gql_empty_response.json",
        )
        data = json.load(f)
        mock_gql_client.side_effect = [data]

        config = OperationCircuitBreakerConfig(datahub_host="dummy")
        cb = OperationCircuitBreaker(config)

        result = cb.is_circuit_breaker_active(
            urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD))"
        )
        assert result is True


@freeze_time("2022-06-20 05:00:00")
@pytest.mark.integration
def test_operation_circuit_breaker_with_valid_response(pytestconfig):
    with patch("gql.client.Client.execute") as mock_gql_client:
        test_resources_dir = pytestconfig.rootpath / "tests/integration/circuit_breaker"
        f = open(
            f"{test_resources_dir}/operation_gql_response.json",
        )
        data = json.load(f)
        mock_gql_client.side_effect = [data]

        config = OperationCircuitBreakerConfig(datahub_host="dummy")
        cb = OperationCircuitBreaker(config)

        result = cb.is_circuit_breaker_active(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.jaffle_shop.customers,PROD)"
        )
        assert result is False


@freeze_time("2022-06-21 07:00:00")
@pytest.mark.integration
def test_operation_circuit_breaker_with_not_recent_operation(pytestconfig):
    with patch("gql.client.Client.execute") as mock_gql_client:
        test_resources_dir = pytestconfig.rootpath / "tests/integration/circuit_breaker"
        f = open(
            f"{test_resources_dir}/operation_gql_response.json",
        )
        data = json.load(f)
        mock_gql_client.side_effect = [data]

        config = OperationCircuitBreakerConfig(datahub_host="dummy")
        cb = OperationCircuitBreaker(config)

        result = cb.is_circuit_breaker_active(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.jaffle_shop.customers,PROD)"
        )
        assert result is True


@pytest.mark.integration
def test_assertion_circuit_breaker_with_empty_response(pytestconfig):
    with patch("gql.client.Client.execute") as mock_gql_client:
        test_resources_dir = pytestconfig.rootpath / "tests/integration/circuit_breaker"
        f = open(
            f"{test_resources_dir}/assertion_gql_empty_response.json",
        )
        data = json.load(f)
        mock_gql_client.side_effect = [lastUpdatedResponseBeforeLastAssertion, data]

        config = AssertionCircuitBreakerConfig(datahub_host="dummy")
        cb = AssertionCircuitBreaker(config)

        result = cb.is_circuit_breaker_active(
            urn="urn:li:dataset:(urn:li:dataPlatform:postgres,postgres1.postgres.public.foo1,PROD)"
        )
        assert result is True


@pytest.mark.integration
def test_assertion_circuit_breaker_with_no_error(pytestconfig):
    with patch("gql.client.Client.execute") as mock_gql_client:
        test_resources_dir = pytestconfig.rootpath / "tests/integration/circuit_breaker"
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


@pytest.mark.integration
def test_assertion_circuit_breaker_updated_at_after_last_assertion(pytestconfig):
    with patch("gql.client.Client.execute") as mock_gql_client:
        test_resources_dir = pytestconfig.rootpath / "tests/integration/circuit_breaker"
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


@pytest.mark.integration
def test_assertion_circuit_breaker_assertion_with_active_assertion(pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/circuit_breaker"
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
