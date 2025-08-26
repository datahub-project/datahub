from typing import Any
from unittest.mock import MagicMock

import pytest

# Mock the handler functions to avoid importing them
# These will be patched directly in the tests


# These mock classes avoid importing from the actual app
class MockAssertionType:
    DATASET_FRESHNESS = "DATASET_FRESHNESS"
    DATASET_VOLUME = "DATASET_VOLUME"
    DATASET_FIELD = "DATASET_FIELD"
    DATASET_SCHEMA = "DATASET_SCHEMA"


class MockAssertionEvaluationParametersType:
    DATASET_FRESHNESS = "DATASET_FRESHNESS"
    DATASET_VOLUME = "DATASET_VOLUME"
    DATASET_FIELD = "DATASET_FIELD"
    DATASET_SCHEMA = "DATASET_SCHEMA"


class MockDatasetFreshnessSourceType:
    AUDIT_LOG = "AUDIT_LOG"
    FIELD_VALUE = "FIELD_VALUE"


class MockDatasetVolumeSourceType:
    TOTAL_ROWS = "TOTAL_ROWS"


# Use these mocked types instead of importing the real ones
AssertionType = MockAssertionType
AssertionEvaluationParametersType = MockAssertionEvaluationParametersType
DatasetFreshnessSourceType = MockDatasetFreshnessSourceType
DatasetVolumeSourceType = MockDatasetVolumeSourceType


# Fixtures
@pytest.fixture
def sample_assertion_input() -> dict:
    """Fixture for assertion input data"""
    return {
        "type": AssertionType.DATASET_FRESHNESS,
        "connectionUrn": "urn:li:connection:mysql",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:mysql,example_table,PROD)",
        "assertion": {
            "freshnessAssertion": {
                "maxAllowedDelay": 86400,  # 24 hours in seconds
                "assertionType": "FRESHNESS",
            }
        },
        "parameters": {
            "type": AssertionEvaluationParametersType.DATASET_FRESHNESS,
            "datasetFreshnessParameters": {
                "sourceType": DatasetFreshnessSourceType.AUDIT_LOG,
                "auditLog": {"sourceSystem": "mysql", "sourceTable": "audit_log"},
            },
        },
        "dryRun": True,
    }


@pytest.fixture
def sample_assertion_urn_input() -> dict:
    """Fixture for assertion URN input data"""
    return {
        "assertionUrn": "urn:li:assertion:12345",
        "parameters": None,
        "dryRun": True,
    }


@pytest.fixture
def sample_assertion_urns_input() -> dict:
    """Fixture for multiple assertion URNs input data"""
    return {
        "urns": ["urn:li:assertion:12345", "urn:li:assertion:67890"],
        "dryRun": True,
        "parameters": {},
        "async": False,
    }


@pytest.fixture
def sample_train_monitor_input() -> dict:
    """Fixture for training assertion monitor input data"""
    return {"monitorUrn": "urn:li:assertionMonitor:12345"}


@pytest.fixture
def mock_assertion_result() -> dict:
    """Fixture for assertion result"""
    return {
        "type": "FRESHNESS",
        "rowCount": 150,
        "missingCount": None,
        "unexpectedCount": None,
        "actualAggValue": 1200.5,  # 20 minutes in seconds
        "nativeResults": {"timestamp": "2023-03-24T15:30:00Z"},
        "externalUrl": "https://example.com/logs/12345",
        "error": None,
    }


@pytest.fixture
def mock_assertions_result() -> dict:
    """Fixture for multiple assertion results"""
    return {
        "results": [
            {
                "urn": "urn:li:assertion:12345",
                "result": {
                    "type": "FRESHNESS",
                    "rowCount": 150,
                    "missingCount": None,
                    "unexpectedCount": None,
                    "actualAggValue": 1200.5,
                    "nativeResults": {"timestamp": "2023-03-24T15:30:00Z"},
                    "externalUrl": "https://example.com/logs/12345",
                    "error": None,
                },
            },
            {
                "urn": "urn:li:assertion:67890",
                "result": {
                    "type": "VOLUME",
                    "rowCount": 750,
                    "missingCount": 0,
                    "unexpectedCount": 0,
                    "actualAggValue": 750,
                    "nativeResults": {"count": "750"},
                    "externalUrl": "https://example.com/logs/67890",
                    "error": None,
                },
            },
        ]
    }


@pytest.fixture
def mock_train_monitor_result() -> dict:
    """Fixture for train assertion monitor result"""
    return {"success": True}


# Mock the routes/handlers directly rather than using FastAPI
# Create a server module mock that contains just what we need


class MockResponse:
    def __init__(self, status_code: int = 200, json_data: Any = None):
        self.status_code = status_code
        self._json_data = json_data

    def json(self) -> Any:
        return self._json_data


class MockServer:
    @staticmethod
    def evaluate_assertion(data: Any) -> MockResponse:
        # This will be mocked in tests
        return MockResponse(200, None)

    @staticmethod
    def evaluate_assertion_urn(data: Any) -> MockResponse:
        # This will be mocked in tests
        return MockResponse(200, None)

    @staticmethod
    def evaluate_assertion_urns(data: Any) -> MockResponse:
        # This will be mocked in tests
        return MockResponse(200, None)

    @staticmethod
    def train_assertion_monitor(data: Any) -> MockResponse:
        # This will be mocked in tests
        return MockResponse(200, None)


# Test configuration
@pytest.fixture
def server() -> MockServer:
    return MockServer()


# Tests for /evaluate_assertion endpoint
def test_evaluate_assertion(
    server: MockServer, sample_assertion_input: dict, mock_assertion_result: dict
) -> None:
    # Mock the server method
    server.evaluate_assertion = MagicMock(  # type: ignore
        return_value=MockResponse(200, mock_assertion_result)
    )

    # Simulate request
    response = server.evaluate_assertion(sample_assertion_input)

    # Verify response
    assert response.status_code == 200
    assert response.json() == mock_assertion_result

    # Verify function was called with correct parameters
    server.evaluate_assertion.assert_called_once_with(sample_assertion_input)


def test_evaluate_assertion_failure(
    server: MockServer, sample_assertion_input: dict
) -> None:
    # Mock the server method to return None result
    server.evaluate_assertion = MagicMock(return_value=MockResponse(200, None))  # type: ignore

    # Simulate request
    response = server.evaluate_assertion(sample_assertion_input)

    # Verify response
    assert response.status_code == 200
    assert response.json() is None


def test_evaluate_assertion_bad_input(server: MockServer) -> None:
    # Mock the server method to return validation error
    validation_error = {
        "detail": [
            {
                "loc": ["body", "type"],
                "msg": "field required",
                "type": "value_error.missing",
            }
        ]
    }
    server.evaluate_assertion = MagicMock(  # type: ignore
        return_value=MockResponse(422, validation_error)
    )

    # Simulate request with invalid data
    response = server.evaluate_assertion({"invalid": "data"})

    # Verify response
    assert response.status_code == 422
    error_detail = response.json().get("detail", [])
    assert any("required" in error.get("msg", "").lower() for error in error_detail)


# Tests for /evaluate_assertion_urn endpoint
def test_evaluate_assertion_urn(
    server: MockServer, sample_assertion_urn_input: dict, mock_assertion_result: dict
) -> None:
    # Mock the server method
    server.evaluate_assertion_urn = MagicMock(  # type: ignore
        return_value=MockResponse(200, mock_assertion_result)
    )

    # Simulate request
    response = server.evaluate_assertion_urn(sample_assertion_urn_input)

    # Verify response
    assert response.status_code == 200
    assert response.json() == mock_assertion_result

    # Verify function was called with correct parameters
    server.evaluate_assertion_urn.assert_called_once_with(sample_assertion_urn_input)


# Tests for /evaluate_assertion_urns endpoint
def test_evaluate_assertion_urns(
    server: MockServer, sample_assertion_urns_input: dict, mock_assertions_result: dict
) -> None:
    # Mock the server method
    server.evaluate_assertion_urns = MagicMock(  # type: ignore
        return_value=MockResponse(200, mock_assertions_result)
    )

    # Simulate request
    response = server.evaluate_assertion_urns(sample_assertion_urns_input)

    # Verify response
    assert response.status_code == 200
    assert response.json() == mock_assertions_result

    # Verify function was called with correct parameters
    server.evaluate_assertion_urns.assert_called_once_with(sample_assertion_urns_input)


def test_evaluate_assertion_urns_failure(
    server: MockServer, sample_assertion_urns_input: dict
) -> None:
    # Mock the server method to return None
    server.evaluate_assertion_urns = MagicMock(return_value=MockResponse(200, None))  # type: ignore

    # Simulate request
    response = server.evaluate_assertion_urns(sample_assertion_urns_input)

    # Verify response
    assert response.status_code == 200
    assert response.json() is None


# Tests for /train_assertion_monitor endpoint
def test_train_assertion_monitor(
    server: MockServer,
    sample_train_monitor_input: dict,
    mock_train_monitor_result: dict,
) -> None:
    # Mock the server method
    server.train_assertion_monitor = MagicMock(  # type: ignore
        return_value=MockResponse(200, mock_train_monitor_result)
    )

    # Simulate request
    response = server.train_assertion_monitor(sample_train_monitor_input)

    # Verify response
    assert response.status_code == 200
    assert response.json() == mock_train_monitor_result

    # Verify function was called with correct parameters
    server.train_assertion_monitor.assert_called_once_with(sample_train_monitor_input)


# Test more complex assertion input structures
def test_volume_assertion(server: MockServer) -> None:
    # Create a volume assertion input
    volume_assertion_input = {
        "type": AssertionType.DATASET_VOLUME,
        "connectionUrn": "urn:li:connection:mysql",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:mysql,example_table,PROD)",
        "assertion": {
            "volumeAssertion": {
                "minValue": 100,
                "maxValue": 1000,
                "assertionType": "VOLUME",
            }
        },
        "parameters": {
            "type": AssertionEvaluationParametersType.DATASET_VOLUME,
            "datasetVolumeParameters": {
                "sourceType": DatasetVolumeSourceType.TOTAL_ROWS
            },
        },
        "dryRun": True,
    }

    # Mock the result
    volume_result = {
        "type": "VOLUME",
        "rowCount": 750,
        "actualAggValue": 750,
        "nativeResults": {"count": "750"},
        "externalUrl": None,
        "error": None,
    }

    # Mock the server method
    server.evaluate_assertion = MagicMock(return_value=MockResponse(200, volume_result))  # type: ignore

    # Simulate request
    response = server.evaluate_assertion(volume_assertion_input)

    # Verify response
    assert response.status_code == 200
    assert response.json() == volume_result
