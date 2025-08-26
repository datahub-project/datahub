import unittest
from typing import Any, Dict, Optional

import pytest
from pydantic.error_wrappers import ValidationError

from datahub_executor.common.types import (
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
)
from datahub_executor.coordinator.utils import (
    extract_assertion_entity_from_graphql,
    extract_assertion_monitor_executor_id,
    extract_assertion_monitor_parameters,
)


class TestExtractAssertionEntityFromGraphQL(unittest.TestCase):
    """Test suite for the extract_assertion_entity_from_graphql function."""

    def test_extract_complete_entity(self) -> None:
        """Test extracting a complete entity with all fields present."""
        # Arrange
        assertion: Dict[str, Any] = {
            "relationships": {
                "relationships": [
                    {
                        "entity": {
                            "platform": {"urn": "urn:li:dataPlatform:snowflake"},
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,example.table,PROD)",
                            "exists": True,
                            "properties": {
                                "name": "example.table",
                                "qualifiedName": "PROD.example.table",
                                "description": "An example table",
                            },
                            "subTypes": {"typeNames": ["TABLE", "VIEW"]},
                        }
                    }
                ]
            }
        }

        # Act
        result = extract_assertion_entity_from_graphql(assertion)

        # Assert
        self.assertEqual(
            result["urn"],
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,example.table,PROD)",
        )
        self.assertEqual(result["platformUrn"], "urn:li:dataPlatform:snowflake")
        self.assertEqual(result["table_name"], "example.table")
        self.assertEqual(result["qualified_name"], "PROD.example.table")
        self.assertEqual(result["subTypes"], ["TABLE", "VIEW"])
        self.assertEqual(result["platformInstance"], None)
        self.assertTrue(result["exists"])

    def test_extract_entity_missing_properties(self) -> None:
        """Test extracting an entity with missing properties field."""
        # Arrange
        assertion: Dict[str, Any] = {
            "relationships": {
                "relationships": [
                    {
                        "entity": {
                            "platform": {"urn": "urn:li:dataPlatform:snowflake"},
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,example.table,PROD)",
                            "exists": True,
                            "subTypes": {"typeNames": ["TABLE"]},
                        }
                    }
                ]
            }
        }

        # Act
        result = extract_assertion_entity_from_graphql(assertion)

        # Assert
        self.assertEqual(
            result["urn"],
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,example.table,PROD)",
        )
        self.assertEqual(result["platformUrn"], "urn:li:dataPlatform:snowflake")
        self.assertIsNone(result["table_name"])
        self.assertIsNone(result["qualified_name"])
        self.assertEqual(result["subTypes"], ["TABLE"])
        self.assertTrue(result["exists"])

    def test_extract_entity_null_properties(self) -> None:
        """Test extracting an entity with properties set to null."""
        # Arrange
        assertion: Dict[str, Any] = {
            "relationships": {
                "relationships": [
                    {
                        "entity": {
                            "platform": {"urn": "urn:li:dataPlatform:snowflake"},
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,example.table,PROD)",
                            "exists": True,
                            "properties": None,
                            "subTypes": {"typeNames": ["TABLE"]},
                        }
                    }
                ]
            }
        }

        # Act
        result = extract_assertion_entity_from_graphql(assertion)

        # Assert
        self.assertIsNone(result["table_name"])
        self.assertIsNone(result["qualified_name"])

    def test_extract_entity_partial_properties(self) -> None:
        """Test extracting an entity with partial properties (only name, no qualifiedName)."""
        # Arrange
        assertion: Dict[str, Any] = {
            "relationships": {
                "relationships": [
                    {
                        "entity": {
                            "platform": {"urn": "urn:li:dataPlatform:snowflake"},
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,example.table,PROD)",
                            "exists": True,
                            "properties": {
                                "name": "example.table"
                                # qualifiedName is missing
                            },
                            "subTypes": {"typeNames": ["TABLE"]},
                        }
                    }
                ]
            }
        }

        # Act
        result = extract_assertion_entity_from_graphql(assertion)

        # Assert
        self.assertEqual(result["table_name"], "example.table")
        self.assertIsNone(result["qualified_name"])

    def test_extract_entity_missing_subtypes(self) -> None:
        """Test extracting an entity with missing subTypes field."""
        # Arrange
        assertion: Dict[str, Any] = {
            "relationships": {
                "relationships": [
                    {
                        "entity": {
                            "platform": {"urn": "urn:li:dataPlatform:snowflake"},
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,example.table,PROD)",
                            "exists": True,
                            "properties": {
                                "name": "example.table",
                                "qualifiedName": "PROD.example.table",
                            },
                            # subTypes is missing
                        }
                    }
                ]
            }
        }

        # Act
        result = extract_assertion_entity_from_graphql(assertion)

        # Assert
        self.assertEqual(result["subTypes"], [])

    def test_extract_entity_null_subtypes(self) -> None:
        """Test extracting an entity with subTypes set to null."""
        # Arrange
        assertion: Dict[str, Any] = {
            "relationships": {
                "relationships": [
                    {
                        "entity": {
                            "platform": {"urn": "urn:li:dataPlatform:snowflake"},
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,example.table,PROD)",
                            "exists": True,
                            "properties": {
                                "name": "example.table",
                                "qualifiedName": "PROD.example.table",
                            },
                            "subTypes": None,
                        }
                    }
                ]
            }
        }

        # Act
        result = extract_assertion_entity_from_graphql(assertion)

        # Assert
        self.assertEqual(result["subTypes"], [])

    def test_extract_entity_missing_exists(self) -> None:
        """Test extracting an entity with missing exists field."""
        # Arrange
        assertion: Dict[str, Any] = {
            "relationships": {
                "relationships": [
                    {
                        "entity": {
                            "platform": {"urn": "urn:li:dataPlatform:snowflake"},
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,example.table,PROD)",
                            # exists is missing
                            "properties": {
                                "name": "example.table",
                                "qualifiedName": "PROD.example.table",
                            },
                            "subTypes": {"typeNames": ["TABLE"]},
                        }
                    }
                ]
            }
        }

        # Act
        result = extract_assertion_entity_from_graphql(assertion)

        # Assert
        self.assertIsNone(result["exists"])

    def test_missing_relationships(self) -> None:
        """Test raising exception when relationships is missing."""
        # Arrange
        assertion: Dict[str, Any] = {
            # Missing relationships field
        }

        # Act/Assert
        with self.assertRaises(Exception) as context:
            extract_assertion_entity_from_graphql(assertion)

        self.assertTrue("Failed to find an entity" in str(context.exception))

    def test_empty_relationships(self) -> None:
        """Test raising exception when relationships array is empty."""
        # Arrange
        assertion: Dict[str, Any] = {
            "relationships": {
                "relationships": []  # Empty array
            }
        }

        # Act/Assert
        with self.assertRaises(Exception) as context:
            extract_assertion_entity_from_graphql(assertion)

        self.assertTrue("Failed to find an entity" in str(context.exception))

    def test_missing_entity(self) -> None:
        """Test raising exception when entity is missing in the relationship."""
        # Arrange
        assertion: Dict[str, Any] = {
            "relationships": {
                "relationships": [
                    {
                        # Missing entity field
                    }
                ]
            }
        }

        # Act/Assert
        with pytest.raises(KeyError):  # Using pytest for more modern testing
            extract_assertion_entity_from_graphql(assertion)


class TestExtractAssertionMonitorParameters(unittest.TestCase):
    def test_successful_extraction(self) -> None:
        # Test case where parameters are successfully extracted
        test_urn = "urn:li:assertion:123"
        test_params = {"type": "DATASET_FRESHNESS"}

        test_monitor = {
            "info": {
                "assertionMonitor": {
                    "assertions": [
                        {
                            "assertion": {"urn": "urn:li:assertion:123"},
                            "parameters": test_params,
                        }
                    ]
                }
            }
        }

        result = extract_assertion_monitor_parameters(test_monitor, test_urn)

        # Verify the result
        assert isinstance(result, AssertionEvaluationParameters)
        assert result.type == AssertionEvaluationParametersType.DATASET_FRESHNESS

    def test_multiple_assertions_finds_correct_one(self) -> None:
        # Test case with multiple assertions, should find the correct one
        test_urn = "urn:li:assertion:456"

        test_monitor = {
            "info": {
                "assertionMonitor": {
                    "assertions": [
                        {
                            "assertion": {"urn": "urn:li:assertion:123"},
                            "parameters": {"type": "DATASET_FRESHNESS"},
                        },
                        {
                            "assertion": {"urn": "urn:li:assertion:456"},
                            "parameters": {"type": "DATASET_VOLUME"},
                        },
                    ]
                }
            }
        }

        result = extract_assertion_monitor_parameters(test_monitor, test_urn)
        assert result.type == AssertionEvaluationParametersType.DATASET_VOLUME
        assert result is not None

    def test_monitor_is_none(self) -> None:
        # Test case where monitor is None
        with pytest.raises(Exception) as excinfo:
            extract_assertion_monitor_parameters(None, "urn:li:assertion:123")  # type: ignore

        assert "Found no monitor" in str(excinfo.value)

    def test_info_missing(self) -> None:
        # Test case where info is missing
        test_monitor: dict = {"not_info": {}}

        with pytest.raises(Exception) as excinfo:
            extract_assertion_monitor_parameters(test_monitor, "urn:li:assertion:123")

        assert "Found no valid monitor parameters" in str(excinfo.value)

    def test_assertion_monitor_missing(self) -> None:
        # Test case where assertionMonitor is missing
        test_monitor: dict = {"info": {"not_assertion_monitor": {}}}

        with pytest.raises(Exception) as excinfo:
            extract_assertion_monitor_parameters(test_monitor, "urn:li:assertion:123")

        assert "Found no valid monitor parameters" in str(excinfo.value)

    def test_assertions_empty(self) -> None:
        # Test case where assertions is empty
        test_monitor: dict = {"info": {"assertionMonitor": {"assertions": []}}}

        with pytest.raises(Exception) as excinfo:
            extract_assertion_monitor_parameters(test_monitor, "urn:li:assertion:123")

        assert "Found no valid monitor parameters" in str(excinfo.value)

    def test_assertion_missing(self) -> None:
        # Test case where assertion key is missing
        test_monitor = {
            "info": {
                "assertionMonitor": {
                    "assertions": [{"parameters": {"max_sample_size": 100}}]
                }
            }
        }

        with pytest.raises(Exception) as excinfo:
            extract_assertion_monitor_parameters(test_monitor, "urn:li:assertion:123")

        assert "Found no valid monitor parameters" in str(excinfo.value)

    def test_urn_mismatch(self) -> None:
        # Test case where urn doesn't match
        test_monitor = {
            "info": {
                "assertionMonitor": {
                    "assertions": [
                        {
                            "assertion": {"urn": "urn:li:assertion:789"},
                            "parameters": {"max_sample_size": 100},
                        }
                    ]
                }
            }
        }

        with pytest.raises(Exception) as excinfo:
            extract_assertion_monitor_parameters(test_monitor, "urn:li:assertion:123")

        assert "Found no valid monitor parameters" in str(excinfo.value)

    def test_parameters_missing(self) -> None:
        # Test case where parameters are missing
        test_monitor = {
            "info": {
                "assertionMonitor": {
                    "assertions": [{"assertion": {"urn": "urn:li:assertion:123"}}]
                }
            }
        }

        with pytest.raises(ValidationError):
            extract_assertion_monitor_parameters(test_monitor, "urn:li:assertion:123")


class TestExtractAssertionMonitorExecutorId(unittest.TestCase):
    def test_executor_id_present(self) -> None:
        monitor: Dict[str, Dict[str, str]] = {"info": {"executorId": "executor-123"}}
        result: str = extract_assertion_monitor_executor_id(
            monitor, default="default-id"
        )
        self.assertEqual(result, "executor-123")

    def test_executor_id_missing(self) -> None:
        monitor: Dict[str, Dict[str, str]] = {"info": {}}
        result: str = extract_assertion_monitor_executor_id(
            monitor, default="default-id"
        )
        self.assertEqual(result, "default-id")

    def test_info_missing(self) -> None:
        monitor: Dict[str, Dict] = {}
        result: str = extract_assertion_monitor_executor_id(
            monitor, default="default-id"
        )
        self.assertEqual(result, "default-id")

    def test_monitor_is_none(self) -> None:
        monitor: Optional[Dict] = None
        result: str = extract_assertion_monitor_executor_id(
            monitor or {}, default="default-id"
        )
        self.assertEqual(result, "default-id")

    def test_executor_id_is_none(self) -> None:
        monitor: Dict[str, Dict[str, Optional[str]]] = {"info": {"executorId": None}}
        result: Optional[str] = extract_assertion_monitor_executor_id(
            monitor, default="default-id"
        )
        self.assertIsNone(result)

    def test_executor_id_is_empty_string(self) -> None:
        monitor: Dict[str, Dict[str, str]] = {"info": {"executorId": ""}}
        result: str = extract_assertion_monitor_executor_id(
            monitor, default="default-id"
        )
        self.assertEqual(result, "")


if __name__ == "__main__":
    unittest.main()
