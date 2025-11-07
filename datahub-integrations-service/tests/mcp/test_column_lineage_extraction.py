"""
Unit tests for column-level lineage extraction from paths.
"""

from datahub_integrations.mcp.mcp_server import _extract_lineage_columns_from_paths


def test_extract_lineage_columns_single_path():
    """Test extracting columns when each result has a single path."""
    search_results = [
        {
            "entity": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,banking.public.account_transactions,PROD)",
                "type": "DATASET",
                "name": "account_transactions",
            },
            "paths": [
                {
                    "path": [
                        {
                            "urn": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:kafka,banking.public.account_transactions,PROD),transaction_type)",
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "transaction_type",
                            "parent": {
                                "urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,banking.public.account_transactions,PROD)",
                                "type": "DATASET",
                            },
                        },
                        {
                            "urn": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,banking.public.account_transactions,PROD),transaction_type)",
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "transaction_type",
                            "parent": {
                                "urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,banking.public.account_transactions,PROD)",
                                "type": "DATASET",
                            },
                        },
                    ]
                }
            ],
            "degree": 1,
        }
    ]

    result = _extract_lineage_columns_from_paths(search_results)

    assert len(result) == 1
    assert result[0]["entity"]["name"] == "account_transactions"
    assert result[0]["lineageColumns"] == ["transaction_type"]
    assert result[0]["degree"] == 1
    assert "paths" not in result[0]  # Paths removed after extraction


def test_extract_lineage_columns_multiple_paths():
    """Test extracting columns when a dataset has multiple downstream columns."""
    search_results = [
        {
            "entity": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.user_events,PROD)",
                "type": "DATASET",
                "name": "user_events",
            },
            "paths": [
                {
                    "path": [
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "user_id",
                        },
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "customer_id",  # First target column
                        },
                    ]
                },
                {
                    "path": [
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "user_id",
                        },
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "user_identifier",  # Second target column
                        },
                    ]
                },
                {
                    "path": [
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "user_id",
                        },
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "uid",  # Third target column
                        },
                    ]
                },
            ],
            "degree": 1,
        }
    ]

    result = _extract_lineage_columns_from_paths(search_results)

    assert len(result) == 1
    assert result[0]["lineageColumns"] == ["customer_id", "user_identifier", "uid"]
    assert result[0]["degree"] == 1


def test_extract_lineage_columns_multi_hop():
    """Test extracting columns from multi-hop lineage paths."""
    search_results = [
        {
            "entity": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.final_table,PROD)",
                "type": "DATASET",
                "name": "final_table",
            },
            "paths": [
                {
                    "path": [
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "user_id",  # Source
                        },
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "uid",  # Intermediate
                        },
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "customer_key",  # Target (last in path)
                        },
                    ]
                }
            ],
            "degree": 2,
        }
    ]

    result = _extract_lineage_columns_from_paths(search_results)

    assert len(result) == 1
    # Should extract only the LAST column in the path
    assert result[0]["lineageColumns"] == ["customer_key"]
    assert result[0]["degree"] == 2


def test_extract_lineage_columns_multiple_results():
    """Test processing multiple dataset results at once."""
    search_results = [
        {
            "entity": {
                "urn": "urn:li:dataset:A",
                "type": "DATASET",
                "name": "table_a",
            },
            "paths": [
                {
                    "path": [
                        {"type": "SCHEMA_FIELD", "fieldPath": "source_col"},
                        {"type": "SCHEMA_FIELD", "fieldPath": "col_a"},
                    ]
                }
            ],
            "degree": 1,
        },
        {
            "entity": {
                "urn": "urn:li:dataset:B",
                "type": "DATASET",
                "name": "table_b",
            },
            "paths": [
                {
                    "path": [
                        {"type": "SCHEMA_FIELD", "fieldPath": "source_col"},
                        {"type": "SCHEMA_FIELD", "fieldPath": "col_b1"},
                    ]
                },
                {
                    "path": [
                        {"type": "SCHEMA_FIELD", "fieldPath": "source_col"},
                        {"type": "SCHEMA_FIELD", "fieldPath": "col_b2"},
                    ]
                },
            ],
            "degree": 1,
        },
    ]

    result = _extract_lineage_columns_from_paths(search_results)

    assert len(result) == 2
    assert result[0]["entity"]["name"] == "table_a"
    assert result[0]["lineageColumns"] == ["col_a"]
    assert result[1]["entity"]["name"] == "table_b"
    assert result[1]["lineageColumns"] == ["col_b1", "col_b2"]


def test_extract_lineage_columns_no_paths():
    """Test that dataset-level lineage (no paths) is unchanged."""
    search_results = [
        {
            "entity": {
                "urn": "urn:li:dataset:A",
                "type": "DATASET",
                "name": "table_a",
            },
            "degree": 1,
        }
    ]

    result = _extract_lineage_columns_from_paths(search_results)

    # Should return unchanged (no lineageColumns added)
    assert len(result) == 1
    assert result[0] == search_results[0]
    assert "lineageColumns" not in result[0]


def test_extract_lineage_columns_empty_paths():
    """Test handling of empty paths array."""
    search_results = [
        {
            "entity": {
                "urn": "urn:li:dataset:A",
                "type": "DATASET",
                "name": "table_a",
            },
            "paths": [],
            "degree": 1,
        }
    ]

    result = _extract_lineage_columns_from_paths(search_results)

    # Should keep result but without lineageColumns
    assert len(result) == 1
    assert "lineageColumns" not in result[0]


def test_extract_lineage_columns_preserves_other_fields():
    """Test that extra fields like explored, truncatedChildren are preserved."""
    search_results = [
        {
            "entity": {
                "urn": "urn:li:dataset:A",
                "type": "DATASET",
                "name": "table_a",
            },
            "paths": [
                {
                    "path": [
                        {"type": "SCHEMA_FIELD", "fieldPath": "col1"},
                        {"type": "SCHEMA_FIELD", "fieldPath": "col2"},
                    ]
                }
            ],
            "degree": 1,
            "explored": True,
            "truncatedChildren": False,
            "ignoredAsHop": False,
        }
    ]

    result = _extract_lineage_columns_from_paths(search_results)

    assert result[0]["explored"] is True
    assert result[0]["truncatedChildren"] is False
    assert result[0]["ignoredAsHop"] is False
    assert result[0]["lineageColumns"] == ["col2"]


def test_extract_lineage_columns_deduplication():
    """Test that duplicate column names are deduplicated."""
    search_results = [
        {
            "entity": {
                "urn": "urn:li:dataset:A",
                "type": "DATASET",
                "name": "table_a",
            },
            "paths": [
                {
                    "path": [
                        {"type": "SCHEMA_FIELD", "fieldPath": "source"},
                        {"type": "SCHEMA_FIELD", "fieldPath": "target_col"},
                    ]
                },
                {
                    "path": [
                        {"type": "SCHEMA_FIELD", "fieldPath": "another_source"},
                        {
                            "type": "SCHEMA_FIELD",
                            "fieldPath": "target_col",
                        },  # Duplicate
                    ]
                },
            ],
            "degree": 1,
        }
    ]

    result = _extract_lineage_columns_from_paths(search_results)

    # Should only include "target_col" once
    assert result[0]["lineageColumns"] == ["target_col"]


def test_extract_lineage_columns_empty_input():
    """Test handling of empty input list."""
    result = _extract_lineage_columns_from_paths([])
    assert result == []


def test_extract_lineage_columns_malformed_path():
    """Test handling of malformed paths (missing required fields)."""
    search_results = [
        {
            "entity": {"urn": "urn:li:dataset:A", "type": "DATASET", "name": "table_a"},
            "paths": [
                {"path": []},  # Empty path
                {
                    "path": [
                        {"type": "DATASET", "urn": "something"}  # Not a SCHEMA_FIELD
                    ]
                },
                {
                    "path": [
                        {"type": "SCHEMA_FIELD"}  # Missing fieldPath
                    ]
                },
            ],
            "degree": 1,
        }
    ]

    result = _extract_lineage_columns_from_paths(search_results)

    # Should handle gracefully, no lineageColumns added
    assert len(result) == 1
    assert "lineageColumns" not in result[0] or result[0]["lineageColumns"] == []
