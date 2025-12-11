# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pytest

from datahub.ingestion.source.mock_data.table_naming_helper import TableNamingHelper


def test_table_naming_helper_generate_table_name():
    """Test table name generation."""
    table_name = TableNamingHelper.generate_table_name(
        lineage_hops=2, lineage_fan_out=3, level=1, table_index=0
    )
    assert table_name == "hops_2_f_3_h1_t0"


def test_table_naming_helper_parse_table_name():
    """Test table name parsing."""
    result = TableNamingHelper.parse_table_name("hops_2_f_3_h1_t0")
    assert result == {
        "lineage_hops": 2,
        "lineage_fan_out": 3,
        "level": 1,
        "table_index": 0,
    }


def test_table_naming_helper_round_trip():
    """Test that generate and parse work together."""
    test_cases = [
        (2, 3, 1, 0),
        (0, 1, 0, 0),
        (10, 100, 5, 42),
    ]

    for lineage_hops, lineage_fan_out, level, table_index in test_cases:
        table_name = TableNamingHelper.generate_table_name(
            lineage_hops, lineage_fan_out, level, table_index
        )
        parsed = TableNamingHelper.parse_table_name(table_name)

        assert parsed["lineage_hops"] == lineage_hops
        assert parsed["lineage_fan_out"] == lineage_fan_out
        assert parsed["level"] == level
        assert parsed["table_index"] == table_index


def test_table_naming_helper_invalid_input():
    """Test that invalid table names are properly rejected."""
    with pytest.raises(ValueError):
        TableNamingHelper.parse_table_name("invalid_name")


def test_table_naming_helper_is_valid_table_name():
    """Test table name validation."""
    assert TableNamingHelper.is_valid_table_name("hops_2_f_3_h1_t0") is True
    assert TableNamingHelper.is_valid_table_name("invalid_name") is False


def test_table_naming_helper_with_prefix():
    """Test table name generation with prefix."""
    # Test with prefix
    table_name = TableNamingHelper.generate_table_name(
        lineage_hops=2, lineage_fan_out=3, level=1, table_index=0, prefix="test_"
    )
    assert table_name == "test_hops_2_f_3_h1_t0"

    # Test without prefix (empty string)
    table_name = TableNamingHelper.generate_table_name(
        lineage_hops=2, lineage_fan_out=3, level=1, table_index=0, prefix=""
    )
    assert table_name == "hops_2_f_3_h1_t0"

    # Test with default (no prefix parameter)
    table_name = TableNamingHelper.generate_table_name(
        lineage_hops=2, lineage_fan_out=3, level=1, table_index=0
    )
    assert table_name == "hops_2_f_3_h1_t0"
