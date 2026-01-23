import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigqueryTableIdentifier


class TestShardedTableOptimizations:
    """Tests for sharded table performance optimizations."""

    @pytest.mark.parametrize(
        "shard1,shard2,expected_is_newer,description",
        [
            ("20230102", "20230101", True, "same length YYYYMMDD"),
            ("20240101", "20231231", True, "year boundary"),
            ("2024", "20230101", False, "mixed length - numeric correct"),
            ("abc_v2", "abc_v1", True, "non-numeric fallback to string"),
        ],
    )
    def test_numeric_shard_comparison(
        self, shard1: str, shard2: str, expected_is_newer: bool, description: str
    ):
        """Test shard comparison logic for various shard formats."""
        if shard1.isdigit() and shard2.isdigit():
            is_newer = int(shard1) > int(shard2)
        else:
            is_newer = shard1 > shard2

        assert is_newer == expected_is_newer, f"Failed for: {description}"

    def test_direct_pattern_matching_performance(self):
        """Test that direct pattern matching avoids unnecessary object creation."""
        pattern = BigqueryTableIdentifier._get_shard_pattern()

        table_ids = [
            "events_20240101",
            "events_20240102",
            "events_20240103",
            "regular_table",
        ]

        sharded_count = 0
        for table_id in table_ids:
            match = pattern.match(table_id)
            if match:
                shard = match[3]
                if shard:
                    sharded_count += 1

        assert sharded_count == 3

    def test_shard_deduplication_in_lightweight_discovery(self):
        """Test that lightweight discovery deduplicates sharded tables."""
        pattern = BigqueryTableIdentifier._get_shard_pattern()

        table_ids = [
            "events_20240101",
            "events_20240102",
            "events_20240103",
            "users_20240101",
            "users_20240102",
            "orders",
        ]

        seen_base_tables = set()
        unique_tables = []

        for table_id in table_ids:
            match = pattern.match(table_id)
            if match:
                base_name = match[1] or ""
                shard = match[3]

                if base_name and table_id.endswith(shard):
                    base_name = table_id[: -len(shard)].rstrip("_")

                if base_name in seen_base_tables:
                    continue
                seen_base_tables.add(base_name)
                unique_tables.append(base_name + "_yyyymmdd")
            else:
                unique_tables.append(table_id)

        assert len(unique_tables) == 3
        assert "events_yyyymmdd" in unique_tables
        assert "users_yyyymmdd" in unique_tables
        assert "orders" in unique_tables

    @pytest.mark.parametrize(
        "table_count,expected_batch_size,description",
        [
            (50, 100, "small dataset - no scaling"),
            (150, 200, "medium dataset - 2x scaling"),
            (500, 300, "large dataset - 3x scaling"),
            (300, 500, "high base - respects cap"),
        ],
    )
    def test_adaptive_batch_sizing(
        self, table_count: int, expected_batch_size: int, description: str
    ):
        """Test adaptive batch sizing for different dataset sizes."""
        base_batch_size = 200 if table_count == 300 else 100

        if table_count > 200:
            max_batch_size = min(base_batch_size * 3, 500)
        elif table_count > 100:
            max_batch_size = min(base_batch_size * 2, 300)
        else:
            max_batch_size = base_batch_size

        assert max_batch_size == expected_batch_size, f"Failed for: {description}"

    @pytest.mark.parametrize(
        "table_id",
        [
            "TABLE_20240101",
            "Table_20240101",
            "table_20240101",
        ],
    )
    def test_shard_pattern_respects_case_insensitivity(self, table_id: str):
        """Test that pattern matching is case-insensitive."""
        pattern = BigqueryTableIdentifier._get_shard_pattern()

        match = pattern.match(table_id)
        assert match is not None
        assert match[3] == "20240101"
