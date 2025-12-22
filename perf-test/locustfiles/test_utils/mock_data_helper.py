import random
from typing import Optional

from datahub.ingestion.source.mock_data.table_naming_helper import TableNamingHelper


class MockDataHelper:
    """
    Helper for generating URNs and table names matching DataHub mock data source conventions.

    Builds on top of TableNamingHelper to add:
    - Random table selection for load testing
    - URN generation for DataHub entities
    - Configuration management for mock data source parameters

    Example usage:
        # Use default configuration
        helper = MockDataHelper()
        urn = helper.get_random_urn()

        # Custom configuration
        helper = MockDataHelper(prefix="custom", lineage_hops=1000)
        table_name = helper.get_random_table_name()
    """

    def __init__(
        self,
        prefix: str = "attempt001",
        lineage_hops: int = 10000,
        lineage_fan_out: int = 100,
        lineage_fan_out_after_first_hop: int = 1,
        platform: str = "fake",
        env: str = "PROD",
    ):
        """
        Initialize with mock data source configuration.

        Args:
            prefix: Table name prefix from mock data config (e.g., "attempt001")
            lineage_hops: Total number of lineage hops
            lineage_fan_out: Fan out at first hop (number of tables per level)
            lineage_fan_out_after_first_hop: Fan out after first hop (currently unused)
            platform: Data platform name (e.g., "fake", "bigquery")
            env: Environment (e.g., "PROD", "DEV")
        """
        self.prefix = prefix
        self.lineage_hops = lineage_hops
        self.lineage_fan_out = lineage_fan_out
        self.lineage_fan_out_after_first_hop = lineage_fan_out_after_first_hop
        self.platform = platform
        self.env = env

        # Calculate table counts per level
        self.tables_at_level_0 = 1
        self.tables_per_level_after_0 = lineage_fan_out

    def get_random_table_name(self) -> str:
        """
        Generate a random table name that matches an existing mock data table.

        Returns:
            Table name like "attempt001hops_10000_f_100_h5000_t42"
        """
        # Randomly choose a level (weighted to avoid mostly hitting level 0)
        level = random.randint(0, self.lineage_hops)

        # Choose table index based on level
        if level == 0:
            table_index = 0  # Only one table at level 0
        else:
            table_index = random.randint(0, self.tables_per_level_after_0 - 1)

        return self.get_table_name(level, table_index)

    def get_random_urn(self) -> str:
        """
        Generate a random URN for a mock data table.

        Returns:
            Full URN like "urn:li:dataset:(urn:li:dataPlatform:fake,attempt001hops_10000_f_100_h5000_t42,PROD)"
        """
        table_name = self.get_random_table_name()
        return self._build_urn(table_name)

    def get_table_name(self, level: int, table_index: int) -> str:
        """
        Generate a specific table name for a given level and index.

        Args:
            level: Lineage level (0 to lineage_hops)
            table_index: Table index within that level

        Returns:
            Table name like "attempt001hops_10000_f_100_h5000_t42"
        """
        return TableNamingHelper.generate_table_name(
            lineage_hops=self.lineage_hops,
            lineage_fan_out=self.lineage_fan_out,
            level=level,
            table_index=table_index,
            prefix=self.prefix,
        )

    def get_urn(self, level: int, table_index: int) -> str:
        """
        Generate a specific URN for a given level and index.

        Args:
            level: Lineage level (0 to lineage_hops)
            table_index: Table index within that level

        Returns:
            Full URN
        """
        table_name = self.get_table_name(level, table_index)
        return self._build_urn(table_name)

    def _build_urn(self, table_name: str) -> str:
        """Build full URN from table name."""
        return f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{table_name},{self.env})"

    def get_total_tables(self) -> int:
        """Calculate total number of tables in the mock data."""
        return self.tables_at_level_0 + (self.lineage_hops * self.tables_per_level_after_0)


# Singleton instance with default configuration matching common perf test setups
# Can be overridden by creating a new instance with custom parameters
default_mock_data = MockDataHelper()
