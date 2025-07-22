from typing import Dict, Optional


class TableNamingHelper:
    """
    Helper class for managing table naming conventions in mock data generation.

    Table naming pattern: "hops_{lineage_hops}_f_{lineage_fan_out}_h{level}_t{table_index}"
    """

    @staticmethod
    def generate_table_name(
        lineage_hops: int,
        lineage_fan_out: int,
        level: int,
        table_index: int,
        prefix: Optional[str] = None,
    ) -> str:
        """
        Generate a table name following the standard naming convention.

        Args:
            lineage_hops: Total number of hops in the lineage graph
            lineage_fan_out: Number of downstream tables per upstream table
            level: Level of the table in the lineage graph (0-based)
            table_index: Index of the table within its level (0-based)
            prefix: Optional prefix to add to the table name

        Returns:
            Table name following the pattern: "{prefix}hops_{lineage_hops}_f_{lineage_fan_out}_h{level}_t{table_index}"
        """
        base_name = f"hops_{lineage_hops}_f_{lineage_fan_out}_h{level}_t{table_index}"
        return f"{prefix}{base_name}" if prefix else base_name

    @staticmethod
    def parse_table_name(table_name: str) -> Dict[str, int]:
        """
        Parse a table name to extract its components.

        Args:
            table_name: Table name following the standard naming convention

        Returns:
            Dictionary containing parsed components:
            - lineage_hops: Total number of hops in the lineage graph
            - lineage_fan_out: Number of downstream tables per upstream table
            - level: Level of the table in the lineage graph (0-based)
            - table_index: Index of the table within its level (0-based)

        Raises:
            ValueError: If the table name doesn't follow the expected pattern
        """
        try:
            # Expected pattern: "hops_{lineage_hops}_f_{lineage_fan_out}_h{level}_t{table_index}"
            parts = table_name.split("_")

            if (
                len(parts) != 6
                or parts[0] != "hops"
                or parts[2] != "f"
                or not parts[4].startswith("h")
                or not parts[5].startswith("t")
            ):
                raise ValueError(f"Invalid table name format: {table_name}")

            lineage_hops = int(parts[1])
            lineage_fan_out = int(parts[3])  # lineage_fan_out is at index 3
            level = int(parts[4][1:])  # Remove 'h' prefix from parts[4]
            table_index = int(parts[5][1:])  # Remove 't' prefix from parts[5]

            return {
                "lineage_hops": lineage_hops,
                "lineage_fan_out": lineage_fan_out,
                "level": level,
                "table_index": table_index,
            }
        except (ValueError, IndexError) as e:
            raise ValueError(
                f"Failed to parse table name '{table_name}': {str(e)}"
            ) from e

    @staticmethod
    def is_valid_table_name(table_name: str) -> bool:
        """
        Check if a table name follows the expected naming convention.

        Args:
            table_name: Table name to validate

        Returns:
            True if the table name follows the expected pattern, False otherwise
        """
        try:
            TableNamingHelper.parse_table_name(table_name)
            return True
        except ValueError:
            return False
