"""
Parser for Snowflake Semantic View DDL definitions.

Parses the CREATE SEMANTIC VIEW DDL to extract:
- Logical tables and their base tables
- Dimensions, facts, and metrics with their expressions
- Column-level lineage information
"""

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class SemanticColumn:
    """Represents a column in a semantic view (dimension, fact, or metric)."""

    name: str  # The alias name (e.g., "CUSTOMER_ID")
    source_table: str  # Logical table name (e.g., "ORDERS")
    source_column: Optional[str] = None  # Source column name (e.g., "CUSTOMER_ID")
    expression: Optional[str] = None  # For metrics (e.g., "SUM(ORDER_TOTAL)")
    column_type: str = "DIMENSION"  # DIMENSION, FACT, METRIC
    comment: Optional[str] = None
    data_type: Optional[str] = None


@dataclass
class SemanticTable:
    """Represents a logical table in a semantic view."""

    name: str  # Logical table name (e.g., "ORDERS")
    base_table: (
        str  # Full base table name (e.g., "WAREHOUSE_COFFEE_COMPANY.PUBLIC.ORDERS")
    )
    dimensions: List[SemanticColumn] = field(default_factory=list)
    facts: List[SemanticColumn] = field(default_factory=list)
    metrics: List[SemanticColumn] = field(default_factory=list)
    time_dimensions: List[str] = field(default_factory=list)


@dataclass
class SemanticViewDefinition:
    """Parsed semantic view definition."""

    name: str
    tables: List[SemanticTable]
    all_dimensions: List[SemanticColumn] = field(default_factory=list)
    all_facts: List[SemanticColumn] = field(default_factory=list)
    all_metrics: List[SemanticColumn] = field(default_factory=list)
    comment: Optional[str] = None

    def get_column_by_name(self, name: str) -> Optional[SemanticColumn]:
        """Get a column by its alias name."""
        for col in self.all_dimensions + self.all_facts + self.all_metrics:
            if col.name.upper() == name.upper():
                return col
        return None

    def get_all_columns(self) -> List[SemanticColumn]:
        """Get all columns (dimensions, facts, metrics)."""
        return self.all_dimensions + self.all_facts + self.all_metrics


class SemanticViewParser:
    """Parser for Snowflake Semantic View DDL."""

    @staticmethod
    def parse(ddl: str) -> Optional[SemanticViewDefinition]:
        """
        Parse a semantic view DDL string.

        Args:
            ddl: The DDL string from GET_DDL('SEMANTIC_VIEW', ...)

        Returns:
            SemanticViewDefinition or None if parsing fails
        """
        try:
            # Extract view name
            name_match = re.search(
                r"create\s+or\s+replace\s+semantic\s+view\s+(\w+)",
                ddl,
                re.IGNORECASE,
            )
            if not name_match:
                logger.warning("Could not extract semantic view name from DDL")
                return None

            view_name = name_match.group(1)

            # Extract sections
            tables_section = SemanticViewParser._extract_section(ddl, "tables")
            dimensions_section = SemanticViewParser._extract_section(ddl, "dimensions")
            facts_section = SemanticViewParser._extract_section(ddl, "facts")
            metrics_section = SemanticViewParser._extract_section(ddl, "metrics")
            comment = SemanticViewParser._extract_comment(ddl)

            # Parse tables
            tables = SemanticViewParser._parse_tables(tables_section)

            # Parse dimensions, facts, metrics
            dimensions = SemanticViewParser._parse_columns(
                dimensions_section, "DIMENSION"
            )
            facts = SemanticViewParser._parse_columns(facts_section, "FACT")
            metrics = SemanticViewParser._parse_columns(metrics_section, "METRIC")

            # Try to extract additional metadata from extension
            time_dims = SemanticViewParser._extract_time_dimensions(ddl)

            # Organize columns by table
            for table in tables:
                for dim in dimensions:
                    if dim.source_table == table.name:
                        table.dimensions.append(dim)
                for fact in facts:
                    if fact.source_table == table.name:
                        table.facts.append(fact)
                for metric in metrics:
                    if metric.source_table == table.name:
                        table.metrics.append(metric)

                # Add time dimensions
                if table.name in time_dims:
                    table.time_dimensions = time_dims[table.name]

            return SemanticViewDefinition(
                name=view_name,
                tables=tables,
                all_dimensions=dimensions,
                all_facts=facts,
                all_metrics=metrics,
                comment=comment,
            )

        except Exception as e:
            logger.error(f"Failed to parse semantic view DDL: {e}", exc_info=True)
            return None

    @staticmethod
    def _extract_section(ddl: str, section_name: str) -> str:
        """Extract a section from the DDL (e.g., 'tables', 'dimensions')."""
        # Match section_name ( ... )
        pattern = rf"{section_name}\s*\((.*?)\)(?:\s+(?:relationships|facts|dimensions|metrics|comment|with)|\s*$)"
        match = re.search(pattern, ddl, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
        return ""

    @staticmethod
    def _parse_tables(tables_section: str) -> List[SemanticTable]:
        """Parse the tables section."""
        tables: List[SemanticTable] = []
        if not tables_section:
            return tables

        # Split by commas (but not commas inside parentheses)
        table_entries = SemanticViewParser._split_by_comma(tables_section)

        for entry in table_entries:
            entry = entry.strip()
            if not entry:
                continue

            # Extract base table name (e.g., "WAREHOUSE_COFFEE_COMPANY.PUBLIC.ORDERS")
            # Pattern: full_table_name [primary key (...)] [with synonyms=(...)]
            base_table_match = re.match(r"^([\w.]+)", entry, re.IGNORECASE)
            if base_table_match:
                base_table_full = base_table_match.group(1)
                # Extract just the table name (last part)
                table_name = base_table_full.split(".")[-1]

                tables.append(
                    SemanticTable(name=table_name, base_table=base_table_full)
                )

        return tables

    @staticmethod
    def _parse_columns(section: str, column_type: str) -> List[SemanticColumn]:
        """Parse dimensions, facts, or metrics section."""
        columns: List[SemanticColumn] = []
        if not section:
            return columns

        # Split by commas (but not commas inside parentheses)
        entries = SemanticViewParser._split_by_comma(section)

        for entry in entries:
            entry = entry.strip()
            if not entry:
                continue

            try:
                # Pattern: TABLE.COLUMN as ALIAS [comment='...']
                # Or for metrics: TABLE.METRIC as EXPRESSION [comment='...']
                # Or derived metrics: METRIC_NAME as EXPRESSION [comment='...']

                # Extract comment first
                comment = None
                comment_match = re.search(r"comment='([^']*)'", entry, re.IGNORECASE)
                if comment_match:
                    comment = comment_match.group(1)
                    entry = re.sub(
                        r"\s*comment='[^']*'", "", entry, flags=re.IGNORECASE
                    )

                # Pattern: SOURCE as ALIAS or just SOURCE
                parts = entry.split(" as ", 1)
                if len(parts) == 2:
                    source_part = parts[0].strip()
                    alias_part = parts[1].strip()
                else:
                    source_part = entry.strip()
                    alias_part = (
                        source_part.split(".")[-1]
                        if "." in source_part
                        else source_part
                    )

                # Parse source (TABLE.COLUMN or expression)
                source_table: Optional[str]
                source_column: Optional[str]

                if "." in source_part and not SemanticViewParser._is_expression(
                    source_part
                ):
                    # Simple TABLE.COLUMN reference
                    table_col = source_part.split(".", 1)
                    source_table = table_col[0].strip()
                    source_column = table_col[1].strip() if len(table_col) > 1 else None

                    columns.append(
                        SemanticColumn(
                            name=alias_part,
                            source_table=source_table,
                            source_column=source_column,
                            column_type=column_type,
                            comment=comment,
                        )
                    )
                else:
                    # Expression (metric formula)
                    # For derived metrics, source_table might not be clear
                    # Try to extract table from expression if simple
                    source_table = SemanticViewParser._extract_table_from_expression(
                        source_part
                    )

                    columns.append(
                        SemanticColumn(
                            name=alias_part,
                            source_table=source_table or "DERIVED",
                            expression=alias_part if column_type == "METRIC" else None,
                            column_type=column_type,
                            comment=comment,
                        )
                    )

            except Exception as e:
                logger.debug(f"Failed to parse column entry '{entry}': {e}")
                continue

        return columns

    @staticmethod
    def _is_expression(text: str) -> bool:
        """Check if text is an expression (contains functions, operators, etc.)."""
        expr_indicators = [
            "(",
            ")",
            "+",
            "-",
            "*",
            "/",
            "SUM",
            "AVG",
            "COUNT",
            "MAX",
            "MIN",
        ]
        return any(indicator in text.upper() for indicator in expr_indicators)

    @staticmethod
    def _extract_table_from_expression(expr: str) -> Optional[str]:
        """Try to extract the primary table from an expression."""
        # Look for TABLE.COLUMN patterns
        table_refs = re.findall(r"(\w+)\.\w+", expr)
        if table_refs:
            return table_refs[0]  # Return first table found
        return None

    @staticmethod
    def _split_by_comma(text: str) -> List[str]:
        """Split by commas, but ignore commas inside parentheses."""
        parts: List[str] = []
        current: List[str] = []
        paren_depth = 0

        for char in text:
            if char == "(":
                paren_depth += 1
            elif char == ")":
                paren_depth -= 1
            elif char == "," and paren_depth == 0:
                parts.append("".join(current))
                current = []
                continue
            current.append(char)

        if current:
            parts.append("".join(current))

        return [p.strip() for p in parts]

    @staticmethod
    def _extract_comment(ddl: str) -> Optional[str]:
        """Extract the view-level comment."""
        match = re.search(r"comment='([^']*)'", ddl, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def _extract_time_dimensions(ddl: str) -> Dict[str, List[str]]:
        """Extract time dimensions from the JSON extension."""
        time_dims: Dict[str, List[str]] = {}

        # Extract the CA extension JSON
        extension_match = re.search(
            r"with extension \(CA='({.*})'\)", ddl, re.IGNORECASE | re.DOTALL
        )
        if not extension_match:
            return time_dims

        try:
            json_str = extension_match.group(1)
            # Unescape any escaped quotes
            json_str = json_str.replace('\\"', '"')
            extension_data = json.loads(json_str)

            # Extract time_dimensions for each table
            for table_data in extension_data.get("tables", []):
                table_name = table_data.get("name")
                time_dimension_list = table_data.get("time_dimensions", [])
                if table_name and time_dimension_list:
                    time_dims[table_name] = [td["name"] for td in time_dimension_list]

        except Exception as e:
            logger.debug(f"Failed to parse extension JSON: {e}")

        return time_dims

    @staticmethod
    def extract_column_dependencies(
        metric_expression: str, all_columns: List[SemanticColumn]
    ) -> List[SemanticColumn]:
        """
        Extract column dependencies from a metric expression.

        For example:
        - "SUM(ORDER_TOTAL)" → [ORDER_TOTAL column]
        - "ORDERS.ORDER_TOTAL_METRIC+TRANSACTIONS.TRANSACTION_AMOUNT_METRIC" → [both metrics]

        Returns list of SemanticColumn objects that this metric depends on.
        """
        dependencies: List[SemanticColumn] = []

        # Extract all column references (TABLE.COLUMN or just COLUMN)
        # Pattern: word.word or just word (when it's a column/metric name)
        refs = re.findall(r"(\w+\.\w+|\w+)", metric_expression)

        for ref in refs:
            # Skip SQL keywords and functions
            if ref.upper() in [
                "SUM",
                "AVG",
                "COUNT",
                "MAX",
                "MIN",
                "AS",
                "AND",
                "OR",
                "NOT",
            ]:
                continue

            # Look up in all_columns
            if "." in ref:
                # TABLE.COLUMN format
                table, col = ref.split(".", 1)
                for column in all_columns:
                    if (
                        column.source_table.upper() == table.upper()
                        and column.name.upper() == col.upper()
                    ):
                        dependencies.append(column)
                        break
            else:
                # Just column name
                for column in all_columns:
                    if column.name.upper() == ref.upper():
                        dependencies.append(column)
                        break

        return dependencies
