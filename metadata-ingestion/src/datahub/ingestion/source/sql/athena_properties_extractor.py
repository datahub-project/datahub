"""
Athena Properties Extractor - A robust tool for parsing CREATE TABLE statements.

This module provides functionality to extract properties, partitioning information,
and row format details from Athena CREATE TABLE SQL statements.
"""

import json
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple, Union

from sqlglot import ParseError, parse_one
from sqlglot.dialects.athena import Athena
from sqlglot.expressions import (
    Anonymous,
    ColumnDef,
    Create,
    Day,
    Expression,
    FileFormatProperty,
    Identifier,
    LocationProperty,
    Month,
    PartitionByTruncate,
    PartitionedByBucket,
    PartitionedByProperty,
    Property,
    RowFormatDelimitedProperty,
    Schema,
    SchemaCommentProperty,
    SerdeProperties,
    Year,
)


class AthenaPropertiesExtractionError(Exception):
    """Custom exception for Athena properties extraction errors."""

    pass


@dataclass
class ColumnInfo:
    """Information about a table column."""

    name: str
    type: str


@dataclass
class TransformInfo:
    """Information about a partition transform."""

    type: str
    column: ColumnInfo
    bucket_count: Optional[int] = None
    length: Optional[int] = None


@dataclass
class PartitionInfo:
    """Information about table partitioning."""

    simple_columns: List[ColumnInfo]
    transforms: List[TransformInfo]


@dataclass
class TableProperties:
    """General table properties."""

    location: Optional[str] = None
    format: Optional[str] = None
    comment: Optional[str] = None
    serde_properties: Optional[Dict[str, str]] = None
    row_format: Optional[Dict[str, str]] = None
    additional_properties: Optional[Dict[str, str]] = None


@dataclass
class RowFormatInfo:
    """Row format information."""

    properties: Dict[str, str]
    json_formatted: str


@dataclass
class AthenaTableInfo:
    """Complete information about an Athena table."""

    partition_info: PartitionInfo
    table_properties: TableProperties
    row_format: RowFormatInfo


class AthenaPropertiesExtractor:
    """A class to extract properties from Athena CREATE TABLE statements."""

    CREATE_TABLE_REGEXP = re.compile(
        r"(CREATE TABLE[\s\n]*)(.*?)(\s*\()", re.MULTILINE | re.IGNORECASE
    )
    PARTITIONED_BY_REGEXP = re.compile(
        r"(PARTITIONED BY[\s\n]*\()((?:[^()]|\([^)]*\))*?)(\))",
        re.MULTILINE | re.IGNORECASE,
    )

    def __init__(self) -> None:
        """Initialize the extractor."""
        pass

    @staticmethod
    def get_table_properties(sql: str) -> AthenaTableInfo:
        """Get all table properties from a SQL statement.

        Args:
            sql: The SQL statement to parse

        Returns:
            An AthenaTableInfo object containing all table properties

        Raises:
            AthenaPropertiesExtractionError: If extraction fails
        """
        extractor = AthenaPropertiesExtractor()
        return extractor._extract_all_properties(sql)

    def _extract_all_properties(self, sql: str) -> AthenaTableInfo:
        """Extract all properties from a SQL statement.

        Args:
            sql: The SQL statement to parse

        Returns:
            An AthenaTableInfo object containing all properties

        Raises:
            AthenaPropertiesExtractionError: If extraction fails
        """
        if not sql or not sql.strip():
            raise AthenaPropertiesExtractionError("SQL statement cannot be empty")

        try:
            # We need to do certain transformations on the sql create statement:
            # - table names are not quoted
            # - column expression is not quoted
            # - sql parser fails if partition columns quoted
            fixed_sql = self._fix_sql_partitioning(sql)
            parsed = parse_one(fixed_sql, dialect=Athena)
        except ParseError as e:
            raise AthenaPropertiesExtractionError(f"Failed to parse SQL: {e}") from e
        except Exception as e:
            raise AthenaPropertiesExtractionError(
                f"Unexpected error during SQL parsing: {e}"
            ) from e

        try:
            partition_info = self._extract_partition_info(parsed)
            table_properties = self._extract_table_properties(parsed)
            row_format = self._extract_row_format(parsed)

            return AthenaTableInfo(
                partition_info=partition_info,
                table_properties=table_properties,
                row_format=row_format,
            )
        except Exception as e:
            raise AthenaPropertiesExtractionError(
                f"Failed to extract table properties: {e}"
            ) from e

    @staticmethod
    def format_column_definition(line):
        # Use regex to parse the line more accurately
        # Pattern: column_name data_type [COMMENT comment_text] [,]
        # Improved pattern to better separate column name, data type, and comment
        pattern = r"^\s*([`\w']+)\s+([\w<>\[\](),\s]+?)(\s+COMMENT\s+(.+?))?(,?)\s*$"
        match = re.match(pattern, line.strip(), re.IGNORECASE)

        if not match:
            return line
        column_name = match.group(1).strip()
        data_type = match.group(2).strip()
        comment_part = match.group(4)  # COMMENT part
        trailing_comma = match.group(5) if match.group(5) else ""

        # Add backticks to column name if not already present
        if not (column_name.startswith("`") and column_name.endswith("`")):
            column_name = f"`{column_name}`"

        # Build the result
        result_parts = [column_name, data_type]

        if comment_part:
            comment_part = comment_part.strip()

            # Handle comment quoting and escaping
            if comment_part.startswith("'") and comment_part.endswith("'"):
                # Already single quoted - but check for proper escaping
                inner_content = comment_part[1:-1]
                # Re-escape any single quotes that aren't properly escaped
                escaped_content = inner_content.replace("'", "''")
                formatted_comment = f"'{escaped_content}'"
            elif comment_part.startswith('"') and comment_part.endswith('"'):
                # Double quoted - convert to single quotes and escape internal single quotes
                inner_content = comment_part[1:-1]
                escaped_content = inner_content.replace("'", "''")
                formatted_comment = f"'{escaped_content}'"
            else:
                # Not quoted - use double quotes to avoid escaping issues with single quotes
                formatted_comment = f'"{comment_part}"'

            result_parts.extend(["COMMENT", formatted_comment])

        result = " " + " ".join(result_parts) + trailing_comma

        return result

    @staticmethod
    def format_athena_column_definitions(sql_statement: str) -> str:
        """
        Format Athena CREATE TABLE statement by:
        1. Adding backticks around column names in column definitions (only in the main table definition)
        2. Quoting comments (if any exist)
        """
        lines = sql_statement.split("\n")
        formatted_lines = []

        in_column_definition = False

        for line in lines:
            stripped_line = line.strip()

            # Check if we're entering column definitions
            if "CREATE TABLE" in line.upper() and "(" in line:
                in_column_definition = True
                formatted_lines.append(line)
                continue

            # Skip processing PARTITIONED BY clauses as column definitions
            if in_column_definition and "PARTITIONED BY" in line.upper():
                formatted_lines.append(line)
                continue

            # Process column definitions first, then check for exit condition
            if in_column_definition and stripped_line:
                # Check if this line contains a column definition (before the closing paren)
                if ")" in line:
                    # Split the line at the closing parenthesis
                    paren_index = line.find(")")
                    column_part = line[:paren_index].strip()
                    closing_part = line[paren_index:]

                    if column_part:
                        # Format the column part
                        formatted_column = (
                            AthenaPropertiesExtractor.format_column_definition(
                                column_part
                            )
                        )
                        # Reconstruct the line
                        formatted_line = formatted_column.rstrip() + closing_part
                        formatted_lines.append(formatted_line)
                    else:
                        formatted_lines.append(line)
                    in_column_definition = False
                else:
                    # Regular column definition line
                    formatted_line = AthenaPropertiesExtractor.format_column_definition(
                        line
                    )
                    formatted_lines.append(formatted_line)
            else:
                # For all other lines, keep as-is
                formatted_lines.append(line)

        return "\n".join(formatted_lines)

    @staticmethod
    def _fix_sql_partitioning(sql: str) -> str:
        """Fix SQL partitioning by removing backticks from partition expressions and quoting table names.

        Args:
            sql: The SQL statement to fix

        Returns:
            The fixed SQL statement
        """
        if not sql:
            return sql

        # Quote table name
        table_name_match = AthenaPropertiesExtractor.CREATE_TABLE_REGEXP.search(sql)

        if table_name_match:
            table_name = table_name_match.group(2).strip()
            if table_name and not (table_name.startswith("`") or "`" in table_name):
                # Split on dots and quote each part
                quoted_parts = [
                    f"`{part.strip()}`"
                    for part in table_name.split(".")
                    if part.strip()
                ]
                if quoted_parts:
                    quoted_table = ".".join(quoted_parts)
                    create_part = table_name_match.group(0).replace(
                        table_name, quoted_table
                    )
                    sql = sql.replace(table_name_match.group(0), create_part)

        # Fix partition expressions
        partition_match = AthenaPropertiesExtractor.PARTITIONED_BY_REGEXP.search(sql)

        if partition_match:
            partition_section = partition_match.group(2)
            if partition_section:
                partition_section_modified = partition_section.replace("`", "")
                sql = sql.replace(partition_section, partition_section_modified)

        return AthenaPropertiesExtractor.format_athena_column_definitions(sql)

    @staticmethod
    def _extract_column_types(create_expr: Create) -> Dict[str, str]:
        """Extract column types from a CREATE TABLE expression.

        Args:
            create_expr: The CREATE TABLE expression to extract types from

        Returns:
            A dictionary mapping column names to their types
        """
        column_types: Dict[str, str] = {}

        if not create_expr.this or not hasattr(create_expr.this, "expressions"):
            return column_types

        try:
            for expr in create_expr.this.expressions:
                if isinstance(expr, ColumnDef) and expr.this:
                    column_types[expr.name] = str(expr.kind)
        except Exception:
            # If we can't extract column types, return empty dict
            pass

        return column_types

    @staticmethod
    def _create_column_info(column_name: str, column_type: str) -> ColumnInfo:
        """Create a column info object.

        Args:
            column_name: Name of the column
            column_type: Type of the column

        Returns:
            A ColumnInfo object
        """
        return ColumnInfo(
            name=str(column_name) if column_name else "unknown",
            type=column_type if column_type else "unknown",
        )

    @staticmethod
    def _handle_function_expression(
        expr: Identifier, column_types: Dict[str, str]
    ) -> Tuple[ColumnInfo, TransformInfo]:
        """Handle function expressions like day(event_timestamp).

        Args:
            expr: The function expression to handle
            column_types: Dictionary of column types

        Returns:
            A tuple of (column_info, transform_info)
        """
        func_str = str(expr)

        if "(" not in func_str or ")" not in func_str:
            # Fallback for malformed function expressions
            column_info = AthenaPropertiesExtractor._create_column_info(
                func_str, "unknown"
            )
            transform_info = TransformInfo(type="unknown", column=column_info)
            return column_info, transform_info

        try:
            func_name = func_str.split("(")[0].lower()
            column_part = func_str.split("(")[1].split(")")[0].strip("`")

            column_info = AthenaPropertiesExtractor._create_column_info(
                column_part, column_types.get(column_part, "unknown")
            )
            transform_info = TransformInfo(type=func_name, column=column_info)

            return column_info, transform_info
        except (IndexError, AttributeError):
            # Fallback for parsing errors
            column_info = AthenaPropertiesExtractor._create_column_info(
                func_str, "unknown"
            )
            transform_info = TransformInfo(type="unknown", column=column_info)
            return column_info, transform_info

    @staticmethod
    def _handle_time_function(
        expr: Union[Year, Month, Day], column_types: Dict[str, str]
    ) -> Tuple[ColumnInfo, TransformInfo]:
        """Handle time-based functions like year, month, day.

        Args:
            expr: The time function expression to handle
            column_types: Dictionary of column types

        Returns:
            A tuple of (column_info, transform_info)
        """
        try:
            # Navigate the expression tree safely
            column_name = "unknown"
            if hasattr(expr, "this") and expr.this:
                if hasattr(expr.this, "this") and expr.this.this:
                    if hasattr(expr.this.this, "this") and expr.this.this.this:
                        column_name = str(expr.this.this.this)
                    else:
                        column_name = str(expr.this.this)
                else:
                    column_name = str(expr.this)

            column_info = AthenaPropertiesExtractor._create_column_info(
                column_name, column_types.get(column_name, "unknown")
            )
            transform_info = TransformInfo(
                type=expr.__class__.__name__.lower(), column=column_info
            )

            return column_info, transform_info
        except (AttributeError, TypeError):
            # Fallback for navigation errors
            column_info = AthenaPropertiesExtractor._create_column_info(
                "unknown", "unknown"
            )
            transform_info = TransformInfo(type="unknown", column=column_info)
            return column_info, transform_info

    @staticmethod
    def _handle_transform_function(
        expr: Anonymous, column_types: Dict[str, str]
    ) -> Tuple[ColumnInfo, TransformInfo]:
        """Handle transform functions like bucket, hour, truncate.

        Args:
            expr: The transform function expression to handle
            column_types: Dictionary of column types

        Returns:
            A tuple of (column_info, transform_info)
        """
        try:
            # Safely extract column name from the last expression
            column_name = "unknown"
            if (
                hasattr(expr, "expressions")
                and expr.expressions
                and len(expr.expressions) > 0
            ):
                last_expr = expr.expressions[-1]
                if hasattr(last_expr, "this") and last_expr.this:
                    if hasattr(last_expr.this, "this") and last_expr.this.this:
                        column_name = str(last_expr.this.this)
                    else:
                        column_name = str(last_expr.this)

            column_info = AthenaPropertiesExtractor._create_column_info(
                column_name, column_types.get(column_name, "unknown")
            )

            transform_type = str(expr.this).lower() if expr.this else "unknown"
            transform_info = TransformInfo(type=transform_type, column=column_info)

            # Add transform-specific parameters safely
            if (
                transform_type == "bucket"
                and hasattr(expr, "expressions")
                and expr.expressions
                and len(expr.expressions) > 0
            ):
                first_expr = expr.expressions[0]
                if hasattr(first_expr, "this"):
                    transform_info.bucket_count = first_expr.this
            elif (
                transform_type == "truncate"
                and hasattr(expr, "expressions")
                and expr.expressions
                and len(expr.expressions) > 0
            ):
                first_expr = expr.expressions[0]
                if hasattr(first_expr, "this"):
                    transform_info.length = first_expr.this

            return column_info, transform_info
        except (AttributeError, TypeError, IndexError):
            # Fallback for any parsing errors
            column_info = AthenaPropertiesExtractor._create_column_info(
                "unknown", "unknown"
            )
            transform_info = TransformInfo(type="unknown", column=column_info)
            return column_info, transform_info

    def _extract_partition_info(self, parsed: Expression) -> PartitionInfo:
        """Extract partitioning information from the parsed SQL statement.

        Args:
            parsed: The parsed SQL expression

        Returns:
            A PartitionInfo object containing simple columns and transforms
        """
        # Get the PARTITIONED BY expression
        partition_by_expr: Optional[Schema] = None

        try:
            for prop in parsed.find_all(Property):
                if isinstance(prop, PartitionedByProperty):
                    partition_by_expr = prop.this
                    break
        except Exception:
            # If we can't find properties, return empty result
            return PartitionInfo(simple_columns=[], transforms=[])

        if not partition_by_expr:
            return PartitionInfo(simple_columns=[], transforms=[])

        # Extract partitioning columns and transforms
        simple_columns: List[ColumnInfo] = []
        transforms: List[TransformInfo] = []

        # Get column types from the table definition
        column_types: Dict[str, str] = {}
        if isinstance(parsed, Create):
            column_types = self._extract_column_types(parsed)

        # Process each expression in the PARTITIONED BY clause
        if hasattr(partition_by_expr, "expressions") and partition_by_expr.expressions:
            for expr in partition_by_expr.expressions:
                try:
                    if isinstance(expr, Identifier) and "(" in str(expr):
                        column_info, transform_info = self._handle_function_expression(
                            expr, column_types
                        )
                        simple_columns.append(column_info)
                        transforms.append(transform_info)
                    elif isinstance(expr, PartitionByTruncate):
                        column_info = AthenaPropertiesExtractor._create_column_info(
                            str(expr.this), column_types.get(str(expr.this), "unknown")
                        )

                        expression = expr.args.get("expression")
                        transform_info = TransformInfo(
                            type="truncate",
                            column=column_info,
                            length=int(expression.name)
                            if expression and expression.name
                            else None,
                        )
                        transforms.append(transform_info)
                        simple_columns.append(column_info)
                    elif isinstance(expr, PartitionedByBucket):
                        column_info = AthenaPropertiesExtractor._create_column_info(
                            str(expr.this), column_types.get(str(expr.this), "unknown")
                        )
                        expression = expr.args.get("expression")
                        transform_info = TransformInfo(
                            type="bucket",
                            column=column_info,
                            bucket_count=int(expression.name)
                            if expression and expression.name
                            else None,
                        )
                        simple_columns.append(column_info)
                        transforms.append(transform_info)
                    elif isinstance(expr, (Year, Month, Day)):
                        column_info, transform_info = self._handle_time_function(
                            expr, column_types
                        )
                        transforms.append(transform_info)
                        simple_columns.append(column_info)
                    elif (
                        isinstance(expr, Anonymous)
                        and expr.this
                        and str(expr.this).lower() in ["bucket", "hour", "truncate"]
                    ):
                        column_info, transform_info = self._handle_transform_function(
                            expr, column_types
                        )
                        transforms.append(transform_info)
                        simple_columns.append(column_info)
                    elif hasattr(expr, "this") and expr.this:
                        column_name = str(expr.this)
                        column_info = self._create_column_info(
                            column_name, column_types.get(column_name, "unknown")
                        )
                        simple_columns.append(column_info)
                except Exception:
                    # Skip problematic expressions rather than failing completely
                    continue

        # Remove duplicates from simple_columns while preserving order
        seen_names: Set[str] = set()
        unique_simple_columns: List[ColumnInfo] = []

        for col in simple_columns:
            if col.name and col.name not in seen_names:
                seen_names.add(col.name)
                unique_simple_columns.append(col)

        return PartitionInfo(
            simple_columns=unique_simple_columns, transforms=transforms
        )

    def _extract_table_properties(self, parsed: Expression) -> TableProperties:
        """Extract table properties from the parsed SQL statement.

        Args:
            parsed: The parsed SQL expression

        Returns:
            A TableProperties object
        """
        location: Optional[str] = None
        format_prop: Optional[str] = None
        comment: Optional[str] = None
        serde_properties: Optional[Dict[str, str]] = None
        row_format: Optional[Dict[str, str]] = None
        additional_properties: Dict[str, str] = {}

        try:
            props = list(parsed.find_all(Property))
        except Exception:
            return TableProperties()

        for prop in props:
            try:
                if isinstance(prop, LocationProperty):
                    location = self._safe_get_property_value(prop)

                elif isinstance(prop, FileFormatProperty):
                    format_prop = self._safe_get_property_value(prop)

                elif isinstance(prop, SchemaCommentProperty):
                    comment = self._safe_get_property_value(prop)

                elif isinstance(prop, PartitionedByProperty):
                    continue  # Skip partition properties here

                elif isinstance(prop, SerdeProperties):
                    serde_props = self._extract_serde_properties(prop)
                    if serde_props:
                        serde_properties = serde_props

                elif isinstance(prop, RowFormatDelimitedProperty):
                    row_format_props = self._extract_row_format_properties(prop)
                    if row_format_props:
                        row_format = row_format_props

                else:
                    # Handle generic properties
                    key, value = self._extract_generic_property(prop)
                    if (
                        key
                        and value
                        and (not serde_properties or key not in serde_properties)
                    ):
                        additional_properties[key] = value

            except Exception:
                # Skip problematic properties rather than failing completely
                continue

        if (
            not location
            and additional_properties
            and additional_properties.get("external_location")
        ):
            location = additional_properties.pop("external_location")

        return TableProperties(
            location=location,
            format=format_prop,
            comment=comment,
            serde_properties=serde_properties,
            row_format=row_format,
            additional_properties=additional_properties
            if additional_properties
            else None,
        )

    def _safe_get_property_value(self, prop: Property) -> Optional[str]:
        """Safely extract value from a property."""
        try:
            if (
                hasattr(prop, "args")
                and "this" in prop.args
                and prop.args["this"]
                and hasattr(prop.args["this"], "name")
            ):
                return prop.args["this"].name
        except (AttributeError, KeyError, TypeError):
            pass
        return None

    def _extract_serde_properties(self, prop: SerdeProperties) -> Dict[str, str]:
        """Extract SERDE properties safely."""
        serde_props: Dict[str, str] = {}
        try:
            if hasattr(prop, "expressions") and prop.expressions:
                for exp in prop.expressions:
                    if (
                        hasattr(exp, "name")
                        and hasattr(exp, "args")
                        and "value" in exp.args
                        and exp.args["value"]
                        and hasattr(exp.args["value"], "name")
                    ):
                        serde_props[exp.name] = exp.args["value"].name
        except Exception:
            pass
        return serde_props

    def _extract_row_format_properties(
        self, prop: RowFormatDelimitedProperty
    ) -> Dict[str, str]:
        """Extract row format properties safely."""
        row_format: Dict[str, str] = {}
        try:
            if hasattr(prop, "args") and prop.args:
                for key, value in prop.args.items():
                    if hasattr(value, "this"):
                        row_format[key] = str(value.this)
                    else:
                        row_format[key] = str(value)
        except Exception:
            pass
        return row_format

    def _extract_generic_property(
        self, prop: Property
    ) -> Tuple[Optional[str], Optional[str]]:
        """Extract key-value pair from generic property."""
        try:
            if (
                hasattr(prop, "args")
                and "this" in prop.args
                and prop.args["this"]
                and hasattr(prop.args["this"], "name")
                and "value" in prop.args
                and prop.args["value"]
                and hasattr(prop.args["value"], "name")
            ):
                key = prop.args["this"].name.lower()
                value = prop.args["value"].name
                return key, value
        except (AttributeError, KeyError, TypeError):
            pass
        return None, None

    def _extract_row_format(self, parsed: Expression) -> RowFormatInfo:
        """Extract and format RowFormatDelimitedProperty.

        Args:
            parsed: The parsed SQL expression

        Returns:
            A RowFormatInfo object
        """
        row_format_props: Dict[str, str] = {}

        try:
            props = parsed.find_all(Property)
            for prop in props:
                if isinstance(prop, RowFormatDelimitedProperty):
                    row_format_props = self._extract_row_format_properties(prop)
                    break
        except Exception:
            pass

        if row_format_props:
            try:
                json_formatted = json.dumps(row_format_props, indent=2)
            except (TypeError, ValueError):
                json_formatted = "Error formatting row format properties"
        else:
            json_formatted = "No RowFormatDelimitedProperty found"

        return RowFormatInfo(properties=row_format_props, json_formatted=json_formatted)
