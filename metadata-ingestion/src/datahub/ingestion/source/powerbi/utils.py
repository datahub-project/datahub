"""
Power BI Utilities Module

This module contains reusable utility functions used across the Power BI connector.
Functions include:
- Expression type detection (DAX vs M-Query)
- URN manipulation
- Default configuration generators
- Factory functions for data classes
"""

import logging
import re
from typing import Dict, Optional

from datahub.ingestion.source.powerbi.models import PowerBIDataset, Workspace
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn

logger = logging.getLogger(__name__)


# =============================================================================
# Expression Type Detection
# =============================================================================


def is_dax_expression(expression: str) -> bool:
    """
    Determine if an expression is a DAX expression (vs M-Query).

    DAX expressions typically:
    - Use DAX functions like SUMMARIZE, ADDCOLUMNS, CALENDARAUTO, RELATED, etc.
    - Reference tables with 'TableName' or TableName[Column] syntax
    - Don't start with "let" or "Source ="

    M-Query expressions typically:
    - Start with "let" or contain "Source ="
    - Use M-Query functions
    - Have step-by-step transformations

    Args:
        expression: The expression string to analyze

    Returns:
        True if the expression appears to be DAX, False otherwise
    """
    if not expression or not expression.strip():
        return False

    expression_upper = expression.strip().upper()

    # M-Query indicators (if these are present, it's NOT DAX)
    mquery_indicators = [
        "LET ",
        "SOURCE =",
        "SOURCE=",
        '#"',  # M-Query step names use #"StepName"
        "TABLE.",  # M-Query table functions
        "LIST.",  # M-Query list functions
    ]

    for indicator in mquery_indicators:
        if indicator in expression_upper[:100]:  # Check first 100 chars
            return False

    # DAX function indicators (if these are present, it's likely DAX)
    dax_functions = [
        "SUMMARIZE(",
        "ADDCOLUMNS(",
        "CALENDARAUTO(",
        "CALENDAR(",
        "RELATED(",
        "RELATEDTABLE(",
        "CALCULATETABLE(",
        "FILTER(",
        "SUMX(",
        "AVERAGEX(",
        "COUNTX(",
        "EVALUATE",
        "ROW(",  # DAX table constructor function
        "DATATABLE(",  # DAX table constructor
        "SELECTCOLUMNS(",
        "TOPN(",
        "SAMPLE(",
    ]

    for func in dax_functions:
        if func in expression_upper:
            return True

    # If expression has table references like 'TableName'[Column] or TableName[Column]
    # it's likely DAX (M-Query uses different syntax)
    if re.search(r"'[^']+'\s*\[", expression) or re.search(
        r"\b[A-Za-z_][A-Za-z0-9_]*\s*\[", expression
    ):
        # But double-check it's not M-Query (which also uses brackets but differently)
        if "SOURCE" not in expression_upper[:50]:
            return True

    return False


# =============================================================================
# URN Manipulation
# =============================================================================


def urn_to_lowercase(value: str, flag: bool) -> str:
    """
    Convert a URN to lowercase based on a flag.

    Args:
        value: The URN string to potentially convert
        flag: If True, convert to lowercase; otherwise return as-is

    Returns:
        The URN, possibly converted to lowercase
    """
    if flag is True:
        return lowercase_dataset_urn(value)
    return value


def lineage_urn_to_lowercase(
    value: str, convert_lineage_urns_to_lowercase: bool
) -> str:
    """
    Convert a lineage URN to lowercase based on configuration.

    Args:
        value: The URN string to potentially convert
        convert_lineage_urns_to_lowercase: Config flag for lineage URN conversion

    Returns:
        The URN, possibly converted to lowercase
    """
    return urn_to_lowercase(value, convert_lineage_urns_to_lowercase)


def assets_urn_to_lowercase(value: str, convert_urns_to_lowercase: bool) -> str:
    """
    Convert an asset URN to lowercase based on configuration.

    Args:
        value: The URN string to potentially convert
        convert_urns_to_lowercase: Config flag for asset URN conversion

    Returns:
        The URN, possibly converted to lowercase
    """
    return urn_to_lowercase(value, convert_urns_to_lowercase)


# =============================================================================
# Configuration Helpers
# =============================================================================


def default_for_dataset_type_mapping() -> Dict[str, str]:
    """
    Generate the default dataset type mapping from PowerBI data platform names
    to DataHub platform names.

    Returns:
        Dictionary mapping PowerBI platform names to DataHub platform names
    """
    from datahub.ingestion.source.powerbi.models import SupportedDataPlatform

    dict_: dict = {}
    for item in SupportedDataPlatform:
        dict_[item.value.powerbi_data_platform_name] = (
            item.value.datahub_data_platform_name
        )

    return dict_


# =============================================================================
# Factory Functions
# =============================================================================


def create_powerbi_dataset(
    workspace: "Workspace", raw_instance: dict
) -> "PowerBIDataset":
    """
    Create a new PowerBIDataset instance from raw API response data.

    Args:
        workspace: The workspace containing this dataset
        raw_instance: Raw dataset data from PowerBI API

    Returns:
        A new PowerBIDataset instance
    """
    from datahub.ingestion.source.powerbi.models import PowerBIDataset

    return PowerBIDataset(
        id=raw_instance["id"],
        name=raw_instance.get("name"),
        description=raw_instance.get("description", ""),
        webUrl=(
            "{}/details".format(raw_instance.get("webUrl"))
            if raw_instance.get("webUrl") is not None
            else None
        ),
        workspace_id=workspace.id,
        workspace_name=workspace.name,
        parameters={},
        tables=[],
        tags=[],
        configuredBy=raw_instance.get("configuredBy"),
    )


# =============================================================================
# String Processing Helpers
# =============================================================================


def sanitize_table_name(table_name: str) -> str:
    """
    Sanitize a table name for use in URNs and identifiers.

    Args:
        table_name: The raw table name

    Returns:
        Sanitized table name
    """
    # Remove leading/trailing whitespace
    sanitized = table_name.strip()

    # Replace special characters that might cause issues
    sanitized = sanitized.replace(" ", "_")
    sanitized = sanitized.replace("-", "_")

    return sanitized


def parse_table_column_reference(reference: str) -> tuple[Optional[str], Optional[str]]:
    """
    Parse a table.column reference string into table and column components.

    Handles various formats:
    - 'TableName'[ColumnName]
    - TableName[ColumnName]
    - TableName.ColumnName

    Args:
        reference: The reference string to parse

    Returns:
        Tuple of (table_name, column_name), with None values if parsing fails
    """
    # Try to match 'TableName'[ColumnName] or TableName[ColumnName]
    match = re.match(r"'?([^'\[]+)'?\s*\[\s*([^\]]+)\s*\]", reference)
    if match:
        return match.group(1).strip(), match.group(2).strip()

    # Try to match TableName.ColumnName
    if "." in reference:
        parts = reference.split(".", 1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()

    return None, None


def extract_measure_name(expression: str) -> Optional[str]:
    """
    Extract measure name from a measure reference expression.

    Args:
        expression: The expression containing a measure reference

    Returns:
        The measure name if found, None otherwise
    """
    # Try to match [MeasureName] pattern
    match = re.search(r"\[([^\]]+)\]", expression)
    if match:
        return match.group(1).strip()

    return None
