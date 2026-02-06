import logging
import re
from typing import Dict, List, Literal, Optional, Set, Tuple

from datahub.ingestion.source.powerbi.models import (
    CalculatedColumnMapping,
    CalculateExpression,
    DAXParameter,
    DAXParseResult,
    DirectColumnMapping,
    FilterContextModifier,
    FilterReference,
    SummarizeParseResult,
    TableColumnReference,
)

logger = logging.getLogger(__name__)


# Models are now imported from models.py
# This enables better organization and reuse


# =============================================================================
# Pre-compiled Regex Patterns for Performance
# =============================================================================
# These patterns are compiled once at module load time to avoid repeated compilation
# during parsing of potentially thousands of DAX expressions

# Pattern for parameter field references: TableName[ColumnName] or 'TableName'[ColumnName]
_PARAM_TABLE_COLUMN_PATTERN = re.compile(r"'?([A-Za-z0-9_\s]+)'?\s*\[\s*([^\]]+)\s*\]")

# Pattern 1: 'TableName'[ColumnName] (with single quotes around table name)
_QUOTED_TABLE_COLUMN_PATTERN = re.compile(r"'([^']+)'\s*\[\s*([^\]]+)\s*\]")

# Pattern 2: TableName[ColumnName] (without quotes, but not measure-only references)
_UNQUOTED_TABLE_COLUMN_PATTERN = re.compile(
    r"(?<!['\"])\b([A-Za-z_][A-Za-z0-9_]*)\s*\[\s*([^\]]+)\s*\]"
)

# Pattern for measure references: [MeasureName] (no table prefix)
_MEASURE_ONLY_PATTERN = re.compile(r"(?<!\w)\[\s*([^\]]+)\s*\](?!\s*\[)")

# Pattern 3: RELATED(TableName[ColumnName])
_RELATED_PATTERN = re.compile(
    r"RELATED\s*\(\s*(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*\[\s*([^\]]+)\s*\]",
    re.IGNORECASE,
)

# Pattern 4: RELATEDTABLE(TableName)
_RELATEDTABLE_PATTERN = re.compile(
    r"RELATEDTABLE\s*\(\s*(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*\)",
    re.IGNORECASE,
)

# Pattern 5: Iterator functions with table references
_ITERATOR_TABLE_PATTERN = re.compile(
    r"(?:SUMX|AVERAGEX|COUNTX|MAXX|MINX|FILTER|ALL|ALLEXCEPT|VALUES|DISTINCT|CALCULATETABLE|ADDCOLUMNS|SELECTCOLUMNS|GROUPBY)\s*\(\s*(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*[,)]",
    re.IGNORECASE,
)

# Pattern 6: Time intelligence functions
_TIME_INTELLIGENCE_PATTERN = re.compile(
    r"(?:SAMEPERIODLASTYEAR|PARALLELPERIOD|DATESINPERIOD|DATESBETWEEN|DATESYTD|DATESMTD|DATESQTD)\s*\(\s*(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*\[\s*([^\]]+)\s*\]",
    re.IGNORECASE,
)

# Pattern for measure dependencies: [MeasureName]
_MEASURE_DEPENDENCY_PATTERN = re.compile(r"\[([^\]]+)\]")

# Pattern for SUMMARIZE function (full expression capture)
_SUMMARIZE_FULL_PATTERN = re.compile(r"SUMMARIZE\s*\((.*)\)", re.IGNORECASE | re.DOTALL)

# Pattern for SUMMARIZE table name extraction
_SUMMARIZE_TABLE_PATTERN = re.compile(
    r"SUMMARIZE\s*\(\s*(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))",
    re.IGNORECASE | re.MULTILINE,
)

# Pattern for NAMEOF function
_NAMEOF_PATTERN = re.compile(
    r"NAMEOF\s*\(\s*'?([A-Za-z0-9_\s]+)'?\s*\[\s*([^\]]+)\s*\]\s*\)", re.IGNORECASE
)

# Pattern for CALCULATE expression parsing
_CALCULATE_PATTERN = re.compile(r"CALCULATE\s*\((.*)\)", re.IGNORECASE | re.DOTALL)

# Pattern for VAR declarations in DAX
_VAR_DECLARATION_PATTERN = re.compile(
    r"VAR\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([^\n]+?)(?=\s+(?:VAR|RETURN)|$)",
    re.IGNORECASE | re.MULTILINE,
)

# Pattern for table/column references in SUMMARIZE expressions
_TABLE_COLUMN_IN_SUMMARIZE_PATTERN = re.compile(
    r"(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*\[\s*([^\]]+)\s*\]"
)


def get_all_column_references_for_parameter(
    param: DAXParameter,
) -> List[TableColumnReference]:
    """
    For field parameters, extract all possible table.column references.
    Returns list of TableColumnReference objects.
    """
    refs: List[TableColumnReference] = []

    # Parse each possible value to extract table.column patterns
    for value in param.possible_values:
        # Pattern: TableName[ColumnName] or 'TableName'[ColumnName]
        matches = _PARAM_TABLE_COLUMN_PATTERN.findall(value)
        for table_name, column_name in matches:
            refs.append(
                TableColumnReference(table_name=table_name, column_name=column_name)
            )

    return refs


def parse_dax_expression(
    dax_expression: str,
    include_measure_refs: bool = False,
    include_advanced_analysis: bool = False,
    extract_parameters: bool = True,
    data_model_tables: Optional[List[Dict]] = None,
    include_all_functions: bool = True,
) -> DAXParseResult:
    """
    Parse a DAX expression and extract all references using Pydantic models.

    This is the recommended function to use for comprehensive DAX parsing.

    Args:
        dax_expression: The DAX expression to parse
        include_measure_refs: If True, also extract measure references
        include_advanced_analysis: If True, extract nested CALCULATE and filter context modifiers
        extract_parameters: If True, extract parameter references (default: True)
        data_model_tables: Optional data model table definitions for resolving field parameters
        include_all_functions: If True, extract references from additional DAX functions (default: True)

    Returns:
        DAXParseResult with all parsed information
    """
    if not dax_expression:
        return DAXParseResult()

    table_col_refs = extract_table_column_references(
        dax_expression, include_measure_refs
    )
    table_column_references = [ref for ref in table_col_refs if ref.table_name]

    if include_all_functions:
        additional_refs = extract_additional_dax_functions(dax_expression)
        existing_refs = {
            (ref.table_name, ref.column_name) for ref in table_column_references
        }
        for ref in additional_refs:
            if (ref.table_name, ref.column_name) not in existing_refs:
                table_column_references.append(ref)
                existing_refs.add((ref.table_name, ref.column_name))

    measure_refs = (
        extract_measure_dependencies(dax_expression) if include_measure_refs else []
    )

    table_only_refs = [
        ref.table_name for ref in table_column_references if not ref.column_name
    ]

    variables = extract_variables(dax_expression)
    parameters = extract_dax_parameters(dax_expression) if extract_parameters else []

    # Resolve field parameters to their possible column values
    # This is CRITICAL for lineage: if a parameter can be Sales[Amount] OR Sales[Quantity],
    # we need lineage to BOTH columns for accurate impact analysis
    parameter_resolved_refs: List[TableColumnReference] = []
    if data_model_tables:
        for param in parameters:
            if param.parameter_type == "field_parameter" and param.table_name:
                possible_values = resolve_field_parameter_values(
                    param.table_name, data_model_tables
                )
                param.possible_values = possible_values

                param_col_refs = get_all_column_references_for_parameter(param)
                for ref in param_col_refs:
                    ref_tuple = (ref.table_name, ref.column_name)
                    if ref_tuple not in existing_refs:
                        parameter_resolved_refs.append(
                            TableColumnReference(
                                table_name=ref.table_name, column_name=ref.column_name
                            )
                        )
                        existing_refs.add(ref_tuple)

                logger.info(
                    f"Resolved field parameter {param.table_name}.{param.name} to "
                    f"{len(param_col_refs)} possible columns for lineage"
                )

    # Merge parameter-resolved references with main references
    table_column_references.extend(parameter_resolved_refs)

    calculate_exprs: List[CalculateExpression] = []
    filter_modifiers: List[FilterContextModifier] = []
    if include_advanced_analysis:
        calculate_exprs = extract_nested_calculate_expressions(dax_expression)
        filter_modifiers = extract_filter_context_modifiers(dax_expression)

    return DAXParseResult(
        table_column_references=[
            ref for ref in table_column_references if ref.column_name
        ],
        measure_references=measure_refs,
        table_references=table_only_refs,
        variables=variables,
        parameters=parameters,
        calculate_expressions=calculate_exprs,
        filter_context_modifiers=filter_modifiers,
    )


def extract_table_column_references(
    dax_expression: str, include_measure_refs: bool = False
) -> List[TableColumnReference]:
    """
    Parse DAX expression and extract table.column references.

    NOTE: For new code, consider using parse_dax_expression() which returns a Pydantic model.

    This function extracts table and column references from DAX expressions using
    regex patterns. It handles common DAX patterns including:
    - 'TableName'[ColumnName] or TableName[ColumnName]
    - RELATED(TableName[ColumnName])
    - Table references in various DAX functions
    - Measure references (when include_measure_refs=True)

    Args:
        dax_expression: The DAX expression to parse
        include_measure_refs: If True, also extract measure references like [MeasureName]

    Returns:
        List of TableColumnReference objects
    """
    if not dax_expression:
        return []

    logger.debug("Parsing DAX expression: %s...", dax_expression[:100])

    references: Set[Tuple[str, str]] = (
        set()
    )  # Still use tuple internally for deduplication

    # Pattern 1: 'TableName'[ColumnName] (with single quotes around table name)
    # Matches: 'Sales'[Amount], 'Order Details'[Price]
    matches1 = _QUOTED_TABLE_COLUMN_PATTERN.findall(dax_expression)
    references.update(matches1)

    # Pattern 2: TableName[ColumnName] (without quotes)
    # Matches: Sales[Amount], OrderDetails[Price]
    # But not things like [ColumnOnly] (no table)
    # Need to be careful not to match measure references without table
    matches2 = _UNQUOTED_TABLE_COLUMN_PATTERN.findall(dax_expression)

    # Filter out common DAX functions that might match this pattern
    dax_functions = {
        "CALCULATE",
        "SUM",
        "SUMX",
        "AVERAGE",
        "AVERAGEX",
        "COUNT",
        "COUNTX",
        "MAX",
        "MAXX",
        "MIN",
        "MINX",
        "IF",
        "SWITCH",
        "RELATED",
        "RELATEDTABLE",
        "FILTER",
        "ALL",
        "ALLEXCEPT",
        "VALUES",
        "DISTINCT",
        "SELECTEDVALUE",
        "DIVIDE",
        "FORMAT",
        "DATEDIFF",
        "DATEADD",
        "DATESBETWEEN",
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "DATE",
        "TIME",
        "NOW",
        "TODAY",
        "EARLIER",
        "EARLIEST",
        "HASONEVALUE",
        "ISBLANK",
        "ISNUMBER",
        "ISTEXT",
        "CONTAINS",
        "SEARCH",
        "FIND",
        "LEN",
        "LEFT",
        "RIGHT",
        "MID",
        "UPPER",
        "LOWER",
        "CONCATENATE",
        "TRIM",
        "SUBSTITUTE",
        "REPLACE",
        "CALCULATETABLE",
        "ADDCOLUMNS",
        "GROUPBY",
        "ROLLUP",
        "SUMMARIZE",
        "SUMMARIZECOLUMNS",
        "CROSSFILTER",
        "USERELATIONSHIP",
        "TREATAS",
        "GENERATE",
        "GENERATEALL",
        "ROW",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "NATURALINNERJOIN",
        "NATURALLEFTOUTERJOIN",
        "BLANK",
        "LOOKUPVALUE",
        "PATH",
        "PATHITEM",
        "PATHITEMREVERSE",
        "PATHLENGTH",
    }

    for table, column in matches2:
        # Skip if the "table" is actually a DAX function
        if table.upper() not in dax_functions:
            references.add((table, column))

    # Pattern for measure references: [MeasureName] (no table prefix)
    # Only extract if include_measure_refs is True
    if include_measure_refs:
        matches_measure = _MEASURE_ONLY_PATTERN.findall(dax_expression)
        for measure_name in matches_measure:
            # Add as ("", measure_name) to indicate it's a measure reference
            # Skip if it looks like a parameter or if we already have it with a table
            if measure_name and not any(col == measure_name for _, col in references):
                references.add(("", measure_name))

    # Pattern 3: RELATED(TableName[ColumnName])
    # This is more specific and reliable
    matches3 = _RELATED_PATTERN.findall(dax_expression)
    for match in matches3:
        table = match[0] if match[0] else match[1]
        column = match[2]
        references.add((table, column))

    # Pattern 4: RELATEDTABLE(TableName)
    # This references a whole table
    matches4 = _RELATEDTABLE_PATTERN.findall(dax_expression)
    for match in matches4:
        table = match[0] if match[0] else match[1]
        # For table references without a specific column, use empty string as column
        references.add((table, ""))

    # Pattern 5: Table references in iterators like SUMX, FILTER, etc.
    # SUMX(TableName, ...) or FILTER(TableName, ...)
    # These reference tables without specific columns initially
    matches5 = _ITERATOR_TABLE_PATTERN.findall(dax_expression)
    for match in matches5:
        table = match[0] if match[0] else match[1]
        # For table references without a specific column, use empty string as column
        references.add((table, ""))

    # Pattern 6: Time intelligence functions that reference tables
    # SAMEPERIODLASTYEAR, PARALLELPERIOD, DATEADD, etc.
    matches6 = _TIME_INTELLIGENCE_PATTERN.findall(dax_expression)
    for match in matches6:
        table = match[0] if match[0] else match[1]
        column = match[2]
        references.add((table, column))

    # Ensure we convert None to empty string for Pydantic validation
    result = [
        TableColumnReference(
            table_name=table if table is not None else "",
            column_name=column if column is not None else "",
        )
        for table, column in sorted(list(references))
    ]

    logger.debug(
        "Extracted %d table/column references from DAX expression", len(result)
    )

    return result


def extract_table_references(dax_expression: str) -> List[str]:
    """
    Extract just the table names (without columns) from a DAX expression.

    Args:
        dax_expression: The DAX expression to parse

    Returns:
        List of unique table names referenced in the expression
    """
    references = extract_table_column_references(dax_expression)
    tables = set(ref.table_name for ref in references if ref.table_name)
    return sorted(list(tables))


def extract_column_references(dax_expression: str, table_name: str) -> List[str]:
    """
    Extract column names for a specific table from a DAX expression.

    Args:
        dax_expression: The DAX expression to parse
        table_name: The table name to filter by

    Returns:
        List of column names referenced for the specified table
    """
    references = extract_table_column_references(dax_expression)
    columns = [
        ref.column_name
        for ref in references
        if ref.table_name == table_name and ref.column_name
    ]
    return sorted(list(set(columns)))


def extract_measure_dependencies(
    dax_expression: str, all_measures: Optional[Set[str]] = None
) -> List[str]:
    """
    Extract measure references from a DAX expression.
    This helps build a dependency graph where measures reference other measures.

    Args:
        dax_expression: The DAX expression to parse
        all_measures: Optional set of known measure names to validate against

    Returns:
        List of measure names referenced in the expression
    """
    if not dax_expression:
        return []

    measure_refs: Set[str] = set()

    # Pattern: [MeasureName] without a table prefix
    # This is common for measures referencing other measures
    matches = _MEASURE_DEPENDENCY_PATTERN.findall(dax_expression)

    for measure_name in matches:
        # Validate against known measures if provided
        if all_measures is None or measure_name in all_measures:
            measure_refs.add(measure_name)

    return sorted(list(measure_refs))


def extract_variables(dax_expression: str) -> Dict[str, str]:
    """
    Extract VAR declarations from DAX expressions.
    Variables can reference columns, measures, or other variables.

    Args:
        dax_expression: The DAX expression to parse

    Returns:
        Dictionary mapping variable names to their expressions
    """
    if not dax_expression:
        return {}

    variables: Dict[str, str] = {}

    # Pattern: VAR VariableName = Expression
    # This is a simplified pattern - production would need better parsing
    matches = _VAR_DECLARATION_PATTERN.findall(dax_expression)

    for var_name, var_expr in matches:
        variables[var_name.strip()] = var_expr.strip()
        logger.debug("Found variable: %s = %s...", var_name, var_expr[:50])

    return variables


def extract_dax_parameters(dax_expression: str) -> List[DAXParameter]:
    """
    Extract parameter references from DAX expressions.

    Parameters in Power BI can come from:
    - Field Parameters (dropdown/multi-select for dynamic field selection)
    - What-if Parameters (slider parameters for scenarios)
    - Query Parameters (URL or M-Query parameters)
    - Slicer values that act as parameters

    Common patterns:
    - SELECTEDVALUE('ParameterTable'[ParameterColumn])
    - SELECTEDVALUE('ParameterTable'[ParameterColumn], DefaultValue)
    - [ParameterName] (direct parameter reference)
    - @ParameterName (query parameter syntax)

    Args:
        dax_expression: The DAX expression to parse

    Returns:
        List of DAXParameter objects found in the expression
    """
    if not dax_expression:
        return []

    parameters: List[DAXParameter] = []
    seen_params: Set[str] = set()

    # Pattern 1: SELECTEDVALUE for field parameters and what-if parameters
    # SELECTEDVALUE('ParameterTable'[Column], DefaultValue)
    selectedvalue_pattern = r"SELECTEDVALUE\s*\(\s*'?([A-Za-z0-9_\s]+)'?\s*\[\s*([^\]]+)\s*\](?:\s*,\s*([^)]+))?\)"
    for match in re.finditer(selectedvalue_pattern, dax_expression, re.IGNORECASE):
        table_name = match.group(1).strip()
        column_name = match.group(2).strip()
        default_value = match.group(3).strip() if match.group(3) else None

        param_key = f"{table_name}.{column_name}"
        if param_key not in seen_params:
            seen_params.add(param_key)

            # Heuristic: If table name contains "Parameter", it's likely a parameter table
            param_type: Literal[
                "field_parameter", "what_if", "query_parameter", "slicer_value"
            ]
            if "parameter" in table_name.lower():
                if "field" in table_name.lower():
                    param_type = "field_parameter"
                else:
                    param_type = "what_if"
            else:
                param_type = "slicer_value"

            parameters.append(
                DAXParameter(
                    name=column_name,
                    parameter_type=param_type,
                    table_name=table_name,
                    default_value=default_value,
                )
            )

    # Pattern 2: Direct parameter references (less common but possible)
    # @ParameterName or direct [ParameterName] in specific contexts
    query_param_pattern = r"@([A-Za-z_][A-Za-z0-9_]*)"
    for match in re.finditer(query_param_pattern, dax_expression):
        param_name = match.group(1)

        if param_name not in seen_params:
            seen_params.add(param_name)
            parameters.append(
                DAXParameter(
                    name=param_name,
                    parameter_type="query_parameter",
                    table_name=None,
                    default_value=None,
                )
            )

    # Pattern 3: TREATAS with parameter tables (for field parameters)
    # TREATAS(VALUES('Parameter'[Column]), Table[Column])
    treatas_pattern = (
        r"TREATAS\s*\(\s*VALUES\s*\(\s*'?([A-Za-z0-9_\s]+)'?\s*\[\s*([^\]]+)\s*\]\s*\)"
    )
    for match in re.finditer(treatas_pattern, dax_expression, re.IGNORECASE):
        table_name = match.group(1).strip()
        column_name = match.group(2).strip()

        param_key = f"{table_name}.{column_name}"
        if param_key not in seen_params and "parameter" in table_name.lower():
            seen_params.add(param_key)
            parameters.append(
                DAXParameter(
                    name=column_name,
                    parameter_type="field_parameter",
                    table_name=table_name,
                    default_value=None,
                )
            )

    # Pattern 4: SWITCH with SELECTEDVALUE (field parameters used in dynamic measures)
    # SWITCH(TRUE(), SELECTEDVALUE(...) = "Value1", Measure1, ...)
    # Already captured by SELECTEDVALUE pattern above

    logger.debug("Found %d parameters in DAX expression", len(parameters))
    return parameters


def resolve_field_parameter_values(
    parameter_table_name: str, data_model_tables: Optional[List[Dict]] = None
) -> List[str]:
    """
    Resolve a field parameter table to its possible column values.

    Field parameters in Power BI are special tables that contain rows representing
    different columns the user can select. Each row typically has:
    - A display name
    - An ordinal/index
    - The actual column reference

    Args:
        parameter_table_name: Name of the parameter table
        data_model_tables: List of table definitions from the data model

    Returns:
        List of column reference strings that this parameter can resolve to
    """
    possible_values: List[str] = []

    if not data_model_tables:
        logger.debug(
            f"No data model provided, cannot resolve parameter {parameter_table_name}"
        )
        return possible_values

    # Find the parameter table in the data model
    param_table = None
    for table in data_model_tables:
        if table.get("name") == parameter_table_name:
            param_table = table
            break

    if not param_table:
        logger.debug("Parameter table %s not found in data model", parameter_table_name)
        return possible_values

    # Look for calculated columns that define the field parameter options
    # Field parameters typically have a column with a DAX expression like:
    # NAMEOF('Sales'[Amount]) or TREATAS(...) or direct column reference
    columns = param_table.get("columns", [])
    for column in columns:
        expression = column.get("expression", "")

        # Skip if no expression (not a calculated column)
        if not expression:
            continue

        # Pattern 1: NAMEOF('Table'[Column])
        nameof_matches = _NAMEOF_PATTERN.findall(expression)
        for table, col in nameof_matches:
            possible_values.append(f"{table}[{col}]")

        # Pattern 2: Direct column references in SELECTCOLUMNS or similar
        # SELECTCOLUMNS(..., "Value", 'Table'[Column])
        direct_refs = _PARAM_TABLE_COLUMN_PATTERN.findall(expression)
        for table, col in direct_refs:
            # Avoid duplicates and non-column references
            ref = f"{table}[{col}]"
            if ref not in possible_values and table != parameter_table_name:
                possible_values.append(ref)

    # Also check if there's a measures array with field parameter definitions
    measures = param_table.get("measures", [])
    for measure in measures:
        expression = measure.get("expression", "")
        if expression:
            # Extract column references from the measure expression
            refs = _PARAM_TABLE_COLUMN_PATTERN.findall(expression)
            for table, col in refs:
                ref = f"{table}[{col}]"
                if ref not in possible_values and table != parameter_table_name:
                    possible_values.append(ref)

    logger.debug(
        f"Resolved field parameter {parameter_table_name} to {len(possible_values)} possible values"
    )
    return possible_values


def _extract_balanced_parentheses_content(
    expression: str, start_pos: int
) -> Optional[str]:
    """
    Extract content within balanced parentheses starting from a position.

    Args:
        expression: The full expression
        start_pos: Position after the opening parenthesis

    Returns:
        Content within parentheses, or None if unbalanced
    """
    paren_count = 1
    pos = start_pos
    content_start = pos

    while pos < len(expression) and paren_count > 0:
        if expression[pos] == "(":
            paren_count += 1
        elif expression[pos] == ")":
            paren_count -= 1
        pos += 1

    if paren_count == 0:
        return expression[content_start : pos - 1]
    return None


def _process_function_references(
    dax_expression: str,
    function_names: List[str],
    seen: Set[Tuple[str, str]],
    references: List[TableColumnReference],
) -> None:
    """
    Process DAX function matches and extract column references.

    Args:
        dax_expression: The DAX expression to parse
        function_names: List of function names to search for
        seen: Set of already-seen (table, column) tuples
        references: List to append new references to
    """
    for func in function_names:
        pattern = rf"{func}\s*\("
        for match in re.finditer(pattern, dax_expression, re.IGNORECASE):
            func_start = match.end()
            content = _extract_balanced_parentheses_content(dax_expression, func_start)

            if content:
                refs = extract_table_column_references(content)
                for ref in refs:
                    ref_tuple = (ref.table_name, ref.column_name)
                    if ref_tuple not in seen:
                        seen.add(ref_tuple)
                        references.append(ref)


def _process_switch_function(
    dax_expression: str,
    seen: Set[Tuple[str, str]],
    references: List[TableColumnReference],
) -> None:
    """
    Process SWITCH(TRUE(), ...) patterns for dynamic column selection.

    Args:
        dax_expression: The DAX expression to parse
        seen: Set of already-seen (table, column) tuples
        references: List to append new references to
    """
    switch_pattern = r"SWITCH\s*\(\s*TRUE\(\)\s*,"
    for match in re.finditer(switch_pattern, dax_expression, re.IGNORECASE):
        # Find SWITCH opening parenthesis and extract content
        paren_count = 0
        content_start = None

        for i, char in enumerate(dax_expression[match.start() :]):
            if char == "(":
                if paren_count == 0:
                    content_start = match.start() + i + 1
                paren_count += 1
            elif char == ")":
                paren_count -= 1
                if paren_count == 0:
                    pos = match.start() + i
                    if content_start:
                        content = dax_expression[content_start:pos]
                        # Extract all column references from SWITCH branches
                        refs = extract_table_column_references(content)
                        for ref in refs:
                            ref_tuple = (ref.table_name, ref.column_name)
                            if ref_tuple not in seen:
                                seen.add(ref_tuple)
                                references.append(ref)
                    break


def extract_additional_dax_functions(dax_expression: str) -> List[TableColumnReference]:
    """
    Extract column references from additional DAX functions not covered by base extraction.

    Covers:
    - Iterator functions: SUMX, AVERAGEX, MINX, MAXX, COUNTX, etc.
    - Table functions: UNION, INTERSECT, EXCEPT, CROSSJOIN, NATURALINNERJOIN, etc.
    - Sampling functions: TOPN, SAMPLE
    - Date/Time intelligence: DATEADD, DATESBETWEEN, SAMEPERIODLASTYEAR, etc. (more comprehensive)
    - Relationship functions: RELATED, RELATEDTABLE, CROSSFILTER
    - Path functions: PATH, PATHITEM, PATHCONTAINS
    - Statistical: RANKX, PERCENTILEX.INC, PERCENTILEX.EXC

    Args:
        dax_expression: The DAX expression to parse

    Returns:
        List of TableColumnReference objects found
    """
    if not dax_expression:
        return []

    references: List[TableColumnReference] = []
    seen: Set[Tuple[str, str]] = set()

    # Iterator functions: SUMX, AVERAGEX, COUNTX, MINX, MAXX, etc.
    iterator_functions = [
        "SUMX",
        "AVERAGEX",
        "COUNTX",
        "MINX",
        "MAXX",
        "PRODUCTX",
        "CONCATENATEX",
        "RANKX",
        "PERCENTILEX.INC",
        "PERCENTILEX.EXC",
    ]
    _process_function_references(dax_expression, iterator_functions, seen, references)

    # Table manipulation functions
    table_functions = [
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "CROSSJOIN",
        "NATURALINNERJOIN",
        "NATURALLEFTOUTERJOIN",
        "TOPN",
        "SAMPLE",
    ]
    _process_function_references(dax_expression, table_functions, seen, references)

    # Comprehensive Time Intelligence functions
    time_intel_functions = [
        "DATEADD",
        "DATESBETWEEN",
        "DATESINPERIOD",
        "SAMEPERIODLASTYEAR",
        "PARALLELPERIOD",
        "PREVIOUSYEAR",
        "PREVIOUSQUARTER",
        "PREVIOUSMONTH",
        "NEXTYEAR",
        "NEXTQUARTER",
        "NEXTMONTH",
        "STARTOFYEAR",
        "STARTOFQUARTER",
        "STARTOFMONTH",
        "ENDOFYEAR",
        "ENDOFQUARTER",
        "ENDOFMONTH",
        "FIRSTDATE",
        "LASTDATE",
        "FIRSTNONBLANK",
        "LASTNONBLANK",
        "TOTALMTD",
        "TOTALQTD",
        "TOTALYTD",
        "OPENINGBALANCEMONTH",
        "OPENINGBALANCEQUARTER",
        "OPENINGBALANCEYEAR",
        "CLOSINGBALANCEMONTH",
        "CLOSINGBALANCEQUARTER",
        "CLOSINGBALANCEYEAR",
    ]
    _process_function_references(dax_expression, time_intel_functions, seen, references)

    # Path functions (for parent-child hierarchies)
    path_functions = [
        "PATH",
        "PATHITEM",
        "PATHLENGTH",
        "PATHCONTAINS",
        "PATHITEMREVERSE",
    ]
    _process_function_references(dax_expression, path_functions, seen, references)

    # SWITCH with dynamic column selection (common with parameters)
    _process_switch_function(dax_expression, seen, references)

    logger.debug(
        f"Found {len(references)} additional column references from specialized DAX functions"
    )
    return references


def extract_nested_calculate_expressions(
    dax_expression: str,
) -> List[CalculateExpression]:
    """
    Extract nested CALCULATE expressions along with their filter contexts.

    CALCULATE is one of the most powerful DAX functions that modifies filter context.
    It can be nested multiple times and each level can have multiple filter arguments.

    Syntax: CALCULATE(<expression>, <filter1>, <filter2>, ...)

    Args:
        dax_expression: The DAX expression to parse

    Returns:
        List of CalculateExpression objects with details and filter contexts
    """
    if not dax_expression or "CALCULATE" not in dax_expression.upper():
        return []

    calculate_calls: List[CalculateExpression] = []

    # Find all CALCULATE(...) patterns with proper nesting handling
    # This is a simplified approach - full parsing would require proper lexical analysis
    def find_calculate_with_context(
        expr: str, depth: int = 0
    ) -> List[CalculateExpression]:
        results: List[CalculateExpression] = []

        # Pattern to match CALCULATE or CALCULATETABLE
        pattern = r"(CALCULATE(?:TABLE)?)\s*\("

        for match in re.finditer(pattern, expr, re.IGNORECASE):
            func_name = match.group(1).upper()
            start_pos = match.end() - 1  # Position of opening parenthesis

            # Find matching closing parenthesis
            paren_count = 1
            pos = start_pos + 1
            content_start = pos

            while pos < len(expr) and paren_count > 0:
                if expr[pos] == "(":
                    paren_count += 1
                elif expr[pos] == ")":
                    paren_count -= 1
                pos += 1

            if paren_count == 0:
                # Successfully found matching parenthesis
                content = expr[content_start : pos - 1]

                # Split content by commas (simplified - doesn't handle nested functions perfectly)
                # First argument is the expression, rest are filters
                parts = []
                paren_level = 0
                current_part = []

                for char in content:
                    if char == "(":
                        paren_level += 1
                        current_part.append(char)
                    elif char == ")":
                        paren_level -= 1
                        current_part.append(char)
                    elif char == "," and paren_level == 0:
                        parts.append("".join(current_part).strip())
                        current_part = []
                    else:
                        current_part.append(char)

                if current_part:
                    parts.append("".join(current_part).strip())

                expression = parts[0] if parts else ""
                filters = parts[1:] if len(parts) > 1 else []

                # Extract table and column references from filters
                filter_refs: List[FilterReference] = []
                for filter_expr in filters:
                    refs = extract_table_column_references(filter_expr)
                    filter_refs.extend(
                        [
                            FilterReference(table=table, column=col)
                            for table, col in refs
                        ]
                    )

                calculate_expr = CalculateExpression(
                    function=func_name,  # type: ignore[arg-type]
                    nesting_depth=depth,
                    expression=expression,
                    filters=filters,
                    filter_references=filter_refs,
                    full_content=content[:200] + "..."
                    if len(content) > 200
                    else content,
                )

                results.append(calculate_expr)

                # Recursively look for nested CALCULATE in the expression
                nested = find_calculate_with_context(expression, depth + 1)
                results.extend(nested)

        return results

    calculate_calls = find_calculate_with_context(dax_expression, depth=0)

    logger.debug(
        f"Found {len(calculate_calls)} CALCULATE expressions (including nested)"
    )
    return calculate_calls


def extract_filter_context_modifiers(
    dax_expression: str,
) -> List[FilterContextModifier]:
    """
    Extract all filter context modifiers from a DAX expression.

    Filter context modifiers include:
    - CALCULATE, CALCULATETABLE
    - ALL, ALLEXCEPT, ALLSELECTED
    - FILTER
    - REMOVEFILTERS
    - KEEPFILTERS
    - USERELATIONSHIP

    Args:
        dax_expression: The DAX expression to parse

    Returns:
        List of FilterContextModifier objects with their targets
    """
    if not dax_expression:
        return []

    filter_modifiers: List[FilterContextModifier] = []

    # ALL and variants
    all_pattern = r"(ALL(?:EXCEPT|SELECTED)?)\s*\(\s*([^\)]+)\)"
    for match in re.finditer(all_pattern, dax_expression, re.IGNORECASE):
        func_name = match.group(1).upper()
        args = match.group(2)

        # Extract table/column references from arguments
        refs = extract_table_column_references(args)
        filter_refs = [FilterReference(table=table, column=col) for table, col in refs]

        filter_modifiers.append(
            FilterContextModifier(
                function=func_name,  # type: ignore[arg-type]
                modifier_type="remove_filter",
                arguments=args,
                references=filter_refs,
            )
        )

    # FILTER function - needs special handling for table and filter expressions
    filter_pattern = r"FILTER\s*\(\s*([^,]+),\s*([^\)]+)\)"
    for match in re.finditer(filter_pattern, dax_expression, re.IGNORECASE):
        table_expr = match.group(1).strip()
        filter_expr = match.group(2).strip()

        # Extract references from both table and filter expressions
        table_refs = extract_table_column_references(table_expr)
        filter_refs_raw = extract_table_column_references(filter_expr)

        # Combine all references
        all_refs = [
            FilterReference(table=table, column=col)
            for table, col in (table_refs + filter_refs_raw)
        ]

        filter_modifiers.append(
            FilterContextModifier(
                function="FILTER",
                modifier_type="apply_filter",
                table_expression=table_expr,
                filter_expression=filter_expr,
                references=all_refs,
            )
        )

    # REMOVEFILTERS
    remove_pattern = r"REMOVEFILTERS\s*\(\s*([^\)]+)\)"
    for match in re.finditer(remove_pattern, dax_expression, re.IGNORECASE):
        args = match.group(1)
        refs = extract_table_column_references(args)
        filter_refs = [FilterReference(table=table, column=col) for table, col in refs]

        filter_modifiers.append(
            FilterContextModifier(
                function="REMOVEFILTERS",
                modifier_type="remove_filter",
                arguments=args,
                references=filter_refs,
            )
        )

    # KEEPFILTERS
    keep_pattern = r"KEEPFILTERS\s*\(\s*([^\)]+)\)"
    for match in re.finditer(keep_pattern, dax_expression, re.IGNORECASE):
        args = match.group(1)
        refs = extract_table_column_references(args)
        filter_refs = [FilterReference(table=table, column=col) for table, col in refs]

        filter_modifiers.append(
            FilterContextModifier(
                function="KEEPFILTERS",
                modifier_type="keep_filter",
                arguments=args,
                references=filter_refs,
            )
        )

    # USERELATIONSHIP
    userel_pattern = r"USERELATIONSHIP\s*\(\s*([^,]+),\s*([^\)]+)\)"
    for match in re.finditer(userel_pattern, dax_expression, re.IGNORECASE):
        col1 = match.group(1).strip()
        col2 = match.group(2).strip()

        refs1 = extract_table_column_references(col1)
        refs2 = extract_table_column_references(col2)
        all_refs = [
            FilterReference(table=table, column=col) for table, col in (refs1 + refs2)
        ]

        filter_modifiers.append(
            FilterContextModifier(
                function="USERELATIONSHIP",
                modifier_type="modify_relationship",
                column1=col1,
                column2=col2,
                references=all_refs,
            )
        )

    logger.debug("Found %d filter context modifiers", len(filter_modifiers))
    return filter_modifiers


def parse_summarize_expression(dax_expression: str) -> Optional[SummarizeParseResult]:
    """
    Parse a SUMMARIZE DAX expression to extract source-to-target column mappings.

    SUMMARIZE syntax:
    SUMMARIZE(
        <table>,
        <groupBy_column>,           -- Direct mapping
        <groupBy_column>,
        "CalculatedName", <expression>  -- Expression-based mapping
    )

    Args:
        dax_expression: The DAX expression (should be a SUMMARIZE call)

    Returns:
        SummarizeParseResult with parsed mappings, or None if not a SUMMARIZE expression
    """
    if not dax_expression or "SUMMARIZE" not in dax_expression.upper():
        return None

    logger.debug("Parsing SUMMARIZE expression")

    # Match SUMMARIZE(...) and extract its contents
    match = _SUMMARIZE_FULL_PATTERN.search(dax_expression)
    if not match:
        return None

    content = match.group(1)

    # Split by commas, but be careful of nested parentheses
    # This is a simplified parser - production would need proper parsing
    parts = []
    current_part = ""
    paren_depth = 0
    bracket_depth = 0
    in_string = False

    for char in content:
        if char == '"' and not in_string:
            in_string = True
        elif char == '"' and in_string:
            in_string = False
        elif char == "(" and not in_string:
            paren_depth += 1
        elif char == ")" and not in_string:
            paren_depth -= 1
        elif char == "[" and not in_string:
            bracket_depth += 1
        elif char == "]" and not in_string:
            bracket_depth -= 1
        elif char == "," and paren_depth == 0 and bracket_depth == 0 and not in_string:
            parts.append(current_part.strip())
            current_part = ""
            continue

        current_part += char

    if current_part.strip():
        parts.append(current_part.strip())

    if len(parts) < 2:
        return None

    # First part is the table name
    table_name = parts[0].strip("'\"")

    direct_mappings = []
    calculated_mappings = []

    # Process remaining parts
    i = 1
    while i < len(parts):
        part = parts[i].strip()

        # Check if this is a calculated column (starts with a string literal)
        if part.startswith('"'):
            # This is a calculated column: "Name", expression
            target_name = part.strip('"')
            if i + 1 < len(parts):
                expression = parts[i + 1].strip()
                calculated_mappings.append(
                    CalculatedColumnMapping(
                        target_column=target_name, expression=expression
                    )
                )
                i += 2
            else:
                i += 1
        else:
            # This is a group-by column: 'Table'[Column] or Table[Column]
            # Extract table and column name
            table_col_match = _TABLE_COLUMN_IN_SUMMARIZE_PATTERN.search(part)
            if table_col_match:
                source_table = table_col_match.group(1) or table_col_match.group(2)
                source_column = table_col_match.group(3)
                # For group-by columns, target name = source column name
                direct_mappings.append(
                    DirectColumnMapping(
                        source_table=source_table,
                        source_column=source_column,
                        target_column=source_column,
                    )
                )
            i += 1

    logger.debug(
        f"SUMMARIZE parsed: {len(direct_mappings)} direct mappings, {len(calculated_mappings)} calculated"
    )

    return SummarizeParseResult(
        source_table=table_name,
        direct_mappings=direct_mappings,
        calculated_mappings=calculated_mappings,
    )


def build_measure_dependency_graph(measures: Dict[str, str]) -> Dict[str, List[str]]:
    """
    Build a dependency graph showing which measures reference other measures.
    This is useful for understanding complex measure hierarchies.

    Args:
        measures: Dictionary mapping measure names to their DAX expressions

    Returns:
        Dictionary mapping each measure to list of measures it depends on
    """
    dependency_graph: Dict[str, List[str]] = {}
    measure_names = set(measures.keys())

    for measure_name, expression in measures.items():
        # Extract measure dependencies from this measure's expression
        deps = extract_measure_dependencies(expression, measure_names)
        dependency_graph[measure_name] = deps

        if deps:
            logger.debug("Measure '%s' depends on: %s", measure_name, ", ".join(deps))

    return dependency_graph
