import logging
import re
from typing import List, Tuple, Set, Dict, Optional

logger = logging.getLogger(__name__)


def extract_table_column_references(dax_expression: str) -> List[Tuple[str, str]]:
    """
    Parse DAX expression and extract table.column references.
    
    This function extracts table and column references from DAX expressions using
    regex patterns. It handles common DAX patterns including:
    - 'TableName'[ColumnName] or TableName[ColumnName]
    - RELATED(TableName[ColumnName])
    - Table references in various DAX functions
    
    Args:
        dax_expression: The DAX expression to parse
        
    Returns:
        List of (table_name, column_name) tuples
    """
    if not dax_expression:
        return []
    
    logger.debug(f"Parsing DAX expression: {dax_expression[:100]}...")
    
    references: Set[Tuple[str, str]] = set()
    
    # Pattern 1: 'TableName'[ColumnName] (with single quotes around table name)
    # Matches: 'Sales'[Amount], 'Order Details'[Price]
    pattern1 = r"'([^']+)'\s*\[\s*([^\]]+)\s*\]"
    matches1 = re.findall(pattern1, dax_expression)
    references.update(matches1)
    
    # Pattern 2: TableName[ColumnName] (without quotes)
    # Matches: Sales[Amount], OrderDetails[Price]
    # But not things like [ColumnOnly] (no table)
    # Need to be careful not to match measure references without table
    pattern2 = r"(?<!['\"])\b([A-Za-z_][A-Za-z0-9_]*)\s*\[\s*([^\]]+)\s*\]"
    matches2 = re.findall(pattern2, dax_expression)
    
    # Filter out common DAX functions that might match this pattern
    dax_functions = {
        'CALCULATE', 'SUM', 'SUMX', 'AVERAGE', 'AVERAGEX', 'COUNT', 'COUNTX',
        'MAX', 'MAXX', 'MIN', 'MINX', 'IF', 'SWITCH', 'RELATED', 'RELATEDTABLE',
        'FILTER', 'ALL', 'ALLEXCEPT', 'VALUES', 'DISTINCT', 'SELECTEDVALUE',
        'DIVIDE', 'FORMAT', 'DATEDIFF', 'DATEADD', 'DATESBETWEEN', 'YEAR',
        'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'DATE', 'TIME', 'NOW',
        'TODAY', 'EARLIER', 'EARLIEST', 'HASONEVALUE', 'ISBLANK', 'ISNUMBER',
        'ISTEXT', 'CONTAINS', 'SEARCH', 'FIND', 'LEN', 'LEFT', 'RIGHT', 'MID',
        'UPPER', 'LOWER', 'CONCATENATE', 'TRIM', 'SUBSTITUTE', 'REPLACE'
    }
    
    for table, column in matches2:
        # Skip if the "table" is actually a DAX function
        if table.upper() not in dax_functions:
            references.add((table, column))
    
    # Pattern 3: RELATED(TableName[ColumnName])
    # This is more specific and reliable
    pattern3 = r"RELATED\s*\(\s*(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*\[\s*([^\]]+)\s*\]"
    matches3 = re.findall(pattern3, dax_expression, re.IGNORECASE)
    for match in matches3:
        table = match[0] if match[0] else match[1]
        column = match[2]
        references.add((table, column))
    
    # Pattern 4: RELATEDTABLE(TableName)
    # This references a whole table
    pattern4 = r"RELATEDTABLE\s*\(\s*(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*\)"
    matches4 = re.findall(pattern4, dax_expression, re.IGNORECASE)
    for match in matches4:
        table = match[0] if match[0] else match[1]
        # For table references without a specific column, use empty string as column
        references.add((table, ""))
    
    # Pattern 5: Table references in iterators like SUMX, FILTER, etc.
    # SUMX(TableName, ...) or FILTER(TableName, ...)
    # These reference tables without specific columns initially
    pattern5 = r"(?:SUMX|AVERAGEX|COUNTX|MAXX|MINX|FILTER|ALL|ALLEXCEPT|VALUES|DISTINCT)\s*\(\s*(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*[,)]"
    matches5 = re.findall(pattern5, dax_expression, re.IGNORECASE)
    for match in matches5:
        table = match[0] if match[0] else match[1]
        # For table references without a specific column, use empty string as column
        references.add((table, ""))
    
    # Convert set to sorted list for consistent output
    result = sorted(list(references))
    
    logger.debug(f"Extracted {len(result)} table/column references from DAX expression")
    
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
    tables = set(table for table, _ in references if table)
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
        column for table, column in references 
        if table == table_name and column
    ]
    return sorted(list(set(columns)))


def parse_summarize_expression(dax_expression: str) -> Optional[Dict[str, List[Tuple[str, str]]]]:
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
        Dictionary with:
        - 'direct_mappings': List of (source_table, source_column, target_column) for group-by columns
        - 'calculated_mappings': List of (target_column, expression) for calculated columns
    """
    if not dax_expression or 'SUMMARIZE' not in dax_expression.upper():
        return None
    
    logger.debug("Parsing SUMMARIZE expression")
    
    # Match SUMMARIZE(...) and extract its contents
    pattern = r'SUMMARIZE\s*\((.*)\)'
    match = re.search(pattern, dax_expression, re.IGNORECASE | re.DOTALL)
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
        elif char == '(' and not in_string:
            paren_depth += 1
        elif char == ')' and not in_string:
            paren_depth -= 1
        elif char == '[' and not in_string:
            bracket_depth += 1
        elif char == ']' and not in_string:
            bracket_depth -= 1
        elif char == ',' and paren_depth == 0 and bracket_depth == 0 and not in_string:
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
                calculated_mappings.append((target_name, expression))
                i += 2
            else:
                i += 1
        else:
            # This is a group-by column: 'Table'[Column] or Table[Column]
            # Extract table and column name
            table_col_match = re.search(r"(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*\[\s*([^\]]+)\s*\]", part)
            if table_col_match:
                source_table = table_col_match.group(1) or table_col_match.group(2)
                source_column = table_col_match.group(3)
                # For group-by columns, target name = source column name
                direct_mappings.append((source_table, source_column, source_column))
            i += 1
    
    logger.debug(f"SUMMARIZE parsed: {len(direct_mappings)} direct mappings, {len(calculated_mappings)} calculated")
    
    return {
        'direct_mappings': direct_mappings,
        'calculated_mappings': calculated_mappings,
        'source_table': table_name
    }

