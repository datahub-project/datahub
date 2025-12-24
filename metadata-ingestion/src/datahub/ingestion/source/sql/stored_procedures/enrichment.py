import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


def fetch_parameters_metadata(
    conn: Connection,
    query: str,
    params: Dict[str, Any],
    format_template: Optional[str] = None,
) -> Optional[str]:
    """
    Fetch and format parameter/argument metadata. Returns formatted string or None.
    format_template: Optional format string, e.g. "{in_out} {argument_name} {data_type}"
    """
    try:
        result = conn.execute(text(query), params)
        rows = list(result)

        if not rows:
            return None

        formatted_params = []
        for row in rows:
            if format_template:
                row_dict = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
                formatted_params.append(format_template.format(**row_dict))
            else:
                formatted_params.append(
                    " ".join(str(val) for val in row if val is not None)
                )

        return ", ".join(formatted_params) if formatted_params else None

    except Exception as e:
        logger.warning(f"Failed to fetch parameters metadata: {e}")
        return None


def fetch_dependencies_metadata(
    conn: Connection,
    upstream_query: Optional[str],
    downstream_query: Optional[str],
    params: Dict[str, Any],
    upstream_format: Optional[str] = None,
    downstream_format: Optional[str] = None,
) -> Optional[Dict[str, List[str]]]:
    """
    Fetch upstream/downstream dependencies. Returns dict with "upstream"/"downstream" keys or None.
    upstream_format/downstream_format: Optional format strings, e.g. "{owner}.{name} ({type})"
    """
    dependencies: Dict[str, List[str]] = {}

    if upstream_query:
        try:
            result = conn.execute(text(upstream_query), params)
            rows = list(result)

            if rows:
                upstream = []
                for row in rows:
                    if upstream_format:
                        row_dict = (
                            dict(row._mapping)
                            if hasattr(row, "_mapping")
                            else dict(row)
                        )
                        upstream.append(upstream_format.format(**row_dict))
                    else:
                        upstream.append(
                            ".".join(str(val) for val in row if val is not None)
                        )
                dependencies["upstream"] = upstream

        except Exception as e:
            logger.warning(f"Failed to fetch upstream dependencies: {e}")

    if downstream_query:
        try:
            result = conn.execute(text(downstream_query), params)
            rows = list(result)

            if rows:
                downstream = []
                for row in rows:
                    if downstream_format:
                        row_dict = (
                            dict(row._mapping)
                            if hasattr(row, "_mapping")
                            else dict(row)
                        )
                        downstream.append(downstream_format.format(**row_dict))
                    else:
                        downstream.append(
                            ".".join(str(val) for val in row if val is not None)
                        )
                dependencies["downstream"] = downstream

        except Exception as e:
            logger.warning(f"Failed to fetch downstream dependencies: {e}")

    return dependencies if dependencies else None


def fetch_multiline_source_code(
    conn: Connection,
    query: str,
    params: Dict[str, Any],
    text_column: str = "text",
    line_column: Optional[str] = "line",
) -> Optional[str]:
    """Fetch source code stored across multiple rows. Concatenates rows in order."""
    try:
        result = conn.execute(text(query), params)
        rows = list(result)

        if not rows:
            return None

        source_lines = []
        for row in rows:
            try:
                text_val = getattr(row, text_column, None)
                if text_val is not None:
                    source_lines.append(text_val)
            except AttributeError:
                row_dict = dict(row)
                text_val = row_dict.get(text_column)
                if text_val is not None:
                    source_lines.append(text_val)

        return "".join(source_lines) if source_lines else None

    except Exception as e:
        logger.warning(f"Failed to fetch multiline source code: {e}")
        return None


def fetch_single_value(
    conn: Connection,
    query: str,
    params: Dict[str, Any],
) -> Optional[Any]:
    """Fetch a single value from a query. Returns the first column of first row or None."""
    try:
        result = conn.execute(text(query), params)
        row = result.fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.warning(f"Failed to fetch single value: {e}")
        return None
