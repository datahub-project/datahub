import logging
import re
from typing import Any, Dict, Optional

from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    apply_runtime_parameters,
)
from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.types import DatasetFilterType

logger = logging.getLogger(__name__)

# Type aliases for clarity
FilterParameters = Dict[
    str, Any
]  # Contains keys: "type" (DatasetFilterType), "sql" (str)
RuntimeParameters = Dict[str, Any]  # Runtime template variables for ${var} substitution


class FilterBuilder:
    """
    Used to convert Dataset Filter objects into formatted SQL strings
    """

    def __init__(
        self,
        filter_params: Optional[FilterParameters] = None,
        runtime_parameters: Optional[RuntimeParameters] = None,
    ):
        self._filter_params = filter_params if filter_params is not None else {}
        self._runtime_parameters = (
            runtime_parameters if runtime_parameters is not None else {}
        )
        self._raw_filter_string = self._build_filter_string()
        self._cleaned_filter_string = self._clean_filter_string()

    def _build_sql_filter_string(self) -> str:
        sql = self._filter_params.get("sql")

        if not sql:
            raise InvalidParametersException(
                message=f"Invalid SQL filter {sql} provided.",
                parameters=self._filter_params,
            )

        # Optionally apply runtime parameter substitution before cleaning.
        return apply_runtime_parameters(sql, self._runtime_parameters)

    def _build_filter_string(self) -> str:
        type = self._filter_params.get("type")

        if type:
            if type == DatasetFilterType.SQL:
                return self._build_sql_filter_string()
            raise InvalidParametersException(
                message=f"Unsupported filter type {type} provided.",
                parameters=self._filter_params,
            )
        else:
            return ""

    def _clean_filter_string(self) -> str:
        # TODO: Future enhancement - replace string manipulation with SQLAlchemy expressions
        # or a custom solution similar to the Python SDK's filter compilation approach
        # (see metadata-ingestion/src/datahub/sdk/search_filters.py).
        # This would be more robust than regex-based string manipulation.
        # Reference: https://github.com/acryldata/datahub-fork/pull/7505#discussion_r1905879364

        # Remove "where" keyword if included in filter
        cleaned_str, num_where_subs = re.subn(
            pattern=r"^\s*(WHERE)+\s+",
            string=self._raw_filter_string,
            repl="",
            flags=re.IGNORECASE,
        )

        # Remove "and" keyword(s) if included at the start of the filter
        # Pattern matches one or more "AND " sequences (handles "AND AND x = value")
        cleaned_str, num_and_subs = re.subn(
            pattern=r"^\s*(AND\s+)+",
            string=cleaned_str,
            repl="",
            flags=re.IGNORECASE,
        )

        # Remove any trailing semicolons included in filter
        cleaned_str, num_suffix_subs = re.subn(
            pattern=r";+\s*$", string=cleaned_str, repl=""
        )

        # Remove any leading or trailing whitespace
        result = cleaned_str.strip()

        num_total_subs = num_where_subs + num_and_subs + num_suffix_subs
        if num_total_subs > 0:
            logger.info(
                f"Formatted SQL filter. Replaced {self._raw_filter_string} with {result}"
            )

        return result

    def get_sql(self) -> str:
        return self._cleaned_filter_string
