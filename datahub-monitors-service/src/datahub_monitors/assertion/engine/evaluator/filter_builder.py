import logging
import re
from typing import Optional

from datahub_monitors.exceptions import InvalidParametersException
from datahub_monitors.types import DatasetFilterType

logger = logging.getLogger(__name__)


class FilterBuilder:
    """
    Used to convert Dataset Filter objects into formatted SQL strings
    """

    def __init__(self, filter_params: Optional[dict]):
        self._filter_params = filter_params if filter_params is not None else {}
        self._raw_filter_string = self._build_filter_string()
        self._cleaned_filter_string = self._clean_filter_string()

    def _build_sql_filter_string(self) -> str:
        sql = self._filter_params.get("sql")

        if not sql:
            raise InvalidParametersException(
                message=f"Invalid SQL filter {sql} provided.",
                parameters=self._filter_params,
            )

        return sql

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
        # Remove "where" keyword if included in filter
        cleaned_str, num_prefix_subs = re.subn(
            pattern="^\s*(WHERE)*\s*",
            string=self._raw_filter_string,
            repl="",
            flags=re.IGNORECASE,
        )

        # Remove any trailing semicolons included in filter
        cleaned_str, num_suffix_subs = re.subn(
            pattern=";*\s*$", string=cleaned_str, repl=""
        )

        if num_prefix_subs + num_suffix_subs > 0:
            logger.info(
                f"Formatted SQL filter. Replaced {self._raw_filter_string} with {cleaned_str}"
            )

        return cleaned_str

    def get_sql(self) -> str:
        return self._cleaned_filter_string
