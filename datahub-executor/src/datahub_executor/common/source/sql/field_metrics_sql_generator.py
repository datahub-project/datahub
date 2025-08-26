import logging
from typing import Optional

from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.types import FieldMetricType, SchemaFieldSpec

logger = logging.getLogger(__name__)


class FieldMetricsSQLGenerator:
    source_name: str

    def _append_filters(
        self,
        query_string: str,
        filter_sql_fragment: Optional[str],
        last_checked_sql_fragment: Optional[str],
    ) -> str:
        if filter_sql_fragment is not None:
            if "WHERE" in query_string:
                query_string = f"{query_string} AND {filter_sql_fragment}"
            else:
                query_string = f"{query_string} WHERE {filter_sql_fragment}"

        if last_checked_sql_fragment is not None:
            if "WHERE" in query_string:
                query_string = f"{query_string} AND {last_checked_sql_fragment}"
            else:
                query_string = f"{query_string} WHERE {last_checked_sql_fragment}"
        return query_string

    def _setup_metric_query_unique_count(
        self, field_path: str, database_string: str
    ) -> str:
        return f"SELECT COUNT(DISTINCT {field_path}) FROM {database_string}"

    def _setup_metric_query_unique_percentage(
        self,
        field_path: str,
        database_string: str,
    ) -> str:
        return f"SELECT COUNT(DISTINCT {field_path}) * 100.0 / COUNT(*) FROM {database_string}"

    def _setup_metric_query_null_count(
        self, field_path: str, database_string: str
    ) -> str:
        return f"SELECT COUNT(*) FROM {database_string} WHERE {field_path} IS NULL"

    def _setup_metric_query_null_percentage(
        self,
        field_path: str,
        database_string: str,
        filter_sql_fragment: Optional[str],
        last_checked_sql_fragment: Optional[str],
    ) -> str:
        sub_query_string = self._append_filters(
            f"SELECT COUNT(*) FROM {database_string} WHERE {field_path} IS NULL",
            filter_sql_fragment,
            last_checked_sql_fragment,
        )
        query_string = self._append_filters(
            f"SELECT (<SUBQUERY>) * 100.0 / COUNT(*) FROM {database_string}",
            filter_sql_fragment,
            last_checked_sql_fragment,
        )
        return query_string.replace("<SUBQUERY>", sub_query_string)

    def _setup_metric_query_min(self, field_path: str, database_string: str) -> str:
        return f"SELECT MIN({field_path}) FROM {database_string}"

    def _setup_metric_query_max(self, field_path: str, database_string: str) -> str:
        return f"SELECT MAX({field_path}) FROM {database_string}"

    def _setup_metric_query_mean(self, field_path: str, database_string: str) -> str:
        return f"SELECT AVG({field_path}) FROM {database_string}"

    def _setup_metric_query_median(self, field_path: str, database_string: str) -> str:
        raise NotImplementedError()

    def _setup_metric_query_stddev(self, field_path: str, database_string: str) -> str:
        return f"SELECT STDDEV({field_path}) FROM {database_string}"

    def _setup_metric_query_negative_count(
        self, field_path: str, database_string: str
    ) -> str:
        return f"SELECT COUNT(*) FROM {database_string} WHERE {field_path} < 0"

    def _setup_metric_query_negative_percentage(
        self,
        field_path: str,
        database_string: str,
        filter_sql_fragment: Optional[str],
        last_checked_sql_fragment: Optional[str],
    ) -> str:
        sub_query_string = self._append_filters(
            f"SELECT COUNT(*) FROM {database_string} WHERE {field_path} < 0",
            filter_sql_fragment,
            last_checked_sql_fragment,
        )
        query_string = self._append_filters(
            f"SELECT (<SUBQUERY>) * 100.0 / COUNT(*) FROM {database_string}",
            filter_sql_fragment,
            last_checked_sql_fragment,
        )
        return query_string.replace("<SUBQUERY>", sub_query_string)

    def _setup_metric_query_zero_count(
        self, field_path: str, database_string: str
    ) -> str:
        return f"SELECT COUNT(*) FROM {database_string} WHERE {field_path} = 0"

    def _setup_metric_query_zero_percentage(
        self,
        field_path: str,
        database_string: str,
        filter_sql_fragment: Optional[str],
        last_checked_sql_fragment: Optional[str],
    ) -> str:
        sub_query_string = self._append_filters(
            f"SELECT COUNT(*) FROM {database_string} WHERE {field_path} = 0",
            filter_sql_fragment,
            last_checked_sql_fragment,
        )
        query_string = self._append_filters(
            f"SELECT (<SUBQUERY>) * 100.0 / COUNT(*) FROM {database_string}",
            filter_sql_fragment,
            last_checked_sql_fragment,
        )
        return query_string.replace("<SUBQUERY>", sub_query_string)

    def _setup_metric_query_min_length(
        self, field_path: str, database_string: str
    ) -> str:
        return f"SELECT MIN(LENGTH({field_path})) FROM {database_string}"

    def _setup_metric_query_max_length(
        self, field_path: str, database_string: str
    ) -> str:
        return f"SELECT MAX(LENGTH({field_path})) FROM {database_string}"

    def _setup_metric_query_empty_count(
        self,
        field_path: str,
        database_string: str,
    ) -> str:
        return f"SELECT COUNT(*) FROM {database_string} WHERE {field_path} IS NOT NULL AND TRIM({field_path}) = ''"

    def _setup_metric_query_empty_percentage(
        self,
        field_path: str,
        database_string: str,
        filter_sql_fragment: Optional[str],
        last_checked_sql_fragment: Optional[str],
    ) -> str:
        sub_query_string = self._append_filters(
            f"SELECT COUNT(*) FROM {database_string} WHERE {field_path} IS NOT NULL AND TRIM({field_path}) = ''",
            filter_sql_fragment,
            last_checked_sql_fragment,
        )
        query_string = self._append_filters(
            f"SELECT (<SUBQUERY>) * 100.0 / COUNT(*) FROM {database_string}",
            filter_sql_fragment,
            last_checked_sql_fragment,
        )
        return query_string.replace("<SUBQUERY>", sub_query_string)

    def _setup_query(
        self,
        database_string: str,
        field: SchemaFieldSpec,
        metric: FieldMetricType,
        filter_sql_fragment: Optional[str],
        last_checked_sql_fragment: Optional[str],
    ) -> str:
        if metric == FieldMetricType.UNIQUE_COUNT:
            return self._append_filters(
                self._setup_metric_query_unique_count(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.UNIQUE_PERCENTAGE:
            return self._append_filters(
                self._setup_metric_query_unique_percentage(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.NULL_COUNT:
            return self._append_filters(
                self._setup_metric_query_null_count(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.NULL_PERCENTAGE:
            return self._setup_metric_query_null_percentage(
                field.path,
                database_string,
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.MIN:
            return self._append_filters(
                self._setup_metric_query_min(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.MAX:
            return self._append_filters(
                self._setup_metric_query_max(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.MEAN:
            return self._append_filters(
                self._setup_metric_query_mean(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.MEDIAN:
            return self._append_filters(
                self._setup_metric_query_median(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.STDDEV:
            return self._append_filters(
                self._setup_metric_query_stddev(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.NEGATIVE_COUNT:
            return self._append_filters(
                self._setup_metric_query_negative_count(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.NEGATIVE_PERCENTAGE:
            return self._setup_metric_query_negative_percentage(
                field.path,
                database_string,
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.ZERO_COUNT:
            return self._append_filters(
                self._setup_metric_query_zero_count(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.ZERO_PERCENTAGE:
            return self._setup_metric_query_zero_percentage(
                field.path,
                database_string,
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.MIN_LENGTH:
            return self._append_filters(
                self._setup_metric_query_min_length(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.MAX_LENGTH:
            return self._append_filters(
                self._setup_metric_query_max_length(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.EMPTY_COUNT:
            return self._append_filters(
                self._setup_metric_query_empty_count(field.path, database_string),
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        if metric == FieldMetricType.EMPTY_PERCENTAGE:
            return self._setup_metric_query_empty_percentage(
                field.path,
                database_string,
                filter_sql_fragment,
                last_checked_sql_fragment,
            )

        raise InvalidParametersException(
            message=f"Failed to evaluate FIELD assertion, invalid metric type {metric.value}",
            parameters={},
        )

    def setup_query(
        self,
        database_string: str,
        field: SchemaFieldSpec,
        metric: FieldMetricType,
        filter_sql_fragment: Optional[str],
        last_checked_sql_fragment: Optional[str],
    ) -> str:
        query = self._setup_query(
            database_string,
            field,
            metric,
            filter_sql_fragment,
            last_checked_sql_fragment,
        )
        logger.debug(query)
        return query
