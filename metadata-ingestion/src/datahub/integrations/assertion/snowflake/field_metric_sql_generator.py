from typing import List, Optional

from datahub.api.entities.assertion.field_assertion import FieldMetricAssertion
from datahub.api.entities.assertion.field_metric import FieldMetric
from datahub.integrations.assertion.common import get_entity_name


class SnowflakeFieldMetricSQLGenerator:
    def unique_count_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select count(distinct {field_name})
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def unique_percentage_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select count(distinct {field_name})/count(*)
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def null_count_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        where_clause = self._setup_where_clause(
            [dataset_filter, f"{field_name} is null"]
        )
        return f"""select count(*)
        from {entity_name} {where_clause}"""

    def null_percentage_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select ({self.null_count_sql(field_name, entity_name, dataset_filter)})/count(*)
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def min_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select min({field_name})
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def max_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select max({field_name})
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def mean_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select avg({field_name})
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def median_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select median({field_name})
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def stddev_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select stddev({field_name})
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def negative_count_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        where_clause = self._setup_where_clause([dataset_filter, f"{field_name} < 0"])
        return f"""select count(*)
        from {entity_name} {where_clause}"""

    def negative_percentage_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select ({self.negative_count_sql(field_name, entity_name, dataset_filter)})/count(*)
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def zero_count_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        where_clause = self._setup_where_clause([dataset_filter, f"{field_name} = 0"])
        return f"""select count(*)
        from {entity_name} {where_clause}"""

    def zero_percentage_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select ({self.zero_count_sql(field_name, entity_name, dataset_filter)})/count(*)
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def min_length_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select min(length({field_name}))
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def max_length_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select max(length({field_name}))
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def empty_count_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        where_clause = self._setup_where_clause(
            [dataset_filter, f"({field_name} is null or trim({field_name})='')"]
        )
        return f"""select count(*)
        from {entity_name} {where_clause}"""

    def empty_percentage_sql(
        self, field_name: str, entity_name: str, dataset_filter: Optional[str]
    ) -> str:
        return f"""select ({self.empty_count_sql(field_name, entity_name, dataset_filter)})/count(*)
        from {entity_name} {self._setup_where_clause([dataset_filter])}"""

    def _setup_where_clause(self, filters: List[Optional[str]]) -> str:
        where_clause = " and ".join(f for f in filters if f)
        return f"where {where_clause}" if where_clause else ""

    def metric_sql(self, assertion: FieldMetricAssertion) -> str:
        metric_sql_mapping = {
            FieldMetric.UNIQUE_COUNT: self.unique_count_sql,
            FieldMetric.UNIQUE_PERCENTAGE: self.unique_percentage_sql,
            FieldMetric.NULL_COUNT: self.null_count_sql,
            FieldMetric.NULL_PERCENTAGE: self.null_percentage_sql,
            FieldMetric.MIN: self.min_sql,
            FieldMetric.MAX: self.max_sql,
            FieldMetric.MEAN: self.mean_sql,
            FieldMetric.MEDIAN: self.median_sql,
            FieldMetric.STDDEV: self.stddev_sql,
            FieldMetric.NEGATIVE_COUNT: self.negative_count_sql,
            FieldMetric.NEGATIVE_PERCENTAGE: self.negative_percentage_sql,
            FieldMetric.ZERO_COUNT: self.zero_count_sql,
            FieldMetric.ZERO_PERCENTAGE: self.zero_percentage_sql,
            FieldMetric.MIN_LENGTH: self.min_length_sql,
            FieldMetric.MAX_LENGTH: self.max_length_sql,
            FieldMetric.EMPTY_COUNT: self.empty_count_sql,
            FieldMetric.EMPTY_PERCENTAGE: self.empty_percentage_sql,
        }

        entity_name = ".".join(get_entity_name(assertion))

        return metric_sql_mapping[assertion.metric](
            assertion.field,
            entity_name,
            (
                assertion.filters.sql
                if assertion.filters and assertion.filters.sql
                else None
            ),
        )
