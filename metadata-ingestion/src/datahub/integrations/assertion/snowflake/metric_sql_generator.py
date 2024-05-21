from dataclasses import dataclass
from functools import singledispatchmethod

from datahub.api.entities.assertion.assertion import BaseEntityAssertion
from datahub.api.entities.assertion.field_assertion import (
    FieldMetricAssertion,
    FieldValuesAssertion,
)
from datahub.api.entities.assertion.sql_assertion import (
    SqlMetricAssertion,
    SqlMetricChangeAssertion,
)
from datahub.api.entities.assertion.volume_assertion import (
    RowCountChangeVolumeAssertion,
    RowCountTotalVolumeAssertion,
)
from datahub.integrations.assertion.common import get_entity_name
from datahub.integrations.assertion.snowflake.field_metric_sql_generator import (
    SnowflakeFieldMetricSQLGenerator,
)
from datahub.integrations.assertion.snowflake.field_values_metric_sql_generator import (
    SnowflakeFieldValuesMetricSQLGenerator,
)


@dataclass
class SnowflakeMetricSQLGenerator:
    field_metric_sql_generator: SnowflakeFieldMetricSQLGenerator
    field_values_metric_sql_generator: SnowflakeFieldValuesMetricSQLGenerator

    @singledispatchmethod
    def metric_sql(
        self,
        assertion: BaseEntityAssertion,
    ) -> str:
        raise ValueError(f"Unsupported assertion type {type(assertion)} ")

    @metric_sql.register
    def _(self, assertion: RowCountChangeVolumeAssertion) -> str:
        raise ValueError(f"Unsupported assertion type {type(assertion)} ")

    @metric_sql.register
    def _(self, assertion: SqlMetricChangeAssertion) -> str:
        raise ValueError(f"Unsupported assertion type {type(assertion)} ")

    @metric_sql.register
    def _(self, assertion: RowCountTotalVolumeAssertion) -> str:

        # Can not use information schema here due to error -
        # Data metric function body cannot refer to the non-deterministic function 'CURRENT_DATABASE_MAIN_METASTORE_ID'.

        entity_name = ".".join(get_entity_name(assertion))
        if assertion.filter and assertion.filter.sql:
            where_clause = f"where {assertion.filter.sql}"
        else:
            where_clause = ""
        return f"select count(*) as metric from {entity_name} {where_clause}"

    @metric_sql.register
    def _(self, assertion: SqlMetricAssertion) -> str:
        return f"select $1 as metric from ({assertion.sql})"

    @metric_sql.register
    def _(self, assertion: FieldMetricAssertion) -> str:
        sql = self.field_metric_sql_generator.metric_sql(assertion)
        return f"select $1 as metric from ({sql})"

    @metric_sql.register
    def _(self, assertion: FieldValuesAssertion) -> str:
        return self.field_values_metric_sql_generator.metric_sql(assertion)
