from functools import singledispatchmethod
from typing import List, Optional

from datahub.api.entities.assertion.assertion_operator import (
    BetweenOperator,
    EqualToOperator,
    GreaterThanOperator,
    GreaterThanOrEqualToOperator,
    LessThanOperator,
    LessThanOrEqualToOperator,
    NotNullOperator,
    Operators,
)
from datahub.api.entities.assertion.field_assertion import (
    FieldTransform,
    FieldValuesAssertion,
)
from datahub.integrations.assertion.common import get_entity_name


class SnowflakeFieldValuesMetricSQLGenerator:
    @singledispatchmethod
    def values_metric_sql(
        self,
        operators: Operators,
        entity_name: str,
        transformed_field: str,
        where_clause: str,
    ) -> str:
        raise ValueError(f"Unsupported values metric operator type {type(operators)} ")

    @values_metric_sql.register
    def _(
        self,
        operators: EqualToOperator,
        entity_name: str,
        transformed_field: str,
        where_clause: str,
    ) -> str:
        return f"""select case when {transformed_field}=={operators.value} then 0 else 1
        from {entity_name} {where_clause}"""

    @values_metric_sql.register
    def _(
        self,
        operators: BetweenOperator,
        entity_name: str,
        transformed_field: str,
        where_clause: str,
    ) -> str:
        return f"""select case when {transformed_field} between {operators.min} and {operators.max} then 0 else 1 end
        from {entity_name} {where_clause}"""

    @values_metric_sql.register
    def _(
        self,
        operators: LessThanOperator,
        entity_name: str,
        transformed_field: str,
        where_clause: str,
    ) -> str:
        return f"""select case when {transformed_field} < {operators.value} then 0 else 1 end
        from {entity_name} {where_clause}"""

    @values_metric_sql.register
    def _(
        self,
        operators: LessThanOrEqualToOperator,
        entity_name: str,
        transformed_field: str,
        where_clause: str,
    ) -> str:
        return f"""select case when {transformed_field} <= {operators.value} then 0 else 1 end
        from {entity_name} {where_clause}"""

    @values_metric_sql.register
    def _(
        self,
        operators: GreaterThanOperator,
        entity_name: str,
        transformed_field: str,
        where_clause: str,
    ) -> str:
        return f"""select case when {transformed_field} > {operators.value} then 0 else 1 end
        from {entity_name} {where_clause}"""

    @values_metric_sql.register
    def _(
        self,
        operators: GreaterThanOrEqualToOperator,
        entity_name: str,
        transformed_field: str,
        where_clause: str,
    ) -> str:
        return f"""select case when {transformed_field} >= {operators.value} then 0 else 1 end
        from {entity_name} {where_clause}"""

    @values_metric_sql.register
    def _(
        self,
        operators: NotNullOperator,
        entity_name: str,
        transformed_field: str,
        where_clause: str,
    ) -> str:
        return f"""select case when {transformed_field} is not null then 0 else 1 end
        from {entity_name} {where_clause}"""

    def _setup_where_clause(self, filters: List[Optional[str]]) -> str:
        where_clause = " and ".join(f for f in filters if f)
        return f"where {where_clause}" if where_clause else ""

    def _setup_field_transform(
        self, field: str, transform: Optional[FieldTransform]
    ) -> str:
        if transform is None:
            return field
        elif transform is FieldTransform.LENGTH:
            return f"length({field})"
        raise ValueError(f"Unsupported transform type {transform}")

    def metric_sql(self, assertion: FieldValuesAssertion) -> str:
        """
        Note that this applies negative operator in order to check whether or not
        number of invalid value rows are less than configured failThreshold.

        Args:
            assertion (FieldValuesAssertion): _description_

        Returns:
            str: _description_
        """
        entity_name = ".".join(get_entity_name(assertion))

        dataset_filter = (
            assertion.filter.sql if assertion.filter and assertion.filter.sql else None
        )
        where_clause = self._setup_where_clause(
            [
                dataset_filter,
                f"{assertion.field} is not null" if assertion.exclude_nulls else None,
            ]
        )
        transformed_field = self._setup_field_transform(
            assertion.field, assertion.field_transform
        )
        # this sql would return boolean value for each table row.  1 if fail and 0 if pass.
        sql = self.values_metric_sql(
            assertion.operator, entity_name, transformed_field, where_clause
        )

        # metric would be number of failing rows OR percentage of failing rows.
        if assertion.fail_threshold.type == "count":
            return f"select sum($1) as metric from ({sql})"
        else:  # percentage
            return f"select sum($1)/count(*) as metric from ({sql})"
