from functools import singledispatchmethod

from datahub.api.entities.assertion.assertion_operator import (
    BetweenOperator,
    EqualToOperator,
    GreaterThanOperator,
    GreaterThanOrEqualToOperator,
    IsFalseOperator,
    IsNullOperator,
    IsTrueOperator,
    LessThanOperator,
    LessThanOrEqualToOperator,
    NotNullOperator,
    Operators,
)


class SnowflakeMetricEvalOperatorSQLGenerator:
    @singledispatchmethod
    def operator_sql(self, operators: Operators, metric_sql: str) -> str:
        """
        Generates Operator SQL that applies operator on `metric`
        and returns a numeric boolean value 1 if PASS, 0 if FAIL

        """
        raise ValueError(f"Unsupported metric operator type {type(operators)} ")

    @operator_sql.register
    def _(self, operators: EqualToOperator, metric_sql: str) -> str:
        return f"select case when metric={operators.value} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: BetweenOperator, metric_sql: str) -> str:
        return f"select case when metric between {operators.min} and {operators.max} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: LessThanOperator, metric_sql: str) -> str:
        return f"select case when metric < {operators.value} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: LessThanOrEqualToOperator, metric_sql: str) -> str:
        return f"select case when metric <= {operators.value} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: GreaterThanOperator, metric_sql: str) -> str:
        return f"select case when metric > {operators.value} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: GreaterThanOrEqualToOperator, metric_sql: str) -> str:
        return f"select case when metric >= {operators.value} then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: NotNullOperator, metric_sql: str) -> str:
        return (
            f"select case when metric is not null then 1 else 0 end from ({metric_sql})"
        )

    @operator_sql.register
    def _(self, operators: IsNullOperator, metric_sql: str) -> str:
        return f"select case when metric is null then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: IsTrueOperator, metric_sql: str) -> str:
        return f"select case when metric then 1 else 0 end from ({metric_sql})"

    @operator_sql.register
    def _(self, operators: IsFalseOperator, metric_sql: str) -> str:
        return f"select case when not metric then 1 else 0 end from ({metric_sql})"
