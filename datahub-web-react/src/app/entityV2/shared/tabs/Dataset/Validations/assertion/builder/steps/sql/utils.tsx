import {
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionValueChangeType,
    SqlAssertionType,
} from '../../../../../../../../../../types.generated';

export type SqlOperationOption = {
    label: string;
    type: SqlAssertionType;
    operator: AssertionStdOperator;
    changeType?: AssertionValueChangeType;
    parameters?: AssertionStdParameters;
    disabled?: boolean;
};

export enum SqlOperationOptionEnum {
    IS_EQUAL_TO = 'IS_EQUAL_TO',
    IS_NOT_EQUAL_TO = 'IS_NOT_EQUAL_TO',
    IS_GREATER_THAN = 'IS_GREATER_THAN',
    IS_LESS_THAN = 'IS_LESS_THAN',
    IS_BETWEEN = 'IS_BETWEEN',
    GROWS_LESS_THAN = 'GROWS_LESS_THAN',
    GROWS_MORE_THAN = 'GROWS_MORE_THAN',
    GROWS_WITHIN_RANGE = 'GROWS_WITHIN_RANGE',
}

export const SQL_OPERATION_OPTIONS: Record<SqlOperationOptionEnum, SqlOperationOption> = {
    [SqlOperationOptionEnum.IS_EQUAL_TO]: {
        label: 'Is equal to',
        type: SqlAssertionType.Metric,
        operator: AssertionStdOperator.EqualTo,
    },
    [SqlOperationOptionEnum.IS_NOT_EQUAL_TO]: {
        label: 'Is not equal to',
        type: SqlAssertionType.Metric,
        operator: AssertionStdOperator.NotEqualTo,
    },
    [SqlOperationOptionEnum.IS_GREATER_THAN]: {
        label: 'Is greater than',
        type: SqlAssertionType.Metric,
        operator: AssertionStdOperator.GreaterThan,
    },
    [SqlOperationOptionEnum.IS_LESS_THAN]: {
        label: 'Is less than',
        type: SqlAssertionType.Metric,
        operator: AssertionStdOperator.LessThan,
    },
    [SqlOperationOptionEnum.IS_BETWEEN]: {
        label: 'Is within a range',
        type: SqlAssertionType.Metric,
        operator: AssertionStdOperator.Between,
    },
    [SqlOperationOptionEnum.GROWS_LESS_THAN]: {
        label: 'Grows at most',
        type: SqlAssertionType.MetricChange,
        operator: AssertionStdOperator.LessThanOrEqualTo,
        changeType: AssertionValueChangeType.Absolute,
    },
    [SqlOperationOptionEnum.GROWS_MORE_THAN]: {
        label: 'Grows at least',
        type: SqlAssertionType.MetricChange,
        operator: AssertionStdOperator.GreaterThanOrEqualTo,
        changeType: AssertionValueChangeType.Absolute,
    },
    [SqlOperationOptionEnum.GROWS_WITHIN_RANGE]: {
        label: 'Grows within a range',
        type: SqlAssertionType.MetricChange,
        operator: AssertionStdOperator.Between,
        changeType: AssertionValueChangeType.Absolute,
    },
};

export const getSqlOperationOptions = () => {
    return Object.entries(SQL_OPERATION_OPTIONS).map(([key, option]) => ({
        label: option.label,
        value: key,
        disabled: !!option.disabled,
    }));
};

export const getOperationOption = (type: SqlAssertionType, operator: AssertionStdOperator) => {
    const currentOption = Object.entries(SQL_OPERATION_OPTIONS).find(
        ([_, option]) => option.type === type && option.operator === operator,
    );
    return currentOption ? (currentOption[0] as SqlOperationOptionEnum) : SqlOperationOptionEnum.IS_EQUAL_TO;
};

export const SQL_CHANGE_TYPE_OPTIONS = [
    {
        label: 'Value',
        value: AssertionValueChangeType.Absolute,
    },
    {
        label: 'Percentage',
        value: AssertionValueChangeType.Percentage,
    },
];
