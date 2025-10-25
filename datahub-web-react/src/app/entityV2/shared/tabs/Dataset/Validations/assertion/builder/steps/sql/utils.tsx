import {
    SECTION_LABELS,
    groupOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/common/groupedOptions';

import { AssertionStdOperator, AssertionStdParameters, AssertionValueChangeType, SqlAssertionType } from '@types';

export type SqlOperationOption = {
    label: string;
    type: SqlAssertionType;
    operator: AssertionStdOperator;
    changeType?: AssertionValueChangeType;
    parameters?: AssertionStdParameters;
    disabled?: boolean;
};

export enum SqlOperationOptionEnum {
    AI_INFERRED = 'AI_INFERRED',
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
    [SqlOperationOptionEnum.AI_INFERRED]: {
        label: 'Detect with AI',
        type: SqlAssertionType.Metric,
        operator: AssertionStdOperator.Between,
    },
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

export const getSqlOperationOptionGroups = () => {
    const all = (Object.entries(SQL_OPERATION_OPTIONS) as [SqlOperationOptionEnum, SqlOperationOption][]).map(
        ([key, option]) => ({ key, option }),
    );

    const toOptions = (predicate: (entry: { key: SqlOperationOptionEnum; option: SqlOperationOption }) => boolean) =>
        all
            .filter(predicate)
            .map(({ key, option }) => ({ label: option.label, value: key, disabled: !!option.disabled }));

    const ai = toOptions(({ key }) => key === SqlOperationOptionEnum.AI_INFERRED);
    const metric = toOptions(
        ({ key, option }) => key !== SqlOperationOptionEnum.AI_INFERRED && option.type === SqlAssertionType.Metric,
    );
    const growth = toOptions(({ option }) => option.type === SqlAssertionType.MetricChange);

    return groupOptions([
        [SECTION_LABELS.anomalyDetection, ai],
        ['Absolute Value', metric],
        [SECTION_LABELS.growthRate, growth],
    ]);
};

export const getOperationOption = (type?: SqlAssertionType | null, operator?: AssertionStdOperator | null) => {
    if (!type || !operator) return undefined;
    const currentOption = Object.entries(SQL_OPERATION_OPTIONS).find(
        ([_, option]) => option.type === type && option.operator === operator,
    );
    return currentOption ? (currentOption[0] as SqlOperationOptionEnum) : undefined;
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
