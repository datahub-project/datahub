import { Typography } from 'antd';
import React from 'react';

import { tryGetSqlAssertionNumericalResult } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultExtractionUtils';
import { getFormattedParameterValue } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

import {
    AssertionResult,
    AssertionResultType,
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    AssertionValueChangeType,
    TestAssertionInput,
} from '@types';

type Props = {
    result: AssertionResult;
    expectedFromTestInput?: TestAssertionInput;
};

// Build a concise Expected line for SQL assertions supporting all common operators
const getSqlExpectedText = (
    sql:
        | {
              operator?: AssertionStdOperator | null;
              parameters?: AssertionStdParameters | null;
              changeType?: AssertionValueChangeType | null;
          }
        | undefined
        | null,
): string | undefined => {
    if (!sql) return undefined;

    const { operator, parameters, changeType } = sql;

    if (operator == null) {
        console.error('SQL assertion operator is null or undefined', sql);
        return undefined;
    }

    const value = parameters?.value != null ? String(getFormattedParameterValue(parameters.value)) : undefined;
    const min = parameters?.minValue != null ? String(getFormattedParameterValue(parameters.minValue)) : undefined;
    const max = parameters?.maxValue != null ? String(getFormattedParameterValue(parameters.maxValue)) : undefined;

    const isChange = !!changeType;
    const valueLabel = isChange ? 'change' : 'value';

    const withPercent = (n?: string) => (n && changeType === AssertionValueChangeType.Percentage ? `${n}%` : n);

    switch (operator) {
        case AssertionStdOperator.EqualTo:
            return value != null ? `${valueLabel} is ${withPercent(value)}` : undefined;
        case AssertionStdOperator.NotEqualTo:
            return value != null ? `${valueLabel} is not ${withPercent(value)}` : undefined;
        case AssertionStdOperator.GreaterThan:
            return value != null ? `${valueLabel} is greater than ${withPercent(value)}` : undefined;
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return value != null ? `${valueLabel} is greater than or equal to ${withPercent(value)}` : undefined;
        case AssertionStdOperator.LessThan:
            return value != null ? `${valueLabel} is less than ${withPercent(value)}` : undefined;
        case AssertionStdOperator.LessThanOrEqualTo:
            return value != null ? `${valueLabel} is less than or equal to ${withPercent(value)}` : undefined;
        case AssertionStdOperator.Between:
            if (min != null && max != null) {
                const minStr = withPercent(min);
                const maxStr = withPercent(max);
                return `${valueLabel} between ${minStr} and ${maxStr}`;
            }
            return undefined;
        case AssertionStdOperator.In:
            return value != null ? `${valueLabel} in ${withPercent(value)}` : undefined;
        case AssertionStdOperator.NotIn:
            return value != null ? `${valueLabel} not in ${withPercent(value)}` : undefined;
        case AssertionStdOperator.RegexMatch:
            return value != null ? `${valueLabel} matches ${value}` : undefined;
        case AssertionStdOperator.Contain:
            return value != null ? `${valueLabel} contains ${value}` : undefined;
        case AssertionStdOperator.StartWith:
            return value != null ? `${valueLabel} starts with ${value}` : undefined;
        case AssertionStdOperator.EndWith:
            return value != null ? `${valueLabel} ends with ${value}` : undefined;
        default:
            console.error(`Unexpected SQL assertion operator: ${operator}`, sql);
            return undefined;
    }
};

// Fallback: if the response doesn't include assertion info, try the original test input
const getSqlExpectedFromInput = (input?: TestAssertionInput): string | undefined => {
    if (!input || input.type !== AssertionType.Sql || !input.sqlTestInput) return undefined;
    const sqlInput = input.sqlTestInput;
    return getSqlExpectedText({
        operator: sqlInput.operator,
        parameters: sqlInput.parameters,
        changeType: sqlInput.changeType,
    });
};

export const DatasetAssertionResultDetails = ({ result, expectedFromTestInput }: Props) => {
    const maybeActualValue = result.actualAggValue;
    const maybeUnexpectedCount = result.unexpectedCount;
    const maybeRowCount = result.rowCount;
    const maybeNativeResults = result.nativeResults;

    const sqlExpectedText =
        result.assertion?.type === AssertionType.Sql ? getSqlExpectedText(result.assertion?.sqlAssertion) : undefined;
    const expectedText = sqlExpectedText || getSqlExpectedFromInput(expectedFromTestInput);
    const isSqlAssertion = result.assertion?.type === AssertionType.Sql;
    const sqlActual = isSqlAssertion ? tryGetSqlAssertionNumericalResult(result) : undefined;
    const valueToShow = isSqlAssertion ? sqlActual : maybeActualValue;

    return (
        <div>
            {isSqlAssertion && valueToShow != null && (
                <>
                    <Typography.Text strong>Value</Typography.Text>: {valueToShow}
                </>
            )}
            {!isSqlAssertion && valueToShow != null && (
                <>
                    <Typography.Text strong>Actual</Typography.Text>: {valueToShow}
                </>
            )}
            {expectedText && result.type !== AssertionResultType.Success && (
                <>
                    <Typography.Text strong>Expected:</Typography.Text> {expectedText}
                </>
            )}
            {maybeUnexpectedCount != null && (
                <>
                    <Typography.Text strong>Invalid Count</Typography.Text>: {maybeUnexpectedCount}
                </>
            )}
            {maybeRowCount != null && (
                <>
                    <Typography.Text strong>Row Count</Typography.Text>: {maybeRowCount}
                </>
            )}
            {maybeNativeResults && (
                <>
                    {maybeNativeResults.map((entry) => (
                        <div key={entry.key}>
                            <Typography.Text strong>{entry.key}</Typography.Text>: {entry.value}
                        </div>
                    ))}
                </>
            )}
        </div>
    );
};
