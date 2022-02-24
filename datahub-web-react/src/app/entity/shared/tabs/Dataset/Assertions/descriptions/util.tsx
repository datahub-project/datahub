import React from 'react';
import { Typography } from 'antd';
import { AssertionStdOperator, DatasetAssertionInfo, StringMapEntry } from '../../../../../../../types.generated';

export const convertParametersArrayToMap = (parameters: Array<StringMapEntry> | undefined) => {
    if (parameters) {
        const map = new Map();
        parameters.forEach((parameter) => {
            map.set(parameter.key, parameter.value);
        });
        return map;
    }
    return undefined;
};

export const validateParameters = (parameters: Map<string, string> | undefined, requiredParams: Array<string>) => {
    if (parameters === null || parameters === undefined) {
        throw new Error(
            `Failed to validate assertion parameters! Missing or null parameters. Required ${requiredParams}.`,
        );
    }
    requiredParams.forEach((requiredParam) => {
        if (!parameters.has(requiredParam)) {
            throw new Error(`Assertion parameters missing required parameter '${requiredParam}'`);
        }
    });
    return parameters;
};

const getFormattedParameterValue = (parameters, name) => {
    const value = parameters.get(name);
    return Number.isNaN(Number(value as any)) ? value : parseFloat(value as string).toLocaleString();
};

export const getOpText = (
    op: AssertionStdOperator,
    nativeOp: string | undefined,
    parameters: Map<string, string> | undefined,
) => {
    switch (op) {
        // Hybrid Operators
        case AssertionStdOperator.Between: {
            const validParameters = validateParameters(parameters, ['min_value', 'max_value']);
            return (
                <Typography.Text>
                    between{' '}
                    <Typography.Text strong>
                        {getFormattedParameterValue(validParameters, 'min_value')}{' '}
                    </Typography.Text>
                    and{' '}
                    <Typography.Text strong>{getFormattedParameterValue(validParameters, 'max_value')}</Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.EqualTo: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'equal to';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(validParameters, 'value')}</Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.Contain: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'contains';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(validParameters, 'value')} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.In: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'in';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(validParameters, 'value')} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.NotNull: {
            const operatorText = 'not null';
            return <Typography.Text strong>{operatorText}</Typography.Text>;
        }
        // Numeric Operators
        case AssertionStdOperator.GreaterThan: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'greater than';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(validParameters, 'value')} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.GreaterThanOrEqualTo: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'greater than or equal to';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(validParameters, 'value')} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.LessThan: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'less than';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(validParameters, 'value')} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.LessThanOrEqualTo: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'less than or equal to';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(validParameters, 'value')} </Typography.Text>
                </Typography.Text>
            );
        }
        // String Operators
        case AssertionStdOperator.StartWith: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'starts with';
            return (
                <Typography.Text>
                    {operatorText} <Typography.Text strong>{validParameters.get('value') as string} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.EndWith: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'ends with';
            return (
                <Typography.Text>
                    {operatorText} <Typography.Text strong>{validParameters.get('value') as string} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.Native: {
            return (
                <Typography.Text>
                    matching assertion <Typography.Text strong>{nativeOp}</Typography.Text>
                </Typography.Text>
            );
        }
        default:
            throw new Error(`Unsupported assertion operator ${op} provided.`);
    }
};

export const validateColumnAssertionFieldPath = (info: DatasetAssertionInfo) => {
    if (info.fieldPaths && info.fieldPaths.length === 1) {
        return info.fieldPaths[0];
    }
    throw new Error('Failed to find fieldPath for column assertion.');
};
