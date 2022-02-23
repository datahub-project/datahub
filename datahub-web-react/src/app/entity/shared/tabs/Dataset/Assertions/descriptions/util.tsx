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

export const getOpText = (
    op: AssertionStdOperator,
    nativeOp: string | undefined,
    parameters: Map<string, string> | undefined,
) => {
    switch (op) {
        case AssertionStdOperator.Between: {
            const validParameters = validateParameters(parameters, ['min_value', 'max_value']);
            return (
                <Typography.Text>
                    between{' '}
                    <Typography.Text strong>
                        {parseInt(validParameters.get('max_value') as string, 10).toLocaleString()}{' '}
                    </Typography.Text>
                    and{' '}
                    <Typography.Text strong>
                        {parseInt(validParameters.get('min_value') as string, 10).toLocaleString()}
                    </Typography.Text>
                </Typography.Text>
            );
        }
        // Hybrid Operators
        case AssertionStdOperator.EqualTo: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'equal to';
            return (
                <Typography.Text>
                    {operatorText} <Typography.Text strong>{validParameters.get('value') as string} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.Contain: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'contains';
            return (
                <Typography.Text>
                    {operatorText} <Typography.Text strong>{validParameters.get('value') as string} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.In: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'in';
            return (
                <Typography.Text>
                    {operatorText} <Typography.Text strong>{validParameters.get('value') as string} </Typography.Text>
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
                    <Typography.Text strong>
                        {parseInt(validParameters.get('value') as string, 10).toLocaleString()}{' '}
                    </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.GreaterThanOrEqualTo: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'greater than or equal to';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>
                        {parseInt(validParameters.get('value') as string, 10).toLocaleString()}{' '}
                    </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.LessThan: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'less than';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>
                        {parseInt(validParameters.get('value') as string, 10).toLocaleString()}{' '}
                    </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.LessThanOrEqualTo: {
            const validParameters = validateParameters(parameters, ['value']);
            const operatorText = 'less than or equal to';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>
                        {parseInt(validParameters.get('value') as string, 10).toLocaleString()}{' '}
                    </Typography.Text>
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
            const operatorText = `meets condition ${nativeOp} given parameters ${parameters}`;
            return <Typography.Text>{operatorText}</Typography.Text>;
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
