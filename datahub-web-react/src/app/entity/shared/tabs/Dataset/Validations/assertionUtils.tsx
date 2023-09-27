import React from 'react';
import { CheckCircleOutlined, CloseCircleOutlined, ExclamationCircleOutlined, StopOutlined } from '@ant-design/icons';
import {
    AssertionResult,
    AssertionResultErrorType,
    AssertionResultType,
    AssertionStdParameter,
    AssertionStdParameterType,
    DatasetAssertionInfo,
    StringMapEntry,
} from '../../../../../../types.generated';

/**
 * Utility methods
 */
export const sortAssertions = (a, b) => {
    if (!a.runEvents?.runEvents?.length) {
        return 1;
    }
    if (!b.runEvents?.runEvents?.length) {
        return -1;
    }
    return b.runEvents.runEvents[0].timestampMillis - a.runEvents.runEvents[0].timestampMillis;
};

/**
 * Returns the display text assoociated with an AssertionResultType
 */
export const getResultText = (result: AssertionResultType) => {
    switch (result) {
        case AssertionResultType.Success:
            return 'Passed';
        case AssertionResultType.Failure:
            return 'Failed';
        case AssertionResultType.Error:
            return 'Error';
        case AssertionResultType.Init:
            return 'Initializing';
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};

/**
 * Returns the display color associated with an AssertionResultType
 */
const SUCCESS_COLOR_HEX = '#4db31b';
const FAILURE_COLOR_HEX = '#F5222D';
const ERROR_COLOR_HEX = '#FAAD14';
const INIT_COLOR_HEX = '#8C8C8C';
export const getResultColor = (result: AssertionResultType) => {
    switch (result) {
        case AssertionResultType.Success:
            return SUCCESS_COLOR_HEX;
        case AssertionResultType.Failure:
            return FAILURE_COLOR_HEX;
        case AssertionResultType.Error:
            return ERROR_COLOR_HEX;
        case AssertionResultType.Init:
            return INIT_COLOR_HEX;
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};

/**
 * Returns the display icon associated with an AssertionResultType
 */
export const getResultIcon = (result: AssertionResultType, color?: string) => {
    const resultColor = color || getResultColor(result);
    switch (result) {
        case AssertionResultType.Success:
            return <CheckCircleOutlined style={{ color: resultColor }} />;
        case AssertionResultType.Failure:
            return <CloseCircleOutlined style={{ color: resultColor }} />;
        case AssertionResultType.Error:
            return <ExclamationCircleOutlined style={{ color: resultColor }} />;
        case AssertionResultType.Init:
            return <StopOutlined style={{ color: resultColor }} />;
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};

export const getResultErrorMessage = (result: AssertionResult) => {
    if (result.type !== AssertionResultType.Error) {
        return undefined;
    }

    const errorType = result.error?.type;
    switch (errorType) {
        case AssertionResultErrorType.SourceConnectionError:
            return 'Unable to connect to dataset. Please check the dataset connection.';
        case AssertionResultErrorType.SourceQueryFailed:
            return 'Unable to evaluate assertion. Please check the assertion configuration.';
        case AssertionResultErrorType.InsufficientData:
            return 'Not enough data to evaluate assertion.';
        case AssertionResultErrorType.InvalidParameters:
            return 'Invalid parameters. Please check the assertion configuration.';
        case AssertionResultErrorType.InvalidSourceType:
            return 'Invalid source type selected.';
        case AssertionResultErrorType.UnsupportedPlatform:
            return 'Unsupported platform.';
        case AssertionResultErrorType.CustomSqlError:
            return 'Custom SQL returned an error.';
        default:
            return 'An unknown error occurred.';
    }
};

/**
 * Convert an array of StringMapEntry into a map, for easy retrieval.
 */
export const convertNativeParametersArrayToMap = (nativeParameters: Array<StringMapEntry> | undefined) => {
    if (nativeParameters) {
        const map = new Map();
        nativeParameters.forEach((parameter) => {
            map.set(parameter.key, parameter.value);
        });
        return map;
    }
    return undefined;
};

/**
 * Returns the value of an AssertionStdParameter suitable for display
 */
const MISSING_PARAMETER_VALUE = '?';
export const getFormattedParameterValue = (parameter: AssertionStdParameter | undefined | null): React.ReactNode => {
    if (parameter === null || parameter === undefined) {
        return MISSING_PARAMETER_VALUE;
    }
    switch (parameter.type) {
        case AssertionStdParameterType.Number:
            return parseFloat(parameter.value as string).toLocaleString();
        case AssertionStdParameterType.String:
            return parameter.value;
        case AssertionStdParameterType.Set:
            return parameter.value;
        default:
            return Number.isNaN(Number(parameter.value as any))
                ? parameter.value
                : parseFloat(parameter.value as string).toLocaleString();
    }
};

/**
 * Throws if an assertion has no input fields
 */
export const validateAssertionsHasInputFields = (info: DatasetAssertionInfo) => {
    if (info.fields && info.fields.length === 1) {
        return info.fields[0].path;
    }
    throw new Error('Failed to find field path(s) for column assertion.');
};
