import React from 'react';
import QueryString from 'query-string';
import { CheckCircleOutlined, CloseCircleOutlined, ExclamationCircleOutlined, StopOutlined } from '@ant-design/icons';
import {
    AssertionResult,
    AssertionResultErrorType,
    AssertionResultType,
    AssertionStdParameter,
    AssertionStdParameterType,
    DatasetAssertionInfo,
    StringMapEntry,
    Maybe,
} from '../../../../../../types.generated';
import AssertionSuccessIcon from '../../../../../../images/assertion_v2_success_dot.svg?react';
import AssertionFailureIcon from '../../../../../../images/assertion_v2_failure_dot.svg?react';
import AssertionErrorIcon from '../../../../../../images/assertion_error_dot.svg?react';
import AssertionInitIcon from '../../../../../../images/assertion_init_dot.svg?react'; // TODO
import AssertionNoResultsIcon from '../../../../../../images/assertion_no_results_dot.svg?react'; // TODO
import { ANTD_GRAY } from '../../../constants';

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
    return b?.runEvents?.runEvents[0]?.timestampMillis - a?.runEvents?.runEvents[0]?.timestampMillis;
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
const INIT_COLOR_HEX = '#2F54EB';
const NO_RESULTS_COLOR_HEX = ANTD_GRAY[8];

export const getResultColor = (result?: AssertionResultType) => {
    if (!result) {
        return NO_RESULTS_COLOR_HEX;
    }
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

/**
 * Convert an array of StringMapEntry into a map, for easy retrieval.
 */
export const convertNativeParametersArrayToMap = (nativeParameters: Maybe<Array<StringMapEntry>> | undefined) => {
    if (nativeParameters) {
        const map = new Map();
        nativeParameters.forEach((parameter) => {
            map.set(parameter.key, parameter.value);
        });
        return map;
    }
    return undefined;
};

export const getResultErrorMessage = (result: AssertionResult) => {
    if (result.type !== AssertionResultType.Error) {
        return undefined;
    }

    const errorType = result.error?.type;

    switch (errorType) {
        case AssertionResultErrorType.SourceConnectionError:
            return 'Unable to connect to source data platform. Please check the connection.';
        case AssertionResultErrorType.SourceQueryFailed:
            return 'Failed to evaluate query against the source platform.';
        case AssertionResultErrorType.InsufficientData:
            return 'Not enough data to evaluate assertion.';
        case AssertionResultErrorType.InvalidParameters:
            return 'Invalid parameters. Please check the assertion configuration.';
        case AssertionResultErrorType.InvalidSourceType:
            return 'Invalid source type selected. Please select different source type in assertion configuration.';
        case AssertionResultErrorType.UnsupportedPlatform:
            return 'Unsupported platform.';
        case AssertionResultErrorType.CustomSqlError:
            return 'Custom SQL query resulted in an error.';
        default:
            return 'An unknown error occurred.';
    }
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

export const getQueryParams = (param: string, location: any): string | null => {
    const params = QueryString.parse(location.search);
    return params[param] ? String(params[param]) : null;
};

export const getResultDotIcon = (result?: AssertionResultType, size?: number, disabled?: boolean) => {
    const opacity = disabled ? 0.4 : 1;
    if (!result) {
        // Todo replace will no results yet icon.
        return <AssertionNoResultsIcon width={size} height={size} opacity={opacity} />;
    }
    switch (result) {
        case AssertionResultType.Success:
            return <AssertionSuccessIcon width={size} height={size} opacity={opacity} />;
        case AssertionResultType.Failure:
            return <AssertionFailureIcon width={size} height={size} opacity={opacity} />;
        case AssertionResultType.Error:
            return <AssertionErrorIcon width={size} height={size} opacity={opacity} />;
        case AssertionResultType.Init:
            return <AssertionInitIcon width={size} height={size} opacity={opacity} />;
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};
