import { CheckCircleOutlined, CloseCircleOutlined, ExclamationCircleOutlined, StopOutlined } from '@ant-design/icons';
import { Location } from 'history';
import QueryString from 'query-string';
import React from 'react';

import {
    ERROR_COLOR_HEX,
    FAILURE_COLOR_HEX,
    INFO_COLOR_HEX,
    SUCCESS_COLOR_HEX,
} from '@components/theme/foundations/colors';

// TODO
import { ANTD_GRAY } from '@app/entityV2/shared/constants';

import {
    AssertionResult,
    AssertionResultType,
    AssertionStatus,
    AssertionStdParameter,
    AssertionStdParameterType,
    DatasetAssertionInfo,
    Maybe,
    StringMapEntry,
} from '@types';

import AssertionErrorIcon from '@images/assertion_error_dot.svg?react';
import AssertionInitIcon from '@images/assertion_init_dot.svg?react';
// TODO
import AssertionNoResultsIcon from '@images/assertion_no_results_dot.svg?react';
import AssertionFailureIcon from '@images/assertion_v2_failure_dot.svg?react';
import AssertionSuccessIcon from '@images/assertion_v2_success_dot.svg?react';

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
export const getResultText = (result: AssertionResultType, isSmartAssertion?: boolean) => {
    switch (result) {
        case AssertionResultType.Success:
            return 'Passed';
        case AssertionResultType.Failure:
            return 'Failed';
        case AssertionResultType.Error:
            return 'Error';
        case AssertionResultType.Init:
            return isSmartAssertion ? 'Training' : 'Initializing';
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};

/**
 * Returns the display color associated with an AssertionResultType
 */
export const NO_RESULTS_COLOR_HEX = ANTD_GRAY[8];

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
            return INFO_COLOR_HEX;
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

export const getQueryParams = (param: string, location: Location): string | null => {
    const params = QueryString.parse(location.search);
    return params[param] ? String(params[param]) : null;
};

export const getResultDotIconFromResultType = (
    resultType: AssertionResultType | undefined,
    size?: number,
    disabled?: boolean,
) => {
    const opacity = disabled ? 0.4 : 1;
    if (!resultType) {
        return <AssertionNoResultsIcon width={size} height={size} opacity={opacity} />;
    }
    switch (resultType) {
        case AssertionResultType.Success:
            return <AssertionSuccessIcon width={size} height={size} opacity={opacity} />;
        case AssertionResultType.Failure:
            return <AssertionFailureIcon width={size} height={size} opacity={opacity} />;
        case AssertionResultType.Error:
            return <AssertionErrorIcon width={size} height={size} opacity={opacity} />;
        case AssertionResultType.Init:
            return <AssertionInitIcon width={size} height={size} opacity={opacity} />;
        default:
            throw new Error(`Unsupported Assertion Result Type ${resultType} provided.`);
    }
};

export const getResultErrorMessage = (result: AssertionResult) => {
    if (result.type !== AssertionResultType.Error) {
        return undefined;
    }

    const displayMessage = result.error?.displayMessage;
    if (displayMessage) {
        return displayMessage;
    }

    const messageProperty = result.error?.properties?.find((property) => property.key === 'message')?.value;
    return messageProperty || 'An unknown error occurred.';
};

export type AssertionHealthDotStatus = 'success' | 'failure' | 'error' | 'init' | 'gray';

export const getAssertionStatusResultType = (
    assertionStatus?: AssertionStatus | null,
): AssertionResultType | undefined => {
    switch (assertionStatus) {
        case AssertionStatus.Passing:
            return AssertionResultType.Success;
        case AssertionStatus.Failing:
            return AssertionResultType.Failure;
        case AssertionStatus.Error:
            return AssertionResultType.Error;
        case AssertionStatus.Init:
            return AssertionResultType.Init;
        default:
            return undefined;
    }
};
