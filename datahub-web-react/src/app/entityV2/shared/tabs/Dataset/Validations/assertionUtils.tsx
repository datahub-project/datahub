import { CheckCircleOutlined, CloseCircleOutlined, ExclamationCircleOutlined, StopOutlined } from '@ant-design/icons';
import { Location } from 'history';
import QueryString from 'query-string';
import React from 'react';

import { ERROR_COLOR_HEX, FAILURE_COLOR_HEX, SUCCESS_COLOR_HEX } from '@components/theme/foundations/colors';

// TODO
import { ANTD_GRAY } from '@app/entityV2/shared/constants';

import {
    AssertionResult,
    AssertionResultErrorType,
    AssertionResultType,
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
 * Returns the display color associated with an AssertionResultType
 */
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

export const getQueryParams = (param: string, location: Location): string | null => {
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
