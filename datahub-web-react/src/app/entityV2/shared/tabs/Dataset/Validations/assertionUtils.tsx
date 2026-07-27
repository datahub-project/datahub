import { Location } from 'history';
import i18next from 'i18next';
import QueryString from 'query-string';
import React from 'react';
import { DefaultTheme } from 'styled-components';

import {
    AssertionResult,
    AssertionResultErrorType,
    AssertionResultType,
    AssertionStdParameter,
    AssertionStdParameterType,
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
export const getResultColor = (theme: DefaultTheme, result?: AssertionResultType) => {
    const initColor = theme?.colors?.textInformation;
    const noResultsColor = theme?.colors?.textTertiary;

    if (!result) {
        return noResultsColor;
    }
    switch (result) {
        case AssertionResultType.Success:
            return theme?.colors?.iconSuccess;
        case AssertionResultType.Failure:
            return theme?.colors?.iconError;
        case AssertionResultType.Error:
            return theme?.colors?.iconWarning;
        case AssertionResultType.Init:
            return initColor;
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
            return i18next.t('entity.profile.validations:error.unableToConnect');
        case AssertionResultErrorType.SourceQueryFailed:
            return i18next.t('entity.profile.validations:error.failedEvaluateQuery');
        case AssertionResultErrorType.InsufficientData:
            return i18next.t('entity.profile.validations:error.notEnoughData');
        case AssertionResultErrorType.InvalidParameters:
            return i18next.t('entity.profile.validations:error.invalidParameters');
        case AssertionResultErrorType.InvalidSourceType:
            return i18next.t('entity.profile.validations:error.invalidSourceType');
        case AssertionResultErrorType.UnsupportedPlatform:
            return i18next.t('entity.profile.validations:error.unsupportedPlatform');
        case AssertionResultErrorType.CustomSqlError:
            return i18next.t('entity.profile.validations:error.customSqlError');
        default:
            return i18next.t('entity.profile.validations:error.unknownError');
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
