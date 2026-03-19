import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import React from 'react';

import {
    AssertionResultType,
    AssertionStdParameter,
    AssertionStdParameterType,
} from '@types';

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
    return b.runEvents?.runEvents[0]?.timestampMillis - a.runEvents?.runEvents[0]?.timestampMillis;
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
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};

/**
 * Returns the display color assoociated with an AssertionResultType
 */
const SUCCESS_COLOR_HEX = '#4db31b';
const FAILURE_COLOR_HEX = '#F5222D';
export const getResultColor = (result: AssertionResultType) => {
    switch (result) {
        case AssertionResultType.Success:
            return SUCCESS_COLOR_HEX;
        case AssertionResultType.Failure:
            return FAILURE_COLOR_HEX;
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};

/**
 * Returns the display icon assoociated with an AssertionResultType
 */
export const getResultIcon = (result: AssertionResultType) => {
    const resultColor = getResultColor(result);
    switch (result) {
        case AssertionResultType.Success:
            return <CheckCircleOutlined style={{ color: resultColor }} />;
        case AssertionResultType.Failure:
            return <CloseCircleOutlined style={{ color: resultColor }} />;
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
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
