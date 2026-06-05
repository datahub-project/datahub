import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import i18next from 'i18next';
import React from 'react';
import { DefaultTheme } from 'styled-components';

import { TestResultType } from '@types';

/**
 * Returns the display text assoociated with an Test Result Type
 */
export const getResultText = (result: TestResultType) => {
    switch (result) {
        case TestResultType.Success:
            return i18next.t('entity.profile.tests:resultText.passing');
        case TestResultType.Failure:
            return i18next.t('entity.profile.tests:resultText.failing');
        default:
            throw new Error(`Unsupported Test Result Type ${result} provided.`);
    }
};

/**
 * Returns the display color assoociated with an TestResultType
 */
export const getResultColor = (result: TestResultType, theme: DefaultTheme) => {
    switch (result) {
        case TestResultType.Success:
            return theme.colors.iconSuccess;
        case TestResultType.Failure:
            return theme.colors.iconError;
        default:
            throw new Error(`Unsupported Test Result Type ${result} provided.`);
    }
};

/**
 * Returns the display icon assoociated with an TestResultType
 */
export const getResultIcon = (result: TestResultType, theme: DefaultTheme) => {
    const resultColor = getResultColor(result, theme);
    switch (result) {
        case TestResultType.Success:
            return <CheckCircleOutlined style={{ color: resultColor }} />;
        case TestResultType.Failure:
            return <CloseCircleOutlined style={{ color: resultColor }} />;
        default:
            throw new Error(`Unsupported Test Result Type ${result} provided.`);
    }
};
