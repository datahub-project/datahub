import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import React from 'react';
import { DefaultTheme } from 'styled-components';

import { TestResultType } from '@types';

export const getResultText = (result: TestResultType) => {
    switch (result) {
        case TestResultType.Success:
            return 'Passing';
        case TestResultType.Failure:
            return 'Failing';
        default:
            throw new Error(`Unsupported Test Result Type ${result} provided.`);
    }
};

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
