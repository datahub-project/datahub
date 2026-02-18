import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import React from 'react';

import ColorTheme from '@src/conf/theme/colorThemes/types';

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

export const getResultColor = (result: TestResultType, colors?: Partial<ColorTheme>) => {
    switch (result) {
        case TestResultType.Success:
            return colors?.iconSuccess ?? '#4db31b';
        case TestResultType.Failure:
            return colors?.iconError ?? '#F5222D';
        default:
            throw new Error(`Unsupported Test Result Type ${result} provided.`);
    }
};

export const getResultIcon = (result: TestResultType, colors?: Partial<ColorTheme>) => {
    const resultColor = getResultColor(result, colors);
    switch (result) {
        case TestResultType.Success:
            return <CheckCircleOutlined style={{ color: resultColor }} />;
        case TestResultType.Failure:
            return <CloseCircleOutlined style={{ color: resultColor }} />;
        default:
            throw new Error(`Unsupported Test Result Type ${result} provided.`);
    }
};
