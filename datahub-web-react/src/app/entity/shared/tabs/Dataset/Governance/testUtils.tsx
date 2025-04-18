import React from 'react';
import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { TestResultType } from '../../../../../../types.generated';

/**
 * Returns the display text assoociated with an Test Result Type
 */
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

/**
 * Returns the display color assoociated with an TestResultType
 */
const SUCCESS_COLOR_HEX = '#4db31b';
const FAILURE_COLOR_HEX = '#F5222D';

export const getResultColor = (result: TestResultType) => {
    switch (result) {
        case TestResultType.Success:
            return SUCCESS_COLOR_HEX;
        case TestResultType.Failure:
            return FAILURE_COLOR_HEX;
        default:
            throw new Error(`Unsupported Test Result Type ${result} provided.`);
    }
};

/**
 * Returns the display icon assoociated with an TestResultType
 */
export const getResultIcon = (result: TestResultType) => {
    const resultColor = getResultColor(result);
    switch (result) {
        case TestResultType.Success:
            return <CheckCircleOutlined style={{ color: resultColor }} />;
        case TestResultType.Failure:
            return <CloseCircleOutlined style={{ color: resultColor }} />;
        default:
            throw new Error(`Unsupported Test Result Type ${result} provided.`);
    }
};
