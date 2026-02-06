import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import {
    getAssertionStatusResultType,
    getResultColor,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { applyOpacityToHexColor } from '@app/shared/styleUtils';

import { AssertionStatus } from '@types';

const Pill = styled.div<{ color: string; highlightColor: string }>`
    display: flex;
    justify-content: center;
    align-items: center;
    border-radius: 20px;
    padding: 2px 8px;
    background-color: ${(props) => props.highlightColor || ANTD_GRAY[3]};
    color: ${(props) => props.color || ANTD_GRAY[3]};
    :hover {
        opacity: 0.8;
    }
`;

type Props = {
    status?: AssertionStatus | null;
    isSmartAssertion?: boolean;
};

const getStatusText = (status?: AssertionStatus | null, isSmartAssertion?: boolean): string => {
    switch (status) {
        case AssertionStatus.Passing:
            return 'Passing';
        case AssertionStatus.Failing:
            return 'Failing';
        case AssertionStatus.Error:
            return 'Error';
        case AssertionStatus.Init:
            return isSmartAssertion ? 'Training' : 'Initializing';
        default:
            return 'No results yet';
    }
};

export const AssertionStatusPill = ({ status, isSmartAssertion }: Props) => {
    const resultType = getAssertionStatusResultType(status);
    const resultColor = getResultColor(resultType);
    const highlightColor = applyOpacityToHexColor(resultColor, 0.15);
    const text = getStatusText(status, isSmartAssertion);
    return (
        <Pill color={resultColor} highlightColor={highlightColor}>
            {text}
        </Pill>
    );
};
