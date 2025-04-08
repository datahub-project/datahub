import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import {
    ResultStatusType,
    getResultStatusText,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';
import { getResultColor } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { applyOpacityToHexColor } from '@app/shared/styleUtils';

import { AssertionResult } from '@types';

const Pill = styled.div<{ color: string; highlightColor: string }>`
    display: flex;
    justify-content: center;
    align-items: center;
    border-radius: 20px;
    padding: 4px 12px;
    background-color: ${(props) => props.highlightColor || ANTD_GRAY[3]};
    color: ${(props) => props.color || ANTD_GRAY[3]};
    :hover {
        opacity: 0.8;
    }
`;

type Props = {
    result?: AssertionResult;
    type?: ResultStatusType;
};

export const AssertionResultPill = ({ result, type = ResultStatusType.LATEST }: Props) => {
    const resultType = result?.type;
    const resultColor = getResultColor(resultType);
    const highlightColor = applyOpacityToHexColor(resultColor, 0.15);
    const text = (resultType && getResultStatusText(resultType, type)) || 'No results yet';
    return (
        <Pill color={resultColor} highlightColor={highlightColor}>
            {text}
        </Pill>
    );
};
