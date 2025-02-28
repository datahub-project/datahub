import React from 'react';

import styled from 'styled-components';

import { ANTD_GRAY } from '../../../../../../../constants';
import { AssertionResult } from '../../../../../../../../../../types.generated';
import { getResultColor } from '../../../../assertionUtils';
import { ResultStatusType, getResultStatusText } from './resultMessageUtils';
import { applyOpacityToHexColor } from '../../../../../../../../../shared/styleUtils';

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
