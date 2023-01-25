import { orange } from '@ant-design/colors';
import { DownOutlined, WarningFilled } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../constants';
import FailingAssertions from './FailingAssertions';
import { UpstreamSummary } from './utils';

const TextWrapper = styled.span`
    font-size: 16px;
    line-height: 24px;
    margin-left: 8px;
`;

const StyledWarning = styled(WarningFilled)`
    color: ${orange[5]};
    font-size: 14px;
`;

const FailingDetailsWrapper = styled.span`
    font-size: 14px;
    color: ${ANTD_GRAY[8]};
    margin-left: 8px;
    &:hover {
        cursor: pointer;
        color: $ ${(props) => props.theme.styles['primary-color']};
    }
`;

const StyledArrow = styled(DownOutlined)<{ isOpen: boolean }>`
    font-size: 12px;
    margin-left: 3px;
    ${(props) =>
        props.isOpen &&
        `
        transform: rotate(180deg);
        padding-top: 1px;
    `}
`;

interface Props {
    upstreamSummary: UpstreamSummary;
}

export default function FailingInputs({ upstreamSummary }: Props) {
    const [areFailingDetailsVisible, setAreFailingDetailsVisible] = useState(false);

    return (
        <div>
            <StyledWarning />
            <TextWrapper>Some data inputs are not healthy</TextWrapper>
            <FailingDetailsWrapper onClick={() => setAreFailingDetailsVisible(!areFailingDetailsVisible)}>
                view details <StyledArrow isOpen={areFailingDetailsVisible} />
            </FailingDetailsWrapper>
            {areFailingDetailsVisible && <FailingAssertions upstreamSummary={upstreamSummary} />}
        </div>
    );
}
