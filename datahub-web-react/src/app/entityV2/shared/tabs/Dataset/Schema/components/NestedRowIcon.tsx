import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../../../../constants';
import RowIcon from '../../../../../../../images/row-icon.svg?react';

const RowIconContainer = styled.div`
    position: relative;
    display: flex;
    align-items: center;
`;
const DepthContainer = styled.div`
    height: 13px;
    width: 13px;
    border-radius: 50%;
    background: ${REDESIGN_COLORS.WHITE};
    margin-left: -7px;
    margin-top: -12px;
    display: flex;
    align-items: center;
`;

const DepthNumber = styled(Typography.Text)`
    margin-left: 4px;
    background: transparent;
    color: ${REDESIGN_COLORS.PRIMARY_PURPLE};
    font-size: 10px;
    font-weight: 400;
`;

type Props = {
    depth: number;
};

export default function NestedRowIcon({ depth }: Props) {
    return (
        <RowIconContainer className="row-icon">
            <RowIcon height={16} width={16} />
            <DepthContainer className="depth-container">
                <DepthNumber className="depth-text">{depth}</DepthNumber>
            </DepthContainer>
        </RowIconContainer>
    );
}
