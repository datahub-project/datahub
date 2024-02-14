import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entityV2/shared/constants';

type Props = {
    icon: any;
    onClick?: () => void;
    enabled?: boolean;
};

const IconContainer = styled.div<{ enabled?: boolean }>`
    width: 22px;
    height: 22px;
    background-color: none;
    cursor: pointer;
    border-radius: 50%;
    text-align: center;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    & svg {
        font-size: 14px;
        // color: #b0a2c2;
        color: ${({ enabled }) => (enabled ? '#3F54D1' : '#b0a2c2')};
    }
    border: 1px solid ${ANTD_GRAY['4']};
    :hover {
        border: 1px solid ${({ enabled }) => (enabled ? '#3F54D1' : ANTD_GRAY['4'])};
    }
`;

const CardActionCircle = ({ icon, onClick, enabled }: Props) => {
    return (
        <IconContainer enabled={enabled} onClick={onClick}>
            {icon}
        </IconContainer>
    );
};

export default CardActionCircle;
