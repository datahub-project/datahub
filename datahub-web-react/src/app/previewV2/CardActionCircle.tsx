import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entityV2/shared/constants';

type Props = {
    icon: any;
    onClick?: () => void;
    enabled?: boolean;
};

const IconContainer = styled.div<{ enabled?: boolean }>`
    width: 28px;
    height: 28px;
    background-color: #f7f7f7;
    cursor: pointer;
    border-radius: 50%;
    text-align: center;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    & svg {
        font-size: 14px;
        color: #5d668b;
    }
    border: 1px solid #eee;
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
