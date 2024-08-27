import React from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';

const CircleContainer = styled.span`
    width: 10px;
    height: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const Circle = styled.span<{ color: string }>`
    width: 6px;
    height: 6px;
    border-radius: 50%;
    padding: 0px;
    margin: 0px;
    background-color: ${(props) => props.color};
    opacity: 1;
`;

type Props = {
    title?: React.ReactNode;
    color: string;
};

export const DefaultViewIcon = ({ title, color }: Props) => {
    return (
        <Tooltip title={title}>
            <CircleContainer>
                <Circle color={color} />
            </CircleContainer>
        </Tooltip>
    );
};
