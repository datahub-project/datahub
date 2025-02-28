import React from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';

const CircleContainer = styled.span<{ size?: number }>`
    width: ${(props) => (props.size ? props.size : 10)}px;
    height: ${(props) => (props.size ? props.size : 10)}px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const Circle = styled.span<{ color: string; size?: number }>`
    width: ${(props) => (props.size ? props.size : 6)}px;
    height: ${(props) => (props.size ? props.size : 6)}px;
    border-radius: 50%;
    padding: 0px;
    margin: 0px;
    background-color: ${(props) => props.color};
    opacity: 1;
`;

type Props = {
    title?: React.ReactNode;
    color: string;
    size?: number;
};

export const DefaultViewIcon = ({ title, color, size }: Props) => {
    return (
        <Tooltip title={title}>
            <CircleContainer size={size}>
                <Circle color={color} size={size} />
            </CircleContainer>
        </Tooltip>
    );
};
