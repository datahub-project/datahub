import React from 'react';
import styled from 'styled-components';

interface Props {
    color: string;
    size?: number;
}

const Swatch = styled.span<Props>`
    display: inline-block;
    width: ${({ size }) => size}px;
    height: ${({ size }) => size}px;
    background-color: ${({ color }) => color};
    border-radius: 4px;
    margin-right: 8px;
    vertical-align: middle;
`;

export function ColorSwatch({ color, size = 16 }: Props) {
    return <Swatch color={color} size={size} />;
}
