import React from 'react';
import styled, { useTheme } from 'styled-components';

const Badge = styled.div<{ color: string }>`
    height: 16px;
    width: 16px;
    border-radius: 50%;
    background-color: ${(props) => props.color};
    font-size: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: ${(props) => props.theme.colors.textOnFillBrand};
    margin-left: -4px;
    margin-top: -12px;
    border: 1px solid ${(props) => props.theme.colors.bg};
`;

type Props = {
    count: number;
    color?: string;
};

export const CountBadge = ({ count, color }: Props) => {
    const theme = useTheme();
    if (!count) {
        return null;
    }
    return <Badge color={color || theme.colors.iconSuccess}>{count}</Badge>;
};
