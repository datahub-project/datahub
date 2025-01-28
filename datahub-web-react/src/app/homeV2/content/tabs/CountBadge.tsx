import React from 'react';
import styled from 'styled-components';

const DEFAULT_BADGE_COLOR = '#3CB47A';

const Badge = styled.div<{ color: string }>`
    height: 16px;
    width: 16px;
    border-radius: 50%;
    background-color: ${(props) => props.color};
    font-size: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #ffffff;
    margin-left: -4px;
    margin-top: -12px;
    border: 1px solid #ffffff;
`;

type Props = {
    count: number;
    color?: string;
};

export const CountBadge = ({ count, color = DEFAULT_BADGE_COLOR }: Props) => {
    if (!count) {
        return null;
    }
    return <Badge color={color}>{count}</Badge>;
};
