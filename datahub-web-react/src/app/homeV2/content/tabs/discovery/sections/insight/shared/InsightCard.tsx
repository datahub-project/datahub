import React from 'react';
import styled from 'styled-components/macro';
import { REDESIGN_COLORS } from '../../../../../../../entity/shared/constants';

const Card = styled.div<{ clickable: boolean; maxWidth: number; minWidth: number; height?: number }>`
    border-radius: 10px;
    background-color: #ffffff;
    padding: 16px;
    border: 2px solid transparent;
    max-width: ${(props) => props.maxWidth}px;
    min-width: ${(props) => props.minWidth}px;
    :hover {
        ${(props) => props.clickable && `border: 2px solid ${REDESIGN_COLORS.BLUE};`}
        ${(props) => props.clickable && 'cursor: pointer;'}
    }
    overflow: hidden;
    height: auto;
`;

const Title = styled.div`
    font-size: 16px;
    font-style: medium;
    color: #403d5c;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

type Props = {
    id?: string;
    title?: React.ReactNode;
    children?: React.ReactNode;
    maxWidth?: number;
    minWidth?: number;
    onClick?: () => void;
};

export const InsightCard = ({ id, title, children, maxWidth = 300, minWidth = 300, onClick }: Props) => {
    return (
        <Card id={id} maxWidth={maxWidth} minWidth={minWidth} clickable={onClick !== undefined} onClick={onClick}>
            <Title>{title}</Title>
            {children}
        </Card>
    );
};
