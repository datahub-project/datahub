import React from 'react';
import styled from 'styled-components';

import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    color: ${(props) => (props.$isShowNavBarRedesign ? props.theme.colors.textSecondary : '#dcdcdc')};
    background-color: ${(props) => (props.$isShowNavBarRedesign ? props.theme.colors.bg : '#171723')};
    opacity: 0.9;
    border-color: ${(props) => props.theme.colors.border};
    border-radius: 6px;
    border: 1px solid ${(props) => (props.$isShowNavBarRedesign ? props.theme.colors.textSecondary : '#dcdcdc')};
    padding-right: 6px;
    padding-left: 6px;
    margin-right: 4px;
    margin-left: 4px;
    height: 24px;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        height: 28px;
        display: flex;
    `}
`;

const Letter = styled.span<{ $isShowNavBarRedesign?: boolean }>`
    padding: 2px;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        color: ${props.theme.colors.textSecondary};
        text-align: center;
        line-height: 23px;
    `}
`;

export const CommandK = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <Container $isShowNavBarRedesign={isShowNavBarRedesign}>
            <Letter $isShowNavBarRedesign={isShowNavBarRedesign}>⌘</Letter>
            <Letter $isShowNavBarRedesign={isShowNavBarRedesign}>K</Letter>
        </Container>
    );
};
