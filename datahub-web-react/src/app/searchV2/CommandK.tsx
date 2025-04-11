import React from 'react';
import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    color: ${(props) => (props.$isShowNavBarRedesign ? colors.gray[1700] : '#dcdcdc')};
    background-color: ${(props) => (props.$isShowNavBarRedesign ? colors.white : '#171723')};
    opacity: 0.9;
    border-color: black;
    border-radius: 6px;
    border: 1px solid ${(props) => (props.$isShowNavBarRedesign ? colors.gray[1700] : '#dcdcdc')};
    padding-right: 6px;
    padding-left: 6px;
    margin-right: 4px;
    margin-left: 4px;
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
        color: ${colors.gray[1700]};
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
