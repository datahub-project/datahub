import React from 'react';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const Container = styled.div<{ isShowNavBarRedesign?: boolean }>`
    color: ${(props) => (props.isShowNavBarRedesign ? REDESIGN_COLORS.GREY_300 : '#dcdcdc')};
    background-color: ${(props) => (props.isShowNavBarRedesign ? 'white' : '#171723')};
    opacity: 0.9;
    border-color: black;
    border-radius: 6px;
    border: 1px solid #dcdcdc;
    padding-right: 6px;
    padding-left: 6px;
    margin-right: 4px;
    margin-left: 4px;
    ${(props) =>
        props.isShowNavBarRedesign &&
        `
        height: 28px;
        display: flex;
    `}
`;

const Letter = styled.span<{ isShowNavBarRedesign?: boolean }>`
    padding: 2px;
    ${(props) =>
        props.isShowNavBarRedesign &&
        `
        color: ${REDESIGN_COLORS.GREY_300};
        text-align: center;
        line-height: 23px;
    `}
`;

export const CommandK = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <Container isShowNavBarRedesign={isShowNavBarRedesign}>
            <Letter isShowNavBarRedesign={isShowNavBarRedesign}>⌘</Letter>
            <Letter isShowNavBarRedesign={isShowNavBarRedesign}>K</Letter>
        </Container>
    );
};
