import React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    color: ${(props) => props.theme.colors.textDisabled};
    background-color: ${(props) => props.theme.colors.bgSurface};
    opacity: 0.9;
    border-color: black;
    border-radius: 6px;
    border: 1px solid ${(props) => props.theme.colors.textDisabled};
    padding-right: 6px;
    padding-left: 6px;
    margin-right: 4px;
    margin-left: 4px;
`;

const Letter = styled.span`
    padding: 2px;
`;

export const CommandK = () => {
    return (
        <Container>
            <Letter>⌘</Letter>
            <Letter>K</Letter>
        </Container>
    );
};
