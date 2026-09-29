import React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    color: ${(props) => props.theme.colors.icon};
    background-color: ${(props) => props.theme.colors.bg};
    opacity: 0.9;
    border-radius: 6px;
    border: 1px solid ${(props) => props.theme.colors.icon};
    padding-right: 6px;
    padding-left: 6px;
    margin-right: 4px;
    margin-left: 4px;
`;

const Letter = styled.span`
    padding: 2px;
`;

export const CommandK = () => {
    // (untranslated-text) keyboard symbols ⌘ and K are not translatable text
    /* eslint-disable i18next/no-literal-string */
    return (
        <Container>
            <Letter>⌘</Letter>
            <Letter>K</Letter>
        </Container>
    );
    /* eslint-enable i18next/no-literal-string */
};
