import React from 'react';
import styled from 'styled-components';

const SecondaryText = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
`;

/**
 * No results yet summarization.
 */
export const NoResultsSummary = () => {
    return <SecondaryText>This assertion has not been evaluated yet! Come back later to view results.</SecondaryText>;
};
