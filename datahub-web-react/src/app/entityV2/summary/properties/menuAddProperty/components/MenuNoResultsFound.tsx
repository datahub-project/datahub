import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

const NoResultsFoundWrapper = styled.div`
    text-align: center;
    padding: 4px;
`;

export default function MenuNoResultsFound() {
    return (
        <NoResultsFoundWrapper>
            <Text color="gray">No results found</Text>
        </NoResultsFoundWrapper>
    );
}
