import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';

export const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
`;

const EmptySearchResults = () => {
    return (
        <EmptyContainer>
            <TextContainer>
                <Text size="lg" color="gray" weight="bold">
                    No search results!
                </Text>
            </TextContainer>
        </EmptyContainer>
    );
};

export default EmptySearchResults;
