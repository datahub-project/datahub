import React from 'react';
import { Text } from '@src/alchemy-components';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 8px 0;
`;

export default function NoSearchingPlaceholder() {
    return (
        <Container>
            <Text color="gray" colorLevel={600} size="md">
                Start searching
            </Text>
            <Text color="gray" size="sm">
                Search through your data catalog to find datasets, schemas, and metadata
            </Text>
        </Container>
    );
}
