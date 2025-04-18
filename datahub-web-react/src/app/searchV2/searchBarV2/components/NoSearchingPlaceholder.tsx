import React from 'react';
<<<<<<< HEAD
import styled from 'styled-components';

import { Text } from '@src/alchemy-components';
=======
import { Text } from '@src/alchemy-components';
import styled from 'styled-components';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
