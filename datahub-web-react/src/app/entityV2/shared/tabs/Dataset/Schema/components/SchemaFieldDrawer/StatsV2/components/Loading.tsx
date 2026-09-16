import React from 'react';
import styled from 'styled-components';

import { Loader } from '@src/alchemy-components';

const Container = styled.div`
    display: flex;
    height: 100%;
`;

export default function Loading() {
    return (
        <Container>
            <Loader />
        </Container>
    );
}
