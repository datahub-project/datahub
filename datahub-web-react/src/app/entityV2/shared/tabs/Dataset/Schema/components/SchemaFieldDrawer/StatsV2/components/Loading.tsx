import React from 'react';
import { Loader } from '@src/alchemy-components';
import styled from 'styled-components';

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
