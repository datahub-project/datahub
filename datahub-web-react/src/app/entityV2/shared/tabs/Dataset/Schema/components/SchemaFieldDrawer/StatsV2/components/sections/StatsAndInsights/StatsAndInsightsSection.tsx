import React from 'react';
import styled from 'styled-components';
import Header from './components/Header';
import Metrics from './components/Metrics';

const Container = styled.div`
    display: flex;
    flex-direction: column;
`;

export default function StatsAndInsightsSection() {
    return (
        <Container>
            <Header />
            <Metrics />
        </Container>
    );
}
