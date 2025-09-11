import React from 'react';
import styled from 'styled-components';

import Header from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/StatsAndInsights/components/Header';
import Metrics from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/StatsAndInsights/components/Metrics';

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
