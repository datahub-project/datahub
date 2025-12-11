/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
