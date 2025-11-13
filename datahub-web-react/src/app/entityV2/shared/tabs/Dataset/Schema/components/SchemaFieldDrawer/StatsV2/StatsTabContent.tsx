import React from 'react';
import styled from 'styled-components';

import { StatsProps } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarView';
import { StatsTabContextProvider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/StatsTabContext';
import Loading from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/Loading';
import NoStats from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/NoStats';
import ChartsSection from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/ChartsSection';
import SamplesSection from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/SamplesSection';
import StatsAndInsightsSection from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/StatsAndInsights/StatsAndInsightsSection';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 16px;

    // Internal tab scrolling
    max-height: calc(100vh - 82px);
    overflow: hidden;
    overflow-y: auto;
    ::-webkit-scrollbar {
        display: none;
    }
`;

const FooterSpace = styled.div`
    // Add extra footer space to handle overlay by filds switcher (DrawerFooter)
    min-height: 80px;
`;

export default function StatsTabContent(props: StatsProps) {
    if (props.properties?.profilesDataLoading) return <Loading />;
    if (props.properties?.fieldProfile === undefined) return <NoStats />;

    return (
        <StatsTabContextProvider properties={props.properties}>
            <Container data-testid="stats-tab-container">
                <StatsAndInsightsSection />
                <SamplesSection />
                <ChartsSection />
                <FooterSpace />
            </Container>
        </StatsTabContextProvider>
    );
}
