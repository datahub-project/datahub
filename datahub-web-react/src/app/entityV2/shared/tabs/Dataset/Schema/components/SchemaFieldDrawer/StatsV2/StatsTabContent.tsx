import React from 'react';
import styled from 'styled-components';
import { StatsProps } from '../StatsSidebarView';
import Loading from './components/Loading';
import NoStats from './components/NoStats';
import SamplesSection from './components/sections/Samples/SamplesSection';
import StatsAndInsightsSection from './components/sections/StatsAndInsights/StatsAndInsightsSection';
import { StatsTabContextProvider } from './StatsTabContext';
import ChartsSection from './components/sections/ChartsSection/ChartsSection';

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
            <Container>
                <StatsAndInsightsSection />
                <SamplesSection />
                <ChartsSection />
                <FooterSpace />
            </Container>
        </StatsTabContextProvider>
    );
}
