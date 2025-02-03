import React from 'react';
import styled from 'styled-components';
import { StatsProps } from '../StatsSidebarView';
import Loading from './components/Loading';
import NoStats from './components/NoStats';
import StatsAndInsightsSection from './components/sections/StatsAndInsights/StatsAndInsightsSection';
import { StatsTabContextProvider } from './StatsTabContext';

const Container = styled.div`
    padding: 16px;
`;

export default function StatsTabContent(props: StatsProps) {
    if (props.properties?.profilesDataLoading) return <Loading />;
    if (props.properties?.fieldProfile === undefined) return <NoStats />;

    return (
        <StatsTabContextProvider properties={props.properties}>
            <Container>
                <StatsAndInsightsSection />
            </Container>
        </StatsTabContextProvider>
    );
}
