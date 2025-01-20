import EntitySidebarContext from '@src/app/sharedV2/EntitySidebarContext';
import React, { useContext, useEffect } from 'react';
import styled from 'styled-components';
import StatsHighlights from './highlights/StatsHighlights';
import HistoricalSectionHeader from './historical/HistoricalSectionHeader';
import HistoricalStats from './historical/HistoricalStats';
import { useGetStatsSections } from './useGetStatsSections';

const TabContainer = styled.div`
    padding: 16px 24px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const StatsTabV2 = () => {
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    const { hasHistoricalStats } = useGetStatsSections();

    useEffect(() => {
        if (!isClosed) setSidebarClosed(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return (
        <>
            <TabContainer>
                <StatsHighlights />
                {hasHistoricalStats && <HistoricalSectionHeader />}
                <HistoricalStats />
            </TabContainer>
        </>
    );
};

export default StatsTabV2;
