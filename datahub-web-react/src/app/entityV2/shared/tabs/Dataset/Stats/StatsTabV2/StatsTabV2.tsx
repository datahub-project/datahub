import EntitySidebarContext from '@src/app/sharedV2/EntitySidebarContext';
import React, { useContext, useEffect } from 'react';
import styled from 'styled-components';
import StatsHighlights from './highlights/StatsHighlights';
import StatsSections from './StatsSections';

const TabContainer = styled.div`
    padding: 16px 24px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const StatsTabV2 = () => {
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    useEffect(() => {
        if (!isClosed) setSidebarClosed(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return (
        <>
            <TabContainer>
                <StatsHighlights />
                <StatsSections />
            </TabContainer>
        </>
    );
};

export default StatsTabV2;
