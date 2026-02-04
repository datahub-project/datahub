import React, { useContext, useEffect } from 'react';
import styled from 'styled-components';

import StatsSections from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSections';
import StatsHighlights from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/StatsHighlights';
import PartitionStatsTable from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/partitionStats/PartitionStatsTable';
import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import EntitySidebarContext from '@src/app/sharedV2/EntitySidebarContext';

const TabContainer = styled.div`
    padding: 16px 24px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const StatsTabV2 = () => {
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);
    const { statsEntity } = useStatsSectionsContext();
    const showPartitionStats = statsEntity?.__typename === 'Dataset';

    useEffect(() => {
        if (!isClosed) setSidebarClosed(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return (
        <>
            <TabContainer>
                <StatsHighlights />
                {showPartitionStats && <PartitionStatsTable />}
                <StatsSections />
            </TabContainer>
        </>
    );
};

export default StatsTabV2;
