import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import StatsTabRowCountGraph from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/StatsTabRowCountGraph/StatsTabRowCountGraph';
import TopUsers from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/historical/TopUsers';
import { useGetStatsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsData';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';

const SectionWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const Container = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
`;

const GraphWrapper = styled.div`
    display: flex;
    width: 70%;

    @media screen and (max-width: 2300px) {
        // align with highlights cards
        max-width: 1225px;
    }
`;

const RowsAndUsers = () => {
    const {
        permissions: { canViewDatasetUsage },
        sections,
        setSectionState,
    } = useStatsSectionsContext();

    const { users: usersData } = useGetStatsData();
    const users = useMemo(() => (canViewDatasetUsage ? usersData : []), [usersData, canViewDatasetUsage]);

    useEffect(() => {
        const currentSection = sections.rowsAndUsers;
        const hasData = sections.rows.hasData || sections.users.hasData;
        const loading = sections.rows.isLoading || sections.users.isLoading;

        if (currentSection.hasData !== hasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.ROWS_AND_USERS, hasData, loading);
        }
    }, [sections.rows, sections.users, sections.rowsAndUsers, setSectionState]);

    return (
        <SectionWrapper>
            <Container>
                <GraphWrapper>
                    <StatsTabRowCountGraph />
                </GraphWrapper>
                <TopUsers users={users || undefined} />
            </Container>
        </SectionWrapper>
    );
};

export default RowsAndUsers;
