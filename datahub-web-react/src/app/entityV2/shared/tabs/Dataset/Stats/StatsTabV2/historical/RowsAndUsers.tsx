import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components';
import RowCountGraph from '../graphs/RowCountGraph/RowCountGraph';
import { useStatsSectionsContext } from '../StatsSectionsContext';
import { useGetStatsData } from '../useGetStatsData';
import TopUsers from './TopUsers';
import { SectionKeys } from '../utils';

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
                <RowCountGraph />
                <TopUsers users={users || undefined} />
            </Container>
        </SectionWrapper>
    );
};

export default RowsAndUsers;
