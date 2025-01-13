import React, { useEffect } from 'react';
import styled from 'styled-components';
import RowCountGraph from '../graphs/RowCountGraph/RowCountGraph';
import { useStatsSectionsContext } from '../StatsSectionsContext';
import { useGetStatsData } from '../useGetStatsData';
import { useGetStatsSections } from '../useGetStatsSections';
import { SectionKeys } from '../utils';
import HistoricalSectionHeader from './HistoricalSectionHeader';
import TopUsers from './TopUsers';

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
    const { hasHistoricalStats } = useGetStatsSections();
    const { users } = useGetStatsData();

    const { sections, setSectionState } = useStatsSectionsContext();

    useEffect(() => {
        if (!sections.rowsAndUsers.hasData && users && users.length > 0)
            setSectionState(SectionKeys.ROWS_AND_USERS, true);
    }, [users, setSectionState, sections.rowsAndUsers]);

    return (
        <SectionWrapper>
            {!hasHistoricalStats && <HistoricalSectionHeader />}
            <Container>
                <RowCountGraph users={users || undefined} />
                <TopUsers users={users || undefined} />
            </Container>
        </SectionWrapper>
    );
};

export default RowsAndUsers;
