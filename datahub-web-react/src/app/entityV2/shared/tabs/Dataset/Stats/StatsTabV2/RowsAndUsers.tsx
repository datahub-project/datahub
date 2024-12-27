import { Maybe, UserUsageCounts } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import RowCountGraph from './graphs/RowCountGraph/RowCountGraph';
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

interface Props {
    hasHistoricalStats: boolean;
    urn?: string;
    users?: Array<Maybe<UserUsageCounts>>;
}

const RowsAndUsers = ({ hasHistoricalStats, urn, users }: Props) => {
    return (
        <SectionWrapper>
            {!hasHistoricalStats && <HistoricalSectionHeader />}
            <Container>
                <RowCountGraph urn={urn} />
                <TopUsers users={users || undefined} />
            </Container>
        </SectionWrapper>
    );
};

export default RowsAndUsers;
