import React from 'react';
import { PageTitle } from '@src/alchemy-components';
import { Maybe, UsageAggregation, UserUsageCounts } from '@src/types.generated';
import styled from 'styled-components';
import TopUsers from './TopUsers';
import ChangeHistoryGraph from './graphs/ChangeHistoryGraph/ChangeHistoryGraph';
import QueryCountChart from './graphs/QueryCountGraph/QueryCountChart';
import RowCountGraph from './graphs/RowCountGraph/RowCountGraph';
import StorageSizeGraph from './graphs/StorageSizeGraph/StorageSizeGraph';

const ChartsRow = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
`;

interface Props {
    users?: Array<Maybe<UserUsageCounts>>;
    queryCountBuckets?: Array<Maybe<UsageAggregation>>;
    urn?: string;
}

const HistoricalStats = ({ users, queryCountBuckets, urn }: Props) => {
    return (
        <>
            <PageTitle title="Historical" subTitle="View important trends for this table" variant="sectionHeader" />
            <ChartsRow>
                <RowCountGraph urn={urn} />
                <TopUsers users={users || undefined} />
            </ChartsRow>
            <QueryCountChart queryCountBuckets={queryCountBuckets} />
            <StorageSizeGraph urn={urn} />
            <ChangeHistoryGraph urn={urn} />
        </>
    );
};

export default HistoricalStats;
