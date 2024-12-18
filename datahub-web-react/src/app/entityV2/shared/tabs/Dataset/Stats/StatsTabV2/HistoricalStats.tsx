import { PageTitle } from '@src/alchemy-components';
import { Maybe, UsageAggregation, UserUsageCounts } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import QueryCountChart from './graphs/QueryCountGraph/QueryCountChart';
import TopUsers from './TopUsers';
import StorageSizeGraph from './graphs/StorageSizeGraph/StorageSizeGraph';
import RowCountGraph from './graphs/RowCountGraph/RowCountGraph';

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
        </>
    );
};

export default HistoricalStats;
