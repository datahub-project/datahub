import { PageTitle } from '@src/alchemy-components';
import { Maybe, UsageAggregation, UserUsageCounts } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import TopUsers from './TopUsers';
import ChangeHistoryGraph from './graphs/ChangeHistoryGraph/ChangeHistoryGraph';
import QueryCountChart from './graphs/QueryCountGraph/QueryCountChart';
import RowCountGraph from './graphs/RowCountGraph/RowCountGraph';
import StorageSizeGraph from './graphs/StorageSizeGraph/StorageSizeGraph';
import { SectionKeys } from './utils';

const ChartsRow = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
`;

interface Props {
    users?: Array<Maybe<UserUsageCounts>>;
    queryCountBuckets?: Array<Maybe<UsageAggregation>>;
    urn?: string;
    sectionRefs: Record<SectionKeys, React.RefObject<HTMLDivElement>>;
}

const HistoricalStats = ({ users, queryCountBuckets, urn, sectionRefs }: Props) => {
    return (
        <>
            <PageTitle title="Historical" subTitle="View important trends for this table" variant="sectionHeader" />
            <ChartsRow ref={sectionRefs.rowsAndUsers}>
                <RowCountGraph urn={urn} />
                <TopUsers users={users || undefined} />
            </ChartsRow>
            <div ref={sectionRefs.queries}>
                <QueryCountChart queryCountBuckets={queryCountBuckets} />
            </div>
            <StorageSizeGraph urn={urn} />
            <div ref={sectionRefs.changes}>
                <ChangeHistoryGraph urn={urn} />
            </div>
        </>
    );
};

export default HistoricalStats;
