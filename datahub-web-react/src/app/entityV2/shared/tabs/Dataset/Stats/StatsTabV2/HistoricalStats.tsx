import { PageTitle } from '@src/alchemy-components';
import { Maybe, UserUsageCounts } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import TopUsers from './TopUsers';
import RowCountGraph from './graphs/RowCountGraph/RowCountGraph';

const ChartsRow = styled.div`
    display: flex;
    gap: 8px;
`;

interface Props {
    users?: Array<Maybe<UserUsageCounts>>;
}

const HistoricalStats = ({ users }: Props) => {
    const { entityData } = useEntityData();
    return (
        <>
            <PageTitle title="Historical" subTitle="View important trends for this table" variant="sectionHeader" />
            <ChartsRow>
                <RowCountGraph urn={entityData?.urn} />
                <TopUsers users={users || undefined} />
            </ChartsRow>
        </>
    );
};

export default HistoricalStats;
