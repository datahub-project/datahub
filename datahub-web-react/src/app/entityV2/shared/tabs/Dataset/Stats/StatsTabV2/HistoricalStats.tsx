import { PageTitle } from '@src/alchemy-components';
import { Maybe, UserUsageCounts } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import TopUsers from './TopUsers';

const ChartsRow = styled.div`
    display: grid;
`;

interface Props {
    users?: Array<Maybe<UserUsageCounts>>;
}

const HistoricalStats = ({ users }: Props) => {
    return (
        <>
            <PageTitle title="Historical" subTitle="View important trends for this table" variant="sectionHeader" />
            <ChartsRow>
                <TopUsers users={users || undefined} />
            </ChartsRow>
        </>
    );
};

export default HistoricalStats;
