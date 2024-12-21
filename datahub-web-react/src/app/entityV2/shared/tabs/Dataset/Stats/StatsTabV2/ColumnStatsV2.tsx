import { PageTitle, SearchBar } from '@src/alchemy-components';
import { DatasetFieldProfile } from '@src/types.generated';
import React, { useState } from 'react';
import styled from 'styled-components';
import ColumnStatsTable from './ColumnStatsTable';

const ColumnStatsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

interface Props {
    columnStats: Array<DatasetFieldProfile>;
}

const ColumnStatsV2 = ({ columnStats }: Props) => {
    const [searchQuery, setSearchQuery] = useState<string>('');

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    return (
        <ColumnStatsContainer>
            <PageTitle
                title="Column Stats"
                subTitle="View latest stats for each column in this table."
                variant="sectionHeader"
            />
            <SearchBar placeholder="Search Column Name" value={searchQuery} onChange={(value) => handleSearch(value)} />
            <ColumnStatsTable columnStats={columnStats} searchQuery={searchQuery} />
        </ColumnStatsContainer>
    );
};

export default ColumnStatsV2;
