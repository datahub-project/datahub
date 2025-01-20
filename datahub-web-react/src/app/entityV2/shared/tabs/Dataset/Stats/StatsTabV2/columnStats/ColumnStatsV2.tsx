import { PageTitle, SearchBar } from '@src/alchemy-components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useStatsSectionsContext } from '../StatsSectionsContext';
import { useGetStatsData } from '../useGetStatsData';
import { SectionKeys } from '../utils';
import ColumnStatsTable from './ColumnStatsTable';

const ColumnStatsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const ColumnStatsV2 = () => {
    const [searchQuery, setSearchQuery] = useState<string>('');
    const { columnStats } = useGetStatsData();
    const { sections, setSectionState } = useStatsSectionsContext();

    const hasColumnStats = columnStats?.length > 0;

    useEffect(() => {
        if (!sections.columnStats.hasData && hasColumnStats) setSectionState(SectionKeys.COLUMN_STATS, true);
        else if (sections.columnStats.hasData && !hasColumnStats) setSectionState(SectionKeys.COLUMN_STATS, false);
    }, [hasColumnStats, setSectionState, sections.columnStats]);

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    if (!hasColumnStats) return null;

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
