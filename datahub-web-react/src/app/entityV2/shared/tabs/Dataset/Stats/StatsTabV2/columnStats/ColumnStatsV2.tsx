/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import ColumnStatsTable from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/ColumnStatsTable';
import { useGetStatsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsData';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { PageTitle, SearchBar } from '@src/alchemy-components';

const ColumnStatsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const ColumnStatsV2 = () => {
    const [searchQuery, setSearchQuery] = useState<string>('');
    const { columnStats } = useGetStatsData();
    const {
        setSectionState,
        sections,
        permissions: { canViewDatasetProfile },
    } = useStatsSectionsContext();

    const hasColumnStats = canViewDatasetProfile && columnStats?.length > 0;

    useEffect(() => {
        const currentSection = sections.columnStats;
        const newHasData = hasColumnStats;
        const loading = false;

        if (currentSection.hasData !== newHasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.COLUMN_STATS, newHasData, loading);
        }
    }, [hasColumnStats, sections.columnStats, setSectionState]);

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    if (!hasColumnStats) return null;

    return (
        <ColumnStatsContainer data-testid="column-stats-container">
            <PageTitle
                title="Column Stats"
                subTitle="View latest stats for each column in this table."
                variant="sectionHeader"
            />
            <SearchBar
                placeholder="Search Column Name"
                value={searchQuery}
                onChange={(value) => handleSearch(value)}
                data-testid="column-stats-search-bar"
            />
            <ColumnStatsTable columnStats={columnStats} searchQuery={searchQuery} />
        </ColumnStatsContainer>
    );
};

export default ColumnStatsV2;
