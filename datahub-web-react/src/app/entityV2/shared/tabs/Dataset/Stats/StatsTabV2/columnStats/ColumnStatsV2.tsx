import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import ColumnStatsTable from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/ColumnStatsTable';
import { useGetStatsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsData';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { PageTitle, SearchBar } from '@src/alchemy-components';
import { useGetDataProfilesLazyQuery } from '@src/graphql/dataset.generated';
import { FilterOperator } from '@src/types.generated';

const ColumnStatsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const MAX_FIELDS_FOR_COLUMN_STATS = 5000;

const ColumnStatsV2 = () => {
    const [searchQuery, setSearchQuery] = useState<string>('');
    const { columnCount } = useGetStatsData();
    const {
        setSectionState,
        sections,
        statsEntityUrn,
        permissions: { canViewDatasetProfile },
    } = useStatsSectionsContext();

    const [getColumnStats, { data: columnStatsData, loading: columnStatsLoading }] =
        useGetDataProfilesLazyQuery();
    const columnStats = columnStatsData?.dataset?.datasetProfiles?.[0]?.fieldProfiles || [];
    const isTooLarge = !!columnCount && columnCount > MAX_FIELDS_FOR_COLUMN_STATS;
    const hasColumnStats = canViewDatasetProfile && columnStats.length > 0 && !isTooLarge;

    useEffect(() => {
        const currentSection = sections.columnStats;
        const newHasData = hasColumnStats;
        const loading = columnStatsLoading;

        if (currentSection.hasData !== newHasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.COLUMN_STATS, newHasData, loading);
        }
    }, [hasColumnStats, sections.columnStats, setSectionState, columnStatsLoading]);

    useEffect(() => {
        if (!statsEntityUrn || !canViewDatasetProfile || isTooLarge) return;
        getColumnStats({
            variables: {
                urn: statsEntityUrn,
                limit: 1,
                filters: {
                    and: [
                        {
                            field: 'partitionSpec.partition',
                            values: ['FULL_TABLE_SNAPSHOT', 'SAMPLE'],
                            condition: FilterOperator.StartWith,
                        },
                    ],
                },
            },
            fetchPolicy: 'cache-first',
        });
    }, [statsEntityUrn, canViewDatasetProfile, isTooLarge, getColumnStats]);

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    const columnStatsMessage = useMemo(() => {
        if (!canViewDatasetProfile) return null;
        if (isTooLarge) {
            return `Column stats are disabled for tables with more than ${MAX_FIELDS_FOR_COLUMN_STATS.toLocaleString()} columns.`;
        }
        if (!columnStatsLoading && columnStats.length === 0) {
            return 'No column stats have been collected for this table yet.';
        }
        return null;
    }, [canViewDatasetProfile, isTooLarge, columnStatsLoading, columnStats.length]);

    if (!canViewDatasetProfile) return null;

    return (
        <ColumnStatsContainer>
            <PageTitle
                title="Column Stats"
                subTitle="View latest stats for each column in this table."
                variant="sectionHeader"
            />
            {columnStatsMessage ? (
                <div>{columnStatsMessage}</div>
            ) : (
                <>
                    <SearchBar
                        placeholder="Search Column Name"
                        value={searchQuery}
                        onChange={(value) => handleSearch(value)}
                    />
                    <ColumnStatsTable columnStats={columnStats} searchQuery={searchQuery} />
                </>
            )}
        </ColumnStatsContainer>
    );
};

export default ColumnStatsV2;
