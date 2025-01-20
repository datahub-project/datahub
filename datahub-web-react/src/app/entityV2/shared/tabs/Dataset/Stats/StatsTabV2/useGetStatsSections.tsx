import React from 'react';
import ColumnStatsV2 from './columnStats/ColumnStatsV2';
import ChangeHistoryGraph from './graphs/ChangeHistoryGraph/ChangeHistoryGraph';
import QueryCountChart from './graphs/QueryCountGraph/QueryCountChart';
import StorageSizeGraph from './graphs/StorageSizeGraph/StorageSizeGraph';
import RowsAndUsers from './historical/RowsAndUsers';
import { useStatsSectionsContext } from './StatsSectionsContext';
import { SectionKeys } from './utils';

export const useGetStatsSections = () => {
    const { sections } = useStatsSectionsContext();

    const historicalSections: SectionKeys[] = [
        SectionKeys.ROWS_AND_USERS,
        SectionKeys.QUERIES,
        SectionKeys.STORAGE,
        SectionKeys.CHANGES,
    ];

    const hasHistoricalStats = historicalSections.some((key) => sections[key].hasData);

    const sectionsList: Record<SectionKeys, React.ReactNode> = {
        rowsAndUsers: <RowsAndUsers />,
        queries: <QueryCountChart />,
        storage: <StorageSizeGraph />,
        changes: <ChangeHistoryGraph />,
        columnStats: <ColumnStatsV2 />,
    };

    const orderedSections = Object.entries(sections).sort(([, a], [, b]) => Number(b.hasData) - Number(a.hasData));

    const scrollToSection = (sectionKey: string) => {
        sections[sectionKey]?.ref?.current?.scrollIntoView({
            behavior: 'smooth',
        });
    };

    return { hasHistoricalStats, sectionsList, orderedSections, scrollToSection };
};
