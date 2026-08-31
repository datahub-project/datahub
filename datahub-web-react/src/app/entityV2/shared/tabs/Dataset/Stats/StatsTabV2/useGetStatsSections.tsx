import React, { useEffect, useState } from 'react';

import {
    Section,
    useStatsSectionsContext,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import ColumnStatsV2 from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/ColumnStatsV2';
import ChangeHistoryGraph from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/ChangeHistoryGraph';
import QueryCountChart from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/QueryCountGraph/QueryCountChart';
import StorageSizeGraph from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/StorageSizeGraph/StorageSizeGraph';
import RowsAndUsers from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/historical/RowsAndUsers';
import { SectionKeys, SectionsToDisplay } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';

export const useGetStatsSections = () => {
    const { sections, areSectionsOrdered, setAreSectionsOrdered } = useStatsSectionsContext();
    const [orderedSections, setOrderedSections] = useState<[string, Section][]>(Object.entries(sections));

    const sectionsList: Record<SectionsToDisplay, React.ReactNode> = {
        rowsAndUsers: <RowsAndUsers />,
        queries: <QueryCountChart />,
        storage: <StorageSizeGraph />,
        changes: <ChangeHistoryGraph />,
        columnStats: <ColumnStatsV2 />,
    };

    const isDisplaySection = (section: [string, Section]) => {
        const [key] = section;
        return key !== SectionKeys.ROWS && key !== SectionKeys.USERS;
    };

    // Reorder sections only once after all the sections are loaded
    useEffect(() => {
        const reorderSections = () => {
            setOrderedSections(
                Object.entries(sections)
                    .filter(isDisplaySection)
                    .sort(([, a], [, b]) => Number(b.hasData) - Number(a.hasData)),
            );
            setAreSectionsOrdered(true);
        };

        const areAllSectionsLoaded = Object.values(sections).every((section) => !section.isLoading);
        if (areAllSectionsLoaded && !areSectionsOrdered) {
            reorderSections();
        }
    }, [sections, areSectionsOrdered, setAreSectionsOrdered]);

    const scrollToSection = (sectionKey: string) => {
        sections[sectionKey]?.ref?.current?.scrollIntoView({
            behavior: 'smooth',
        });
    };

    return { sectionsList, orderedSections, scrollToSection };
};
