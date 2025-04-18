import React, { useEffect, useState } from 'react';
import ColumnStatsV2 from './columnStats/ColumnStatsV2';
import ChangeHistoryGraph from './graphs/ChangeHistoryGraph/ChangeHistoryGraph';
import QueryCountChart from './graphs/QueryCountGraph/QueryCountChart';
import StorageSizeGraph from './graphs/StorageSizeGraph/StorageSizeGraph';
import RowsAndUsers from './historical/RowsAndUsers';
import { Section, useStatsSectionsContext } from './StatsSectionsContext';
import { SectionKeys, SectionsToDisplay } from './utils';

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
