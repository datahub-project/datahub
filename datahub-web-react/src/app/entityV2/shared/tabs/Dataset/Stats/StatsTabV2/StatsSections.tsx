import React from 'react';
import { useGetStatsSections } from './useGetStatsSections';
import { SectionKeys } from './utils';
import HistoricalSectionHeader from './historical/HistoricalSectionHeader';

const StatsSections = () => {
    const { sectionsList, orderedSections } = useGetStatsSections();

    const isColumnStatsFirstSection = orderedSections[0][0] === SectionKeys.COLUMN_STATS;

    return (
        <>
            {!isColumnStatsFirstSection && <HistoricalSectionHeader />}
            {orderedSections.map(([key, section], index) => {
                const SectionComponent = sectionsList[key];
                return (
                    <>
                        {isColumnStatsFirstSection && index === 1 && <HistoricalSectionHeader />}
                        <div key={key} ref={section.ref}>
                            {SectionComponent}
                        </div>
                    </>
                );
            })}
        </>
    );
};

export default StatsSections;
