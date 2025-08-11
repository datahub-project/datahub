import React from 'react';

import HistoricalSectionHeader from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/historical/HistoricalSectionHeader';
import { useGetStatsSections } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsSections';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';

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
