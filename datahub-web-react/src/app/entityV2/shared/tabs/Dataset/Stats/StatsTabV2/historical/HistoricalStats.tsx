import React from 'react';
import { useGetStatsSections } from '../useGetStatsSections';

const HistoricalStats = () => {
    const { sectionsList, orderedSections } = useGetStatsSections();

    return (
        <>
            {orderedSections.map(([key, section]) => {
                const SectionComponent = sectionsList[key];
                return (
                    <div key={key} ref={section.ref}>
                        {SectionComponent}
                    </div>
                );
            })}
        </>
    );
};

export default HistoricalStats;
