import React from 'react';
import { Section } from './StatsSectionsContext';
import { SectionKeys } from './utils';

interface Props {
    sortedSections: [string, Section][];
    sectionsList: Record<SectionKeys, React.ReactNode>;
}

const HistoricalStats = ({ sortedSections, sectionsList }: Props) => {
    return (
        <>
            {sortedSections.map(([key, section]) => {
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
