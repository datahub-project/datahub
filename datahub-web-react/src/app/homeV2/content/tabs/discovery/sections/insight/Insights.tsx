import React from 'react';

import { useUserPersona } from '../../../../../persona/useUserPersona';
import { PersonaType } from '../../../../../shared/types';
import { InsightStatusProvider } from './InsightStatusProvider';
import { InsightsSection } from './InsightsSection';
import { MostFrequentlyUpdated } from './cards/MostFrequentlyUpdated';
import { MostQueriedCard } from './cards/MostQueriedCard';
import { MostRowsCard } from './cards/MostRowsCard';
import { MostUsersCard } from './cards/MostUsersCard';
import { MostViewedDashboardsCard } from './cards/MostViewedDashboards';
import { PopularGlossaryTerms } from './cards/PopularGlossaryTerms';
import { RecentlyCreatedDatasetsCard } from './cards/RecentlyCreatedDatasetsCard';
import { RecentlyUpdatedDatasetsCard } from './cards/RecentlyUpdatedDatasetsCard';

type InsightSection = {
    id: string;
    component: React.ComponentType;
    personas?: PersonaType[];
};

const ALL_INSIGHTS: InsightSection[] = [
    {
        id: 'MostUsers',
        component: MostUsersCard,
        personas: [PersonaType.TECHNICAL_USER, PersonaType.DATA_STEWARD, PersonaType.DATA_LEADER],
    },
    {
        id: 'MostViewedDashboards',
        component: MostViewedDashboardsCard,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
            PersonaType.TECHNICAL_USER,
        ],
    },
    {
        id: 'Popular Glossary Terms',
        component: PopularGlossaryTerms,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
            PersonaType.BUSINESS_USER,
        ],
    },
    {
        id: 'MostQueried',
        component: MostQueriedCard,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
        ],
    },
    {
        id: 'MostFrequentlyUpdated',
        component: MostFrequentlyUpdated,
        personas: [PersonaType.TECHNICAL_USER, PersonaType.DATA_STEWARD, PersonaType.DATA_LEADER],
    },
    {
        id: 'RecentlyUpdatedDatasets',
        component: RecentlyUpdatedDatasetsCard,
        personas: [PersonaType.TECHNICAL_USER],
    },
    {
        id: 'RecentlyCreatedDatasets',
        component: RecentlyCreatedDatasetsCard,
        personas: [PersonaType.TECHNICAL_USER],
    },
    {
        id: 'MostRows',
        component: MostRowsCard,
        personas: [PersonaType.TECHNICAL_USER],
    },
];

export const Insights = () => {
    const currentUserPersona = useUserPersona();
    return (
        <InsightStatusProvider>
            <InsightsSection>
                {ALL_INSIGHTS.filter(
                    (section) => !section.personas || section.personas.includes(currentUserPersona),
                ).map((section) => (
                    <section.component key={section.id} />
                ))}
            </InsightsSection>
        </InsightStatusProvider>
    );
};
