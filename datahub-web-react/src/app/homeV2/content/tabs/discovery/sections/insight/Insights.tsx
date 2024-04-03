import React from 'react';

import { FoundationalAssetsCard } from './cards/FoundationalAssetsCard';
import { MostUsersCard } from './cards/MostUsersCard';
import { MostQueriedCard } from './cards/MostQueriedCard';
import { MostFrequentlyUpdated } from './cards/MostFrequentlyUpdated';
import { PersonaType } from '../../../../../shared/types';
import { MostViewedDashboardsCard } from './cards/MostViewedDashboards';
import { useUserPersona } from '../../../../../persona/useUserPersona';
import { MostRowsCard } from './cards/MostRowsCard';
import { RecentlyUpdatedDatasetsCard } from './cards/RecentlyUpdatedDatasetsCard';
import { RecentlyCreatedDatasetsCard } from './cards/RecentlyCreatedDatasetsCard';
import { InsightsSection } from './InsightsSection';
import { InsightStatusProvider } from './InsightStatusProvider';

type InsightSection = {
    id: string;
    component: React.ComponentType;
    personas?: PersonaType[];
};

const ALL_INSIGHTS: InsightSection[] = [
    {
        id: 'FoundationalAssets',
        component: FoundationalAssetsCard,
        personas: [PersonaType.BUSINESS_USER, PersonaType.TECHNICAL_USER, PersonaType.DATA_STEWARD, PersonaType.DATA_LEADER],
    },
    {
        id: 'MostUsers',
        component: MostUsersCard,
        personas: [PersonaType.TECHNICAL_USER, PersonaType.DATA_STEWARD, PersonaType.DATA_LEADER],
    },
    {
        id: 'MostQueried',
        component: MostQueriedCard,
        personas: [PersonaType.BUSINESS_USER, PersonaType.TECHNICAL_USER, PersonaType.DATA_STEWARD, PersonaType.DATA_LEADER],
    },
    {
        id: 'MostViewedDashboards',
        component: MostViewedDashboardsCard,
        personas: [PersonaType.BUSINESS_USER, PersonaType.DATA_STEWARD, PersonaType.DATA_LEADER],
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
