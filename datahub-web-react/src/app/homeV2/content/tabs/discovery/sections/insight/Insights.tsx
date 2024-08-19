import React, { useContext } from 'react';
import styled from 'styled-components';
import { useUserContext } from '../../../../../../context/useUserContext';
import { HOME_PAGE_INSIGHTS_ID } from '../../../../../../onboarding/config/HomePageOnboardingConfig';
import OnboardingContext from '../../../../../../onboarding/OnboardingContext';
import HorizontalScroller from '../../../../../../sharedV2/carousel/HorizontalScroller';

import { useUserPersona } from '../../../../../persona/useUserPersona';
import { PersonaType } from '../../../../../shared/types';
import { HorizontalListSkeletons } from '../../../../HorizontalListSkeletons';
import { Section } from '../Section';
import { INSIGHT_CARD_MIN_WIDTH } from './cards/SearchListInsightCard';
import { InsightStatusProvider } from './InsightStatusProvider';
import { MostFrequentlyUpdated } from './cards/MostFrequentlyUpdated';
import { MostQueriedCard } from './cards/MostQueriedCard';
import { MostRowsCard } from './cards/MostRowsCard';
import { MostUsersCard } from './cards/MostUsersCard';
import { MostViewedDashboardsCard } from './cards/MostViewedDashboards';
import { PopularGlossaryTerms } from './cards/PopularGlossaryTerms';
import { RecentlyCreatedDatasetsCard } from './cards/RecentlyCreatedDatasetsCard';
import { RecentlyUpdatedDatasetsCard } from './cards/RecentlyUpdatedDatasetsCard';
import InsightCardSkeleton from './shared/InsightCardSkeleton';

const GAP_PX = 12;

type InsightSection = {
    id: string;
    component: React.ComponentType;
    personas?: PersonaType[];
};

const ALL_INSIGHTS: InsightSection[] = [
    {
        id: 'MostUsers',
        component: MostUsersCard,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_ENGINEER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
        ],
    },
    {
        id: 'MostViewedDashboards',
        component: MostViewedDashboardsCard,
        personas: [
            PersonaType.BUSINESS_USER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_ENGINEER,
        ],
    },
    {
        id: 'Popular Glossary Terms',
        component: PopularGlossaryTerms,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_ENGINEER,
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
            PersonaType.DATA_ENGINEER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
        ],
    },
    {
        id: 'MostFrequentlyUpdated',
        component: MostFrequentlyUpdated,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_ENGINEER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
        ],
    },
    {
        id: 'RecentlyUpdatedDatasets',
        component: RecentlyUpdatedDatasetsCard,
        personas: [PersonaType.TECHNICAL_USER, PersonaType.DATA_ENGINEER],
    },
    {
        id: 'RecentlyCreatedDatasets',
        component: RecentlyCreatedDatasetsCard,
        personas: [PersonaType.TECHNICAL_USER, PersonaType.DATA_ENGINEER],
    },
    {
        id: 'MostRows',
        component: MostRowsCard,
        personas: [PersonaType.TECHNICAL_USER, PersonaType.DATA_ENGINEER],
    },
];

const StyledCarousel = styled(HorizontalScroller)`
    display: flex;
    align-items: stretch;
    gap: ${GAP_PX}px;
`;

export const Insights = () => {
    const { loaded } = useUserContext();
    const { isUserInitializing } = useContext(OnboardingContext);
    const currentUserPersona = useUserPersona();
    const filteredInsights = ALL_INSIGHTS.filter(
        (section) => !section.personas || section.personas.includes(currentUserPersona),
    );

    if (!loaded || isUserInitializing) {
        return <HorizontalListSkeletons Component={InsightCardSkeleton} />;
    }

    if (!filteredInsights.length) return null;
    return (
        <InsightStatusProvider>
            <div id={HOME_PAGE_INSIGHTS_ID}>
                <Section title="For you">
                    <StyledCarousel scrollDistance={INSIGHT_CARD_MIN_WIDTH + GAP_PX}>
                        {filteredInsights.map((section) => (
                            <section.component key={section.id} />
                        ))}
                    </StyledCarousel>
                </Section>
            </div>
        </InsightStatusProvider>
    );
};
