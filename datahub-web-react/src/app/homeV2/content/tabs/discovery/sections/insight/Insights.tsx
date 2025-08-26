import React, { useContext, useMemo } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { HorizontalListSkeletons } from '@app/homeV2/content/HorizontalListSkeletons';
import { Section } from '@app/homeV2/content/tabs/discovery/sections/Section';
import { InsightStatusProvider } from '@app/homeV2/content/tabs/discovery/sections/insight/InsightStatusProvider';
import {
    MOST_FREQUENTLY_UPDATED_ID,
    MostFrequentlyUpdated,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/MostFrequentlyUpdated';
import {
    MOST_QUERIED_ID,
    MostQueriedCard,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/MostQueriedCard';
import { MOST_ROWS_ID, MostRowsCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/MostRowsCard';
import { MOST_USERS_ID, MostUsersCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/MostUsersCard';
import {
    MOST_VIEWED_DASHBOARDS_ID,
    MostViewedDashboardsCard,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/MostViewedDashboards';
import {
    POPULAR_GLOSSARY_TERMS_ID,
    PopularGlossaryTerms,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/PopularGlossaryTerms';
import {
    RECENTLY_CREATED_DATASETS_ID,
    RecentlyCreatedDatasetsCard,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/RecentlyCreatedDatasetsCard';
import {
    RECENTLY_UPDATED_ID,
    RecentlyUpdatedDatasetsCard,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/RecentlyUpdatedDatasetsCard';
import { INSIGHT_CARD_MIN_WIDTH } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import InsightCardSkeleton from '@app/homeV2/content/tabs/discovery/sections/insight/shared/InsightCardSkeleton';
import { useUserPersona } from '@app/homeV2/persona/useUserPersona';
import { PersonaType } from '@app/homeV2/shared/types';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { HOME_PAGE_INSIGHTS_ID } from '@app/onboarding/config/HomePageOnboardingConfig';
import HorizontalScroller from '@app/sharedV2/carousel/HorizontalScroller';

const GAP_PX = 12;

type InsightSection = {
    id: string;
    component: React.ComponentType;
    personas?: PersonaType[];
};

const ALL_INSIGHTS: InsightSection[] = [
    {
        id: MOST_USERS_ID,
        component: MostUsersCard,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_ENGINEER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
        ],
    },
    {
        id: MOST_VIEWED_DASHBOARDS_ID,
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
        id: POPULAR_GLOSSARY_TERMS_ID,
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
        id: MOST_QUERIED_ID,
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
        id: MOST_FREQUENTLY_UPDATED_ID,
        component: MostFrequentlyUpdated,
        personas: [
            PersonaType.TECHNICAL_USER,
            PersonaType.DATA_ENGINEER,
            PersonaType.DATA_STEWARD,
            PersonaType.DATA_LEADER,
        ],
    },
    {
        id: RECENTLY_UPDATED_ID,
        component: RecentlyUpdatedDatasetsCard,
        personas: [PersonaType.TECHNICAL_USER, PersonaType.DATA_ENGINEER],
    },
    {
        id: RECENTLY_CREATED_DATASETS_ID,
        component: RecentlyCreatedDatasetsCard,
        personas: [PersonaType.TECHNICAL_USER, PersonaType.DATA_ENGINEER],
    },
    {
        id: MOST_ROWS_ID,
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
    const filteredInsights = useMemo(
        () => ALL_INSIGHTS.filter((section) => !section.personas || section.personas.includes(currentUserPersona)),
        [currentUserPersona],
    );

    if (!loaded || isUserInitializing) {
        return <HorizontalListSkeletons Component={InsightCardSkeleton} />;
    }

    if (!filteredInsights.length) return null;

    return (
        <InsightStatusProvider displayedInsightIds={filteredInsights.map((insight) => insight.id)}>
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
