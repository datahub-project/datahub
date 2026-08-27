import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const SEARCH_RESULTS_FILTERS_ID = 'search-results-filters';
export const SEARCH_RESULTS_ADVANCED_SEARCH_ID = 'search-results-advanced-search';
export const SEARCH_RESULTS_BROWSE_SIDEBAR_ID = 'search-results-browse-sidebar';
export const SEARCH_RESULTS_FILTERS_V2_INTRO = 'search-results-filters-v2-intro';

export const SearchOnboardingConfig: OnboardingStep[] = [
    {
        id: SEARCH_RESULTS_FILTERS_ID,
        selector: `#${SEARCH_RESULTS_FILTERS_ID}`,
        title: <Trans i18nKey="onboarding:search.filtersTitle" />,
        content: (
            <Text type="div" size="md">
                <Trans i18nKey="onboarding:search.filtersDescription" components={{ bold: <strong /> }} />
            </Text>
        ),
    },
    {
        id: SEARCH_RESULTS_ADVANCED_SEARCH_ID,
        selector: `#${SEARCH_RESULTS_ADVANCED_SEARCH_ID}`,
        title: <Trans i18nKey="onboarding:search.advancedTitle" />,
        content: (
            <Text type="div" size="md">
                <Trans i18nKey="onboarding:search.advancedDescription" components={{ bold: <strong /> }} />
            </Text>
        ),
    },
    {
        id: SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
        selector: `#${SEARCH_RESULTS_BROWSE_SIDEBAR_ID}`,
        title: <Trans i18nKey="onboarding:search.browseTitle" />,
        style: { minWidth: '425px' },
        content: (
            <Text type="div" size="md">
                <Trans i18nKey="onboarding:search.browseDescription" />
            </Text>
        ),
    },
    {
        id: SEARCH_RESULTS_FILTERS_V2_INTRO,
        prerequisiteStepId: SEARCH_RESULTS_FILTERS_ID,
        selector: `#${SEARCH_RESULTS_FILTERS_V2_INTRO}`,
        title: <Trans i18nKey="onboarding:search.filtersMovedTitle" />,
        content: (
            <Text type="div" size="md">
                <Trans i18nKey="onboarding:search.filtersMovedDescription" />
            </Text>
        ),
    },
];
