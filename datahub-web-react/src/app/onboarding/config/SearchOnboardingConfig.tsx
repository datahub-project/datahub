import { Typography } from 'antd';
import React from 'react';

import { OnboardingStep } from '@app/onboarding/types';

export const SEARCH_RESULTS_FILTERS_ID = 'search-results-filters';
export const SEARCH_RESULTS_ADVANCED_SEARCH_ID = 'search-results-advanced-search';
export const SEARCH_RESULTS_BROWSE_SIDEBAR_ID = 'search-results-browse-sidebar';
export const SEARCH_RESULTS_FILTERS_V2_INTRO = 'search-results-filters-v2-intro';

export const SearchOnboardingConfig: OnboardingStep[] = [
    {
        id: SEARCH_RESULTS_FILTERS_ID,
        selector: `#${SEARCH_RESULTS_FILTERS_ID}`,
        title: '🕵️ Narrow your search',
        content: (
            <Typography.Paragraph>
                Quickly find relevant assets by applying one or more filters. Try filtering by <strong>Type</strong>,{' '}
                <strong>Owned By</strong>, and more!
            </Typography.Paragraph>
        ),
    },
    {
        id: SEARCH_RESULTS_ADVANCED_SEARCH_ID,
        selector: `#${SEARCH_RESULTS_ADVANCED_SEARCH_ID}`,
        title: '💪 Dive deeper with advanced filters',
        content: (
            <Typography.Paragraph>
                <strong>Advanced Filters</strong> offer additional capabilities to create more specific search queries.
            </Typography.Paragraph>
        ),
    },
    {
        id: SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
        selector: `#${SEARCH_RESULTS_BROWSE_SIDEBAR_ID}`,
        title: '🧭 Explore and refine your search by platform',
        style: { minWidth: '425px' },
        content: (
            <Typography.Paragraph>
                Have a clear idea of the schema or folder you&apos;re searching for? Easily navigate your
                organization&apos;s platforms inline. Then select a specific container you want to filter your results
                by.
            </Typography.Paragraph>
        ),
    },
];
