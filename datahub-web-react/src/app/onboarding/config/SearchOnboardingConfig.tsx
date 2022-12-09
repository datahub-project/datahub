import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const SEARCH_RESULTS_FILTERS_ID = 'search-results-filters';
export const SEARCH_RESULTS_ADVANCED_SEARCH_ID = 'search-results-advanced-search';

export const SearchOnboardingConfig: OnboardingStep[] = [
    {
        id: SEARCH_RESULTS_FILTERS_ID,
        selector: `#${SEARCH_RESULTS_FILTERS_ID}`,
        title: 'Narrow your search ⚡',
        content: (
            <Typography.Paragraph>
                Quickly find relevant assets by applying one or more filters. Try filtering by{' '}
                <strong>Entity Type</strong>, <strong>Owner</strong>, and more!
            </Typography.Paragraph>
        ),
    },
    {
        id: SEARCH_RESULTS_ADVANCED_SEARCH_ID,
        selector: `#${SEARCH_RESULTS_ADVANCED_SEARCH_ID}`,
        title: 'Dive Deeper with Advanced Search 💪',
        content: (
            <Typography.Paragraph>
                Use <strong>Advanced Search</strong> to find specific assets using granular filter predicates.
            </Typography.Paragraph>
        ),
    },
];
