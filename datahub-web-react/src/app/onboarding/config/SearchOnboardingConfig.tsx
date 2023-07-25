import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const SEARCH_RESULTS_FILTERS_ID = 'search-results-filters';
export const SEARCH_RESULTS_ADVANCED_SEARCH_ID = 'search-results-advanced-search';
export const SEARCH_RESULTS_BROWSE_SIDEBAR_ID = 'search-results-browse-sidebar';
export const SEARCH_RESULTS_FILTERS_V2_INTRO = 'search-results-filters-v2-intro';

export const SearchOnboardingConfig: OnboardingStep[] = [
    {
        id: SEARCH_RESULTS_FILTERS_ID,
        selector: `#${SEARCH_RESULTS_FILTERS_ID}`,
        title: '🕵️ 构建您的查询',
        content: (
            <Typography.Paragraph>
                通过设置一个或多个过滤器，您可以快速找到相关数据资产. 尝试一下在 <strong>Type</strong>,{' '}
                <strong>Owner</strong>进行过滤, 您还可以过滤更多哦!
            </Typography.Paragraph>
        ),
    },
    {
        id: SEARCH_RESULTS_ADVANCED_SEARCH_ID,
        selector: `#${SEARCH_RESULTS_ADVANCED_SEARCH_ID}`,
        title: '💪 使用高级过滤',
        content: (
            <Typography.Paragraph>
                <strong>高级过滤</strong> 为特殊查询需求提供额外的能力.
            </Typography.Paragraph>
        ),
    },
    {
        id: SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
        selector: `#${SEARCH_RESULTS_BROWSE_SIDEBAR_ID}`,
        title: '🧭 通过 platform 浏览和查找',
        style: { minWidth: '425px' },
        content: (
            <Typography.Paragraph>
                Have a clear idea of the schema or folder you&apos;re searching for? Easily navigate your
                organization&apos;s platforms inline. Then select a specific container you want to filter your results
                by.
            </Typography.Paragraph>
        ),
    },
    {
        id: SEARCH_RESULTS_FILTERS_V2_INTRO,
        prerequisiteStepId: SEARCH_RESULTS_FILTERS_ID,
        selector: `#${SEARCH_RESULTS_FILTERS_V2_INTRO}`,
        title: 'Filters Have Moved',
        content: (
            <Typography.Paragraph>
                Quickly find relevant assets with our new and improved filter interface! Our latest update has relocated
                filters to the top of the screen for ease of access.
            </Typography.Paragraph>
        ),
    },
];
