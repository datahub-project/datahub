import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const V2_SEARCH_BAR_ID = 'v2-search-bar';

const HomePageOnboardingConfig: OnboardingStep[] = [
    {
        id: V2_SEARCH_BAR_ID,
        selector: `#${V2_SEARCH_BAR_ID}`,
        title: 'Find your Data 🔍',
        content: (
            <Typography.Paragraph>
                <p>
                    This is the <strong>Search Bar</strong>. It will serve as your launch point for discovering and
                    collaborating around the data most important to you.
                </p>
                <p>
                    Not sure where to start? Click on <strong>Explore All</strong>!
                </p>
            </Typography.Paragraph>
        ),
    },
];

export default HomePageOnboardingConfig;
