import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const ENTITY_PROFILE_V2_COLUMNS_ID = 'entity-profile-v2-columns';

const EntityProfileOnboardingConfig: OnboardingStep[] = [
    {
        id: ENTITY_PROFILE_V2_COLUMNS_ID,
        selector: `[id^='rc-tabs'][id$='Columns']`,
        title: 'Columns Tab',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view a Dataset&apos;s <strong>Schema</strong> on this tab.
                </p>
                <p>
                    You can also view or add <strong>Documentation</strong>, <strong>Tags</strong>, and{' '}
                    <strong>Glossary Terms</strong> for specific columns.
                </p>
            </Typography.Paragraph>
        ),
    },
];

export default EntityProfileOnboardingConfig;
