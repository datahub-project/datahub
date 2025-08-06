import { Typography } from 'antd';
import React from 'react';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const ROLES_INTRO_ID = 'roles-intro';

export const RolesOnboardingConfig: OnboardingStep[] = [
    {
        id: ROLES_INTRO_ID,
        title: 'Roles',
        content: (
            <Typography.Paragraph>
                <p>
                    Welcome to DataHub <strong>Roles</strong>!
                </p>
                <p>
                    <strong>Roles</strong> are the recommended way to manage permissions on DataHub.
                </p>
                <p>
                    DataHub currently supports three out-of-the-box Roles: <strong>Admin</strong>,{' '}
                    <strong>Editor</strong> and <strong>Reader</strong>.
                </p>
                <p>
                    Learn more about <strong>Roles</strong> and the different permissions for each Role{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://docs.datahub.com/docs/authorization/roles"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
];
