import { Text } from '@components';
import React from 'react';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const POLICIES_INTRO_ID = 'policies-intro';
export const POLICIES_CREATE_POLICY_ID = 'policies-create-policy';

export const PoliciesOnboardingConfig: OnboardingStep[] = [
    {
        id: POLICIES_INTRO_ID,
        title: 'Policies',
        content: (
            <Text type="div" size="md">
                <p>
                    Welcome to DataHub <strong>Policies</strong>!
                </p>
                <p>
                    In most cases, <strong>Roles</strong> are the best option for granting privileges to DataHub users.
                </p>
                <p>
                    When more fine-grained control over user and group permissions is required, then{' '}
                    <strong>Policies</strong> will do the trick.
                </p>
                <p>
                    Learn more about <strong>Policies</strong>{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://docs.datahub.com/docs/authorization/policies"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Text>
        ),
    },
    {
        id: POLICIES_CREATE_POLICY_ID,
        selector: `#${POLICIES_CREATE_POLICY_ID}`,
        title: 'Create a new Policy',
        content: (
            <Text type="div" size="md">
                <p>
                    Click here to create a new <strong>Policy</strong>.
                </p>
            </Text>
        ),
    },
];
