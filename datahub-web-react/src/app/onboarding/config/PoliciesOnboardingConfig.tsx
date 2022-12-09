import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const POLICIES_INTRO_ID = 'policies-intro';
export const POLICIES_CREATE_POLICY_ID = 'policies-create-policy';

export const PoliciesOnboardingConfig: OnboardingStep[] = [
    {
        id: POLICIES_INTRO_ID,
        title: 'Policies',
        content: (
            <Typography.Paragraph>
                <p>
                    Welcome to DataHub <strong>Policies</strong>!
                </p>
                <p>
                    If you need fine-grained access controls, <strong>Policies</strong> will do the trick.
                </p>
                <p>
                    For most users, <strong>Roles</strong> are the recommended way to manage permissions on DataHub. If
                    Roles do not fit your use case, then Policies can be used.
                </p>
                <p>
                    Learn more about <strong>Policies</strong>{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/authorization/policies"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: POLICIES_CREATE_POLICY_ID,
        selector: `#${POLICIES_CREATE_POLICY_ID}`,
        title: 'Create a new Policy',
        content: (
            <Typography.Paragraph>
                <p>
                    Click here to create a new <strong>Policy</strong>.
                </p>
            </Typography.Paragraph>
        ),
    },
];
