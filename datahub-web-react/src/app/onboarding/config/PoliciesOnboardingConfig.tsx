/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

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
