import { Text } from '@components';
import React from 'react';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const DOMAINS_INTRO_ID = 'domains-intro';
export const DOMAINS_CREATE_DOMAIN_ID = 'domains-create-domain';

export const DomainsOnboardingConfig: OnboardingStep[] = [
    {
        id: DOMAINS_INTRO_ID,
        title: 'Domains',
        content: (
            <Text type="div" size="md">
                <p>
                    Welcome to DataHub <strong>Domains</strong>!
                </p>
                <p>
                    <strong>Domains</strong> are collections of related data assets associated with a specific part of
                    your organization, such as the <strong>Marketing</strong> department.
                </p>
                <p>
                    Learn more about <strong>Domains</strong>{' '}
                    <a target="_blank" rel="noreferrer noopener" href="https://docs.datahub.com/docs/domains">
                        {' '}
                        here.
                    </a>
                </p>
            </Text>
        ),
    },
    {
        id: DOMAINS_CREATE_DOMAIN_ID,
        selector: `#${DOMAINS_CREATE_DOMAIN_ID}`,
        title: 'Create a new Domain',
        content: (
            <Text type="div" size="md">
                <p>
                    Click here to create a new <strong>Domain</strong>.
                </p>
            </Text>
        ),
    },
];
