import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const DOMAINS_INTRO_ID = 'domains-intro';
export const DOMAINS_CREATE_DOMAIN_ID = 'domains-create-domain';

export const DomainsOnboardingConfig: OnboardingStep[] = [
    {
        id: DOMAINS_INTRO_ID,
        title: 'Domains',
        content: (
            <Typography.Paragraph>
                <p>
                    Welcome to DataHub <strong>Domains</strong>!
                </p>
                <p>
                    <strong>Domains</strong> are collections of related data assets associated with a specific part of
                    your organization, such as the <strong>Marketing</strong> department.
                </p>
                <p>
                    Learn more about <strong>Domains</strong>{' '}
                    <a target="_blank" rel="noreferrer noopener" href="https://datahubproject.io/docs/domains">
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: DOMAINS_CREATE_DOMAIN_ID,
        selector: `#${DOMAINS_CREATE_DOMAIN_ID}`,
        title: 'Create a new Domain',
        content: (
            <Typography.Paragraph>
                <p>
                    Click here to create a new <strong>Domain</strong>.
                </p>
            </Typography.Paragraph>
        ),
    },
];
