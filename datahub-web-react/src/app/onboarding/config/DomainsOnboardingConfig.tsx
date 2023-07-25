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
                    欢迎使用 DataHub <strong>Domains</strong>!
                </p>
                <p>
                    <strong>Domains</strong> 是您组织内特定领域内的数据集合，通常是指业务领域。
                    比如<strong>Marketing</strong>相关.
                </p>
                <p>
                    了解更多 <strong>Domains</strong>{' '}
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
        title: '创建新的Domain',
        content: (
            <Typography.Paragraph>
                <p>
                    点击创建新的 <strong>Domain</strong>.
                </p>
            </Typography.Paragraph>
        ),
    },
];
