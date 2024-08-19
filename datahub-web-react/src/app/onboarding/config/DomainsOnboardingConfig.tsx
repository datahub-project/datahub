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
                Bem-vindo ao DataHub <strong>Domínios</strong>!
                </p>
                <p>
                    <strong>Domínios</strong> são coleções de ativos de dados relacionados associados a uma parte específica do
                    sua organização, como o departamento de  <strong>Marketing</strong>.
                </p>
                <p>
                Saiba mais sobre <strong>Domínios</strong>{' '}
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
        title: 'Create a new domain',
        content: (
            <Typography.Paragraph>
                <p>
                Clique aqui para criar um novo <strong>Domínio</strong>.
                </p>
            </Typography.Paragraph>
        ),
    },
];
