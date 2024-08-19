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
                    Bem-vindo às <strong>Políticas</strong> do DataHub!
                </p>
                <p>
                    Na maioria dos casos, <strong>Funções</strong> são a melhor opção para conceder privilégios aos usuários do DataHub.
                </p>
                <p>
                    Quando for necessário um controle mais refinado sobre as permissões de usuários e grupos,{' '}
                    <strong>Políticas</strong> resolverão o problema.
                </p>
                <p>
                    Saiba mais sobre <strong>Políticas</strong>{' '}
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
                Clique aqui para criar uma nova <strong>Política</strong>.
                </p>
            </Typography.Paragraph>
        ),
    },
];
