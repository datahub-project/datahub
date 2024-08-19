import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const USERS_INTRO_ID = 'users-intro';
export const USERS_SSO_ID = 'users-sso';
export const USERS_INVITE_LINK_ID = 'users-invite-link';
export const USERS_ASSIGN_ROLE_ID = 'users-assign-role';

export const UsersOnboardingConfig: OnboardingStep[] = [
    {
        id: USERS_INTRO_ID,
        title: 'Users',
        content: (
            <Typography.Paragraph>
                <p>
                Bem-vindo aos <strong>usuários</strong> do DataHub!                </p>
                <p>
                Existem algumas maneiras diferentes de integrar novos <strong>usuários</strong> ao DataHub.                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: USERS_SSO_ID,
        title: 'Configuring Single Sign-On (SSO)',
        content: (
            <Typography.Paragraph>
                <p>
                A maneira preferida de integrar novos <strong>usuários</strong> é usar o <strong>Logon único</strong>.
                Atualmente, o DataHub oferece suporte ao SSO OIDC.
                </p>
                <p>
                Saiba mais sobre como configurar o <strong>Logon único</strong>{' '}                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/authentication/guides/sso/configure-oidc-react/#configuring-oidc-in-react"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: USERS_INVITE_LINK_ID,
        selector: `#${USERS_INVITE_LINK_ID}`,
        title: 'Invite New Users',
        content: (
            <Typography.Paragraph>
                <p>
                Compartilhe facilmente um link de convite com seus colegas para integrá-los ao DataHub. Opcionalmente, atribua um{' '}
                <strong>Função</strong> para qualquer pessoa que ingressar usando o link.
                </p>
                <p>
                Saiba mais sobre como configurar links de convite{' '}                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/authentication/guides/add-users/#send-prospective-users-an-invite-link"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: USERS_ASSIGN_ROLE_ID,
        selector: `#${USERS_ASSIGN_ROLE_ID}`,
        title: 'Assigning Roles',
        content: (
            <Typography.Paragraph>
                <p>
                Você pode atribuir <strong>funções</strong> a <strong>usuários</strong> existentes aqui.                </p>
                <p>
                Saiba mais sobre <strong>funções</strong>{' '}                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/authorization/roles"
                    >
                        {' '}
                        aqui.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
];
