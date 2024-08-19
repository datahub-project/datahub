import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const ROLES_INTRO_ID = 'roles-intro';

export const RolesOnboardingConfig: OnboardingStep[] = [
    {
        id: ROLES_INTRO_ID,
        title: 'Roles',
        content: (
            <Typography.Paragraph>
              <p>
                    Bem-vindo ao DataHub <strong>Funções</strong>!
                </p>
                <p>
                    <strong>Funções</strong> são a forma recomendada de gerenciar permissões no DataHub.
                </p>
                <p>
                    Atualmente, o DataHub oferece suporte a três funções prontas para uso: <strong>Administrador</strong>,{' '}
                    <strong>Editor</strong> e <strong>Leitor</strong>.
                </p>
                <p>
                    Saiba mais sobre <strong>Funções</strong> e as diferentes permissões para cada função{' '}
                    <a
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
