import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const GROUPS_INTRO_ID = 'groups-intro';
export const GROUPS_CREATE_GROUP_ID = 'groups-create-group';

export const GroupsOnboardingConfig: OnboardingStep[] = [
    {
        id: GROUPS_INTRO_ID,
        title: 'Groups',
        content: (
            <Typography.Paragraph>
                <p>
                    Bem-vindo ao Datahub <strong>Grupos</strong>!
                </p>
                <p>
                    <strong>Grupos</strong> são coleções de usuários que podem ser usadas para atribuir propriedade a ativos e
                    gerenciar o acesso.
                </p>
                <p>
                    <strong>Grupos</strong> pode ser criado nativamente no DataHub ou sincronizado a partir do seu Identity
                    Fornecedor.
                </p>
                <p>
                Aprender mais sobre <strong>Grupos</strong>{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/authorization/groups"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: GROUPS_CREATE_GROUP_ID,
        selector: `#${GROUPS_CREATE_GROUP_ID}`,
        title: 'Create a new Group',
        content: (
            <Typography.Paragraph>
                <p>
                Clique aqui para criar um novo <strong>Grupo</strong>.
                </p>
            </Typography.Paragraph>
        ),
    },
];
