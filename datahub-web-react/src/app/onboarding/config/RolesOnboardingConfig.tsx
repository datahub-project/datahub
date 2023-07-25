import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const ROLES_INTRO_ID = 'roles-intro';

export const RolesOnboardingConfig: OnboardingStep[] = [
    {
        id: ROLES_INTRO_ID,
        title: '角色',
        content: (
            <Typography.Paragraph>
                <p>
                    欢迎使用 <strong>角色</strong>!
                </p>
                <p>
                    <strong>角色</strong> 是我们推荐的管理权限的方式.
                </p>
                <p>
                    DataHub 当下支持三种开箱即用的角色: <strong>Admin</strong>,{' '}
                    <strong>Editor</strong> 及 <strong>Reader</strong>.
                </p>
                <p>
                    学习更多关于 <strong>角色</strong> 以及每种角色的不同权限{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/authorization/roles"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
];
