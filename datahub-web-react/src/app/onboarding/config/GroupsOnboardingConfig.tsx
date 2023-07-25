import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const GROUPS_INTRO_ID = 'groups-intro';
export const GROUPS_CREATE_GROUP_ID = 'groups-create-group';

export const GroupsOnboardingConfig: OnboardingStep[] = [
    {
        id: GROUPS_INTRO_ID,
        title: '用户组',
        content: (
            <Typography.Paragraph>
                <p>
                    欢迎使用 <strong>用户组</strong>!
                </p>
                <p>
                    <strong>用户组</strong> 是用户的集合，用来管理数据资产的所有者以及管理相关权限.
                </p>
                <p>
                    <strong>用户组</strong> 可以在DataHub中创建,也可以从外部系统进行集成并同步到Datahub中。
                </p>
                <p>
                    学习更多关于 <strong>用户组</strong>{' '}
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
        title: '创建新的用户组',
        content: (
            <Typography.Paragraph>
                <p>
                    点击这里创建 <strong>用户组</strong>.
                </p>
            </Typography.Paragraph>
        ),
    },
];
