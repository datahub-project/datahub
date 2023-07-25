import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const POLICIES_INTRO_ID = 'policies-intro';
export const POLICIES_CREATE_POLICY_ID = 'policies-create-policy';

export const PoliciesOnboardingConfig: OnboardingStep[] = [
    {
        id: POLICIES_INTRO_ID,
        title: '权限规则',
        content: (
            <Typography.Paragraph>
                <p>
                    欢迎使用 <strong>权限规则</strong>!
                </p>
                <p>
                    大多数情况下, <strong>角色</strong> 都是给Datahub用户授权的最好选择.
                </p>
                <p>
                    但是当需要给用户和用户组提供更细粒度的权限控制时，{' '}<strong>权限规则</strong> 是最好的选择.
                </p>
                <p>
                    学习更多关于 <strong>权限规则</strong>{' '}
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
        title: '创建新的规则',
        content: (
            <Typography.Paragraph>
                <p>
                    点击这里创建 <strong>规则</strong>.
                </p>
            </Typography.Paragraph>
        ),
    },
];
