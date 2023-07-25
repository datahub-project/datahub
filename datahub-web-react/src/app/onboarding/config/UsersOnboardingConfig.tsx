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
        title: '用户',
        content: (
            <Typography.Paragraph>
                <p>
                    欢迎使用 <strong>用户</strong>!
                </p>
                <p>
                    DataHub提供多种方式来支持<strong>新用户</strong>使用该系统.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: USERS_SSO_ID,
        title: '配置点单登陆 (SSO)',
        content: (
            <Typography.Paragraph>
                <p>
                    The preferred way to onboard new <strong>Users</strong> is to use <strong>Single Sign-On</strong>.
                    Currently, DataHub supports OIDC SSO.
                </p>
                <p>
                    学习更多关于 <strong>Single Sign-On</strong>{' '}
                    <a
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
        title: '邀请新用户',
        content: (
            <Typography.Paragraph>
                <p>
                    最简便的方式是通过分享 <strong>邀请链接</strong> 给要使用Datahub的用户.
                    邀请的同时也可以通过链接来分配{' '}<strong>角色</strong>。
                </p>
                <p>
                    学习更多关于如何配置邀请链接{' '}
                    <a
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
        title: '分配角色',
        content: (
            <Typography.Paragraph>
                <p>
                    您可以为系统内已经<strong>存在用户</strong>分配<strong>角色</strong>.
                </p>
                <p>
                    学习更多关于 <strong>角色</strong>{' '}
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
