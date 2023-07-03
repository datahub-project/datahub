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
                    Welcome to DataHub <strong>Users</strong>!
                </p>
                <p>
                    There are a few different ways to onboard new <strong>Users</strong> onto DataHub.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: USERS_SSO_ID,
        title: 'Configuring Single Sign-On (SSO)',
        content: (
            <Typography.Paragraph>
                <p>
                    The preferred way to onboard new <strong>Users</strong> is to use <strong>Single Sign-On</strong>.
                    Currently, DataHub supports OIDC SSO.
                </p>
                <p>
                    Learn more about how to configure <strong>Single Sign-On</strong>{' '}
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
        title: 'Invite New Users',
        content: (
            <Typography.Paragraph>
                <p>
                    Easily share an invite link with your colleagues to onboard them onto DataHub. Optionally assign a{' '}
                    <strong>Role</strong> to anyone who joins using the link.
                </p>
                <p>
                    Learn more about configuring invite links{' '}
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
        title: 'Assigning Roles',
        content: (
            <Typography.Paragraph>
                <p>
                    You can assign <strong>Roles</strong> to existing <strong>Users</strong> here.
                </p>
                <p>
                    Learn more about <strong>Roles</strong>{' '}
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
