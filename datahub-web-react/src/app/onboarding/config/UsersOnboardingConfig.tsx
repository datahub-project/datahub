import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const USERS_INTRO_ID = 'users-intro';
export const USERS_SSO_ID = 'users-sso';
export const USERS_INVITE_LINK_ID = 'users-invite-link';
export const USERS_ASSIGN_ROLE_ID = 'users-assign-role';

export const UsersOnboardingConfig: OnboardingStep[] = [
    {
        id: USERS_INTRO_ID,
        title: <Trans i18nKey="onboarding:users.introTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:users.introWelcome" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:users.introDescription" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: USERS_SSO_ID,
        title: <Trans i18nKey="onboarding:users.ssoTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:users.ssoDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:users.ssoLearnMore"
                        components={{
                            bold: <strong />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/authentication/guides/sso/configure-oidc-react/#configuring-oidc-in-react"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: USERS_INVITE_LINK_ID,
        selector: `#${USERS_INVITE_LINK_ID}`,
        title: <Trans i18nKey="onboarding:users.inviteTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:users.inviteDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:users.inviteLearnMore"
                        components={{
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/authentication/guides/add-users/#send-prospective-users-an-invite-link"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: USERS_ASSIGN_ROLE_ID,
        selector: `#${USERS_ASSIGN_ROLE_ID}`,
        title: <Trans i18nKey="onboarding:users.assignRoleTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:users.assignRoleDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:users.assignRoleLearnMore"
                        components={{
                            bold: <strong />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/authorization/roles"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
];
