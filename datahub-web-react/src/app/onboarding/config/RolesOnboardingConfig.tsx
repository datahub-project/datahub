import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const ROLES_INTRO_ID = 'roles-intro';

export const RolesOnboardingConfig: OnboardingStep[] = [
    {
        id: ROLES_INTRO_ID,
        title: <Trans i18nKey="onboarding:roles.introTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:roles.introWelcome" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:roles.introDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:roles.introDescription2" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:roles.learnMore"
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
