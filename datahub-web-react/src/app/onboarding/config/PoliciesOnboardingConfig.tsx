import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const POLICIES_INTRO_ID = 'policies-intro';
export const POLICIES_CREATE_POLICY_ID = 'policies-create-policy';

export const PoliciesOnboardingConfig: OnboardingStep[] = [
    {
        id: POLICIES_INTRO_ID,
        title: <Trans i18nKey="onboarding:policies.introTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:policies.introWelcome" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:policies.introDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:policies.introDescription2" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:policies.learnMore"
                        components={{
                            bold: <strong />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/authorization/policies"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: POLICIES_CREATE_POLICY_ID,
        selector: `#${POLICIES_CREATE_POLICY_ID}`,
        title: <Trans i18nKey="onboarding:policies.createTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:policies.create" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
];
