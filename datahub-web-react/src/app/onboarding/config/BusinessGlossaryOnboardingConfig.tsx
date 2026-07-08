import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const BUSINESS_GLOSSARY_INTRO_ID = 'business-glossary-intro';
export const BUSINESS_GLOSSARY_CREATE_TERM_ID = 'business-glossary-create-term';
export const BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID = 'business-glossary-create-term-group';

export const BusinessGlossaryOnboardingConfig: OnboardingStep[] = [
    {
        id: BUSINESS_GLOSSARY_INTRO_ID,
        title: <Trans i18nKey="onboarding:glossary.introTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:glossary.introWelcome" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:glossary.introDescription" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: BUSINESS_GLOSSARY_CREATE_TERM_ID,
        selector: `#${BUSINESS_GLOSSARY_CREATE_TERM_ID}`,
        title: <Trans i18nKey="onboarding:glossary.termsTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:glossary.createTerm" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:glossary.termsDescription" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID,
        selector: `#${BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID}`,
        title: <Trans i18nKey="onboarding:glossary.termGroupsTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:glossary.createTermGroup" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:glossary.termGroupsDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:glossary.termGroupsExample" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:glossary.learnMore"
                        components={{
                            bold: <strong />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/glossary/business-glossary"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
];
