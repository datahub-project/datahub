import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const GROUPS_INTRO_ID = 'groups-intro';
const GROUPS_CREATE_GROUP_ID = 'groups-create-group';

export const GroupsOnboardingConfig: OnboardingStep[] = [
    {
        id: GROUPS_INTRO_ID,
        title: <Trans i18nKey="onboarding:groups.introTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:groups.introWelcome" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:groups.introDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:groups.introDescription2" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:groups.learnMore"
                        components={{
                            bold: <strong />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/authorization/groups"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: GROUPS_CREATE_GROUP_ID,
        selector: `#${GROUPS_CREATE_GROUP_ID}`,
        title: <Trans i18nKey="onboarding:groups.createTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:groups.create" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
];
