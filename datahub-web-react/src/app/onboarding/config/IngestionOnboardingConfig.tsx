import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const INGESTION_CREATE_SOURCE_ID = 'ingestion-create-source';
export const INGESTION_REFRESH_SOURCES_ID = 'ingestion-refresh-sources';
export const INGESTION_SELECT_SOURCE_ID = 'ingestion-select-source';

export const IngestionOnboardingConfig: OnboardingStep[] = [
    {
        id: INGESTION_CREATE_SOURCE_ID,
        selector: `#${INGESTION_CREATE_SOURCE_ID}`,
        title: <Trans i18nKey="onboarding:ingestion.createSourceTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:ingestion.createSourceDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:ingestion.learnMore"
                        components={{
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/metadata-ingestion"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: INGESTION_REFRESH_SOURCES_ID,
        selector: `#${INGESTION_REFRESH_SOURCES_ID}`,
        title: <Trans i18nKey="onboarding:ingestion.refreshTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:ingestion.refreshDescription" />
                </p>
            </Text>
        ),
    },
    {
        id: INGESTION_SELECT_SOURCE_ID,
        selector: `#${INGESTION_SELECT_SOURCE_ID}`,
    },
];
