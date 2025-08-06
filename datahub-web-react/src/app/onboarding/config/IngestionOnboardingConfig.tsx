import { Typography } from 'antd';
import React from 'react';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const INGESTION_CREATE_SOURCE_ID = 'ingestion-create-source';
export const INGESTION_REFRESH_SOURCES_ID = 'ingestion-refresh-sources';

export const IngestionOnboardingConfig: OnboardingStep[] = [
    {
        id: INGESTION_CREATE_SOURCE_ID,
        selector: `#${INGESTION_CREATE_SOURCE_ID}`,
        title: 'Create a new Ingestion Source',
        content: (
            <Typography.Paragraph>
                <p>
                    Configure new Integrations from DataHub to your <strong>Data Platforms</strong>, including
                    Transactional Databases like <strong>MySQL</strong>, Data Warehouses such as{' '}
                    <strong>Snowflake</strong>, Dashboarding tools like <strong>Looker</strong>, and more!
                </p>
                <p>
                    Learn more about ingestion and view the full list of supported Integrations{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://docs.datahub.com/docs/metadata-ingestion"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: INGESTION_REFRESH_SOURCES_ID,
        selector: `#${INGESTION_REFRESH_SOURCES_ID}`,
        title: 'Refresh Ingestion Sources',
        content: (
            <Typography.Paragraph>
                <p>Click to force a refresh of running ingestion sources.</p>
            </Typography.Paragraph>
        ),
    },
];
