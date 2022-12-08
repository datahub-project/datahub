import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

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
                    Click here to configure new Integrations from DataHub to your <strong>Data Platforms</strong>,
                    including Transactional Databases like <strong>MySQL</strong>, Data Warehouses like{' '}
                    <strong>Snowflake</strong>, Dashboarding tools like <strong>Looker</strong>, and many more!
                </p>
                <p>
                    Learn more about ingestion and view the full list of supported Integrations{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/metadata-ingestion"
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
        title: 'Refresh Ingestion Pipelines',
        content: (
            <Typography.Paragraph>
                <p>Click here to refresh and check whether new ingestion pipelines have been set up.</p>
                <p>You can view both pipelines created on this page and those set up using the DataHub CLI.</p>
            </Typography.Paragraph>
        ),
    },
];
