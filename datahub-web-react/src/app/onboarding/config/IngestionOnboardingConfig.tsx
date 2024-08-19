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
                Configure novas integrações do DataHub para suas <strong>plataformas de dados</strong>, incluindo
                    Bancos de dados transacionais como <strong>MySQL</strong>, data warehouses como{' '}
                    <strong>Snowflake</strong>, ferramentas de dashboard como o <strong>Looker</strong> e muito mais!
                </p>
                <p>
                    Saiba mais sobre a ingestão e veja a lista completa de integrações compatíveis{' '}
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
        title: 'Refresh Ingestion Sources',
        content: (
            <Typography.Paragraph>
            <p>Clique para forçar uma atualização das fontes de ingestão em execução.</p>            </Typography.Paragraph>
        ),
    },
];
