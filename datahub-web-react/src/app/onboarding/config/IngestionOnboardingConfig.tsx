import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const INGESTION_CREATE_SOURCE_ID = 'ingestion-create-source';
export const INGESTION_REFRESH_SOURCES_ID = 'ingestion-refresh-sources';

export const IngestionOnboardingConfig: OnboardingStep[] = [
    {
        id: INGESTION_CREATE_SOURCE_ID,
        selector: `#${INGESTION_CREATE_SOURCE_ID}`,
        title: '创建新的元数据集成数据源',
        content: (
            <Typography.Paragraph>
                <p>
                    为 Datahub 提供元数据源，这些数据源将用于后续的元数据集成。 <strong>Data Platforms</strong>, 包括
                    操作型数据库， 比如 <strong>MySQL</strong>,<strong>Oracle</strong>。 数据仓库，比如 {' '}
                    <strong>Snowflake</strong>。 仪表盘工具，比如 <strong>Looker</strong>, 等等!
                </p>
                <p>
                    学习更多，了解所有支持的数据源，请查看如下链接{' '}
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
        title: '数据源刷新',
        content: (
            <Typography.Paragraph>
                <p>点击刷新数据源</p>
            </Typography.Paragraph>
        ),
    },
];
