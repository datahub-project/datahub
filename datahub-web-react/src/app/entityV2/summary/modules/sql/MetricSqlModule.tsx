import { CodeBlock, Pill } from '@components';
import { Code } from '@phosphor-icons/react/dist/csr/Code';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { Dialect, DialectExpression, Metric } from '@types';

const ContentWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 4px 8px 8px;
`;

/* dialect enum display labels, mirror GraphQL Dialect enum */
const DIALECT_LABELS: Record<Dialect, string> = {
    [Dialect.AnsiSql]: 'ANSI SQL',
    [Dialect.Snowflake]: 'Snowflake',
    [Dialect.Mdx]: 'MDX',
    [Dialect.Tableau]: 'Tableau',
    [Dialect.Databricks]: 'Databricks',
    [Dialect.Maql]: 'MAQL',
    [Dialect.Other]: 'Other',
};

function DialectSubpanel({
    dialectExpression,
    platformLabel,
}: {
    dialectExpression: DialectExpression;
    platformLabel?: string | null;
}) {
    const dialectLabel = DIALECT_LABELS[dialectExpression.dialect] ?? dialectExpression.dialect;
    const showPlatformPill = !!platformLabel && platformLabel.toLowerCase() !== dialectLabel.toLowerCase();

    return (
        <CodeBlock
            code={dialectExpression.expression}
            language="sql"
            showHeader
            showCopy
            wrap
            languageLabel={dialectLabel}
            headerLeft={
                showPlatformPill ? (
                    <Pill label={platformLabel} color="primary" size="sm" clickable={false} />
                ) : undefined
            }
            data-testid={`sql-dialect-subpanel-${dialectExpression.dialect}`}
            contentDataTestId="sql-code-block"
            copyDataTestId="sql-copy-button"
        />
    );
}

export default function MetricSqlModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();
    const metric = entityData as Metric | null | undefined;
    const dialects = metric?.info?.expression?.dialects ?? [];
    const platformLabel = metric?.platform?.properties?.displayName || metric?.platform?.name || null;

    if (!dialects.length) {
        return (
            <LargeModule {...props} dataTestId="sql-module">
                <EmptyContent icon={Code} title={t('sql.emptyTitle')} description={t('sql.emptyDescription')} />
            </LargeModule>
        );
    }

    return (
        <LargeModule {...props} dataTestId="sql-module">
            <ContentWrapper>
                {dialects.map((dialectExpression) => (
                    <DialectSubpanel
                        key={dialectExpression.dialect}
                        dialectExpression={dialectExpression}
                        platformLabel={platformLabel}
                    />
                ))}
            </ContentWrapper>
        </LargeModule>
    );
}
