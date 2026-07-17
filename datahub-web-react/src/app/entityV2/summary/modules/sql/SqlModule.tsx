import { Pill, Text } from '@components';
import { Code } from '@phosphor-icons/react/dist/csr/Code';
import { CopySimple } from '@phosphor-icons/react/dist/csr/CopySimple';
import { message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { ghcolors } from 'react-syntax-highlighter/dist/esm/styles/prism';
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

const Subpanel = styled.div`
    display: flex;
    flex-direction: column;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    overflow: hidden;
    background: ${(props) => props.theme.colors.bg};
`;

const SubpanelHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    padding: 8px 12px;
    background: ${(props) => props.theme.colors.bgSurface};
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const HeaderLeft = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
`;

const DialectLabel = styled(Text).attrs({ size: 'sm' })`
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: nowrap;
`;

const CopyButton = styled.button`
    display: flex;
    align-items: center;
    gap: 5px;
    flex-shrink: 0;
    font-size: 12px;
    font-weight: 500;
    padding: 2px 4px;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: ${(props) => props.theme.colors.textSecondary};
    cursor: pointer;
    transition: color 0.12s ease, background 0.12s ease;

    &:hover {
        color: ${(props) => props.theme.colors.text};
        background: ${(props) => props.theme.colors.bgHover};
    }
`;

const CodeWrapper = styled.div`
    overflow: auto;
    background: ${(props) => props.theme.colors.bg};

    pre {
        margin: 0 !important;
        padding: 12px !important;
        background: ${(props) => props.theme.colors.bg} !important;
        font-family: 'Roboto Mono', monospace !important;
        font-size: 12px !important;
        line-height: 1.5 !important;
    }

    code {
        background: transparent !important;
    }
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
    onCopy,
    copyLabel,
}: {
    dialectExpression: DialectExpression;
    platformLabel?: string | null;
    onCopy: (expression: string) => void;
    copyLabel: string;
}) {
    const dialectLabel = DIALECT_LABELS[dialectExpression.dialect] ?? dialectExpression.dialect;

    const pillLabel = platformLabel || dialectLabel;
    const showDialectText = !!platformLabel && platformLabel.toLowerCase() !== dialectLabel.toLowerCase();

    return (
        <Subpanel data-testid={`sql-dialect-subpanel-${dialectExpression.dialect}`}>
            <SubpanelHeader>
                <HeaderLeft>
                    <Pill label={pillLabel} color="primary" size="sm" clickable={false} />
                    {showDialectText && <DialectLabel>{dialectLabel}</DialectLabel>}
                </HeaderLeft>
                <CopyButton
                    onClick={() => onCopy(dialectExpression.expression)}
                    data-testid="sql-copy-button"
                    type="button"
                >
                    <CopySimple size={13} />
                    {copyLabel}
                </CopyButton>
            </SubpanelHeader>
            <CodeWrapper data-testid="sql-code-block">
                <SyntaxHighlighter language="sql" style={ghcolors} wrapLongLines>
                    {dialectExpression.expression}
                </SyntaxHighlighter>
            </CodeWrapper>
        </Subpanel>
    );
}

export default function SqlModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();
    const metric = entityData as Metric | null | undefined;
    const dialects = metric?.info?.expression?.dialects ?? [];
    const platformLabel = metric?.platform?.properties?.displayName || metric?.platform?.name || null;

    const handleCopy = (expression: string) => {
        navigator.clipboard.writeText(expression).then(() => {
            message.success(t('copied', { ns: 'common.feedback' }));
        });
    };

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
                        onCopy={handleCopy}
                        copyLabel={t('copy', { ns: 'common.actions' })}
                    />
                ))}
            </ContentWrapper>
        </LargeModule>
    );
}
