import { Code } from '@phosphor-icons/react/dist/csr/Code';
import { CopySimple } from '@phosphor-icons/react/dist/csr/CopySimple';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { DialectExpression, Metric } from '@types';

const ContentWrapper = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    gap: 8px;
`;

const TabRow = styled.div`
    display: flex;
    gap: 4px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    padding: 0 8px;
`;

const Tab = styled.button<{ $active: boolean }>`
    font-size: 13px;
    font-weight: ${(props) => (props.$active ? 600 : 400)};
    color: ${(props) => (props.$active ? props.theme.colors.textBrand : props.theme.colors.textSecondary)};
    background: none;
    border: none;
    border-bottom: 2px solid ${(props) => (props.$active ? props.theme.colors.textBrand : 'transparent')};
    padding: 6px 10px;
    cursor: pointer;
    margin-bottom: -1px;
    transition: color 0.12s ease, border-color 0.12s ease;

    &:hover {
        color: ${(props) => props.theme.colors.text};
    }
`;

const Toolbar = styled.div`
    display: flex;
    justify-content: flex-end;
    padding: 0 8px;
`;

const CopyButton = styled.button`
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 12px;
    font-weight: 500;
    padding: 4px 10px;
    border-radius: 6px;
    border: 1px solid ${(props) => props.theme.colors.border};
    background: ${(props) => props.theme.colors.bg};
    color: ${(props) => props.theme.colors.text};
    cursor: pointer;
    transition: background 0.12s ease;

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
    }
`;

const CodeWrapper = styled.div`
    flex: 1;
    overflow: auto;
    border-radius: 6px;
    margin: 0 8px 8px;

    pre {
        margin: 0 !important;
        border-radius: 6px !important;
        font-family: 'Roboto Mono', monospace !important;
        font-size: 12px !important;
        line-height: 1.5 !important;
    }
`;

export default function SqlModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();
    const dialects = (entityData as Metric)?.info?.expression?.dialects ?? [];

    const [activeIdx, setActiveIdx] = useState(0);

    if (!dialects.length) {
        return (
            <LargeModule {...props} dataTestId="sql-module">
                <EmptyContent
                    icon={Code}
                    title={t('sql.emptyTitle')}
                    description={t('sql.emptyDescription')}
                />
            </LargeModule>
        );
    }

    const active = dialects[activeIdx] as DialectExpression;

    const handleCopy = () => {
        navigator.clipboard.writeText(active.expression).then(() => {
            message.success(t('copied', { ns: 'common.feedback' }));
        });
    };

    return (
        <LargeModule {...props} dataTestId="sql-module">
            <ContentWrapper>
                {dialects.length > 1 && (
                    <TabRow>
                        {dialects.map((d: DialectExpression, idx: number) => (
                            <Tab
                                key={d.dialect}
                                $active={idx === activeIdx}
                                onClick={() => setActiveIdx(idx)}
                            >
                                {d.dialect}
                            </Tab>
                        ))}
                    </TabRow>
                )}
                <Toolbar>
                    <CopyButton onClick={handleCopy} data-testid="sql-copy-button">
                        <CopySimple size={13} />
                        {t('copy', { ns: 'common.actions' })}
                    </CopyButton>
                </Toolbar>
                <CodeWrapper data-testid="sql-code-block">
                    <SyntaxHighlighter language="sql" wrapLongLines>
                        {active.expression}
                    </SyntaxHighlighter>
                </CodeWrapper>
            </ContentWrapper>
        </LargeModule>
    );
}
