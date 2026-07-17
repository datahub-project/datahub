import { CopySimple } from '@phosphor-icons/react/dist/csr/CopySimple';
import { message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EmptyTab } from '@app/entityV2/shared/components/styled/EmptyTab';

import { GetSemanticModelQuery } from '@graphql/semanticModel.generated';

const TabContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    padding: 20px;
    gap: 12px;
    overflow: auto;
`;

const Toolbar = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-end;
`;

const CopyButton = styled.button`
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    font-weight: 500;
    padding: 6px 12px;
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
    border-radius: 8px;
    overflow: auto;

    pre {
        margin: 0 !important;
        border-radius: 8px !important;
        font-family: 'Roboto Mono', monospace !important;
        font-size: 13px !important;
        line-height: 1.6 !important;
    }
`;

export function DefinitionTab() {
    const { t } = useTranslation('common.actions');
    const baseEntity = useBaseEntity<GetSemanticModelQuery>();
    const nativeDefinition = baseEntity?.semanticModel?.info?.nativeDefinition;

    if (!nativeDefinition) {
        return (
            <TabContainer>
                <EmptyTab tab="definition" />
            </TabContainer>
        );
    }

    const handleCopy = () => {
        navigator.clipboard.writeText(nativeDefinition).then(() => {
            message.success(t('copied', { ns: 'common.feedback' }));
        });
    };

    return (
        <TabContainer>
            <Toolbar>
                <CopyButton onClick={handleCopy} data-testid="definition-copy-button">
                    <CopySimple size={14} />
                    {t('copy')}
                </CopyButton>
            </Toolbar>
            <CodeWrapper data-testid="definition-code-block">
                <SyntaxHighlighter language="yaml" wrapLongLines showLineNumbers>
                    {nativeDefinition}
                </SyntaxHighlighter>
            </CodeWrapper>
        </TabContainer>
    );
}
