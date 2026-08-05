import { Button, Loader, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { isDocumentUnpublished } from '@app/document/utils/documentUtils';
import { DocumentTreeItem } from '@app/homeV2/layout/sidebar/documents/DocumentTreeItem';

import { Document } from '@types';

const ResultsWrap = styled.div<{ $dimmed?: boolean }>`
    display: flex;
    flex-direction: column;
    gap: 2px;
    opacity: ${(props) => (props.$dimmed ? 0.6 : 1)};
    transition: opacity 0.15s ease;
`;

const ResultsHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 0 8px 8px;
`;

const StateWrap = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;
    padding: 24px 16px;
`;

type Props = {
    documents: Document[];
    /** Index total (may be > documents.length when the page is capped). */
    total: number;
    loading: boolean;
    /** Refetch in flight while previous hits are still on screen. */
    isRefreshing?: boolean;
    selectedUrn?: string | null;
    onSelect: (urn: string) => void;
    onClear: () => void;
    onCreateChild?: (parentUrn: string) => void;
};

/**
 * Flat search results for documents sidebar search mode (query and/or filters).
 * Uses DocumentTreeItem so hover / + / ⋮ match the browse tree.
 */
export default function DocumentSidebarSearchResults({
    documents,
    total,
    loading,
    isRefreshing = false,
    selectedUrn,
    onSelect,
    onClear,
    onCreateChild,
}: Props) {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const { t: tet } = useTranslation('entity.types');

    if (loading && documents.length === 0) {
        return (
            <StateWrap>
                <Loader size="md" />
            </StateWrap>
        );
    }

    if (documents.length === 0) {
        return (
            <StateWrap>
                <Text size="sm" color="gray">
                    {tc('noResults')}
                </Text>
                <Button variant="text" size="sm" onClick={onClear} data-testid="context-sidebar-clear-search">
                    {t('context.clearSearch')}
                </Button>
            </StateWrap>
        );
    }

    const countLabel =
        total > documents.length
            ? t('context.searchResultsCountTruncated', { count: documents.length, total })
            : t('context.searchResultsCount', { count: total > 0 ? total : documents.length });

    return (
        <ResultsWrap data-testid="context-sidebar-search-results" $dimmed={isRefreshing}>
            <ResultsHeader>
                <Text size="sm" color="gray">
                    {countLabel}
                </Text>
                <Button variant="text" size="sm" onClick={onClear} data-testid="context-sidebar-clear-search">
                    {t('context.clearSearch')}
                </Button>
            </ResultsHeader>
            {documents.map((doc) => {
                const title = doc.info?.title || tet('document.untitledFallback');
                return (
                    <DocumentTreeItem
                        key={doc.urn}
                        urn={doc.urn}
                        title={title}
                        level={0}
                        hasChildren={false}
                        isExpanded={false}
                        isSelected={doc.urn === selectedUrn}
                        isUnpublished={isDocumentUnpublished(doc)}
                        onToggleExpand={() => {}}
                        onClick={() => onSelect(doc.urn)}
                        onCreateChild={onCreateChild ?? (() => {})}
                        hideCreate={!onCreateChild}
                        parentUrn={doc.parentDocuments?.documents?.[0]?.urn ?? null}
                    />
                );
            })}
        </ResultsWrap>
    );
}
