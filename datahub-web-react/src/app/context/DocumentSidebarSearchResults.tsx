import React from 'react';
import { useTranslation } from 'react-i18next';

import { isDocumentUnpublished } from '@app/document/utils/documentUtils';
import { DocumentTreeItem } from '@app/homeV2/layout/sidebar/documents/DocumentTreeItem';
import SidebarFilteredResults from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarFilteredResults';

import { Document } from '@types';

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
 * Shared empty / clear / count chrome comes from `SidebarFilteredResults`.
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
    const { t: tet } = useTranslation('entity.types');

    return (
        <SidebarFilteredResults
            count={documents.length}
            total={total}
            loading={loading}
            isRefreshing={isRefreshing}
            onClear={onClear}
            clearTestId="context-sidebar-clear-search"
            dataTestId="context-sidebar-search-results"
        >
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
        </SidebarFilteredResults>
    );
}
