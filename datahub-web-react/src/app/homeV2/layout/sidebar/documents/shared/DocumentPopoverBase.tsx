import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useSearchDocuments } from '@app/document/hooks/useSearchDocuments';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { SearchResultItem } from '@app/homeV2/layout/sidebar/documents/SearchResultItem';
import { Button, Input } from '@src/alchemy-components';

import { Document, DocumentSourceType, DocumentState } from '@types';

const PopoverContainer = styled.div`
    width: 400px;
    max-height: ${(props: { $maxHeight?: number }) => props.$maxHeight || 300}px;
    display: flex;
    flex-direction: column;
    background: ${(props) => props.theme.colors.bg};
    border-radius: 8px;
    // The parent antd Popover overlay renders with boxShadow: none (transparent
    // wrapper), so this container owns the elevation — use a large shadow so the
    // dropdown reads clearly against light summary backgrounds.
    box-shadow: ${(props) => props.theme.colors.shadowXl};
`;

const HeaderContainer = styled.div`
    padding: 6px 6px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const SearchContainer = styled.div`
    padding: 8px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const TreeScrollContainer = styled.div`
    flex: 1;
    overflow-y: auto;
    max-height: ${(props: { $maxHeight?: number }) => props.$maxHeight || 300}px;
    padding: 8px 4px;

    /* Custom scrollbar styling */
    &::-webkit-scrollbar {
        width: 6px;
    }

    &::-webkit-scrollbar-track {
        background: transparent;
    }

    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.bgSurface};
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: ${(props) => props.theme.colors.bgSurface};
    }
`;

const EmptyState = styled.div`
    padding: 24px;
    text-align: center;
    color: ${(props) => props.theme.colors.text};
    font-size: 14px;
`;

const FooterContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 8px;
    padding: 8px 12px;
    border-top: 1px solid ${(props) => props.theme.colors.border};
`;

const BREADCRUMB_SEPARATOR = ' > ';

interface DocumentPopoverBaseProps {
    /** Optional header content to render above search */
    headerContent?: React.ReactNode;
    /** Callback when a document is selected from tree */
    onSelectDocument?: (urn: string) => void;
    /** Callback when a document is selected from search results */
    onSelectSearchResult?: (urn: string) => void;
    /** Callback for creating a child document */
    onCreateChild?: (parentUrn: string | null) => void;
    /** Whether to hide actions in the tree */
    hideActions?: boolean;
    /** Hide move/delete menu actions */
    hideActionsMenu?: boolean;
    /** Hide create/add button */
    hideCreate?: boolean;
    /** Selected URN for highlighting (optional) */
    selectedUrn?: string;
    /** Maximum height of the popover */
    maxHeight?: number;
    /** Whether search is disabled */
    searchDisabled?: boolean;
    /** Filter function for search results */
    filterSearchResults?: (doc: Document) => boolean;
    /**
     * Source type filter for document search.
     * - [DocumentSourceType.Native]: Only search native (DataHub-created) documents
     * - [DocumentSourceType.External]: Only search external (ingested) documents
     * - [DocumentSourceType.Native, DocumentSourceType.External]: Search all documents
     */
    sourceTypes: DocumentSourceType[];
    /**
     * Enables multi-select (checkbox) mode. When true, every row renders a checkbox
     * whose checked state is driven by the controlled `checkedUrns` set, and clicking
     * a row (or its checkbox) fires `onToggleUrn` with the post-click state. Toggling
     * only updates the parent's staged selection — nothing is persisted until `onSave`.
     */
    multiSelect?: boolean;
    /** Controlled set of currently-checked URNs (already-linked docs render pre-checked). */
    checkedUrns?: Set<string>;
    /** Fired when a row toggles; `isNowChecked` is the state after the click. */
    onToggleUrn?: (urn: string, isNowChecked: boolean) => void;
    /** Commits the staged selection. When provided (with `multiSelect`), a Save footer renders. */
    onSave?: () => void;
    /** Disables the Save button (e.g. no pending changes). */
    saveDisabled?: boolean;
    /** Shows the Save button in a loading state while the commit is in flight. */
    isSaving?: boolean;
}

/**
 * Base component for document popovers with search and tree functionality.
 * Provides common UI and logic that can be shared across different popover types.
 */
export const DocumentPopoverBase: React.FC<DocumentPopoverBaseProps> = ({
    headerContent,
    onSelectDocument,
    onSelectSearchResult,
    onCreateChild,
    hideActions = false,
    hideActionsMenu = false,
    hideCreate = false,
    selectedUrn,
    maxHeight = 300,
    searchDisabled = false,
    filterSearchResults,
    sourceTypes,
    multiSelect = false,
    checkedUrns,
    onToggleUrn,
    onSave,
    saveDisabled = false,
    isSaving = false,
}) => {
    const { t } = useTranslation('home.v2');
    const { t: tc } = useTranslation('common.actions');
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');

    // Debounce search query
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchQuery(searchQuery);
        }, 300);
        return () => clearTimeout(timer);
    }, [searchQuery]);

    // Search for documents
    const { documents: searchResults, loading: searchLoading } = useSearchDocuments({
        query: debouncedSearchQuery || '*',
        states: [DocumentState.Published, DocumentState.Unpublished],
        count: 50,
        fetchPolicy: 'network-only',
        includeParentDocuments: true,
        sourceTypes,
    });

    const isSearching = debouncedSearchQuery.trim().length > 0;
    const filteredSearchResults = useMemo(
        () => (filterSearchResults ? searchResults.filter(filterSearchResults) : searchResults),
        [searchResults, filterSearchResults],
    );

    const handleDocumentTreeSelect = (urn: string) => {
        if (multiSelect) {
            onToggleUrn?.(urn, !checkedUrns?.has(urn));
            return;
        }
        if (onSelectDocument) {
            onSelectDocument(urn);
        }
    };

    const handleSearchResultSelect = (urn: string) => {
        if (multiSelect) {
            onToggleUrn?.(urn, !checkedUrns?.has(urn));
            return;
        }
        if (onSelectSearchResult) {
            onSelectSearchResult(urn);
        }
    };

    return (
        <PopoverContainer $maxHeight={maxHeight} data-testid="document-popover-base">
            <SearchContainer>
                <Input
                    label=""
                    placeholder={t('documents.searchPlaceholder')}
                    value={searchQuery}
                    setValue={setSearchQuery}
                    disabled={searchDisabled}
                />
            </SearchContainer>
            {headerContent && <HeaderContainer>{headerContent}</HeaderContainer>}
            <TreeScrollContainer $maxHeight={maxHeight}>
                {isSearching ? (
                    <>
                        {searchLoading && <EmptyState>{t('documents.searching')}</EmptyState>}
                        {!searchLoading && filteredSearchResults.length === 0 && (
                            <EmptyState>{tc('noResults')}</EmptyState>
                        )}
                        {!searchLoading &&
                            filteredSearchResults.map((doc) => {
                                // Build breadcrumb from parentDocuments array
                                let breadcrumb: string | null = null;
                                if (doc.parentDocuments?.documents && doc.parentDocuments.documents.length > 0) {
                                    const parents = [...doc.parentDocuments.documents].reverse();
                                    breadcrumb = parents
                                        .map((parent) => parent.info?.title || t('untitled'))
                                        .join(BREADCRUMB_SEPARATOR);
                                }

                                const isSelected = multiSelect ? !!checkedUrns?.has(doc.urn) : selectedUrn === doc.urn;

                                return (
                                    <SearchResultItem
                                        key={doc.urn}
                                        doc={doc}
                                        level={0}
                                        isSelected={isSelected}
                                        hasChildren={false}
                                        isExpanded={false}
                                        isLoading={false}
                                        breadcrumb={breadcrumb}
                                        onSelect={() => handleSearchResultSelect(doc.urn)}
                                        onToggleExpand={() => {}}
                                        onCreateChild={onCreateChild}
                                        multiSelect={multiSelect}
                                    />
                                );
                            })}
                    </>
                ) : (
                    <>
                        {/* Show tree when not searching */}
                        <DocumentTree
                            onCreateChild={onCreateChild || (() => {})}
                            selectedUrn={selectedUrn}
                            onSelectDocument={handleDocumentTreeSelect}
                            hideActions={hideActions}
                            hideActionsMenu={hideActionsMenu}
                            hideCreate={hideCreate}
                            multiSelect={multiSelect}
                            checkedUrns={checkedUrns}
                        />
                    </>
                )}
            </TreeScrollContainer>
            {multiSelect && onSave && (
                <FooterContainer>
                    <Button
                        variant="filled"
                        color="primary"
                        size="sm"
                        onClick={onSave}
                        disabled={saveDisabled || isSaving}
                        isLoading={isSaving}
                        data-testid="document-popover-save"
                    >
                        {tc('save')}
                    </Button>
                </FooterContainer>
            )}
        </PopoverContainer>
    );
};
