import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useSearchDocuments } from '@app/document/hooks/useSearchDocuments';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { SearchResultItem } from '@app/homeV2/layout/sidebar/documents/SearchResultItem';
import { Input } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

import { Document, DocumentSourceType, DocumentState } from '@types';

const PopoverContainer = styled.div`
    width: 400px;
    max-height: ${(props: { $maxHeight?: number }) => props.$maxHeight || 300}px;
    display: flex;
    flex-direction: column;
    background: white;
    border-radius: 8px;
    box-shadow: 0px 4px 16px rgba(0, 0, 0, 0.1);
`;

const HeaderContainer = styled.div`
    padding: 6px 6px;
    border-bottom: 1px solid ${colors.gray[100]};
`;

const SearchContainer = styled.div`
    padding: 8px;
    border-bottom: 1px solid ${colors.gray[100]};
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
        background: ${colors.gray[200]};
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: ${colors.gray[500]};
    }
`;

const EmptyState = styled.div`
    padding: 24px;
    text-align: center;
    color: ${colors.gray[600]};
    font-size: 14px;
`;

export interface DocumentPopoverBaseProps {
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
}) => {
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
    const filteredSearchResults = filterSearchResults ? searchResults.filter(filterSearchResults) : searchResults;

    const handleDocumentTreeSelect = (urn: string) => {
        if (onSelectDocument) {
            onSelectDocument(urn);
        }
    };

    const handleSearchResultSelect = (urn: string) => {
        if (onSelectSearchResult) {
            onSelectSearchResult(urn);
        }
    };

    return (
        <PopoverContainer $maxHeight={maxHeight} data-testid="document-popover-base">
            <SearchContainer>
                <Input
                    label=""
                    placeholder="Search context..."
                    value={searchQuery}
                    setValue={setSearchQuery}
                    disabled={searchDisabled}
                />
            </SearchContainer>
            {headerContent && <HeaderContainer>{headerContent}</HeaderContainer>}
            <TreeScrollContainer $maxHeight={maxHeight}>
                {isSearching ? (
                    <>
                        {searchLoading && <EmptyState>Searching...</EmptyState>}
                        {!searchLoading && filteredSearchResults.length === 0 && (
                            <EmptyState>No results found</EmptyState>
                        )}
                        {!searchLoading &&
                            filteredSearchResults.map((doc) => {
                                // Build breadcrumb from parentDocuments array
                                let breadcrumb: string | null = null;
                                if (doc.parentDocuments?.documents && doc.parentDocuments.documents.length > 0) {
                                    const parents = [...doc.parentDocuments.documents].reverse();
                                    breadcrumb = parents.map((parent) => parent.info?.title || 'Untitled').join(' > ');
                                }

                                const isSelected = selectedUrn === doc.urn;

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
                        />
                    </>
                )}
            </TreeScrollContainer>
        </PopoverContainer>
    );
};
