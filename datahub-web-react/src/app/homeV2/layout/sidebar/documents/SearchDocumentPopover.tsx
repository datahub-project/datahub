import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { useSearchDocuments } from '@app/document/hooks/useSearchDocuments';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { SearchResultItem } from '@app/homeV2/layout/sidebar/documents/SearchResultItem';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Input } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

import { DocumentSourceType, DocumentState, EntityType } from '@types';

const PopoverContainer = styled.div`
    width: 400px;
    max-height: 300px; /* Fixed height to prevent popover jumping */
    display: flex;
    flex-direction: column;
    background: white;
    border-radius: 8px;
    box-shadow: 0px 4px 16px rgba(0, 0, 0, 0.1);
`;

const SearchContainer = styled.div`
    padding: 8px;
`;

const TreeScrollContainer = styled.div`
    flex: 1;
    overflow-y: auto;
    height: 300px; /* Fixed height to prevent popover jumping */

    padding: 8px 4px;

    border-top: 1px solid ${colors.gray[100]};
    border-bottom: 1px solid ${colors.gray[100]};

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

interface SearchDocumentPopoverProps {
    onClose: () => void;
}

export const SearchDocumentPopover: React.FC<SearchDocumentPopoverProps> = ({ onClose }) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');
    const history = useHistory();
    const entityRegistry = useEntityRegistry();

    // Debounce search query
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchQuery(searchQuery);
        }, 300);
        return () => clearTimeout(timer);
    }, [searchQuery]);

    // Search for documents (only when searching)
    const { documents: searchResults, loading: searchLoading } = useSearchDocuments({
        query: debouncedSearchQuery || '*',
        states: [DocumentState.Published, DocumentState.Unpublished],
        count: 50,
        fetchPolicy: 'network-only', // Always fetch fresh for search
        includeParentDocuments: true, // Fetch parent documents for breadcrumb display
        sourceTypes: [DocumentSourceType.Native],
    });

    const isSearching = debouncedSearchQuery.trim().length > 0;
    const filteredSearchResults = searchResults;

    const handleDocumentClick = (urn: string) => {
        const url = entityRegistry.getEntityUrl(EntityType.Document, urn);
        history.push(url);
        onClose();
    };

    return (
        <PopoverContainer data-testid="search-document-popover">
            <SearchContainer>
                <Input label="" placeholder="Search context..." value={searchQuery} setValue={setSearchQuery} />
            </SearchContainer>

            <TreeScrollContainer>
                {/* Show search results when searching */}
                {isSearching ? (
                    <>
                        {searchLoading && <EmptyState>Searching...</EmptyState>}
                        {!searchLoading && filteredSearchResults.length === 0 && (
                            <EmptyState>No results found</EmptyState>
                        )}
                        {!searchLoading &&
                            filteredSearchResults.map((doc) => {
                                // Build breadcrumb from parentDocuments array if there are parents
                                // parentDocuments is ordered: [direct parent, parent's parent, ...]
                                // We want to show: grandparent > parent
                                let breadcrumb: string | null = null;
                                if (doc.parentDocuments?.documents && doc.parentDocuments.documents.length > 0) {
                                    const parents = [...doc.parentDocuments.documents].reverse(); // Reverse to get root first
                                    breadcrumb = parents.map((parent) => parent.info?.title || 'Untitled').join(' > ');
                                }

                                return (
                                    <SearchResultItem
                                        key={doc.urn}
                                        doc={doc}
                                        level={0}
                                        isSelected={false}
                                        hasChildren={false}
                                        isExpanded={false}
                                        isLoading={false}
                                        breadcrumb={breadcrumb}
                                        onSelect={() => handleDocumentClick(doc.urn)}
                                        onToggleExpand={() => {}}
                                    />
                                );
                            })}
                    </>
                ) : (
                    <>
                        {/* Show tree when not searching */}
                        <DocumentTree onCreateChild={() => {}} onSelectDocument={handleDocumentClick} hideActions />
                    </>
                )}
            </TreeScrollContainer>
        </PopoverContainer>
    );
};
