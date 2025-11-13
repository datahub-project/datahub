import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useMoveDocumentTreeMutation } from '@app/documentV2/hooks/useDocumentTreeMutations';
import { useSearchDocuments } from '@app/documentV2/hooks/useSearchDocuments';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { SearchResultItem } from '@app/homeV2/layout/sidebar/documents/SearchResultItem';
import { Button, Input } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

import { DocumentState } from '@types';

const PopoverContainer = styled.div`
    width: 400px;
    height: 300px; /* Fixed height to prevent popover jumping */
    display: flex;
    flex-direction: column;
    background: white;
    border-radius: 8px;
    box-shadow: 0px 4px 16px rgba(0, 0, 0, 0.1);
    padding: 16px;
`;

const SearchContainer = styled.div`
    margin-bottom: 12px;
`;

const TreeScrollContainer = styled.div`
    flex: 1;
    overflow-y: auto;
    height: 300px; /* Fixed height to prevent popover jumping */
    border: 1px solid ${colors.gray[100]};
    border-radius: 6px;
    padding: 8px;
    margin-bottom: 12px;

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

const RootOption = styled.div<{ $isSelected: boolean }>`
    padding: 8px 12px;
    border-radius: 6px;
    cursor: pointer;
    margin-bottom: 0px;
    transition: background-color 0.15s ease;
    font-size: 14px;

    ${(props) =>
        props.$isSelected
            ? `
        background: linear-gradient(
            180deg,
            rgba(83, 63, 209, 0.04) -3.99%,
            rgba(112, 94, 228, 0.04) 53.04%,
            rgba(112, 94, 228, 0.04) 100%
        );
        box-shadow: 0px 0px 0px 1px rgba(108, 71, 255, 0.08);
    `
            : `
        &:hover {
            background-color: ${colors.gray[100]};
        }
    `}
`;

const EmptyState = styled.div`
    padding: 24px;
    text-align: center;
    color: ${colors.gray[600]};
    font-size: 14px;
`;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

interface MoveDocumentPopoverProps {
    documentUrn: string;
    currentParentUrn?: string | null;
    onClose: () => void;
}

export const MoveDocumentPopover: React.FC<MoveDocumentPopoverProps> = ({ documentUrn, currentParentUrn, onClose }) => {
    const [selectedParentUrn, setSelectedParentUrn] = useState<string | null | undefined>(currentParentUrn);
    const [movingDocument, setMovingDocument] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');
    const { moveDocument } = useMoveDocumentTreeMutation();

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
        includeDrafts: false,
        count: 50,
        fetchPolicy: 'network-only', // Always fetch fresh for search
        includeParentDocuments: true, // Fetch parent documents for breadcrumb display
    });

    const isSearching = debouncedSearchQuery.trim().length > 0;
    const filteredSearchResults = searchResults.filter((doc) => doc.urn !== documentUrn);

    const handleSelectRoot = () => {
        setSelectedParentUrn(null);
    };

    const handleSelectDocument = (urn: string) => {
        setSelectedParentUrn(urn);
    };

    const handleMove = async () => {
        setMovingDocument(true);
        try {
            // Tree mutation handles optimistic move + backend call + rollback on error!
            await moveDocument(documentUrn, selectedParentUrn === undefined ? null : selectedParentUrn);
            // Success - close popover
            setTimeout(() => {
                onClose();
            }, 300);
        } finally {
            setMovingDocument(false);
        }
    };

    const isRootSelected = selectedParentUrn === null;
    const hasSelectionChanged = selectedParentUrn !== currentParentUrn;

    return (
        <PopoverContainer>
            <SearchContainer>
                <Input label="" placeholder="Search documents..." value={searchQuery} setValue={setSearchQuery} />
            </SearchContainer>

            <TreeScrollContainer>
                {/* Show search results when searching */}
                {isSearching ? (
                    <>
                        {searchLoading && <EmptyState>Searching...</EmptyState>}
                        {!searchLoading && filteredSearchResults.length === 0 && (
                            <EmptyState>No documents found</EmptyState>
                        )}
                        {!searchLoading &&
                            filteredSearchResults.map((doc) => {
                                const isSelected = selectedParentUrn === doc.urn;

                                // Build breadcrumb from parentDocuments array
                                // parentDocuments is ordered: [direct parent, parent's parent, ...]
                                // We want to show: Root > grandparent > parent
                                let breadcrumb = 'Root';
                                if (doc.parentDocuments?.documents && doc.parentDocuments.documents.length > 0) {
                                    const parents = [...doc.parentDocuments.documents].reverse(); // Reverse to get root first
                                    breadcrumb = parents.map((parent) => parent.info?.title || 'Untitled').join(' > ');
                                }

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
                                        onSelect={() => handleSelectDocument(doc.urn)}
                                        onToggleExpand={() => {}}
                                    />
                                );
                            })}
                    </>
                ) : (
                    <>
                        {/* Only show "Move to Root" if document is not already at root */}
                        {currentParentUrn !== null && currentParentUrn !== undefined && (
                            <RootOption $isSelected={isRootSelected} onClick={handleSelectRoot}>
                                Move to Root
                            </RootOption>
                        )}

                        {/* Show tree when not searching */}
                        <DocumentTree
                            onCreateChild={() => {}}
                            selectedUrn={selectedParentUrn || undefined}
                            onSelectDocument={handleSelectDocument}
                            hideActions
                        />
                    </>
                )}
            </TreeScrollContainer>

            <ButtonContainer>
                <Button variant="outline" onClick={onClose} disabled={movingDocument}>
                    Cancel
                </Button>
                <Button
                    onClick={handleMove}
                    disabled={!hasSelectionChanged || movingDocument}
                    isLoading={movingDocument}
                >
                    Move
                </Button>
            </ButtonContainer>
        </PopoverContainer>
    );
};
