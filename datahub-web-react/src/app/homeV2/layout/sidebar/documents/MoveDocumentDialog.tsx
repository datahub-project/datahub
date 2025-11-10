import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { DocumentChild, useDocumentChildren } from '@app/documentV2/hooks/useDocumentChildren';
import { useDocumentTreeExpansion } from '@app/documentV2/hooks/useDocumentTreeExpansion';
import { useMoveDocument } from '@app/documentV2/hooks/useMoveDocument';
import { useSearchDocuments } from '@app/documentV2/hooks/useSearchDocuments';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { SearchResultItem } from '@app/homeV2/layout/sidebar/documents/SearchResultItem';
import { Button, Input } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

import { Document, DocumentState } from '@types';

const DialogContainer = styled.div`
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

interface MoveDocumentDialogProps {
    documentUrn: string;
    currentParentUrn?: string | null;
    onClose: () => void;
}

export const MoveDocumentDialog: React.FC<MoveDocumentDialogProps> = ({ documentUrn, currentParentUrn, onClose }) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');
    const [selectedParentUrn, setSelectedParentUrn] = useState<string | null | undefined>(currentParentUrn);
    const { moveDocument, loading: movingDocument } = useMoveDocument();
    const { checkForChildren } = useDocumentChildren();

    // Use shared expansion hook to manage tree expansion state
    // Exclude the document being moved from results
    const { expandedUrns, hasChildrenMap, childrenCache, loadingUrns, handleToggleExpand, setHasChildrenMap } =
        useDocumentTreeExpansion({ excludeUrn: documentUrn });

    // Debounce search query
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchQuery(searchQuery);
        }, 300); // 300ms debounce

        return () => clearTimeout(timer);
    }, [searchQuery]);

    // Search for documents
    const { documents, loading: searchLoading } = useSearchDocuments({
        query: debouncedSearchQuery || '*',
        states: [DocumentState.Published, DocumentState.Unpublished],
        includeDrafts: false,
        rootOnly: !debouncedSearchQuery, // Only show root documents if no search query
        count: 50,
    });

    // Filter out the document being moved and its children
    const filteredDocuments = documents.filter((doc) => doc.urn !== documentUrn);

    // Check if we're in search mode
    const isSearching = debouncedSearchQuery.trim().length > 0;

    // Create stable key for when filtered documents change
    const filteredDocumentUrnsKey = useMemo(
        () => filteredDocuments.map((doc) => doc.urn).join(','),
        [filteredDocuments],
    );

    // Check for children when documents change (both search and tree view)
    useEffect(() => {
        if (filteredDocuments.length > 0) {
            const checkChildren = async () => {
                const urns = filteredDocuments.map((doc) => doc.urn);
                const childrenMap = await checkForChildren(urns);
                setHasChildrenMap((prev) => ({ ...prev, ...childrenMap }));
            };
            checkChildren();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [filteredDocumentUrnsKey]);

    const handleMove = useCallback(async () => {
        const success = await moveDocument({
            urn: documentUrn,
            parentDocument: selectedParentUrn,
        });

        if (success) {
            // Delay closing to allow user to see the success message
            setTimeout(() => {
                onClose();
            }, 500);
        }
    }, [documentUrn, selectedParentUrn, moveDocument, onClose]);

    const handleSelectRoot = useCallback(() => {
        setSelectedParentUrn(null);
    }, []);

    const handleSelectDocument = useCallback((urn: string) => {
        setSelectedParentUrn(urn);
    }, []);

    const getParentBreadcrumb = useCallback(
        (doc: Document) => {
            const parentDoc = doc.info?.parentDocument?.document;
            if (!parentDoc) {
                return null; // Don't show breadcrumb for root-level documents
            }
            // For now, just show immediate parent title
            // TODO: Add resolver to fetch full parent chain
            const parentTitle = documents.find((d) => d.urn === parentDoc.urn)?.info?.title || parentDoc.info?.title;
            return parentTitle || 'Unknown Parent';
        },
        [documents],
    );

    // handleToggleExpand is now provided by useDocumentTreeExpansion hook

    /**
     * Recursive helper to render a document and its children in the search results tree
     */
    const renderSearchResultItem = (doc: Document | DocumentChild, level: number): React.ReactNode => {
        const isSelected = selectedParentUrn === doc.urn;
        const hasChildren = hasChildrenMap[doc.urn] || false;
        const isExpanded = expandedUrns.has(doc.urn);
        const isLoading = loadingUrns.has(doc.urn);
        const isDocument = 'info' in doc;
        const breadcrumb = isDocument ? getParentBreadcrumb(doc as Document) : '';
        const children = childrenCache[doc.urn]?.filter((c) => c.urn !== documentUrn) || [];

        return (
            <SearchResultItem
                key={doc.urn}
                doc={doc}
                level={level}
                isSelected={isSelected}
                hasChildren={hasChildren}
                isExpanded={isExpanded}
                isLoading={isLoading}
                breadcrumb={breadcrumb}
                onSelect={() => handleSelectDocument(doc.urn)}
                onToggleExpand={() => handleToggleExpand(doc.urn)}
            >
                {children.length > 0 && <>{children.map((child) => renderSearchResultItem(child, level + 1))}</>}
            </SearchResultItem>
        );
    };

    const isRootSelected = selectedParentUrn === null;
    const hasSelectionChanged = selectedParentUrn !== currentParentUrn;

    return (
        <DialogContainer>
            <SearchContainer>
                <Input label="" placeholder="Move document to..." value={searchQuery} setValue={setSearchQuery} />
            </SearchContainer>

            <TreeScrollContainer>
                {/* Only show "Move to Root" if document is not already at root */}
                {!isSearching && currentParentUrn !== null && currentParentUrn !== undefined && (
                    <RootOption $isSelected={isRootSelected} onClick={handleSelectRoot}>
                        Move to Root
                    </RootOption>
                )}

                {/* Only show loading if we have no documents yet (initial load) */}
                {searchLoading && filteredDocuments.length === 0 && <EmptyState>Loading...</EmptyState>}

                {!searchLoading && filteredDocuments.length === 0 && isSearching && (
                    <EmptyState>No documents found</EmptyState>
                )}

                {filteredDocuments.length > 0 && (
                    <>
                        {isSearching ? (
                            // Tree search results view with expansion support
                            filteredDocuments.map((doc) => renderSearchResultItem(doc, 0))
                        ) : (
                            // Tree view when not searching
                            <DocumentTree
                                documents={filteredDocuments as any}
                                onCreateChild={() => {}}
                                selectedUrn={selectedParentUrn || undefined}
                                onSelectDocument={handleSelectDocument}
                                hideActions
                            />
                        )}
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
        </DialogContainer>
    );
};
