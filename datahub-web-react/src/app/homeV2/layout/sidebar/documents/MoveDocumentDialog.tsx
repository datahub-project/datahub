import { CaretDown, CaretRight, FileText, Folder } from '@phosphor-icons/react';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useSearchDocuments } from '@app/documentV2/hooks/useSearchDocuments';
import { useMoveDocument } from '@app/documentV2/hooks/useMoveDocument';
import { useDocumentChildren, DocumentChild } from '@app/documentV2/hooks/useDocumentChildren';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { Button, Input } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';
import Loading from '@app/shared/Loading';

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

const SearchResultItem = styled.div<{ $isSelected: boolean; $level: number }>`
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
    border-radius: 6px;
    cursor: pointer;
    margin-bottom: 2px;
    transition: background-color 0.15s ease;
    display: flex;
    align-items: center;
    gap: 4px;
    
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

const SearchResultContent = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 0;
    margin-left: 8px; /* Add spacing between icon and text */
`;

const SearchResultTitle = styled.div`
    font-size: 14px;
    line-height: 20px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    /* No explicit color - inherits default text color like DocumentTreeItem */
`;

const SearchResultBreadcrumb = styled.div`
    font-size: 12px;
    color: ${colors.gray[600]};
    line-height: 16px;
    margin-top: 2px;
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;
    color: ${colors.gray[600]};
    flex-shrink: 0;
`;

const ExpandButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: ${colors.gray[600]};
    flex-shrink: 0;

    &:hover {
        opacity: 0.7;
    }
`;

interface MoveDocumentDialogProps {
    documentUrn: string;
    currentParentUrn?: string | null;
    onClose: () => void;
}

export const MoveDocumentDialog: React.FC<MoveDocumentDialogProps> = ({
    documentUrn,
    currentParentUrn,
    onClose,
}) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');
    const [selectedParentUrn, setSelectedParentUrn] = useState<string | null | undefined>(currentParentUrn);
    const { moveDocument, loading: movingDocument } = useMoveDocument();
    const { checkForChildren, fetchChildren } = useDocumentChildren();
    
    // Track expanded documents in search results
    const [expandedUrns, setExpandedUrns] = useState<Set<string>>(new Set());
    const [hasChildrenMap, setHasChildrenMap] = useState<Record<string, boolean>>({});
    const [childrenCache, setChildrenCache] = useState<Record<string, DocumentChild[]>>({});
    const [loadingUrns, setLoadingUrns] = useState<Set<string>>(new Set());

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
        [filteredDocuments]
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

    const getParentBreadcrumb = useCallback((doc: Document) => {
        const parentDoc = doc.info?.parentDocument?.document;
        if (!parentDoc) {
            return null; // Don't show breadcrumb for root-level documents
        }
        // For now, just show immediate parent title
        // TODO: Add resolver to fetch full parent chain
        const parentTitle = documents.find((d) => d.urn === parentDoc.urn)?.info?.title || parentDoc.info?.title;
        return parentTitle || 'Unknown Parent';
    }, [documents]);

    const handleToggleExpand = useCallback(
        async (urn: string) => {
            const isExpanded = expandedUrns.has(urn);

            if (isExpanded) {
                // Collapse
                setExpandedUrns((prev) => {
                    const next = new Set(prev);
                    next.delete(urn);
                    return next;
                });
            } else {
                // Expand - load children if not already loaded
                setExpandedUrns((prev) => new Set(prev).add(urn));

                if (!childrenCache[urn] && !loadingUrns.has(urn)) {
                    setLoadingUrns((prev) => new Set(prev).add(urn));
                    const children = await fetchChildren(urn);
                    setLoadingUrns((prev) => {
                        const next = new Set(prev);
                        next.delete(urn);
                        return next;
                    });

                    setChildrenCache((prev) => ({
                        ...prev,
                        [urn]: children,
                    }));

                    // Check if these children have children and filter out the document being moved
                    const validChildren = children.filter((c) => c.urn !== documentUrn);
                    if (validChildren.length > 0) {
                        const childUrns = validChildren.map((c) => c.urn);
                        const childrenMap = await checkForChildren(childUrns);
                        setHasChildrenMap((prev) => ({ ...prev, ...childrenMap }));
                    }
                }
            }
        },
        [expandedUrns, childrenCache, loadingUrns, fetchChildren, checkForChildren, documentUrn],
    );

    // Component for rendering individual search result items with hover state
    const SearchResultItemComponent: React.FC<{
        doc: Document | DocumentChild;
        level: number;
    }> = ({ doc, level }) => {
        const [isHovered, setIsHovered] = useState(false);
        const isSelected = selectedParentUrn === doc.urn;
        const hasChildren = hasChildrenMap[doc.urn] || false;
        const isExpanded = expandedUrns.has(doc.urn);
        const isLoading = loadingUrns.has(doc.urn);
        const isDocument = 'info' in doc;
        const title = isDocument ? doc.info?.title || 'Untitled' : (doc as DocumentChild).title;
        const breadcrumb = isDocument ? getParentBreadcrumb(doc as Document) : '';
        const children = childrenCache[doc.urn]?.filter((c) => c.urn !== documentUrn) || [];

        // Match DocumentTreeItem behavior: show expand button on hover or when expanded
        const showExpandButton = hasChildren && (isExpanded || isHovered);
        const showIcon = !showExpandButton;

        return (
            <>
                <SearchResultItem
                    $isSelected={isSelected}
                    $level={level}
                    onClick={() => handleSelectDocument(doc.urn)}
                    onMouseEnter={() => setIsHovered(true)}
                    onMouseLeave={() => setIsHovered(false)}
                >
                    {showExpandButton && (
                        <ExpandButton
                            onClick={(e) => {
                                e.stopPropagation();
                                handleToggleExpand(doc.urn);
                            }}
                        >
                            {isLoading ? (
                                <Loading height={16} marginTop={0} alignItems="center" />
                            ) : isExpanded ? (
                                <CaretDown size={16} weight="bold" />
                            ) : (
                                <CaretRight size={16} weight="bold" />
                            )}
                        </ExpandButton>
                    )}
                    {showIcon && (
                        <IconWrapper>
                            {hasChildren ? (
                                <Folder size={16} weight={isSelected ? 'fill' : 'regular'} />
                            ) : (
                                <FileText size={16} weight={isSelected ? 'fill' : 'regular'} />
                            )}
                        </IconWrapper>
                    )}
                    <SearchResultContent>
                        <SearchResultTitle>{title}</SearchResultTitle>
                        {level === 0 && breadcrumb && <SearchResultBreadcrumb>{breadcrumb}</SearchResultBreadcrumb>}
                    </SearchResultContent>
                </SearchResultItem>
                {isExpanded && children.length > 0 && (
                    <>
                        {children.map((child) => (
                            <SearchResultItemComponent key={child.urn} doc={child} level={level + 1} />
                        ))}
                    </>
                )}
            </>
        );
    };

    const isRootSelected = selectedParentUrn === null;
    const hasSelectionChanged = selectedParentUrn !== currentParentUrn;

    return (
        <DialogContainer>
            <SearchContainer>
                <Input
                    label=""
                    placeholder="Move document to..."
                    value={searchQuery}
                    setValue={setSearchQuery}
                />
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
                            filteredDocuments.map((doc) => (
                                <SearchResultItemComponent key={doc.urn} doc={doc} level={0} />
                            ))
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

