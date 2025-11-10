import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';
import { useDocumentChildren } from '@app/documentV2/hooks/useDocumentChildren';
import { useDocumentTreeExpansion } from '@app/documentV2/hooks/useDocumentTreeExpansion';
import { refetchExpandedChildren } from '@app/documentV2/utils/refetchDocumentChildren';
import { DocumentTreeItem } from '@app/homeV2/layout/sidebar/documents/DocumentTreeItem';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const TreeContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

interface DocumentNode {
    urn: string;
    title: string;
    parentUrn?: string | null;
    children?: DocumentNode[];
}

interface DocumentTreeProps {
    documents: Array<{ urn: string; info?: { title?: string } }>;
    onCreateChild: (parentUrn: string) => void;
    selectedUrn?: string; // For selection mode (e.g., in move dialog)
    onSelectDocument?: (urn: string) => void; // Callback when document is selected
    hideActions?: boolean; // Hide action buttons (e.g., in move dialog)
}

export const DocumentTree: React.FC<DocumentTreeProps> = ({
    documents,
    onCreateChild,
    selectedUrn,
    onSelectDocument,
    hideActions = false,
}) => {
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();
    const { checkForChildren, fetchChildren } = useDocumentChildren();
    const { newDocument, optimisticDocuments, updatedDocument, deletedDocument } = useDocumentsContext();

    // Use shared expansion hook to manage tree expansion state
    const {
        expandedUrns,
        hasChildrenMap,
        childrenCache,
        loadingUrns,
        handleToggleExpand,
        setExpandedUrns,
        setHasChildrenMap,
        setChildrenCache,
    } = useDocumentTreeExpansion();

    // Track optimistic title updates (urn -> title)
    const [titleOverrides, setTitleOverrides] = useState<Record<string, string>>({});

    // Create a stable identifier for when documents actually change
    const documentUrnsKey = useMemo(() => documents.map((doc) => doc.urn).join(','), [documents]);

    // Check for children on mount and when documents change
    useEffect(() => {
        const checkChildren = async () => {
            const urns = documents.map((doc) => doc.urn);
            if (urns.length > 0) {
                const childrenMap = await checkForChildren(urns);
                setHasChildrenMap((prev) => ({ ...prev, ...childrenMap }));
            }
        };
        checkChildren();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [documentUrnsKey]);

    // Listen for title updates and apply them optimistically
    useEffect(() => {
        if (updatedDocument?.urn && updatedDocument?.title !== undefined) {
            setTitleOverrides((prev) => ({
                ...prev,
                [updatedDocument.urn]: updatedDocument.title!,
            }));
        }
    }, [updatedDocument]);

    // Handle document move - refresh caches when parent changes
    useEffect(() => {
        if (updatedDocument?.urn && updatedDocument?.parentDocument !== undefined) {
            // Optimistically mark the destination parent as having children
            if (updatedDocument.parentDocument !== null) {
                setHasChildrenMap((prev) => ({
                    ...prev,
                    [updatedDocument.parentDocument!]: true,
                }));
            }

            // Clear all children caches to force refetch
            // This ensures moved documents disappear from old location and appear in new location
            setChildrenCache({});

            // Refetch children data for expanded nodes
            refetchExpandedChildren({
                documentUrns: documents.map((doc) => doc.urn),
                expandedUrns,
                checkForChildren,
                fetchChildren,
                onHasChildrenUpdate: (childrenMap) => {
                    setHasChildrenMap((prev) => ({ ...prev, ...childrenMap }));
                },
                onChildrenCacheUpdate: (urn, children) => {
                    setChildrenCache((prev) => ({
                        ...prev,
                        [urn]: children,
                    }));
                },
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [updatedDocument?.urn, updatedDocument?.parentDocument]);

    // Optimistically add new document when created
    useEffect(() => {
        if (newDocument?.parentDocument && newDocument?.urn && newDocument?.title) {
            // Mark parent as having children
            setHasChildrenMap((prev) => ({
                ...prev,
                [newDocument.parentDocument!]: true,
            }));

            // Auto-expand the parent
            setExpandedUrns((prev) => new Set(prev).add(newDocument.parentDocument!));

            // Optimistically add the new document to the parent's children cache
            setChildrenCache((prev) => {
                const parentChildren = prev[newDocument.parentDocument!] || [];
                const newChild: DocumentNode = {
                    urn: newDocument.urn,
                    title: newDocument.title!,
                    parentUrn: newDocument.parentDocument!,
                };

                // Check if the document already exists (to avoid duplicates)
                const childExists = parentChildren.some((child) => child.urn === newDocument.urn);
                if (childExists) {
                    return prev;
                }

                return {
                    ...prev,
                    [newDocument.parentDocument!]: [...parentChildren, newChild],
                };
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [newDocument]);

    // Handle document deletion - clear children cache to refetch
    useEffect(() => {
        if (deletedDocument?.urn) {
            // Clear all children caches to force refetch
            // This ensures deleted documents disappear from the tree
            setChildrenCache({});

            // Refetch children data for expanded nodes
            refetchExpandedChildren({
                documentUrns: documents.map((doc) => doc.urn),
                expandedUrns,
                checkForChildren,
                fetchChildren,
                onHasChildrenUpdate: (childrenMap) => {
                    setHasChildrenMap((prev) => ({ ...prev, ...childrenMap }));
                },
                onChildrenCacheUpdate: (urn, children) => {
                    setChildrenCache((prev) => ({
                        ...prev,
                        [urn]: children,
                    }));
                },
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [deletedDocument?.urn]);

    // handleToggleExpand is now provided by useDocumentTreeExpansion hook

    const handleDocumentClick = useCallback(
        (urn: string) => {
            if (onSelectDocument) {
                // Selection mode
                onSelectDocument(urn);
            } else {
                // Navigation mode
                const url = entityRegistry.getEntityUrl(EntityType.Document, urn);
                history.push(url);
            }
        },
        [onSelectDocument, entityRegistry, history],
    );

    const getCurrentDocumentUrn = useCallback(() => {
        const match = location.pathname.match(/\/document\/([^/]+)/);
        return match ? decodeURIComponent(match[1]) : null;
    }, [location.pathname]);

    const renderTreeNode = useCallback(
        (node: DocumentNode, level: number): React.ReactNode => {
            const isExpanded = expandedUrns.has(node.urn);
            const hasChildren = hasChildrenMap[node.urn] || false;
            const isLoading = loadingUrns.has(node.urn);
            const cachedChildren = childrenCache[node.urn] || [];
            const currentUrn = selectedUrn || getCurrentDocumentUrn();
            const isSelected = currentUrn === node.urn;

            // Apply title override if exists (for optimistic updates)
            const displayTitle = titleOverrides[node.urn] || node.title;

            // Include optimistic children for this parent
            const optimisticChildren = optimisticDocuments
                .filter((opt) => opt.parentDocument === node.urn)
                .map((opt) => ({
                    urn: opt.urn,
                    title: opt.title,
                    parentUrn: node.urn,
                }));

            // Merge optimistic and cached children, removing duplicates
            const allChildren = [
                ...optimisticChildren,
                ...cachedChildren.filter((c) => !optimisticChildren.some((opt) => opt.urn === c.urn)),
            ];

            return (
                <React.Fragment key={node.urn}>
                    <DocumentTreeItem
                        urn={node.urn}
                        title={displayTitle}
                        level={level}
                        hasChildren={hasChildren || optimisticChildren.length > 0}
                        isExpanded={isExpanded}
                        isSelected={isSelected}
                        isLoading={isLoading && isExpanded}
                        onToggleExpand={() => handleToggleExpand(node.urn)}
                        onClick={() => handleDocumentClick(node.urn)}
                        onCreateChild={onCreateChild}
                        hideActions={hideActions}
                        parentUrn={node.parentUrn}
                    />
                    {isExpanded && allChildren.length > 0 && (
                        <>{allChildren.map((child) => renderTreeNode(child, level + 1))}</>
                    )}
                </React.Fragment>
            );
        },
        [
            expandedUrns,
            hasChildrenMap,
            loadingUrns,
            childrenCache,
            getCurrentDocumentUrn,
            selectedUrn,
            handleToggleExpand,
            handleDocumentClick,
            onCreateChild,
            optimisticDocuments,
            titleOverrides,
            hideActions,
        ],
    );

    return (
        <TreeContainer>
            {documents.map((doc) => {
                // Apply title override if exists (for optimistic updates)
                const title = titleOverrides[doc.urn] || doc.info?.title || 'New Document';
                return renderTreeNode(
                    {
                        urn: doc.urn,
                        title,
                        parentUrn: null, // Root documents have no parent
                    },
                    0,
                );
            })}
        </TreeContainer>
    );
};
