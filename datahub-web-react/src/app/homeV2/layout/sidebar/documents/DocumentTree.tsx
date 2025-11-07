import React, { useCallback, useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';
import { useDocumentChildren } from '@app/documentV2/hooks/useDocumentChildren';
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
}

export const DocumentTree: React.FC<DocumentTreeProps> = ({ documents, onCreateChild }) => {
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();
    const { checkForChildren, fetchChildren } = useDocumentChildren();
    const { newDocument, optimisticDocuments } = useDocumentsContext();

    // Track which documents are expanded
    const [expandedUrns, setExpandedUrns] = useState<Set<string>>(new Set());
    // Track which documents have children
    const [hasChildrenMap, setHasChildrenMap] = useState<Record<string, boolean>>({});
    // Track loaded children for each parent
    const [childrenCache, setChildrenCache] = useState<Record<string, DocumentNode[]>>({});
    // Track which documents are currently loading children
    const [loadingUrns, setLoadingUrns] = useState<Set<string>>(new Set());

    // All documents passed in are root-level (filtered by useContextMenuItems)
    const rootDocuments = documents;

    // Check for children on mount and when root documents change
    useEffect(() => {
        const checkChildren = async () => {
            const urns = rootDocuments.map((doc) => doc.urn);
            if (urns.length > 0) {
                const childrenMap = await checkForChildren(urns);
                setHasChildrenMap((prev) => ({ ...prev, ...childrenMap }));
            }
        };
        checkChildren();
    }, [rootDocuments, checkForChildren]);

    // Refetch children when a new document is created
    useEffect(() => {
        if (newDocument?.parentDocument) {
            // Mark parent as having children
            setHasChildrenMap((prev) => ({
                ...prev,
                [newDocument.parentDocument!]: true,
            }));

            // Auto-expand the parent
            setExpandedUrns((prev) => new Set(prev).add(newDocument.parentDocument!));

            // Invalidate cache for the parent so it refetches
            setChildrenCache((prev) => {
                const updated = { ...prev };
                delete updated[newDocument.parentDocument!];
                return updated;
            });
        }
    }, [newDocument]);

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

                    const childNodes: DocumentNode[] = children.map((c) => ({
                        urn: c.urn,
                        title: c.title,
                        parentUrn: urn,
                    }));

                    setChildrenCache((prev) => ({
                        ...prev,
                        [urn]: childNodes,
                    }));

                    // Check if these children have children
                    if (childNodes.length > 0) {
                        const childUrns = childNodes.map((c) => c.urn);
                        const childrenMap = await checkForChildren(childUrns);
                        setHasChildrenMap((prev) => ({ ...prev, ...childrenMap }));
                    }
                }
            }
        },
        [expandedUrns, childrenCache, loadingUrns, fetchChildren, checkForChildren],
    );

    const handleDocumentClick = useCallback(
        (urn: string) => {
            const url = entityRegistry.getEntityUrl(EntityType.Document, urn);
            history.push(url);
        },
        [entityRegistry, history],
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
            const currentUrn = getCurrentDocumentUrn();
            const isSelected = currentUrn === node.urn;

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
                        title={node.title}
                        level={level}
                        hasChildren={hasChildren || optimisticChildren.length > 0}
                        isExpanded={isExpanded}
                        isSelected={isSelected}
                        isLoading={isLoading && isExpanded}
                        onToggleExpand={() => handleToggleExpand(node.urn)}
                        onClick={() => handleDocumentClick(node.urn)}
                        onCreateChild={onCreateChild}
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
            handleToggleExpand,
            handleDocumentClick,
            onCreateChild,
            optimisticDocuments,
        ],
    );

    return (
        <TreeContainer>
            {rootDocuments.map((doc) =>
                renderTreeNode(
                    {
                        urn: doc.urn,
                        title: doc.info?.title || 'New Document',
                    },
                    0,
                ),
            )}
        </TreeContainer>
    );
};
