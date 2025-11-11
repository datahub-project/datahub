import React, { useCallback, useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { useDocumentTree } from '@app/documentV2/DocumentTreeContext';
import { useLoadDocumentTree } from '@app/documentV2/hooks/useLoadDocumentTree';
import { DocumentTreeItem } from '@app/homeV2/layout/sidebar/documents/DocumentTreeItem';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

/**
 * DocumentTree - Displays the hierarchical document tree.
 *
 * This component is now dramatically simpler! It just:
 * 1. Reads tree state from DocumentTreeContext
 * 2. Loads children on-demand when nodes are expanded
 * 3. Renders the tree
 *
 * No Apollo cache, no manual state management, no event bus complexity.
 */

const TreeContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

interface DocumentTreeProps {
    onCreateChild: (parentUrn: string | null) => void;
    selectedUrn?: string; // For selection mode (e.g., in move dialog)
    onSelectDocument?: (urn: string) => void; // Callback when document is selected
    hideActions?: boolean; // Hide action buttons (e.g., in move dialog)
}

export const DocumentTree: React.FC<DocumentTreeProps> = ({
    onCreateChild,
    selectedUrn,
    onSelectDocument,
    hideActions = false,
}) => {
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    // Tree state (single source of truth!)
    const { getRootNodes, getNode } = useDocumentTree();
    const { loadChildren, loading } = useLoadDocumentTree();

    // Local UI state for expansion
    const [expandedUrns, setExpandedUrns] = useState<Set<string>>(new Set());
    const [loadingUrns, setLoadingUrns] = useState<Set<string>>(new Set());

    const rootNodes = getRootNodes();

    const getCurrentDocumentUrn = useCallback(() => {
        const match = location.pathname.match(/\/document\/([^/]+)/);
        return match ? decodeURIComponent(match[1]) : null;
    }, [location.pathname]);

    const handleToggleExpand = useCallback(
        async (urn: string) => {
            const node = getNode(urn);
            if (!node) return;

            const isExpanded = expandedUrns.has(urn);

            if (isExpanded) {
                // Collapse
                setExpandedUrns((prev) => {
                    const next = new Set(prev);
                    next.delete(urn);
                    return next;
                });
            } else {
                // Expand
                setExpandedUrns((prev) => new Set(prev).add(urn));

                // Always fetch from server when expanding (if has children)
                // The merge logic will combine server data with any optimistic updates
                if (node.hasChildren) {
                    setLoadingUrns((prev) => new Set(prev).add(urn));
                    await loadChildren(urn);
                    setLoadingUrns((prev) => {
                        const next = new Set(prev);
                        next.delete(urn);
                        return next;
                    });
                }
            }
        },
        [getNode, expandedUrns, loadChildren],
    );

    const handleDocumentClick = useCallback(
        (urn: string) => {
            if (onSelectDocument) {
                // Selection mode (e.g., in move dialog)
                onSelectDocument(urn);
            } else {
                // Navigation mode
                const url = entityRegistry.getEntityUrl(EntityType.Document, urn);
                history.push(url);
            }
        },
        [onSelectDocument, entityRegistry, history],
    );

    const renderTreeNode = useCallback(
        (urn: string, level: number): React.ReactNode => {
            const node = getNode(urn);
            if (!node) return null;

            const isExpanded = expandedUrns.has(urn);
            const isLoading = loadingUrns.has(urn);
            const currentUrn = selectedUrn || getCurrentDocumentUrn();
            const isSelected = currentUrn === urn;

            const children = node.children || [];

            return (
                <React.Fragment key={urn}>
                    <DocumentTreeItem
                        urn={node.urn}
                        title={node.title}
                        level={level}
                        hasChildren={node.hasChildren}
                        isExpanded={isExpanded}
                        isSelected={isSelected}
                        isLoading={isLoading && isExpanded}
                        onToggleExpand={() => handleToggleExpand(node.urn)}
                        onClick={() => handleDocumentClick(node.urn)}
                        onCreateChild={onCreateChild}
                        hideActions={hideActions}
                        parentUrn={node.parentUrn}
                    />
                    {isExpanded && children.length > 0 && (
                        <>{children.map((child) => renderTreeNode(child.urn, level + 1))}</>
                    )}
                </React.Fragment>
            );
        },
        [
            getNode,
            expandedUrns,
            loadingUrns,
            selectedUrn,
            getCurrentDocumentUrn,
            handleToggleExpand,
            handleDocumentClick,
            onCreateChild,
            hideActions,
        ],
    );

    if (loading) {
        return <TreeContainer>Loading documents...</TreeContainer>;
    }

    return <TreeContainer>{rootNodes.map((node) => renderTreeNode(node.urn, 0))}</TreeContainer>;
};
