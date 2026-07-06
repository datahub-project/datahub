import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useInView } from 'react-intersection-observer';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { useDocumentTree } from '@app/document/DocumentTreeContext';
import { useLoadDocumentTree } from '@app/document/hooks/useLoadDocumentTree';
import {
    DocumentTreeFilterSelection,
    NO_FILTER_SELECTION,
    filterDocumentNodes,
} from '@app/document/utils/documentTreeFilters';
import {
    DocumentSourceGroup,
    formatPlatformLabel,
    partitionRootNodesByLayer,
} from '@app/document/utils/documentTreeGrouping';
import { DocumentTreeItem } from '@app/homeV2/layout/sidebar/documents/DocumentTreeItem';
import Loading from '@app/shared/Loading';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const TreeContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const ObserverContainer = styled.div`
    height: 1px;
    margin-top: 1px;
`;

const LoadMoreContainer = styled.div<{ $level: number }>`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
    min-height: 28px;
`;

// Section header for the DataHub / per-platform group rows. Indent mirrors
// DocumentTreeItem (8 + level*16) so a level-1 platform header lines up with a
// level-1 doc row. The chevron sits on the right (vs. left on tree items) to
// signal that this row is a structural group, not an interactive doc.
// No hover background — it's a tree label, not a nav row; the pointer
// cursor alone is enough affordance for the toggle.
const SectionHeader = styled.button<{ $level: number }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    width: 100%;
    padding: 6px 8px 6px ${(props) => 8 + props.$level * 16}px;
    min-height: 32px;
    border: none;
    background: transparent;
    cursor: pointer;
    text-align: left;
    color: ${(props) => props.theme.colors.textTertiary};
    font-family: Mulish;
    font-size: 14px;
    font-weight: 700;
`;

const SectionHeaderLabel = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

/**
 * Collapsible header row for a tree section (DataHub or a per-platform
 * sub-section). Pure presentation — owns no expansion state of its own.
 */
function TreeSectionHeader({
    level,
    label,
    icon,
    isExpanded,
    onToggle,
    testId,
}: {
    level: number;
    label: string;
    icon?: React.ReactNode;
    isExpanded: boolean;
    onToggle: () => void;
    testId?: string;
}) {
    const Chevron = isExpanded ? CaretDown : CaretRight;
    return (
        <SectionHeader type="button" $level={level} onClick={onToggle} aria-expanded={isExpanded} data-testid={testId}>
            <SectionHeaderLabel>
                {icon}
                {label}
            </SectionHeaderLabel>
            <Chevron size={14} weight="regular" />
        </SectionHeader>
    );
}

/**
 * Invisible trigger that auto-loads more children when scrolled into view.
 */
function ChildLoadMoreTrigger({
    parentUrn,
    level,
    loading: isLoading,
    onLoad,
}: {
    parentUrn: string;
    level: number;
    loading: boolean;
    onLoad: (parentUrn: string) => void;
}) {
    const [ref, inView] = useInView();

    useEffect(() => {
        if (inView && !isLoading) {
            onLoad(parentUrn);
        }
    }, [inView, isLoading, parentUrn, onLoad]);

    if (isLoading) {
        return (
            <LoadMoreContainer $level={level}>
                <Loading height={12} />
            </LoadMoreContainer>
        );
    }

    return <ObserverContainer ref={ref} />;
}

interface DocumentTreeProps {
    onCreateChild: (parentUrn: string | null) => void;
    selectedUrn?: string; // For selection mode (e.g., in move dialog)
    onSelectDocument?: (urn: string) => void; // Callback when document is selected
    hideActions?: boolean; // Hide action buttons (e.g., in move dialog)
    hideActionsMenu?: boolean; // Hide move/delete menu actions
    hideCreate?: boolean; // Hide create/add button
    /**
     * Active filter selection. When omitted, no filtering is applied (all nodes
     * render). The filter is applied uniformly at every tree level — root rows
     * and each expanded children list — so child rows can be filtered out while
     * their parents remain visible (and vice versa).
     */
    filterSelection?: DocumentTreeFilterSelection;
}

export const DocumentTree: React.FC<DocumentTreeProps> = ({
    onCreateChild,
    selectedUrn,
    onSelectDocument,
    hideActions = false,
    hideActionsMenu = false,
    hideCreate = false,
    filterSelection = NO_FILTER_SELECTION,
}) => {
    const { t } = useTranslation('misc');
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    // Tree state (single source of truth!)
    // Note: expandedUrns is now in context to persist across component remounts
    const { getRootNodes, getNode, expandedUrns, expandNode, collapseNode } = useDocumentTree();
    const {
        loadChildren,
        loadMoreChildren,
        loading,
        loadingMoreRoots,
        hasMoreRoots,
        hasMoreChildren,
        rootObserverRef,
    } = useLoadDocumentTree();

    // Local UI state for loading indicators only
    const [loadingUrns, setLoadingUrns] = useState<Set<string>>(new Set());

    // Child infinite scroll loading state
    const [loadingChildrenUrns, setLoadingChildrenUrns] = useState<Set<string>>(new Set());

    const handleLoadMoreChildren = useCallback(
        async (parentUrn: string) => {
            setLoadingChildrenUrns((prev) => new Set(prev).add(parentUrn));
            await loadMoreChildren(parentUrn);
            setLoadingChildrenUrns((prev) => {
                const next = new Set(prev);
                next.delete(parentUrn);
                return next;
            });
        },
        [loadMoreChildren],
    );

    const rootNodes = getRootNodes();
    const visibleRootNodes = useMemo(
        () => filterDocumentNodes(rootNodes, filterSelection),
        [rootNodes, filterSelection],
    );

    // Partition visible roots into the native ("DataHub") layer and per-platform
    // source groups for the layered sidebar layout.
    const { native: nativeRootNodes, sourcesByPlatform } = useMemo(
        () => partitionRootNodesByLayer(visibleRootNodes),
        [visibleRootNodes],
    );

    // Section expansion state — local to this component since these are UI
    // groupings, not part of the tree's underlying data model. Each source
    // platform is a top-level collapsible group, sibling to the DataHub section.
    const [isNativeExpanded, setIsNativeExpanded] = useState(true);
    const [collapsedPlatformUrns, setCollapsedPlatformUrns] = useState<Set<string>>(new Set());

    const togglePlatformGroup = useCallback((urn: string) => {
        setCollapsedPlatformUrns((prev) => {
            const next = new Set(prev);
            if (next.has(urn)) next.delete(urn);
            else next.add(urn);
            return next;
        });
    }, []);

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
                collapseNode(urn);
            } else {
                // Expand
                expandNode(urn);

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
        [getNode, expandedUrns, expandNode, collapseNode, loadChildren],
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

            // Filter loaded children at render time. Done here (rather than mutating tree state)
            // so toggling filters never refetches or mutates the underlying tree.
            const visibleChildren = filterDocumentNodes(node.children || [], filterSelection);

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
                        isUnpublished={node.isUnpublished}
                        isExternal={node.isExternal}
                        platform={node.platform}
                        onToggleExpand={() => handleToggleExpand(node.urn)}
                        onClick={() => handleDocumentClick(node.urn)}
                        onCreateChild={onCreateChild}
                        hideActions={hideActions}
                        hideActionsMenu={hideActionsMenu}
                        hideCreate={hideCreate}
                        parentUrn={node.parentUrn}
                    />
                    {isExpanded && visibleChildren.length > 0 && (
                        <>
                            {visibleChildren.map((child) => renderTreeNode(child.urn, level + 1))}
                            {hasMoreChildren(urn) && (
                                <ChildLoadMoreTrigger
                                    parentUrn={urn}
                                    level={level + 1}
                                    loading={loadingChildrenUrns.has(urn)}
                                    onLoad={handleLoadMoreChildren}
                                />
                            )}
                        </>
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
            hideActionsMenu,
            hideCreate,
            hasMoreChildren,
            loadingChildrenUrns,
            handleLoadMoreChildren,
            filterSelection,
        ],
    );

    // Each source platform renders as a sibling section to the DataHub section —
    // same label treatment (Mulish/textTertiary), right-side caret, no icon. Its
    // docs sit at level 0 underneath, matching how DataHub docs sit under the
    // DataHub header.
    const renderPlatformGroup = useCallback(
        (group: DocumentSourceGroup) => {
            const { platform } = group;
            const isExpanded = !collapsedPlatformUrns.has(platform.urn);

            return (
                <React.Fragment key={platform.urn}>
                    <TreeSectionHeader
                        level={0}
                        label={formatPlatformLabel(platform)}
                        isExpanded={isExpanded}
                        onToggle={() => togglePlatformGroup(platform.urn)}
                        testId={`document-tree-platform-${platform.urn}`}
                    />
                    {isExpanded && group.nodes.map((node) => renderTreeNode(node.urn, 0))}
                </React.Fragment>
            );
        },
        [collapsedPlatformUrns, renderTreeNode, togglePlatformGroup],
    );

    if (loading) {
        return <Loading height={16} />;
    }

    return (
        <TreeContainer>
            {nativeRootNodes.length > 0 && (
                <>
                    <TreeSectionHeader
                        level={0}
                        label={t('context.tree.dataHubSection')}
                        isExpanded={isNativeExpanded}
                        onToggle={() => setIsNativeExpanded((v) => !v)}
                        testId="document-tree-datahub-section"
                    />
                    {isNativeExpanded && nativeRootNodes.map((node) => renderTreeNode(node.urn, 0))}
                </>
            )}
            {sourcesByPlatform.map(renderPlatformGroup)}
            {hasMoreRoots && <ObserverContainer ref={rootObserverRef} />}
            {loadingMoreRoots && <Loading height={12} />}
        </TreeContainer>
    );
};
