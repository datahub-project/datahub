import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useDocumentTree } from '@app/document/DocumentTreeContext';
import { useDocumentNavigation } from '@app/document/hooks/useDocumentNavigation';
import { useLoadDocumentTree } from '@app/document/hooks/useLoadDocumentTree';
import { useNodeChildrenLoading } from '@app/document/hooks/useNodeChildrenLoading';
import { useSectionExpansion } from '@app/document/hooks/useSectionExpansion';
import {
    DocumentTreeFilterSelection,
    NO_FILTER_SELECTION,
    filterDocumentNodes,
} from '@app/document/utils/documentTreeFilters';
import {
    DocumentSourceGroup,
    partitionRootNodesByLayer,
} from '@app/document/utils/documentTreeGrouping';
import { ChildLoadMoreTrigger } from '@app/homeV2/layout/sidebar/documents/ChildLoadMoreTrigger';
import { DocumentTreeItem } from '@app/homeV2/layout/sidebar/documents/DocumentTreeItem';
import { TreeSectionHeader } from '@app/homeV2/layout/sidebar/documents/TreeSectionHeader';
import Loading from '@app/shared/Loading';

// Section id for the built-in "DataHub" (native docs) group. Platform groups use
// their platform urn as the id; this sentinel keeps the native group distinct.
const NATIVE_SECTION_ID = '__native__';

const TreeContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

// Sentinel row for the root-level infinite scroll — mirrors the child trigger's
// observer, but for the top-level document list.
const RootObserver = styled.div`
    height: 1px;
    margin-top: 1px;
`;

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
    /**
     * Enables multi-select mode: each row renders a leading checkbox driven by
     * `checkedUrns`, and clicking a row fires `onSelectDocument` for the parent
     * to toggle the URN in its own set. Per-row actions (menu, create-child) are
     * hidden. When omitted, the tree behaves normally (single navigation mode).
     */
    multiSelect?: boolean;
    checkedUrns?: Set<string>;
}

export const DocumentTree: React.FC<DocumentTreeProps> = ({
    onCreateChild,
    selectedUrn,
    onSelectDocument,
    hideActions = false,
    hideActionsMenu = false,
    hideCreate = false,
    filterSelection = NO_FILTER_SELECTION,
    multiSelect = false,
    checkedUrns,
}) => {
    const { t } = useTranslation('misc');

    // Tree state (single source of truth!)
    // Note: expandedUrns is now in context to persist across component remounts
    const { getRootNodes, getNode, expandedUrns } = useDocumentTree();
    const {
        loadChildren,
        loadMoreChildren,
        loading,
        loadingMoreRoots,
        hasMoreRoots,
        hasMoreChildren,
        rootObserverRef,
    } = useLoadDocumentTree();

    // Per-node expand + lazy child loading, and routing/selection glue.
    const { loadingUrns, loadingChildrenUrns, handleToggleExpand, handleLoadMoreChildren } = useNodeChildrenLoading({
        loadChildren,
        loadMoreChildren,
    });
    const { getCurrentDocumentUrn, handleDocumentClick } = useDocumentNavigation(onSelectDocument);

    // Section-scoped expand-all / collapse-all (per DataHub + per-platform group).
    const { isSectionExpanded, isSectionExpanding, toggleSectionExpandAll } = useSectionExpansion(loadChildren);
    const expandAllLabel = t('context.tree.expandAll');
    const collapseAllLabel = t('context.tree.collapseAll');

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

    const renderTreeNode = useCallback(
        (urn: string, level: number): React.ReactNode => {
            const node = getNode(urn);
            if (!node) return null;

            const isExpanded = expandedUrns.has(urn);
            const isLoading = loadingUrns.has(urn);
            const currentUrn = selectedUrn || getCurrentDocumentUrn();
            // In multi-select mode, "selected" means "checked in the parent's URN set".
            // Otherwise, keep the single-selection navigation semantics.
            const isSelected = multiSelect ? !!checkedUrns?.has(urn) : currentUrn === urn;

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
                        multiSelect={multiSelect}
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
            multiSelect,
            checkedUrns,
        ],
    );

    // Each source platform renders as a sibling section to the DataHub section —
    // same label treatment (Mulish/textTertiary), right-side caret, no icon. Its
    // docs sit at level 0 underneath, matching how DataHub docs sit under the
    // DataHub header.
    const renderPlatformGroup = useCallback(
        (group: DocumentSourceGroup) => {
            const { platform, label } = group;
            const isExpanded = !collapsedPlatformUrns.has(platform.urn);
            const allExpanded = isSectionExpanded(group.nodes);

            return (
                <React.Fragment key={platform.urn}>
                    <TreeSectionHeader
                        level={0}
                        label={label}
                        isExpanded={isExpanded}
                        onToggle={() => togglePlatformGroup(platform.urn)}
                        testId={`document-tree-platform-${platform.urn}`}
                        onToggleExpandAll={() => {
                            // Opening the folders is useless if the section itself is collapsed.
                            if (!allExpanded && collapsedPlatformUrns.has(platform.urn)) {
                                togglePlatformGroup(platform.urn);
                            }
                            toggleSectionExpandAll(platform.urn, group.nodes);
                        }}
                        isAllExpanded={allExpanded}
                        expandAllLoading={isSectionExpanding(platform.urn)}
                        expandAllLabel={expandAllLabel}
                        collapseAllLabel={collapseAllLabel}
                    />
                    {isExpanded && group.nodes.map((node) => renderTreeNode(node.urn, 0))}
                </React.Fragment>
            );
        },
        [
            collapsedPlatformUrns,
            renderTreeNode,
            togglePlatformGroup,
            isSectionExpanded,
            isSectionExpanding,
            toggleSectionExpandAll,
            expandAllLabel,
            collapseAllLabel,
        ],
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
                        onToggleExpandAll={() => {
                            if (!isSectionExpanded(nativeRootNodes)) setIsNativeExpanded(true);
                            toggleSectionExpandAll(NATIVE_SECTION_ID, nativeRootNodes);
                        }}
                        isAllExpanded={isSectionExpanded(nativeRootNodes)}
                        expandAllLoading={isSectionExpanding(NATIVE_SECTION_ID)}
                        expandAllLabel={expandAllLabel}
                        collapseAllLabel={collapseAllLabel}
                    />
                    {isNativeExpanded && nativeRootNodes.map((node) => renderTreeNode(node.urn, 0))}
                </>
            )}
            {sourcesByPlatform.map(renderPlatformGroup)}
            {hasMoreRoots && <RootObserver ref={rootObserverRef} />}
            {loadingMoreRoots && <Loading height={12} />}
        </TreeContainer>
    );
};
