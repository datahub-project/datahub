import { Checkbox } from '@components';
import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components';

import ChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/treeView/ChildrenLoader';
import DepthMargin from '@app/homeV3/modules/hierarchyViewModule/treeView/DepthMargin';
import ExpandToggler from '@app/homeV3/modules/hierarchyViewModule/treeView/ExpandToggler';
import Row from '@app/homeV3/modules/hierarchyViewModule/treeView/components/Row';
import TreeNodesViewLoader from '@app/homeV3/modules/hierarchyViewModule/treeView/components/TreeNodesViewLoader';
import NodesLoaderWrapper from '@app/homeV3/modules/hierarchyViewModule/treeView/components/itemsLoaderWrapper/NodesLoaderWrapper';
import useTreeViewContext from '@app/homeV3/modules/hierarchyViewModule/treeView/context/useTreeViewContext';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

interface Props {
    node: TreeNode;
    depth: number;
}

export default function TreeNodeRenderer({ node, depth }: Props) {
    const {
        loadingTriggerType,
        getHasParentNode,
        getIsRootNode,
        getChildrenTotal,
        getChildrenLength,
        renderNodeLabel,
        getIsExpandable,
        getIsExpanded,
        toggleExpanded,
        getHasAnyExpandableSiblings,
        getIsSelectable,
        getIsSelected,
        getIsParentSelected,
        getHasSelectedChildren,
        select,
        explicitlySelectChildren,
        enableIntermediateSelectState,
        toggleSelected,
        getIsChildrenLoading,
        loadChildren,
        loadBatchSize: numberOfChildrenToLoad,
    } = useTreeViewContext();

    const isExpandable = useMemo(() => getIsExpandable(node), [node, getIsExpandable]);
    const isExpanded = useMemo(() => getIsExpanded(node), [node, getIsExpanded]);

    const isSelectable = useMemo(() => getIsSelectable(node), [node, getIsSelectable]);
    const isSelected = useMemo(() => getIsSelected(node), [node, getIsSelected]);
    const hasSelectedChildren = useMemo(() => getHasSelectedChildren(node), [node, getHasSelectedChildren]);
    const isIntermediatelySelected = useMemo(
        () => !isSelected && hasSelectedChildren && !!enableIntermediateSelectState,
        [isSelected, hasSelectedChildren, enableIntermediateSelectState],
    );

    const isChildrenLoading = useMemo(() => getIsChildrenLoading(node), [node, getIsChildrenLoading]);

    const hasParentNode = useMemo(() => getHasParentNode(node), [node, getHasParentNode]);
    const isParentSelected = useMemo(() => getIsParentSelected(node), [node, getIsParentSelected]);

    const hasAnySiblingsExpandable = useMemo(
        () => getHasAnyExpandableSiblings(node),
        [node, getHasAnyExpandableSiblings],
    );

    const isRootNode = useMemo(() => getIsRootNode(node), [node, getIsRootNode]);

    const childrenTotal = useMemo(() => getChildrenTotal(node), [node, getChildrenTotal]);
    const childrenLength = useMemo(() => getChildrenLength(node), [node, getChildrenLength]);

    // Automatically select the current node if parent is selected if explicitlySelectChildren is not enabled
    // FYI: required to get loaded children selected if parent is selected
    useEffect(() => {
        if (!explicitlySelectChildren && isSelectable && !isSelected && hasParentNode && isParentSelected) {
            select(node);
        }
    }, [explicitlySelectChildren, hasParentNode, isParentSelected, select, node, isSelected, isSelectable]);

    return (
        <NodesLoaderWrapper
            trigger={loadingTriggerType}
            onLoad={() => loadChildren(node)}
            total={childrenTotal}
            current={childrenLength}
            pageSize={numberOfChildrenToLoad}
            depth={depth + 1}
            enabled={isExpanded}
            loading={isChildrenLoading}
        >
            <Row>
                <DepthMargin depth={depth} />

                {/* Expand/collapse toggler */}
                {(hasAnySiblingsExpandable || !isRootNode) && (
                    <ExpandToggler
                        expandable={isExpandable}
                        expanded={isExpanded}
                        onToggle={() => toggleExpanded(node)}
                    />
                )}

                {renderNodeLabel ? renderNodeLabel({ node, depth }) : <>{node.value}</>}

                {/* Checkbox */}
                {isSelectable && (
                    <>
                        <SpaceFiller />
                        <Checkbox
                            isChecked={isSelected}
                            setIsChecked={() => toggleSelected(node)}
                            isIntermediate={isIntermediatelySelected}
                            dataTestId="hierarchy-selection-checkbox"
                        />
                    </>
                )}
            </Row>

            {/* Children nodes */}
            {isExpanded &&
                node.children?.map((childNode) => (
                    <TreeNodeRenderer node={childNode} depth={depth + 1} key={childNode.value} />
                ))}

            {/* Run loading on expand */}
            {isExpanded && <ChildrenLoader node={node} />}

            {/* Loading indicator */}
            {isExpanded && isChildrenLoading && <TreeNodesViewLoader depth={depth + 1} />}
        </NodesLoaderWrapper>
    );
}
