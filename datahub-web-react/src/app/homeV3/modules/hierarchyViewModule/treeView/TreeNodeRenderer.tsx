import { Button, Checkbox, Loader } from '@components';
import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components';

import ChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/treeView/ChildrenLoader';
import DepthMargin from '@app/homeV3/modules/hierarchyViewModule/treeView/DepthMargin';
import ExpandToggler from '@app/homeV3/modules/hierarchyViewModule/treeView/ExpandToggler';
import useTreeViewContext from '@app/homeV3/modules/hierarchyViewModule/treeView/context/useTreeViewContext';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

const Row = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
    width: 100%;
`;

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

const LoaderWrapper = styled.div``;

interface Props {
    node: TreeNode;
    depth: number;
}

export default function TreeNodeRenderer({ node, depth }: Props) {
    const {
        getHasParentNode,
        getIsRootNode,
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
        toggleSelected,
        getIsChildrenLoading,
        getNumberOfNotLoadedChildren,
        loadChildren,
        numberOfChildrenToLoad,
    } = useTreeViewContext();

    const isExpandable = useMemo(() => getIsExpandable(node), [node, getIsExpandable]);
    const isExpanded = useMemo(() => getIsExpanded(node), [node, getIsExpanded]);

    const isSelectable = useMemo(() => getIsSelectable(node), [node, getIsSelectable]);
    const isSelected = useMemo(() => getIsSelected(node), [node, getIsSelected]);
    const hasSelectedChildren = useMemo(() => getHasSelectedChildren(node), [node, getHasSelectedChildren]);

    const isChildrenLoading = useMemo(() => getIsChildrenLoading(node), [node, getIsChildrenLoading]);

    const hasParentNode = useMemo(() => getHasParentNode(node), [node, getHasParentNode]);
    const isParentSelected = useMemo(() => getIsParentSelected(node), [node, getIsParentSelected]);

    const hasAnySiblingsExpandable = useMemo(
        () => getHasAnyExpandableSiblings(node),
        [node, getHasAnyExpandableSiblings],
    );

    const isRootNode = useMemo(() => getIsRootNode(node), [node, getIsRootNode]);

    const numberOfNotLoadedChildren = useMemo(
        () => getNumberOfNotLoadedChildren(node),
        [node, getNumberOfNotLoadedChildren],
    );

    const maxNumberOfChildrenToLoad = Math.min(numberOfNotLoadedChildren, numberOfChildrenToLoad);
    const shouldShowLoadMoreButton = isExpanded && !isChildrenLoading && numberOfNotLoadedChildren > 0;

    // Automatically select the current node if parent is selected if explicitlySelectChildren is not enabled
    // FYI: required to get loaded children selected if parent is selected
    useEffect(() => {
        if (!explicitlySelectChildren && isSelectable && !isSelected && hasParentNode && isParentSelected) {
            select(node);
        }
    }, [explicitlySelectChildren, hasParentNode, isParentSelected, select, node, isSelected, isSelectable]);

    return (
        <>
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
                            isIntermediate={!isSelected && hasSelectedChildren}
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

            {/* Show more button */}
            {shouldShowLoadMoreButton && (
                <Row>
                    <DepthMargin depth={depth + 1} />
                    <ExpandToggler expandable={false} />
                    <Button onClick={() => loadChildren(node)} variant="link">
                        Load {maxNumberOfChildrenToLoad} more
                    </Button>
                </Row>
            )}

            {/* Loading indicator */}
            {isExpanded && isChildrenLoading && (
                <Row>
                    <DepthMargin depth={depth + 1} />
                    <ExpandToggler expandable={false} />
                    <LoaderWrapper>
                        <Loader size="xs" />
                    </LoaderWrapper>
                </Row>
            )}
        </>
    );
}
