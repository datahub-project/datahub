import { Checkbox, Loader } from '@components';
import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components';

import DepthMargin from '@app/homeV3/modules/hierarchyViewModule/treeView/DepthMargin';
import ExpandToggler from '@app/homeV3/modules/hierarchyViewModule/treeView/ExpandToggler';
import { useTreeViewContext } from '@app/homeV3/modules/hierarchyViewModule/treeView/context';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
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
        renderNodeLabel,
        getIsExpandable,
        getIsExpanded,
        toggleExpanded,
        getIsSelectable,
        getIsSelected,
        getIsParentSelected,
        getHasSelectedChildren,
        select,
        explicitlySelectChildren,
        toggleSelected,
        getIsChildrenLoading,
    } = useTreeViewContext();

    const isExpandable = useMemo(() => getIsExpandable(node), [node, getIsExpandable]);
    const isExpanded = useMemo(() => getIsExpanded(node), [node, getIsExpanded]);

    const isSelectable = useMemo(() => getIsSelectable(node), [node, getIsSelectable]);
    const isSelected = useMemo(() => getIsSelected(node), [node, getIsSelected]);
    const hasSelectedChildren = useMemo(() => getHasSelectedChildren(node), [node, getHasSelectedChildren]);

    const isChildrenLoading = useMemo(() => getIsChildrenLoading(node), [node, getIsChildrenLoading]);

    const hasParentNode = useMemo(() => getHasParentNode(node), [node, getHasParentNode]);
    const isParentSelected = useMemo(() => getIsParentSelected(node), [node, getIsParentSelected]);

    // Automatically select the current node if parent is selected if explicitlySelectChildren is not enabled
    // FYI: required to get loaded children selected if parent is selected
    useEffect(() => {
        if (!explicitlySelectChildren && isSelectable && !isSelected && hasParentNode && isParentSelected) {
            select(node);
        }
    }, [explicitlySelectChildren, hasParentNode, isParentSelected, select, node, isSelected, isSelectable]);

    return (
        <>
            <Wrapper>
                <DepthMargin depth={depth} />
                <ExpandToggler expandable={isExpandable} expanded={isExpanded} onToggle={() => toggleExpanded(node)} />

                {renderNodeLabel ? renderNodeLabel({ node, depth }) : <>{node.value}</>}

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
            </Wrapper>

            {isExpanded &&
                node.children?.map((childNode) => (
                    <TreeNodeRenderer node={childNode} depth={depth + 1} key={childNode.value} />
                ))}

            {isExpanded && isChildrenLoading && (
                <Wrapper>
                    <DepthMargin depth={depth + 1} />
                    <ExpandToggler expandable={false} />
                    <LoaderWrapper>
                        <Loader size="xs" />
                    </LoaderWrapper>
                </Wrapper>
            )}
        </>
    );
}
