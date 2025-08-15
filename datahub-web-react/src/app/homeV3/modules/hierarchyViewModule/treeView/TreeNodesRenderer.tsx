import React, { useCallback } from 'react';
import styled from 'styled-components';

import TreeNodeRenderer from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeNodeRenderer';
import TreeNodesViewLoader from '@app/homeV3/modules/hierarchyViewModule/treeView/components/TreeNodesViewLoader';
import NodesLoaderWrapper from '@app/homeV3/modules/hierarchyViewModule/treeView/components/itemsLoaderWrapper/NodesLoaderWrapper';
import useTreeViewContext from '@app/homeV3/modules/hierarchyViewModule/treeView/context/useTreeViewContext';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

export default function TreeNodesRenderer() {
    const {
        nodes,
        loadBatchSize: numberOfChildrenToLoad,
        loadRootNodes,
        rootNodesLength,
        rootNodesTotal,
        rootNodesLoading,
        loadingTriggerType,
    } = useTreeViewContext();

    const renderNode = useCallback((node: TreeNode) => <TreeNodeRenderer node={node} depth={0} key={node.value} />, []);

    return (
        <Wrapper>
            {loadRootNodes ? (
                <NodesLoaderWrapper
                    trigger={loadingTriggerType}
                    total={rootNodesTotal}
                    current={rootNodesLength}
                    enabled={!rootNodesLoading}
                    depth={0}
                    onLoad={loadRootNodes}
                    pageSize={numberOfChildrenToLoad}
                >
                    {nodes.map(renderNode)}
                </NodesLoaderWrapper>
            ) : (
                nodes.map(renderNode)
            )}
            {rootNodesLoading && <TreeNodesViewLoader depth={0} />}
        </Wrapper>
    );
}
