/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';

import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import {
    mergeTrees,
    sortTree,
    updateNodeInTree,
    updateTree,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';
import { useStableValue } from '@app/sharedV2/hooks/useStableValue';

export default function useTree(tree: TreeNode[] | undefined, nodesSorter?: (nodes: TreeNode[]) => TreeNode[]) {
    const stableTree = useStableValue(tree ?? []);
    const [nodes, setNodes] = useState<TreeNode[]>(stableTree);
    const isTreeUndefined = useMemo(() => tree === undefined, [tree]);

    useEffect(() => {
        if (!isTreeUndefined) setNodes(stableTree);
    }, [stableTree, isTreeUndefined]);

    const unstableSortedNodes = useMemo(() => {
        if (!nodesSorter) {
            return nodes;
        }
        return sortTree(nodes, nodesSorter);
    }, [nodes, nodesSorter]);

    const sortedNodes = useStableValue(unstableSortedNodes);

    const replace = useCallback((newNodes: TreeNode[]) => setNodes(newNodes), []);

    const merge = useCallback((treeToMerge: TreeNode[]) => {
        setNodes((prevNodes) => mergeTrees(prevNodes, treeToMerge));
    }, []);

    const update = useCallback((newNodes: TreeNode[], parentValue?: string) => {
        setNodes((prevNodes) => updateTree(prevNodes, newNodes, parentValue));
    }, []);

    const updateNode = useCallback((value: string, changes: Partial<TreeNode>) => {
        setNodes((prevNodes) => updateNodeInTree(prevNodes, value, changes));
    }, []);

    return {
        nodes: sortedNodes,
        replace,
        merge,
        update,
        updateNode,
    };
}
