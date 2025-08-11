import { useCallback, useEffect, useMemo, useState } from 'react';

import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import {
    mergeTrees,
    sortTree,
    updateNodeInTree,
    updateTree,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

export default function useTree(tree: TreeNode[] | undefined, nodesSorter?: (nodes: TreeNode[]) => TreeNode[]) {
    const [nodes, setNodes] = useState<TreeNode[]>(tree ?? []);

    useEffect(() => {
        if (tree !== undefined) setNodes(tree);
    }, [tree]);

    const sortedNodes = useMemo(() => {
        if (!nodesSorter) return nodes;
        return sortTree(nodes, nodesSorter);
    }, [nodes, nodesSorter]);

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
