import { useCallback, useEffect, useState } from 'react';

import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { mergeTrees, updateNodeInTree, updateTree } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

export default function useTree(tree?: TreeNode[]) {
    const [nodes, setNodes] = useState<TreeNode[]>(tree ?? []);

    useEffect(() => {
        if (tree !== undefined) setNodes(tree);
    }, [tree]);

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
        nodes,
        replace,
        merge,
        update,
        updateNode,
    };
}
