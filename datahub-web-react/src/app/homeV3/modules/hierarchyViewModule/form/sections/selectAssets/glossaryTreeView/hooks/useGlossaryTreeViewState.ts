import { useEffect, useState } from 'react';

import useInitialGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useInitialGlossaryNodesAndTerms';
import useRootGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryNodesAndTerms';
import useTreeNodesFromFlatGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromFlatGlossaryNodesAndTerms';
import useTreeNodesFromGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromGlossaryNodesAndTerms';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { mergeTrees } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

export default function useGlossaryTreeViewState(initialSelectedGlossaryNodesAndTermsUrns: string[]) {
    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const [nodes, setNodes] = useState<TreeNode[]>([]);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialSelectedGlossaryNodesAndTermsUrns ?? []);

    // Get initial nodes
    const { glossaryNodes: initialGlossaryNodes, glossaryTerms: initialGlossaryTerms } =
        useInitialGlossaryNodesAndTerms(initialSelectedGlossaryNodesAndTermsUrns);
    const { treeNodes: initialSelectedTreeNodes } = useTreeNodesFromFlatGlossaryNodesAndTerms(
        initialGlossaryNodes,
        initialGlossaryTerms,
    );

    // Get root nodes
    const { glossaryNodes: rootGlossaryNodes, glossaryTerms: rootGlossaryTerms } = useRootGlossaryNodesAndTerms();
    const { treeNodes: rootTreeNodes } = useTreeNodesFromGlossaryNodesAndTerms(rootGlossaryNodes, rootGlossaryTerms);

    // Initialize nodes
    useEffect(() => {
        if (
            !isInitialized &&
            initialGlossaryNodes !== undefined &&
            initialGlossaryTerms !== undefined &&
            rootGlossaryNodes !== undefined &&
            rootGlossaryTerms !== undefined
        ) {
            setNodes(mergeTrees(rootTreeNodes, initialSelectedTreeNodes));
            setIsInitialized(true);
        }
    }, [
        isInitialized,
        initialGlossaryNodes,
        initialGlossaryTerms,
        rootGlossaryNodes,
        rootGlossaryTerms,
        rootTreeNodes,
        initialSelectedTreeNodes,
    ]);

    return {
        nodes,
        setNodes,
        selectedValues,
        setSelectedValues,
        loading: !isInitialized,
    };
}
