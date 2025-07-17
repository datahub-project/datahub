import { useEffect, useState } from 'react';

import useInitialDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useInitialDomains';
import useRootDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useRootDomains';
import useTreeNodesFromFlatDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromFlatDomains';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromListDomains';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { mergeTrees } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

export default function useDomainsTreeViewState(initialSelectedDomainUrns: string[], includeRootNodes: boolean = true, shouldUnwrapParents: boolean = true) {
    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const [nodes, setNodes] = useState<TreeNode[]>([]);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialSelectedDomainUrns ?? []);

    const { domains: initialDomains } = useInitialDomains(initialSelectedDomainUrns);
    const { domains: rootDomains } = useRootDomains();

    const initialSelectedTreeNodes = useTreeNodesFromFlatDomains(initialDomains, shouldUnwrapParents);
    const rootTreeNodes = useTreeNodesFromDomains(rootDomains);

    console.log('>>>DOMAINS initialSelectedTreeNodes', {initialSelectedTreeNodes, initialDomains});

    useEffect(() => {
        if (!isInitialized && initialDomains !== undefined && (!includeRootNodes || rootDomains !== undefined)) {
            if (includeRootNodes) {
                setNodes(mergeTrees(rootTreeNodes, initialSelectedTreeNodes));
            } else {
                setNodes(initialSelectedTreeNodes);
            }
            setIsInitialized(true);
        }
    }, [includeRootNodes, isInitialized, initialDomains, rootDomains, rootTreeNodes, initialSelectedTreeNodes]);

    return {
        nodes,
        setNodes,
        selectedValues,
        setSelectedValues,
        loading: !isInitialized,
    };
}
