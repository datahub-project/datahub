import { useEffect, useState } from 'react';

import useInitialDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useInitialDomains';
import useRootDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useRootDomains';
import useTreeNodesFromFlatDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromFlatDomains';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromListDomains';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { mergeTrees } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

export default function useDomainsTreeViewState(initialSelectedDomainUrns: string[]) {
    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const [nodes, setNodes] = useState<TreeNode[]>([]);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialSelectedDomainUrns ?? []);

    const { domains: initialDomains } = useInitialDomains(initialSelectedDomainUrns);
    const { domains: rootDomains } = useRootDomains();

    const initialSelectedTreeNodes = useTreeNodesFromFlatDomains(initialDomains);
    const rootTreeNodes = useTreeNodesFromDomains(rootDomains);

    useEffect(() => {
        if (!isInitialized && initialDomains !== undefined && rootDomains !== undefined) {
            setNodes(mergeTrees(rootTreeNodes, initialSelectedTreeNodes));
            setIsInitialized(true);
        }
    }, [isInitialized, initialDomains, rootDomains, rootTreeNodes, initialSelectedTreeNodes]);

    return {
        nodes,
        setNodes,
        selectedValues,
        setSelectedValues,
        loading: !isInitialized,
    };
}
