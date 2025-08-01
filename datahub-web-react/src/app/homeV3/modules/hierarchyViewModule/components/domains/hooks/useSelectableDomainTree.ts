import { useEffect, useState } from 'react';

import useInitialDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useInitialDomains';
import useRootDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useRootDomains';
import useTreeNodesFromFlatDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromFlatDomains';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromListDomains';
import useTree from '@app/homeV3/modules/hierarchyViewModule/treeView/useTree';
import { mergeTrees } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

export default function useSelectableDomainTree(initialSelectedDomainUrns: string[] | undefined) {
    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialSelectedDomainUrns ?? []);
    const tree = useTree();

    const { domains: initialDomains } = useInitialDomains(initialSelectedDomainUrns ?? []);
    const initialSelectedTreeNodes = useTreeNodesFromFlatDomains(initialDomains);

    const { domains: rootDomains, loading: rootDomainsLoading } = useRootDomains();
    const rootTreeNodes = useTreeNodesFromDomains(rootDomains);

    useEffect(() => {
        if (
            !rootDomainsLoading &&
            !isInitialized &&
            rootTreeNodes !== undefined &&
            initialSelectedTreeNodes !== undefined
        ) {
            tree.replace(mergeTrees(rootTreeNodes, initialSelectedTreeNodes));
            setIsInitialized(true);
        }
    }, [tree, isInitialized, rootTreeNodes, initialSelectedTreeNodes, rootDomainsLoading]);

    return {
        tree,
        loading: !isInitialized,
        selectedValues,
        setSelectedValues,
    };
}
