import { useCallback, useEffect, useState } from 'react';

import useDomainTreeNodesSorter from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomainTreeNodesSorter';
import useInitialDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useInitialDomains';
import useLoadMoreRootDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useLoadMoreRootDomains';
import useRootDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useRootDomains';
import useTreeNodesFromFlatDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromFlatDomains';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromListDomains';
import { convertDomainToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/components/domains/utils';
import { DEFAULT_LOAD_BATCH_SIZE } from '@app/homeV3/modules/hierarchyViewModule/treeView/constants';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import useTree from '@app/homeV3/modules/hierarchyViewModule/treeView/useTree';
import { mergeTrees } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

export default function useSelectableDomainTree(
    initialSelectedDomainUrns: string[] | undefined,
    loadBatchSize = DEFAULT_LOAD_BATCH_SIZE,
) {
    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialSelectedDomainUrns ?? []);
    const [finalRootDomainsTotal, setFinalRootDomainsTotal] = useState<number>(0);
    const nodesSorter = useDomainTreeNodesSorter();
    const tree = useTree(undefined, nodesSorter);

    // FYI: We get and extract parents from initially selected domains
    // to have possibility to detect if some node has any selected nested nodes
    const { domains: initialDomains } = useInitialDomains(initialSelectedDomainUrns ?? []);
    const initialSelectedTreeNodes = useTreeNodesFromFlatDomains(initialDomains);

    const {
        domains: rootDomains,
        loading: rootDomainsLoading,
        total: rootDomainsTotal,
    } = useRootDomains(loadBatchSize);
    const rootTreeNodes = useTreeNodesFromDomains(rootDomains, false);

    useEffect(() => setFinalRootDomainsTotal(rootDomainsTotal ?? 0), [rootDomainsTotal]);

    const { loadMoreRootDomains, loading: rootDomainsMoreLoading } = useLoadMoreRootDomains();

    const preprocessRootNodes = useCallback(
        (rootNodes: TreeNode[]) => {
            if (!initialSelectedTreeNodes) return rootNodes;

            // Merge initial selected nodes with the same root nodes
            const initialSelectedTreeNodesToMerge = initialSelectedTreeNodes.filter((initialSelectedTreeNode) =>
                rootNodes.some((rootNode) => rootNode.value === initialSelectedTreeNode.value),
            );
            return mergeTrees(rootNodes, initialSelectedTreeNodesToMerge);
        },
        [initialSelectedTreeNodes],
    );

    const loadMoreRootNodes = useCallback(async () => {
        const hasMoreRootNodes = (finalRootDomainsTotal ?? 0) > tree.nodes.length;
        if (!rootDomainsMoreLoading && hasMoreRootNodes) {
            const domains = await loadMoreRootDomains(tree.nodes.length, loadBatchSize);
            if (domains.length) {
                const treeNodes = domains.map((domain) => convertDomainToTreeNode(domain));
                tree.merge(preprocessRootNodes(treeNodes));
            } else {
                // If there are no more domains to fetch or some error happened during loading
                // set the root domains total to the current length of nodes to prevent 
                // infinite calls of loading more root domains in case of using infinite scroll
                setFinalRootDomainsTotal(tree.nodes.length);
            }
        }
    }, [tree, loadMoreRootDomains, finalRootDomainsTotal, rootDomainsMoreLoading, preprocessRootNodes, loadBatchSize]);

    useEffect(() => {
        if (
            !rootDomainsLoading &&
            !isInitialized &&
            rootTreeNodes !== undefined &&
            initialSelectedTreeNodes !== undefined
        ) {
            tree.replace(preprocessRootNodes(rootTreeNodes));
            setIsInitialized(true);
        }
    }, [tree, isInitialized, rootTreeNodes, initialSelectedTreeNodes, rootDomainsLoading, preprocessRootNodes]);

    return {
        tree,
        loading: !isInitialized,
        selectedValues,
        setSelectedValues,
        loadMoreRootNodes,
        rootDomainsMoreLoading,
        rootNodesTotal: finalRootDomainsTotal,
    };
}
