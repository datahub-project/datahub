import useDomainTreeNodesSorter from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomainTreeNodesSorter';
import useDomainsByUrns from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomainsByUrns';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromListDomains';
import useTree from '@app/homeV3/modules/hierarchyViewModule/treeView/useTree';

export default function useDomainsTree(domainsUrns: string[], shouldShowRelatedEntities: boolean) {
    const { domains, loading } = useDomainsByUrns(domainsUrns);
    const treeNodes = useTreeNodesFromDomains(domains, shouldShowRelatedEntities);

    const nodesSorter = useDomainTreeNodesSorter();
    const tree = useTree(treeNodes, nodesSorter);

    return {
        tree,
        loading,
    };
}
