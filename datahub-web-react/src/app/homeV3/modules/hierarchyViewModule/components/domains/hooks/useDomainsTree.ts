import useDomainsByUrns from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomainsByUrns';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromListDomains';
import useTree from '@app/homeV3/modules/hierarchyViewModule/treeView/useTree';

export default function useDomainsTree(domainsUrns: string[], shouldShowRelatedEntities: boolean) {
    const { domains, loading } = useDomainsByUrns(domainsUrns);
    const treeNodes = useTreeNodesFromDomains(domains, shouldShowRelatedEntities);

    const tree = useTree(treeNodes);

    return {
        tree,
        loading,
    };
}
