/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
