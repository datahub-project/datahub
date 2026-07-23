import { useMemo } from 'react';

import {
    convertDomainToTreeNode,
    unwrapFlatDomainsToTreeNodes,
} from '@app/homeV3/modules/hierarchyViewModule/components/domains/utils';

import { Domain } from '@types';

export default function useTreeNodesFromFlatDomains(domains: Domain[] | undefined, shouldUnwrapParents = true) {
    return useMemo(() => {
        if (shouldUnwrapParents) return unwrapFlatDomainsToTreeNodes(domains ?? []);
        return domains?.map((domain) => convertDomainToTreeNode(domain));
    }, [domains, shouldUnwrapParents]);
}
