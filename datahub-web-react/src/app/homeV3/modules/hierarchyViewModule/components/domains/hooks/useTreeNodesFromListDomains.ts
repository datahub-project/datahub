import { useMemo } from 'react';

import { DomainItem } from '@app/homeV3/modules/hierarchyViewModule/components/domains/types';
import { convertDomainToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/components/domains/utils';

export default function useTreeNodesFromDomains(domains: DomainItem[] | undefined, forceHasAsyncChildren = true) {
    return useMemo(() => {
        return domains?.map((domain) => convertDomainToTreeNode(domain, forceHasAsyncChildren));
    }, [domains, forceHasAsyncChildren]);
}
