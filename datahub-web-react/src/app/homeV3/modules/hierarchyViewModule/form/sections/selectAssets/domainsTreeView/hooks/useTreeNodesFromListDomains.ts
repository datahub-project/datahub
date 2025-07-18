import { useMemo } from 'react';

import { DomainItem } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/types';
import { convertDomainToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/utils';

export default function useTreeNodesFromDomains(domains?: DomainItem[]) {
    return useMemo(() => {
        return (domains ?? []).map(convertDomainToTreeNode);
    }, [domains]);
}
