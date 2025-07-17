import { useMemo } from 'react';

import { unwrapFlatDomainsToTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/utils';

import { Domain } from '@types';

export default function useTreeNodesFromFlatDomains(domains: Domain[] | undefined) {
    return useMemo(() => {
        return unwrapFlatDomainsToTreeNodes(domains ?? []);
    }, [domains]);
}
