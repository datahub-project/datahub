import { DomainItem } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/types';
import { unwrapParentEntitiesToTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { Domain } from '@types';

export function convertDomainToTreeNode(domain: DomainItem): TreeNode {
    return {
        value: domain.urn,
        label: domain.urn,
        hasAsyncChildren: !!domain.children?.total,
        totalChildren: domain.children?.total ?? undefined,
        entity: domain,
    };
}

export function unwrapFlatDomainsToTreeNodes(domains: Domain[]): TreeNode[] {
    return unwrapParentEntitiesToTreeNodes(domains, (item) => [...(item.parentDomains?.domains ?? [])].reverse());
}
