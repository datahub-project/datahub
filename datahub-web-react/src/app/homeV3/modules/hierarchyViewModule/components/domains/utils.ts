import { DomainItem } from '@app/homeV3/modules/hierarchyViewModule/components/domains/types';
import { unwrapParentEntitiesToTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/selectAssets/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { Domain } from '@types';

export function convertDomainToTreeNode(domain: DomainItem, forceHasAsyncChildren = false): TreeNode {
    return {
        value: domain.urn,
        label: domain.urn,
        hasAsyncChildren: forceHasAsyncChildren || !!domain.children?.total,
        totalChildren: domain.children?.total ?? undefined,
        entity: domain,
    };
}

export function unwrapFlatDomainsToTreeNodes(domains: Domain[] | undefined): TreeNode[] | undefined {
    return unwrapParentEntitiesToTreeNodes(domains, (item) => [...(item.parentDomains?.domains ?? [])].reverse());
}
