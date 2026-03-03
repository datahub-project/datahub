import { useCallback } from 'react';

import { sortDomainTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/components/domains/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

export default function useDomainTreeNodesSorter() {
    const entityRegistry = useEntityRegistryV2();

    return useCallback((nodes: TreeNode[]) => sortDomainTreeNodes(nodes, entityRegistry), [entityRegistry]);
}
