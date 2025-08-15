import { useCallback } from 'react';

import { sortGlossaryTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/components/glossary/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

export default function useGlossaryTreeNodesSorter() {
    const entityRegistry = useEntityRegistryV2();

    return useCallback((nodes: TreeNode[]) => sortGlossaryTreeNodes(nodes, entityRegistry), [entityRegistry]);
}
