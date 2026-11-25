import { useCallback } from 'react';

import { sortGlossaryTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/components/glossary/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { useEntityRegistry } from '@app/useEntityRegistry';

export default function useGlossaryTreeNodesSorter() {
    const entityRegistry = useEntityRegistry();

    return useCallback((nodes: TreeNode[]) => sortGlossaryTreeNodes(nodes, entityRegistry), [entityRegistry]);
}
