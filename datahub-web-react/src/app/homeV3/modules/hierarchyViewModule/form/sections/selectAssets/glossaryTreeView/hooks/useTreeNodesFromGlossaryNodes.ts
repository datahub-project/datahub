import { useMemo } from 'react';

import { GlossaryNodeType } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/types';
import { convertGlossaryNodeToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils';

export default function useTreeNodesFromGlossaryNodes(glossaryNodes?: GlossaryNodeType[]) {
    const treeNodes = useMemo(() => {
        return glossaryNodes?.map(convertGlossaryNodeToTreeNode) ?? [];
    }, [glossaryNodes]);

    return treeNodes;
}
