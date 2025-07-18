import { useMemo } from 'react';

import { GlossaryTermType } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/types';
import { convertGlossaryTermToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils';

export default function useTreeNodesFromGlossaryTerms(glossaryTerms?: GlossaryTermType[]) {
    const treeNodes = useMemo(() => {
        return glossaryTerms?.map(convertGlossaryTermToTreeNode) ?? [];
    }, [glossaryTerms]);

    return treeNodes;
}
