import { useMemo } from 'react';

import {
    GlossaryNodeType,
    GlossaryTermType,
} from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/types';
import {
    convertGlossaryNodeToTreeNode,
    convertGlossaryTermToTreeNode,
} from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils';

export default function useTreeNodesFromGlossaryNodesAndTerms(
    glossaryNodes: GlossaryNodeType[] | undefined,
    glossaryTerms: GlossaryTermType[] | undefined,
) {
    const glossaryNodesTreeNodes = useMemo(
        () => glossaryNodes?.map(convertGlossaryNodeToTreeNode) ?? [],
        [glossaryNodes],
    );
    const glossaryTermsTreeNodes = useMemo(
        () => glossaryTerms?.map(convertGlossaryTermToTreeNode) ?? [],
        [glossaryTerms],
    );

    const treeNodes = useMemo(() => {
        return [...glossaryNodesTreeNodes, ...glossaryTermsTreeNodes];
    }, [glossaryNodesTreeNodes, glossaryTermsTreeNodes]);

    return {
        glossaryNodesTreeNodes,
        glossaryTermsTreeNodes,
        treeNodes,
    };
}
