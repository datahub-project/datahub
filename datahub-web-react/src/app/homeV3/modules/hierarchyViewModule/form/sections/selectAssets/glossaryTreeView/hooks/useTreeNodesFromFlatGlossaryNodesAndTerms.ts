import { useMemo } from 'react';

import {
    unwrapFlatGlossaryNodesToTreeNodes,
    unwrapFlatGlossaryTermsToTreeNodes,
} from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils';

import { GlossaryNode, GlossaryTerm } from '@types';

export default function useTreeNodesFromFlatGlossaryNodesAndTerms(
    glossaryNodes: GlossaryNode[] | undefined,
    glossaryTerms: GlossaryTerm[] | undefined,
) {
    const glossaryNodesTreeNodes = useMemo(
        () => unwrapFlatGlossaryNodesToTreeNodes(glossaryNodes ?? []),
        [glossaryNodes],
    );
    const glossaryTermsTreeNodes = useMemo(
        () => unwrapFlatGlossaryTermsToTreeNodes(glossaryTerms ?? []),
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
