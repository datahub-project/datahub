import { useMemo } from 'react';

import {
    convertGlossaryNodeToTreeNode,
    convertGlossaryTermToTreeNode,
    unwrapFlatGlossaryNodesToTreeNodes,
    unwrapFlatGlossaryTermsToTreeNodes,
} from '@app/homeV3/modules/hierarchyViewModule/components/glossary/utils';

import { GlossaryNode, GlossaryTerm } from '@types';

export default function useTreeNodesFromFlatGlossaryNodesAndTerms(
    glossaryNodes: GlossaryNode[] | undefined,
    glossaryTerms: GlossaryTerm[] | undefined,
    shouldUnwrapParents = true,
) {
    const glossaryNodesTreeNodes = useMemo(() => {
        if (shouldUnwrapParents) return unwrapFlatGlossaryNodesToTreeNodes(glossaryNodes);
        return glossaryNodes?.map((glossaryNode) => convertGlossaryNodeToTreeNode(glossaryNode));
    }, [glossaryNodes, shouldUnwrapParents]);

    const glossaryTermsTreeNodes = useMemo(() => {
        if (shouldUnwrapParents) return unwrapFlatGlossaryTermsToTreeNodes(glossaryTerms);
        return glossaryTerms?.map((glossaryTerm) => convertGlossaryTermToTreeNode(glossaryTerm));
    }, [glossaryTerms, shouldUnwrapParents]);

    const treeNodes = useMemo(() => {
        if (glossaryNodesTreeNodes === undefined || glossaryTermsTreeNodes === undefined) return undefined;
        return [...glossaryNodesTreeNodes, ...glossaryTermsTreeNodes];
    }, [glossaryNodesTreeNodes, glossaryTermsTreeNodes]);

    return {
        glossaryNodesTreeNodes,
        glossaryTermsTreeNodes,
        treeNodes,
    };
}
