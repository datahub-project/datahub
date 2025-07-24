import { useMemo } from 'react';

import { GlossaryNodeType, GlossaryTermType } from '@app/homeV3/modules/hierarchyViewModule/components/glossary/types';
import {
    convertGlossaryNodeToTreeNode,
    convertGlossaryTermToTreeNode,
} from '@app/homeV3/modules/hierarchyViewModule/components/glossary/utils';

export default function useTreeNodesFromGlossaryNodesAndTerms(
    glossaryNodes: GlossaryNodeType[] | undefined,
    glossaryTerms: GlossaryTermType[] | undefined,
    forceHasAsyncChildren?: boolean,
) {
    const glossaryNodesTreeNodes = useMemo(
        () =>
            glossaryNodes?.map((glossaryNode) =>
                convertGlossaryNodeToTreeNode(glossaryNode, !!forceHasAsyncChildren),
            ) ?? [],
        [glossaryNodes, forceHasAsyncChildren],
    );
    const glossaryTermsTreeNodes = useMemo(
        () =>
            glossaryTerms?.map((glossaryTerm) =>
                convertGlossaryTermToTreeNode(glossaryTerm, !!forceHasAsyncChildren),
            ) ?? [],
        [glossaryTerms, forceHasAsyncChildren],
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
