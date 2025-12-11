/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
