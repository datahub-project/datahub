/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
        () => glossaryNodes?.map((glossaryNode) => convertGlossaryNodeToTreeNode(glossaryNode)) ?? [],
        [glossaryNodes],
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
