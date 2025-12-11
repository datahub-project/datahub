/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import useGlossaryNodesAndTermsByUrns from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTermsByUrns';
import useGlossaryTreeNodesSorter from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryTreeNodesSorter';
import useTreeNodesFromGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useTreeNodesFromGlossaryNodesAndTerms';
import useTree from '@app/homeV3/modules/hierarchyViewModule/treeView/useTree';

export default function useGlossaryTree(glossaryNodesAndTermsUrns: string[], shouldShowRelatedEntities: boolean) {
    const { glossaryNodes, glossaryTerms, loading } = useGlossaryNodesAndTermsByUrns(glossaryNodesAndTermsUrns);

    const { treeNodes } = useTreeNodesFromGlossaryNodesAndTerms(
        glossaryNodes,
        glossaryTerms,
        shouldShowRelatedEntities,
    );

    const nodesSorter = useGlossaryTreeNodesSorter();
    const tree = useTree(treeNodes, nodesSorter);

    return {
        tree,
        loading,
    };
}
