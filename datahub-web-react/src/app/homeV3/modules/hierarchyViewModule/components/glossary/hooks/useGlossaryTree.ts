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
