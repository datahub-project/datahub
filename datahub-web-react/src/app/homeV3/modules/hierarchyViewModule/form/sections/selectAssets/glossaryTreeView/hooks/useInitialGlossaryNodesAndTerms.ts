import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useGlossaryNodesAndTerms';

export default function useInitialGlossaryNodesAndTerms(glossaryNodesAndTermsUrns: string[]) {
    const { glossaryNodes, glossaryTerms } = useGlossaryNodesAndTerms({ glossaryNodesAndTermsUrns });

    return { glossaryNodes, glossaryTerms };
}
