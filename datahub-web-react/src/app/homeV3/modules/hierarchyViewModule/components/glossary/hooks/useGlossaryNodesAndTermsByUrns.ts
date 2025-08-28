import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTerms';

export default function useGlossaryNodesAndTermsByUrns(glossaryNodesAndTermsUrns: string[]) {
    return useGlossaryNodesAndTerms({
        glossaryNodesAndTermsUrns,
        count: glossaryNodesAndTermsUrns.length,
    });
}
