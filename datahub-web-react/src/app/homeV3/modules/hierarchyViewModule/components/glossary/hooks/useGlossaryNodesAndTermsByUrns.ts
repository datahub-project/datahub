import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTerms';

export default function useGlossaryNodesAndTermsByUrns(glossaryNodesAndTermsUrns: string[]) {
    const { isReloading, onReloadingFinished } = useModuleContext();

    return useGlossaryNodesAndTerms({
        glossaryNodesAndTermsUrns,
        count: glossaryNodesAndTermsUrns.length,
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
        onCompleted: () => onReloadingFinished(),
    });
}
