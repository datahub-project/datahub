import { useMemo } from 'react';

import useRootGlossaryNodes from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useRootGlossaryNodes';
import useRootGlossaryTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useRootGlossaryTerms';

export default function useRootGlossaryNodesAndTerms() {
    const { glossaryNodes, loading: glossaryNodesLoading } = useRootGlossaryNodes();
    const { glossaryTerms, loading: glossaryTermsLoading } = useRootGlossaryTerms();

    const glossaryItems = useMemo(
        () => [...(glossaryNodes ?? []), ...(glossaryTerms ?? [])],
        [glossaryNodes, glossaryTerms],
    );

    return { glossaryItems, glossaryNodes, glossaryTerms, loading: glossaryNodesLoading || glossaryTermsLoading };
}
