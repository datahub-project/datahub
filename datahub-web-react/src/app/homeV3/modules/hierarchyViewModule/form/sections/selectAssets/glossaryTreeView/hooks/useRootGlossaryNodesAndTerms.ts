import { useMemo } from 'react';

import useRootGlossaryNodes from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryNodes';
import useRootGlossaryTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryTerms';

export default function useRootGlossaryNodesAndTerms() {
    const { glossaryNodes } = useRootGlossaryNodes();
    const { glossaryTerms } = useRootGlossaryTerms();

    const glossaryItems = useMemo(
        () => [...(glossaryNodes ?? []), ...(glossaryTerms ?? [])],
        [glossaryNodes, glossaryTerms],
    );

    return { glossaryItems, glossaryNodes, glossaryTerms };
}
