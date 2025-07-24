import { useMemo } from 'react';

import {
    ChildrenLoaderInputType,
    ChildrenLoaderResultType,
} from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTerms';
import useTreeNodesFromGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useTreeNodesFromGlossaryNodesAndTerms';

export default function useChildrenGlossaryLoader({
    parentValue,
    metadata,
    maxNumberToLoad,
}: ChildrenLoaderInputType): ChildrenLoaderResultType {
    const totalNumberOfChildren = metadata?.totalNumberOfChildren ?? maxNumberToLoad;
    const numberOfAlreadyLoadedChildren = metadata?.numberOfLoadedChildren ?? 0;
    const numberOfChildrenToLoad = Math.min(
        Math.max(totalNumberOfChildren - numberOfAlreadyLoadedChildren, 0),
        maxNumberToLoad,
    );

    const shouldLoad = numberOfChildrenToLoad > 0;

    const { glossaryNodes, glossaryTerms, total, loading } = useGlossaryNodesAndTerms({
        parentGlossaryNodeUrn: parentValue,
        start: numberOfAlreadyLoadedChildren,
        count: numberOfChildrenToLoad,
        skip: numberOfChildrenToLoad === 0,
    });
    const { treeNodes: nodes } = useTreeNodesFromGlossaryNodesAndTerms(glossaryNodes, glossaryTerms, true);

    const isLoading = useMemo(() => {
        if (!shouldLoad) return false;
        return loading || nodes === undefined;
    }, [shouldLoad, loading, nodes]);

    return {
        nodes,
        total,
        loading: isLoading,
    };
}
