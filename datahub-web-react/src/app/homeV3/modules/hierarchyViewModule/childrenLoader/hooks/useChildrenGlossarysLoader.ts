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
    ChildrenLoaderInputType,
    ChildrenLoaderResultType,
} from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTerms';
import useTreeNodesFromGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useTreeNodesFromGlossaryNodesAndTerms';

export default function useChildrenGlossaryLoader({
    parentValue,
    metadata,
    maxNumberToLoad,
    forceHasAsyncChildren,
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
    const { treeNodes: nodes } = useTreeNodesFromGlossaryNodesAndTerms(
        glossaryNodes,
        glossaryTerms,
        forceHasAsyncChildren,
    );

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
