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
import useDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomains';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromListDomains';

export default function useChildrenDomainsLoader({
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

    const { domains, loading, total } = useDomains(
        parentValue,
        numberOfAlreadyLoadedChildren,
        numberOfChildrenToLoad,
        numberOfChildrenToLoad === 0,
    );
    const nodes = useTreeNodesFromDomains(domains, forceHasAsyncChildren);

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
