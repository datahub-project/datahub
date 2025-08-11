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
    const nodes = useTreeNodesFromDomains(domains);

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
