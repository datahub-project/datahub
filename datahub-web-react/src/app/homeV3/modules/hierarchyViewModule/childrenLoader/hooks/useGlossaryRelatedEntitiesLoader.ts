import { useMemo } from 'react';

import useRelatedEntitiesLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useRelatedEntitiesLoader';
import {
    ChildrenLoaderInputType,
    ChildrenLoaderResultType,
} from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import { combineOrFilters } from '@app/searchV2/utils/filterUtils';

export default function useGlossaryRelatedEntitiesLoader({
    parentValue,
    metadata,
    maxNumberToLoad,
    dependenciesIsLoading,
    orFilters,
}: ChildrenLoaderInputType): ChildrenLoaderResultType {
    const finalOrFilters = useMemo(
        () =>
            combineOrFilters(orFilters ?? [], [
                {
                    and: [{ field: 'glossaryTerms', values: [parentValue] }],
                },
                {
                    and: [{ field: 'fieldGlossaryTerms', values: [parentValue] }],
                },
            ]),
        [orFilters, parentValue],
    );

    return useRelatedEntitiesLoader({
        parentValue,
        metadata,
        maxNumberToLoad,
        dependenciesIsLoading,
        orFilters: finalOrFilters,
    });
}
