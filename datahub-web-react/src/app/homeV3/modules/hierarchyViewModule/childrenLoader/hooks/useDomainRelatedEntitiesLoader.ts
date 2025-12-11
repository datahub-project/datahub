/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import useRelatedEntitiesLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useRelatedEntitiesLoader';
import {
    ChildrenLoaderInputType,
    ChildrenLoaderResultType,
} from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import { combineOrFilters } from '@app/searchV2/utils/filterUtils';

export default function useDomainRelatedEntitiesLoader({
    parentValue,
    metadata,
    maxNumberToLoad,
    orFilters,
}: ChildrenLoaderInputType): ChildrenLoaderResultType {
    const finalOrFilters = useMemo(
        () =>
            combineOrFilters(orFilters ?? [], [
                {
                    and: [{ field: 'domains', values: [parentValue] }],
                },
            ]),
        [orFilters, parentValue],
    );

    return useRelatedEntitiesLoader({
        parentValue,
        metadata,
        maxNumberToLoad,
        orFilters: finalOrFilters,
    });
}
