/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useMemo } from 'react';

import { useChildrenLoaderContext } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/useChildrenLoaderContext';
import {
    ChildrenLoaderMetadata,
    ChildrenLoaderType,
} from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';

import { AndFilterInput } from '@types';

export default function useLoader(
    parentValue: string,
    loadChildren: ChildrenLoaderType,
    loadRelatedEntities: ChildrenLoaderType | undefined,
    relatedEntitiesOrFilters: AndFilterInput[] | undefined,
) {
    const { get, onLoad, maxNumberOfChildrenToLoad: maxNumberToLoad } = useChildrenLoaderContext();

    const metadata = useMemo(() => get(parentValue), [get, parentValue]);

    const {
        nodes: childrenNodes,
        loading: childrenLoading,
        total: childrenTotal,
    } = loadChildren({
        parentValue,
        metadata,
        maxNumberToLoad,
        forceHasAsyncChildren: relatedEntitiesOrFilters !== undefined,
    });

    const relatedEntitiesResponse = loadRelatedEntities?.({
        parentValue,
        metadata,
        dependenciesIsLoading: childrenLoading,
        maxNumberToLoad: maxNumberToLoad - (childrenNodes?.length ? childrenNodes.length : 0),
        orFilters: relatedEntitiesOrFilters,
    });

    const {
        nodes: relatedEntitiesNodes,
        total: totalRelatedEntities,
        loading: relatedEntitiesLoading,
    } = relatedEntitiesResponse || {
        nodes: [],
        total: 0,
        loading: false,
    };

    useEffect(() => {
        if (!childrenLoading && !relatedEntitiesLoading) {
            const newMetadata: ChildrenLoaderMetadata = {
                numberOfLoadedChildren: (metadata?.numberOfLoadedChildren ?? 0) + (childrenNodes?.length ?? 0),
                numberOfLoadedRelatedEntities:
                    (metadata?.numberOfLoadedRelatedEntities ?? 0) + (relatedEntitiesNodes?.length ?? 0),
            };

            if (childrenTotal !== undefined) {
                newMetadata.totalNumberOfChildren = childrenTotal;
            }
            if (totalRelatedEntities !== undefined) {
                newMetadata.totalNumberOfRelatedEntities = totalRelatedEntities;
            }

            onLoad([...(childrenNodes ?? []), ...(relatedEntitiesNodes ?? [])], newMetadata, parentValue);
        }
    }, [
        childrenNodes,
        childrenLoading,
        childrenTotal,

        metadata,
        parentValue,

        relatedEntitiesNodes,
        totalRelatedEntities,
        relatedEntitiesLoading,

        onLoad,
    ]);
}
