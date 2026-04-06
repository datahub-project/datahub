import { QueryHookOptions, QueryResult } from '@apollo/client';
import { useEffect, useRef } from 'react';

import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { useAppConfig } from '@src/app/useAppConfig';

import { GetFormsForEntityQuery } from '@graphql/form.generated';
import { EntityType, Exact } from '@types';

interface Props<T> {
    urn: string;
    entityType: EntityType;
    useEntityQuery: (
        baseOptions: QueryHookOptions<
            T,
            Exact<{
                urn: string;
            }>
        >,
    ) => QueryResult<
        T,
        Exact<{
            urn: string;
        }>
    >;
    getOverrideProperties?: (T) => GenericEntityProperties;
    formsData?: GetFormsForEntityQuery;
}

export default function useGetDataForProfile<T>({
    urn,
    entityType,
    useEntityQuery,
    getOverrideProperties,
    formsData,
}: Props<T>) {
    const flags = useAppConfig().config.featureFlags;
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const { shouldBypassCache, clearCacheBypass } = useReloadableContext();

    // Check bypass cache for the current urn, but use ref to prevent mid-query changes
    // When urn changes, we want to check the new urn's bypass state
    const urnRef = useRef(urn);
    const bypassCacheRef = useRef(shouldBypassCache(urn));

    // Update refs when urn changes (new entity)
    if (urnRef.current !== urn) {
        urnRef.current = urn;
        bypassCacheRef.current = shouldBypassCache(urn);
    }

    const bypassCache = bypassCacheRef.current;

    const {
        loading,
        error,
        data: dataNotCombinedWithSiblings,
        refetch,
    } = useEntityQuery({
        variables: { urn },
        fetchPolicy: bypassCache ? 'network-only' : 'cache-first',
    });

    // Remove from bypass list after successful fetch
    useEffect(() => {
        if (!loading && bypassCache) {
            clearCacheBypass(urn);
        }
    }, [loading, bypassCache, urn, clearCacheBypass]);

    const dataPossiblyCombinedWithSiblings = isHideSiblingMode
        ? dataNotCombinedWithSiblings
        : combineEntityDataWithSiblings(dataNotCombinedWithSiblings);

    const entityData =
        (dataPossiblyCombinedWithSiblings &&
            Object.keys(dataPossiblyCombinedWithSiblings).length > 0 &&
            getDataForEntityType({
                data: {
                    ...formsData?.entity,
                    ...dataPossiblyCombinedWithSiblings[Object.keys(dataPossiblyCombinedWithSiblings)[0]],
                },
                entityType,
                getOverrideProperties,
                isHideSiblingMode,
                flags,
            })) ||
        null;

    return { entityData, dataPossiblyCombinedWithSiblings, dataNotCombinedWithSiblings, loading, error, refetch };
}
