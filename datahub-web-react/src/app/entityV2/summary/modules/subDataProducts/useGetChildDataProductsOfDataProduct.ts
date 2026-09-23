import { useCallback, useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

const MAX_ASSETS_TO_FETCH = 50;

/** Search field matching DataProductChildrenResolver.PARENT_DATA_PRODUCT_FIELD_NAME */
const PARENT_DATA_PRODUCT_FILTER_NAME = 'parentDataProduct';

export const useGetChildDataProductsOfDataProduct = (initialCount = MAX_ASSETS_TO_FETCH) => {
    const { isReloading, onReloadingFinished } = useModuleContext();
    const { urn } = useEntityData();

    const getInputVariables = useCallback(
        (start: number, count: number) => ({
            input: {
                query: '*',
                start,
                count,
                types: [EntityType.DataProduct],
                filters: [
                    {
                        field: PARENT_DATA_PRODUCT_FILTER_NAME,
                        values: [urn],
                    },
                ],
                searchFlags: { skipCache: true },
            },
        }),
        [urn],
    );

    const {
        loading: searchLoading,
        data,
        error,
        refetch,
    } = useGetSearchResultsForMultipleQuery({
        variables: getInputVariables(0, initialCount),
        skip: !urn,
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
        onCompleted: () => onReloadingFinished?.(),
    });

    const entityRegistry = useEntityRegistryV2();
    const originEntities = useMemo(
        () => data?.searchAcrossEntities?.searchResults?.map((result) => result.entity) || [],
        [data?.searchAcrossEntities?.searchResults],
    );
    const entities =
        originEntities.map((entity) => entityRegistry.getGenericEntityProperties(entity.type, entity)) || [];
    const total = data?.searchAcrossEntities?.total || 0;
    const loading = searchLoading || !data;

    const fetchEntities = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (start === 0) {
                return originEntities;
            }

            const result = await refetch(getInputVariables(start, count));

            return result.data?.searchAcrossEntities?.searchResults?.map((res) => res.entity) || [];
        },
        [refetch, getInputVariables, originEntities],
    );

    return { originEntities, entities, loading, error, total, fetchEntities };
};
