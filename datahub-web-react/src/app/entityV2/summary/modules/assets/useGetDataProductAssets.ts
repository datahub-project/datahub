import { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useListDataProductAssetsQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

const NUMBER_OF_ASSETS_TO_FETCH = 10;

export const useGetDataProductAssets = (initialCount = NUMBER_OF_ASSETS_TO_FETCH) => {
    const { urn, entityType } = useEntityData();
    const history = useHistory();
    const { isReloading, onReloadingFinished } = useModuleContext();

    const getInputVariables = useCallback(
        (start: number, count: number) => ({
            urn,
            input: {
                query: '*',
                start,
                count,
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
    } = useListDataProductAssetsQuery({
        variables: getInputVariables(0, initialCount),
        skip: entityType !== EntityType.DataProduct,
        fetchPolicy: isReloading ? 'cache-and-network' : 'cache-first',
        onCompleted: () => onReloadingFinished?.(),
    });

    const entityRegistry = useEntityRegistryV2();
    const originEntities = useMemo(
        () => data?.listDataProductAssets?.searchResults?.map((result) => result.entity) || [],
        [data?.listDataProductAssets?.searchResults],
    );
    const entities =
        originEntities.map((entity) => entityRegistry.getGenericEntityProperties(entity.type, entity)) || [];
    const total = data?.listDataProductAssets?.total || 0;
    const loading = searchLoading || !data;

    // For fetching paginated entities based on start and count
    const fetchAssets = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (start === 0) {
                return originEntities;
            }

            const result = await refetch(getInputVariables(start, count));

            return result.data?.listDataProductAssets?.searchResults?.map((res) => res.entity) || [];
        },
        [refetch, getInputVariables, originEntities],
    );

    const navigateToAssetsTab = () => {
        history.push(`${entityRegistry.getEntityUrl(entityType, urn)}/Assets`);
    };

    return { originEntities, entities, loading, error, total, fetchAssets, navigateToAssetsTab };
};
