import { useCallback, useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { DOMAINS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

const MAX_ASSETS_TO_FETCH = 50;

export const useGetChildDataProducts = (initialCount = MAX_ASSETS_TO_FETCH) => {
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
                        field: DOMAINS_FILTER_NAME,
                        value: urn,
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

    // For fetching paginated entities based on start and count
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
