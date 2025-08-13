import { useCallback, useMemo } from 'react';

import { ASSET_ENTITY_TYPES, OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import useGetUserGroupUrns from '@src/app/entityV2/user/useGetUserGroupUrns';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { CorpUser, Entity } from '@types';

const MAX_ASSETS_TO_FETCH = 50;

export const useGetAssetsYouOwn = (user?: CorpUser | null, initialCount = MAX_ASSETS_TO_FETCH) => {
    const { groupUrns, loading: groupDataLoading } = useGetUserGroupUrns(user?.urn);

    const getInputVariables = useCallback(
        (start: number, count: number) => ({
            input: {
                query: '*',
                start,
                count,
                types: ASSET_ENTITY_TYPES,
                filters: [
                    {
                        field: OWNERS_FILTER_NAME,
                        value: user?.urn,
                        values: [user?.urn || '', ...groupUrns],
                    },
                ],
                searchFlags: { skipCache: true },
            },
        }),
        [user?.urn, groupUrns],
    );

    const {
        loading: searchLoading,
        data,
        error,
        refetch,
    } = useGetSearchResultsForMultipleQuery({
        variables: getInputVariables(0, initialCount),
        skip: !user?.urn || groupDataLoading,
    });

    const entityRegistry = useEntityRegistryV2();
    const originEntities = useMemo(
        () => data?.searchAcrossEntities?.searchResults?.map((result) => result.entity) || [],
        [data?.searchAcrossEntities?.searchResults],
    );
    const entities =
        originEntities.map((entity) => entityRegistry.getGenericEntityProperties(entity.type, entity)) || [];
    const total = data?.searchAcrossEntities?.total || 0;
    const loading = searchLoading || groupDataLoading || !data;

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
