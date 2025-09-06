import { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { navigateToDomainEntities } from '@app/entityV2/shared/containers/profile/sidebar/Domain/utils';
import { DOMAINS_FILTER_NAME, ENTITY_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

const NUMBER_OF_ASSETS_TO_FETCH = 10;

export const useGetDomainAssets = (initialCount = NUMBER_OF_ASSETS_TO_FETCH) => {
    const { urn, entityType } = useEntityData();
    const history = useHistory();

    const getInputVariables = useCallback(
        (start: number, count: number) => ({
            input: {
                query: '*',
                start,
                count,
                filters: [
                    {
                        field: ENTITY_FILTER_NAME,
                        values: [EntityType.DataProduct],
                        value: EntityType.DataProduct,
                        negated: true,
                    },
                    {
                        field: DOMAINS_FILTER_NAME,
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
        skip: entityType !== EntityType.Domain,
        fetchPolicy: 'cache-first',
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
    const fetchAssets = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (start === 0) {
                return originEntities;
            }

            const result = await refetch(getInputVariables(start, count));

            return result.data?.searchAcrossEntities?.searchResults?.map((res) => res.entity) || [];
        },
        [refetch, getInputVariables, originEntities],
    );

    const navigateToAssetsTab = () => {
        navigateToDomainEntities(urn, entityType, history, entityRegistry);
    };

    return { originEntities, entities, loading, error, total, fetchAssets, navigateToAssetsTab };
};
