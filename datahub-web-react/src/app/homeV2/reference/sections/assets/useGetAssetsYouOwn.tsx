import { ASSET_ENTITY_TYPES, OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';
import useGetUserGroupUrns from '@src/app/entityV2/user/useGetUserGroupUrns';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { CorpUser } from '@types';

const MAX_ASSETS_TO_FETCH = 50;

export const useGetAssetsYouOwn = (user?: CorpUser | null, count = MAX_ASSETS_TO_FETCH) => {
    const { groupUrns, loading: groupDataLoading } = useGetUserGroupUrns(user?.urn);

    const { loading, data, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                start: 0,
                count,
                types: ASSET_ENTITY_TYPES,
                filters: [
                    {
                        field: OWNERS_FILTER_NAME,
                        value: user?.urn,
                        values: [user?.urn || '', ...groupUrns],
                    },
                ],
                searchFlags: {
                    skipCache: true,
                },
            },
        },
        skip: !user?.urn || groupDataLoading,
        fetchPolicy: 'cache-first',
    });

    const entityRegistry = useEntityRegistry();
    const originEntities = data?.searchAcrossEntities?.searchResults?.map((result) => result.entity) || [];
    const entities =
        originEntities.map((entity) => entityRegistry.getGenericEntityProperties(entity.type, entity)) || [];
    const total = data?.searchAcrossEntities?.total || 0;

    return { originEntities, entities, loading: loading || groupDataLoading, error, total };
};
