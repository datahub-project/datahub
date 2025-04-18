<<<<<<< HEAD
import { ASSET_ENTITY_TYPES, OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';
import useGetUserGroupUrns from '@src/app/entityV2/user/useGetUserGroupUrns';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { CorpUser } from '@types';
=======
import useGetUserGroupUrns from '@src/app/entityV2/user/useGetUserGroupUrns';
import { useGetSearchResultsForMultipleQuery } from '../../../../../graphql/search.generated';
import { CorpUser } from '../../../../../types.generated';
import { ASSET_ENTITY_TYPES, OWNERS_FILTER_NAME } from '../../../../searchV2/utils/constants';
import { useEntityRegistry } from '../../../../useEntityRegistry';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

const MAX_ASSETS_TO_FETCH = 50;

export const useGetAssetsYouOwn = (user?: CorpUser | null, count = MAX_ASSETS_TO_FETCH) => {
    const userUrn = user?.urn || '';
    const { groupUrns, loading: groupDataLoading } = useGetUserGroupUrns(userUrn);

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
                        value: userUrn,
                        values: [userUrn, ...groupUrns],
                    },
                ],
                searchFlags: {
                    skipCache: true,
                },
            },
        },
        skip: !userUrn || groupDataLoading,
        fetchPolicy: 'cache-first',
    });

    const entityRegistry = useEntityRegistry();
    const entities =
        data?.searchAcrossEntities?.searchResults?.map((result) =>
            entityRegistry.getGenericEntityProperties(result.entity.type, result.entity),
        ) || [];
    const total = data?.searchAcrossEntities?.total || 0;

    return { entities, loading: loading || groupDataLoading, error, total };
};
