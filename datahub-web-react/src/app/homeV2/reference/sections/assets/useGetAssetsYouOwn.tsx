import { useGetSearchResultsForMultipleQuery } from '../../../../../graphql/search.generated';
import { CorpUser } from '../../../../../types.generated';
import { ASSET_ENTITY_TYPES, OWNERS_FILTER_NAME } from '../../../../searchV2/utils/constants';
import { useEntityRegistry } from '../../../../useEntityRegistry';

const MAX_ASSETS_TO_FETCH = 50;

// TODO: Add Group Ownership here as well.
export const useGetAssetsYouOwn = (user?: CorpUser | null, count = MAX_ASSETS_TO_FETCH) => {
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
                        values: [user?.urn as string],
                    },
                ],
                searchFlags: {
                    skipCache: true,
                },
            },
        },
        skip: !user?.urn,
        fetchPolicy: 'cache-first',
    });

    const entityRegistry = useEntityRegistry();
    const entities =
        data?.searchAcrossEntities?.searchResults?.map((result) =>
            entityRegistry.getGenericEntityProperties(result.entity.type, result.entity),
        ) || [];
    const total = data?.searchAcrossEntities?.total || 0;

    return { entities, loading, error, total };
};
