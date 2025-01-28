import { useGetSearchResultsForMultipleQuery } from '../../../../../graphql/search.generated';
import { CorpUser, EntityType } from '../../../../../types.generated';
import { OWNERS_FILTER_NAME } from '../../../../searchV2/utils/constants';
import { useEntityRegistry } from '../../../../useEntityRegistry';

const MAX_ASSETS_TO_FETCH = 50;

// TODO: Add Group Ownership here as well.
export const useGetGlossaryNodesYouOwn = (user?: CorpUser | null, count = MAX_ASSETS_TO_FETCH) => {
    const { loading, data, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                start: 0,
                count,
                types: [EntityType.GlossaryTerm],
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
        skip: !user,
        fetchPolicy: 'cache-first',
    });

    const entityRegistry = useEntityRegistry();
    const entities =
        data?.searchAcrossEntities?.searchResults?.map((result) =>
            entityRegistry.getGenericEntityProperties(result.entity.type, result.entity),
        ) || [];

    return { entities, loading, error };
};
