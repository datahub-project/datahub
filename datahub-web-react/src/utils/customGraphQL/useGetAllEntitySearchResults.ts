import { EntityType, SearchInput } from '../../types.generated';
import { useGetSearchResultsQuery } from '../../graphql/search.generated';
import { useEntityRegistry } from '../../app/useEntityRegistry';

type AllEntityInput<T, K> = Pick<T, Exclude<keyof T, keyof K>> & K;

export function useGetAllEntitySearchResults(input: AllEntityInput<SearchInput, { type?: EntityType }>) {
    const result: any = {};

    const entityRegistry = useEntityRegistry();

    const searchTypes = entityRegistry.getSearchEntityTypes();

    for (let i = 0; i < searchTypes.length; i++) {
        const type = searchTypes[i];
        // eslint-disable-next-line react-hooks/rules-of-hooks
        result[type] = useGetSearchResultsQuery({
            variables: {
                input: {
                    type,
                    ...input,
                },
            },
        });
    }

    return result;
}
