import { EntityType, SearchInput } from '../../types.generated';
import { useGetSearchResultsQuery } from '../../graphql/search.generated';

type AllEntityInput<T, K> = Pick<T, Exclude<keyof T, keyof K>> & K;

export function useGetAllEntitySearchResults(input: AllEntityInput<SearchInput, { type?: EntityType }>) {
    const result: any = {};

    result[EntityType.Chart] = useGetSearchResultsQuery({
        variables: {
            input: {
                type: EntityType.Chart,
                ...input,
            },
        },
    });

    result[EntityType.Dashboard] = useGetSearchResultsQuery({
        variables: {
            input: {
                type: EntityType.Dashboard,
                ...input,
            },
        },
    });

    result[EntityType.DataPlatform] = useGetSearchResultsQuery({
        variables: {
            input: {
                type: EntityType.DataPlatform,
                ...input,
            },
        },
    });

    result[EntityType.Dataset] = useGetSearchResultsQuery({
        variables: {
            input: {
                type: EntityType.Dataset,
                ...input,
            },
        },
    });

    return result;
}
