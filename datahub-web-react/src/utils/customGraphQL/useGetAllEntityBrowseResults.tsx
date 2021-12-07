import { useMemo } from 'react';
import { QueryResult } from '@apollo/client';
import { BrowseInput, EntityType, Exact } from '../../types.generated';
import { GetSearchResultsQuery } from '../../graphql/search.generated';
import { useEntityRegistry } from '../../app/useEntityRegistry';
import { GetBrowseResultsQuery, useGetBrowseResultsQuery } from '../../graphql/browse.generated';

type GetAllBrowseResults<K> = Record<EntityType, K>;
type AllEntityInput<T, K> = Pick<T, Exclude<keyof T, keyof K>> & K;

export type GetAllEntityBrowseResultsType = GetAllBrowseResults<
    QueryResult<GetBrowseResultsQuery, Exact<{ input: BrowseInput }>>
>;

export function useGetAllEntityBrowseResults(
    input: AllEntityInput<BrowseInput, { type?: EntityType }>,
): GetAllEntityBrowseResultsType {
    const result: {
        [key in EntityType]: QueryResult<
            GetBrowseResultsQuery,
            Exact<{
                input: BrowseInput;
            }>
        >;
    } = {} as {
        [key in EntityType]: QueryResult<
            GetSearchResultsQuery,
            Exact<{
                input: BrowseInput;
            }>
        >;
    };
    const entityRegistry = useEntityRegistry();

    const browseTypes = entityRegistry.getBrowseEntityTypes();

    for (let i = 0; i < browseTypes.length; i++) {
        const type = browseTypes[i];
        // eslint-disable-next-line react-hooks/rules-of-hooks
        result[type] = useGetBrowseResultsQuery({
            variables: {
                input: {
                    type,
                    ...input,
                },
            },
        });
    }

    return useMemo(
        () => result,
        // eslint-disable-next-line react-hooks/exhaustive-deps
        Object.keys(result).map((key) => result[key]),
    );
}
