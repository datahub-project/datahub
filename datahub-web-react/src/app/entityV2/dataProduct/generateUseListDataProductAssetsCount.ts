import { GetSearchResultsParams } from '@src/app/entity/shared/components/styled/search/types';
import { useListDataProductAssetsQuery } from '@src/graphql/search.generated';

import { FacetFilterInput } from '@types';

export function generateUseListDataProductAssetsCount({
    urn,
    extraFilters,
}: {
    urn: string;
    extraFilters?: FacetFilterInput[];
}) {
    return function useListDataProductAssetsCount({ variables: { input } }: GetSearchResultsParams) {
        const { data, loading, error } = useListDataProductAssetsQuery({
            variables: {
                urn,
                input: {
                    ...input,
                    count: 0,
                    filters: [...(input.filters || []), ...(extraFilters || [])],
                },
            },
            fetchPolicy: 'cache-first',
        });

        return { total: data?.listDataProductAssets?.total, loading, error };
    };
}
