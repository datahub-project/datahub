import { useListDataProductAssetsQuery } from '@src/graphql/search.generated';
import { GetSearchResultsParams } from '@src/app/entity/shared/components/styled/search/types';

export function generateUseListDataProductAssetsCount({ urn }: { urn: string }) {
    return function useListDataProductAssetsCount({ variables: { input } }: GetSearchResultsParams) {
        const { data, loading, error } = useListDataProductAssetsQuery({
            variables: {
                urn,
                input: { ...input, count: 0 },
            },
            fetchPolicy: 'cache-first',
        });

        return { total: data?.listDataProductAssets?.total, loading, error };
    };
}
