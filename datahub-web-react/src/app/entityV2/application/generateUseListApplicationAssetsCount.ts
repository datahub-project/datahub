import { GetSearchResultsParams } from '@src/app/entity/shared/components/styled/search/types';
import { useListApplicationAssetsQuery } from '@src/graphql/search.generated';

export function generateUseListApplicationAssetsCount({ urn }: { urn: string }) {
    return function useListApplicationAssetsCount({ variables: { input } }: GetSearchResultsParams) {
        const { data, loading, error } = useListApplicationAssetsQuery({
            variables: {
                urn,
                start: 0,
                count: 0,
            },
            fetchPolicy: 'cache-first',
        });

        return { total: data?.application?.children?.total, loading, error };
    };
}