import { GetSearchResultsParams } from '@app/entityV2/shared/components/styled/search/types';

import { useListDataProductAssetsQuery } from '@graphql/search.generated';

export default function generateUseListDataProductAssets({ urn }: { urn: string }) {
    return (params: GetSearchResultsParams) => {
        const {
            variables: { input },
        } = params;

        const { data, loading, error, refetch } = useListDataProductAssetsQuery({
            variables: { urn, input },
        });

        return {
            data: data?.listDataProductAssets,
            loading,
            error,
            refetch: (refetchParams: GetSearchResultsParams['variables']) => {
                return refetch({ urn, input: refetchParams.input }).then((res) => res.data.listDataProductAssets);
            },
        };
    };
}
