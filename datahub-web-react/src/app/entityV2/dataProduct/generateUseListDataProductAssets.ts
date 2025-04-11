import { useListDataProductAssetsQuery } from '../../../graphql/search.generated';
import { GetSearchResultsParams } from '../shared/components/styled/search/types';

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
