import { GetSearchResultsParams } from '@app/entityV2/shared/components/styled/search/types';

import { useListApplicationAssetsQuery } from '@graphql/search.generated';

export default function generateUseListDataProductAssets({ urn }: { urn: string }) {
    return (params: GetSearchResultsParams) => {
        const {
            variables: { input },
        } = params;

        const { data, loading, error, refetch } = useListApplicationAssetsQuery({
            variables: { urn, input },
        });

        return {
            data: data?.listApplicationAssets,
            loading,
            error,
            refetch: (refetchParams: GetSearchResultsParams['variables']) => {
                return refetch({ urn, input: refetchParams.input }).then((res) => res.data.listApplicationAssets);
            },
        };
    };
}
