import { GetSearchResultsParams } from '@app/entityV2/shared/components/styled/search/types';

import { useListDataProductAssetsQuery } from '@graphql/search.generated';
import { FacetFilterInput } from '@types';

export default function generateUseListDataProductAssets({
    urn,
    extraFilters,
}: {
    urn: string;
    extraFilters?: FacetFilterInput[];
}) {
    return (params: GetSearchResultsParams) => {
        const {
            variables: { input },
        } = params;

        const inputWithFilters = {
            ...input,
            filters: [...(input.filters || []), ...(extraFilters || [])],
        };

        const { data, loading, error, refetch } = useListDataProductAssetsQuery({
            variables: { urn, input: inputWithFilters },
        });

        return {
            data: data?.listDataProductAssets,
            loading,
            error,
            refetch: (refetchParams: GetSearchResultsParams['variables']) => {
                return refetch({
                    urn,
                    input: {
                        ...refetchParams.input,
                        filters: [...(refetchParams.input.filters || []), ...(extraFilters || [])],
                    },
                }).then((res) => res.data.listDataProductAssets);
            },
        };
    };
}
