/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { GetSearchResultsParams } from '@app/entity/shared/components/styled/search/types';

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
