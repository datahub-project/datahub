/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { GetSearchResultsParams } from '@src/app/entity/shared/components/styled/search/types';
import { useListDataProductAssetsQuery } from '@src/graphql/search.generated';

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
