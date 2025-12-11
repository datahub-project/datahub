/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';

export default function useGlossaryRelatedAssetsTabCount() {
    const { entityData } = useEntityData();

    // To get the number of related assets
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [],
                query: '*',
                count: 0,
                orFilters: [
                    {
                        and: [
                            {
                                field: 'glossaryTerms',
                                values: [entityData?.urn || ''],
                            },
                        ],
                    },
                ],
                searchFlags: {
                    skipCache: true,
                },
            },
        },
        skip: !entityData?.urn,
        fetchPolicy: 'cache-and-network',
    });

    return data?.searchAcrossEntities?.total || 0;
}
