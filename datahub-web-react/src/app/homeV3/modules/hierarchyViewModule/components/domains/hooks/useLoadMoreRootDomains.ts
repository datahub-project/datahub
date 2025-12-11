/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback, useState } from 'react';

import { isDomain } from '@app/entityV2/domain/utils';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator, SortOrder } from '@types';

export default function useLoadMoreRootDomains() {
    const [loading, setLoading] = useState<boolean>(false);

    const { refetch } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.Domain],
                start: 0,
                count: 0,
                sortInput: {
                    sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
                },
            },
        },
        skip: true,
    });

    const loadMoreRootDomains = useCallback(
        async (start: number, pageSize: number) => {
            setLoading(true);

            try {
                const response = await refetch({
                    input: {
                        start,
                        types: [EntityType.Domain],
                        query: '*',
                        count: pageSize,
                        orFilters: [
                            { and: [{ field: 'parentDomain', condition: FilterOperator.Exists, negated: true }] },
                        ],
                        sortInput: {
                            sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
                        },
                    },
                });

                return (
                    response.data.searchAcrossEntities?.searchResults
                        ?.map((searchResult) => searchResult.entity)
                        .filter(isDomain) ?? []
                );
            } catch (e) {
                console.error('Something went wrong during fetching root domains', e);
                return [];
            } finally {
                setLoading(false);
            }
        },
        [refetch],
    );

    return {
        loadMoreRootDomains,
        loading,
    };
}
