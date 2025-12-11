/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DEFAULT_PAGE_SIZE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';
import { filterQueries, getQueryEntitiesFilter } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/filterQueries';
import { mapQuery } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/mapQuery';
import usePagination from '@app/sharedV2/pagination/usePagination';
import useSorting from '@app/sharedV2/sorting/useSorting';

import { useListQueriesQuery } from '@graphql/query.generated';
import { QueryEntity, QuerySource } from '@types';

interface Props {
    entityUrn?: string;
    siblingUrn?: string;
    filterText: string;
}

export const useHighlightedQueries = ({ entityUrn, siblingUrn, filterText }: Props) => {
    const pagination = usePagination(DEFAULT_PAGE_SIZE);
    const { start, count } = pagination;
    const sorting = useSorting();
    const { sortField, sortOrder } = sorting;

    const entityFilter = getQueryEntitiesFilter(entityUrn, siblingUrn);
    const {
        data: highlightedQueriesData,
        client,
        loading,
    } = useListQueriesQuery({
        variables: {
            input: {
                start,
                count,
                source: QuerySource.Manual,
                orFilters: [{ and: [entityFilter] }],
                sortInput: sortField && sortOrder ? { sortCriterion: { field: sortField, sortOrder } } : undefined,
            },
        },
        skip: !entityUrn,
        fetchPolicy: 'cache-first',
    });

    const queries = [...(highlightedQueriesData?.listQueries?.queries || [])] as QueryEntity[];

    const highlightedQueries = filterQueries(
        filterText,
        queries.map((queryEntity) => mapQuery({ queryEntity, entityUrn, siblingUrn })),
    );

    const total = highlightedQueriesData?.listQueries?.total || 0;

    return { highlightedQueries, client, loading, total, pagination, sorting };
};
