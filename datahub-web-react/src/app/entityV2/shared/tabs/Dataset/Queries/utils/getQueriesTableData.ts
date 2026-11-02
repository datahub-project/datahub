import { SortingState } from '@components/components/Table/types';

import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';

type QuerySorter = (queryA: Query, queryB: Query) => number;

type GetQueriesTableDataArgs = {
    queries: Query[];
    isServerPaginated: boolean;
    page: number;
    pageSize: number;
    sorter?: QuerySorter;
    sortOrder?: SortingState;
};

/**
 * Orders a client-paged list before slicing.
 *
 * Alchemy's Table sorts only the rows it is given. Downstream passes a function sorter
 * (Powers) and no server sort, so the full list has to be ordered here or each page
 * sorts in isolation.
 */
function orderClientQueries(queries: Query[], sorter?: QuerySorter, sortOrder?: SortingState): Query[] {
    if (!sorter || !sortOrder || sortOrder === SortingState.ORIGINAL) {
        return queries;
    }
    return queries
        .slice()
        .sort((queryA, queryB) =>
            sortOrder === SortingState.ASCENDING ? sorter(queryA, queryB) : sorter(queryB, queryA),
        );
}

/**
 * Returns the rows the Queries table should render for the current page.
 *
 * Server-paginated sections (Popular / Highlighted) already receive one page of results.
 * Client-paginated sections (Downstream / Recent) receive the full list. Sort that list
 * first, then slice — Alchemy's Table renders the rows it is given and does not page them.
 *
 * @param args.queries - Query rows for the section
 * @param args.isServerPaginated - Whether the parent already paged via listQueries start/count
 * @param args.page - 1-based page index used for client slicing
 * @param args.pageSize - Page size used for client slicing
 * @param args.sorter - Client column comparator. Ignored for server-paged sections
 * @param args.sortOrder - Client sort direction. Original order skips sorting
 * @returns The rows to pass to the table `data` prop
 */
export function getQueriesTableData({
    queries,
    isServerPaginated,
    page,
    pageSize,
    sorter,
    sortOrder,
}: GetQueriesTableDataArgs): Query[] {
    if (isServerPaginated) {
        return queries;
    }
    const ordered = orderClientQueries(queries, sorter, sortOrder);
    const start = (page - 1) * pageSize;
    return ordered.slice(start, start + pageSize);
}

/**
 * Whether the Queries table should show its full-page loading state.
 *
 * Alchemy Table replaces all rows with a spinner when `isLoading` is true. Only show that
 * on the initial empty load so refetch/sort/filter keeps previous rows mounted.
 *
 * @param loading - Apollo loading flag for the section
 * @param rowCount - Number of rows currently being rendered
 * @returns True when the table should render its loading placeholder
 */
export function shouldShowQueriesTableLoading(loading: boolean | undefined, rowCount: number): boolean {
    return Boolean(loading && rowCount === 0);
}
