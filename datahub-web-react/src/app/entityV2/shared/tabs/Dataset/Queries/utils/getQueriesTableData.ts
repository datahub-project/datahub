import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';

type GetQueriesTableDataArgs = {
    queries: Query[];
    isServerPaginated: boolean;
    page: number;
    pageSize: number;
};

/**
 * Returns the rows the Queries table should render for the current page.
 *
 * Server-paginated sections (Popular / Highlighted) already receive one page of results.
 * Client-paginated sections (Downstream / Recent) receive the full list and must be sliced
 * here — Alchemy's Table has no built-in pagination like Ant Design's did.
 *
 * @param args.queries - Query rows for the section
 * @param args.isServerPaginated - Whether the parent already paged via listQueries start/count
 * @param args.page - 1-based page index used for client slicing
 * @param args.pageSize - Page size used for client slicing
 * @returns The rows to pass to the table `data` prop
 */
export function getQueriesTableData({ queries, isServerPaginated, page, pageSize }: GetQueriesTableDataArgs): Query[] {
    if (isServerPaginated) {
        return queries;
    }
    const start = (page - 1) * pageSize;
    return queries.slice(start, start + pageSize);
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
