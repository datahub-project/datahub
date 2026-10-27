import { describe, expect, it } from 'vitest';

import { SortingState } from '@components/components/Table/types';

import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import {
    getQueriesTableData,
    shouldShowQueriesTableLoading,
} from '@app/entityV2/shared/tabs/Dataset/Queries/utils/getQueriesTableData';

function query(title: string): Query {
    return { query: title, title };
}

const queries = [query('c'), query('a'), query('b'), query('d')];
const byTitle = (queryA: Query, queryB: Query) => (queryA.title ?? '').localeCompare(queryB.title ?? '');

describe('getQueriesTableData', () => {
    it('returns the server page unchanged', () => {
        const page = getQueriesTableData({
            queries,
            isServerPaginated: true,
            page: 1,
            pageSize: 2,
            sorter: byTitle,
            sortOrder: SortingState.ASCENDING,
        });

        expect(page.map((row) => row.title)).toEqual(['c', 'a', 'b', 'd']);
    });

    it('sorts the full client list before slicing a page', () => {
        const firstPage = getQueriesTableData({
            queries,
            isServerPaginated: false,
            page: 1,
            pageSize: 2,
            sorter: byTitle,
            sortOrder: SortingState.ASCENDING,
        });
        const secondPage = getQueriesTableData({
            queries,
            isServerPaginated: false,
            page: 2,
            pageSize: 2,
            sorter: byTitle,
            sortOrder: SortingState.DESCENDING,
        });

        expect(firstPage.map((row) => row.title)).toEqual(['a', 'b']);
        expect(secondPage.map((row) => row.title)).toEqual(['b', 'a']);
    });

    it('keeps the original order when the sort is cleared', () => {
        const page = getQueriesTableData({
            queries,
            isServerPaginated: false,
            page: 1,
            pageSize: 2,
            sorter: byTitle,
            sortOrder: SortingState.ORIGINAL,
        });

        expect(page.map((row) => row.title)).toEqual(['c', 'a']);
    });

    it('returns an empty list when there are no queries', () => {
        expect(
            getQueriesTableData({
                queries: [],
                isServerPaginated: false,
                page: 1,
                pageSize: 5,
            }),
        ).toEqual([]);
    });
});

describe('shouldShowQueriesTableLoading', () => {
    it('shows loading only when fetching with no rows', () => {
        expect(shouldShowQueriesTableLoading(true, 0)).toBe(true);
        expect(shouldShowQueriesTableLoading(true, 5)).toBe(false);
        expect(shouldShowQueriesTableLoading(false, 0)).toBe(false);
        expect(shouldShowQueriesTableLoading(undefined, 0)).toBe(false);
    });
});
