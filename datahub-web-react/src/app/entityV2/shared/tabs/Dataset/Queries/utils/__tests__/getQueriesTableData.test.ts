import { describe, expect, it } from 'vitest';

import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import {
    getQueriesTableData,
    shouldShowQueriesTableLoading,
} from '@app/entityV2/shared/tabs/Dataset/Queries/utils/getQueriesTableData';

const queries = Array.from({ length: 12 }, (_, i) => ({
    urn: `urn:li:query:q${i + 1}`,
    query: `SELECT ${i + 1}`,
})) as Query[];

describe('getQueriesTableData', () => {
    it('should return the full list for server-paginated sections', () => {
        expect(
            getQueriesTableData({
                queries,
                isServerPaginated: true,
                page: 2,
                pageSize: 5,
            }),
        ).toEqual(queries);
    });

    it('should slice client-paginated sections by page', () => {
        expect(
            getQueriesTableData({
                queries,
                isServerPaginated: false,
                page: 1,
                pageSize: 5,
            }).map((q) => q.urn),
        ).toEqual(['urn:li:query:q1', 'urn:li:query:q2', 'urn:li:query:q3', 'urn:li:query:q4', 'urn:li:query:q5']);

        expect(
            getQueriesTableData({
                queries,
                isServerPaginated: false,
                page: 2,
                pageSize: 5,
            }).map((q) => q.urn),
        ).toEqual(['urn:li:query:q6', 'urn:li:query:q7', 'urn:li:query:q8', 'urn:li:query:q9', 'urn:li:query:q10']);

        expect(
            getQueriesTableData({
                queries,
                isServerPaginated: false,
                page: 3,
                pageSize: 5,
            }).map((q) => q.urn),
        ).toEqual(['urn:li:query:q11', 'urn:li:query:q12']);
    });

    it('should return an empty list when there are no queries', () => {
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
    it('should show loading only when fetching with no rows', () => {
        expect(shouldShowQueriesTableLoading(true, 0)).toBe(true);
        expect(shouldShowQueriesTableLoading(true, 5)).toBe(false);
        expect(shouldShowQueriesTableLoading(false, 0)).toBe(false);
        expect(shouldShowQueriesTableLoading(undefined, 0)).toBe(false);
    });
});
