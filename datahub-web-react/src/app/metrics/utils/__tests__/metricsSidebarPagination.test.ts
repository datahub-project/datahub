import {
    advanceMetricsSidebarPagination,
    createMetricsSidebarPaginationState,
    getMetricsSidebarPaginationView,
    mergeMetricsSidebarPage,
    mergeMetricsSidebarPaginationPage,
} from '@app/metrics/utils/metricsSidebarPagination';

type TestEntity = {
    urn: string;
    name: string;
};

describe('metrics sidebar pagination', () => {
    it('replaces accumulated rows with the first page', () => {
        const current: TestEntity[] = [{ urn: 'old', name: 'Old' }];
        const fresh: TestEntity[] = [{ urn: 'new', name: 'New' }];

        expect(mergeMetricsSidebarPage(current, fresh, true)).toBe(fresh);
    });

    it('updates duplicates and appends new rows in server order', () => {
        const current: TestEntity[] = [
            { urn: 'a', name: 'A' },
            { urn: 'b', name: 'Old B' },
        ];
        const updatedB = { urn: 'b', name: 'New B' };
        const addedC = { urn: 'c', name: 'C' };

        expect(mergeMetricsSidebarPage(current, [updatedB, addedC], false)).toEqual([current[0], updatedB, addedC]);
    });

    it('preserves the existing array when a repeated page changes nothing', () => {
        const current: TestEntity[] = [
            { urn: 'a', name: 'A' },
            { urn: 'b', name: 'B' },
        ];

        expect(mergeMetricsSidebarPage(current, [current[1]], false)).toBe(current);
    });

    it('exposes a null cursor and no stale rows as soon as criteria change', () => {
        const initial = createMetricsSidebarPaginationState<TestEntity>('name-asc');
        const firstPage = mergeMetricsSidebarPaginationPage(initial, 'name-asc', [{ urn: 'a', name: 'A' }]);
        const paginated = advanceMetricsSidebarPagination(firstPage, 'name-asc', 'next-page');

        expect(getMetricsSidebarPaginationView(paginated, 'name-desc')).toEqual({
            scrollId: null,
            entities: [],
        });
    });

    it('replaces old criteria when the new first page arrives', () => {
        const oldState = advanceMetricsSidebarPagination(
            mergeMetricsSidebarPaginationPage(createMetricsSidebarPaginationState<TestEntity>('old'), 'old', [
                { urn: 'old', name: 'Old' },
            ]),
            'old',
            'old-cursor',
        );
        const fresh = [{ urn: 'new', name: 'New' }];

        expect(mergeMetricsSidebarPaginationPage(oldState, 'new', fresh)).toEqual({
            criteriaKey: 'new',
            scrollId: null,
            entities: fresh,
        });
    });
});
