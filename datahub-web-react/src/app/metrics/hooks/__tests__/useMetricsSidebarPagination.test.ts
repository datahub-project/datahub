import { act, renderHook } from '@testing-library/react-hooks';

import useMetricsSidebarPagination from '@app/metrics/hooks/useMetricsSidebarPagination';
import {
    advanceMetricsSidebarPagination,
    mergeMetricsSidebarPaginationPage,
} from '@app/metrics/utils/metricsSidebarPagination';

type TestEntity = {
    urn: string;
};

describe('useMetricsSidebarPagination', () => {
    it('clears the old cursor when criteria leave and return before a new page arrives', () => {
        const { result, rerender } = renderHook(
            ({ criteriaKey }) => useMetricsSidebarPagination<TestEntity>(criteriaKey),
            { initialProps: { criteriaKey: 'a' } },
        );

        act(() => {
            result.current.setPagination((current) =>
                advanceMetricsSidebarPagination(
                    mergeMetricsSidebarPaginationPage(current, 'a', [{ urn: 'old' }]),
                    'a',
                    'page-2',
                ),
            );
        });

        rerender({ criteriaKey: 'b' });
        expect(result.current).toMatchObject({ scrollId: null, entities: [] });

        rerender({ criteriaKey: 'a' });
        expect(result.current).toMatchObject({ scrollId: null, entities: [] });
    });
});
