import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useAggregationsQuery from '@app/searchV2/sidebar/useAggregationsQuery';
import useSidebarPlatforms from '@app/searchV2/sidebar/useSidebarPlatforms';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

vi.mock('@app/searchV2/sidebar/useAggregationsQuery');

describe('useSidebarPlatforms', () => {
    const mockUseAggregationsQuery = vi.mocked(useAggregationsQuery);

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('returns every platform matching the query, including long-tail platforms', () => {
        // confluence (count 2) would previously be dropped because it falls below the
        // top-N cap of the unfiltered base aggregation it used to be intersected with.
        const platformAggregations = [
            { value: 'urn:li:dataPlatform:snowflake', count: 35 },
            { value: 'urn:li:dataPlatform:confluence', count: 2 },
        ];
        mockUseAggregationsQuery.mockReturnValue({
            platformAggregations,
            error: undefined,
            retry: vi.fn(),
        } as any);

        const { result } = renderHook(() => useSidebarPlatforms({ skip: false }));

        expect(result.current.platformAggregations).toEqual(platformAggregations);
    });

    it('only issues the filtered platform aggregation (no unfiltered base query)', () => {
        mockUseAggregationsQuery.mockReturnValue({
            platformAggregations: [],
            error: undefined,
            retry: vi.fn(),
        } as any);

        renderHook(() => useSidebarPlatforms({ skip: false }));

        expect(mockUseAggregationsQuery).toHaveBeenCalledTimes(1);
        expect(mockUseAggregationsQuery).toHaveBeenCalledWith({ skip: false, facets: [PLATFORM_FILTER_NAME] });
        expect(mockUseAggregationsQuery).not.toHaveBeenCalledWith(expect.objectContaining({ excludeFilters: true }));
    });
});
