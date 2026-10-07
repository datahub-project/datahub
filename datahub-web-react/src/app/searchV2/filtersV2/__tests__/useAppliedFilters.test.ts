import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import analytics, { EventType } from '@app/analytics';
import useAppliedFilters from '@app/searchV2/filtersV2/context/useAppliedFilters';

vi.mock('@app/analytics', () => ({
    __esModule: true,
    default: { event: vi.fn() },
    EventType: { SearchBarFilter: 'SearchBarFilter' },
}));

const PLATFORM_URN = 'urn:li:dataPlatform:snowflake';

describe('useAppliedFilters', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('emits SearchBarFilter with filterValues rather than values', () => {
        const { result } = renderHook(() => useAppliedFilters());

        act(() => {
            result.current.updateFieldFilters('platform', {
                filters: [{ field: 'platform', values: [PLATFORM_URN] }],
            });
        });

        // `values` collides with the object-typed `values` field written by structured property
        // events in the usage-event index, so the filter values must be sent under another name.
        expect(analytics.event).toHaveBeenCalledWith({
            type: EventType.SearchBarFilter,
            field: 'platform',
            filterValues: [PLATFORM_URN],
        });
        expect(result.current.flatAppliedFilters).toEqual([{ field: 'platform', values: [PLATFORM_URN] }]);
    });

    it('does not emit an event when a filter is cleared', () => {
        const { result } = renderHook(() => useAppliedFilters());

        act(() => {
            result.current.updateFieldFilters('platform', { filters: [{ field: 'platform', values: [] }] });
        });

        expect(analytics.event).not.toHaveBeenCalled();
        expect(result.current.hasAppliedFilters).toBe(false);
    });
});
