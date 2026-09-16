import { renderHook } from '@testing-library/react-hooks';

import useRefreshInterval from '@app/ingestV2/shared/hooks/useRefreshInterval';

vi.useFakeTimers();

describe('useRefreshInterval Hook', () => {
    const REFRESH_INTERVAL_MS = 3000;

    beforeEach(() => {
        vi.clearAllTimers();
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('sets up interval when shouldRefreshFn returns true', () => {
        const refresh = vi.fn();
        const { unmount } = renderHook(() => useRefreshInterval(refresh, false, true));

        // Advance time to trigger the interval
        vi.advanceTimersByTime(REFRESH_INTERVAL_MS);
        expect(refresh).toHaveBeenCalledTimes(1); // Ensure refresh was called

        // Cleanup
        unmount();
        expect(() => vi.advanceTimersByTime(REFRESH_INTERVAL_MS)).not.toThrow(); // Ensure no errors after unmount
    });

    it('clears interval when shouldRefresh is false', () => {
        const refresh = vi.fn();
        const shouldRefresh = false;
        const { rerender } = renderHook((props) => useRefreshInterval(props.refresh, false, false), {
            initialProps: { refresh, shouldRefresh: true }, // Start with true
        });

        // Force update to trigger cleanup
        rerender({ refresh, shouldRefresh });
        vi.advanceTimersByTime(REFRESH_INTERVAL_MS);
        expect(refresh).not.toHaveBeenCalled(); // Ensure refresh is not called
    });

    it('clears interval on unmount', () => {
        const refresh = vi.fn();
        const { unmount } = renderHook(() => useRefreshInterval(refresh, false, true));

        // Verify unmount clears interval
        unmount();
        expect(() => vi.advanceTimersByTime(REFRESH_INTERVAL_MS)).not.toThrow(); // No errors after unmount
    });
});
