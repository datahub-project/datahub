import { act, cleanup } from '@testing-library/react';
import { createMemoryHistory } from 'history';
import React from 'react';
import { Router } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { SearchContext } from '@app/search/context/SearchContext';
import {
    SEARCH_STARTUP_HOLD_TIMEOUT_MS,
    useIsSearchStartupReleased,
    useReportSearchSettled,
} from '@app/searchV2/useIsSearchStartupReleased';
import { renderHook } from '@utils/test-utils/renderHook';

function renderReleased(initialPath: string, isSearchSettled = false) {
    const history = createMemoryHistory({ initialEntries: [initialPath] });
    const context = { isSearchSettled };
    const wrapper = ({ children }: { children: React.ReactNode }) => (
        <Router history={history}>
            <SearchContext.Provider
                value={{
                    query: undefined,
                    selectedSortOption: undefined,
                    setSelectedSortOption: () => null,
                    isFullViewCard: false,
                    setIsFullViewCard: () => null,
                    isSearchSettled: context.isSearchSettled,
                    markSearchSettled: () => null,
                }}
            >
                {children}
            </SearchContext.Provider>
        </Router>
    );
    const rendered = renderHook(() => useIsSearchStartupReleased(), { wrapper });
    return {
        ...rendered,
        history,
        setSettled: (settled: boolean) => {
            context.isSearchSettled = settled;
            rendered.rerender();
        },
    };
}

describe('useIsSearchStartupReleased', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });

    afterEach(() => {
        cleanup();
        vi.useRealTimers();
    });

    it('holds on /search until the search settles, then releases', () => {
        const { result, setSettled } = renderReleased('/search?query=test');
        expect(result.current).toBe(false);

        act(() => setSettled(true));
        expect(result.current).toBe(true);
    });

    it('holds on a /search entity-type route too', () => {
        const { result } = renderReleased('/search/dataset?query=test');
        expect(result.current).toBe(false);
    });

    it('releases after the timeout when the search never settles', () => {
        const { result } = renderReleased('/search?query=test');

        act(() => {
            vi.advanceTimersByTime(SEARCH_STARTUP_HOLD_TIMEOUT_MS - 1);
        });
        expect(result.current).toBe(false);

        act(() => {
            vi.advanceTimersByTime(1);
        });
        expect(result.current).toBe(true);
    });

    it.each(['/', '/dataset/urn:li:dataset:1', '/browse'])('is never held on %s', (path) => {
        const { result } = renderReleased(path);
        expect(result.current).toBe(true);
    });

    it('stays released after the settled signal clears', () => {
        const { result, setSettled } = renderReleased('/search?query=test');
        act(() => setSettled(true));
        expect(result.current).toBe(true);

        act(() => setSettled(false));
        expect(result.current).toBe(true);
    });

    it('stays released when navigating back to /search', () => {
        const { result, history } = renderReleased('/');
        expect(result.current).toBe(true);

        act(() => history.push('/search?query=test'));
        expect(result.current).toBe(true);
    });
});

describe('useReportSearchSettled', () => {
    function renderReport(settled: boolean) {
        const markSearchSettled = vi.fn();
        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <SearchContext.Provider
                value={{
                    query: undefined,
                    selectedSortOption: undefined,
                    setSelectedSortOption: () => null,
                    isFullViewCard: false,
                    setIsFullViewCard: () => null,
                    isSearchSettled: false,
                    markSearchSettled,
                }}
            >
                {children}
            </SearchContext.Provider>
        );
        const { unmount } = renderHook(() => useReportSearchSettled(settled), { wrapper });
        return { markSearchSettled, unmount };
    }

    beforeEach(() => vi.useFakeTimers());
    afterEach(() => {
        cleanup();
        vi.useRealTimers();
        vi.unstubAllGlobals();
    });

    it('does not mark the search settled while it is loading', () => {
        const { markSearchSettled } = renderReport(false);
        act(() => {
            vi.runAllTimers();
        });
        expect(markSearchSettled).not.toHaveBeenCalled();
    });

    it('marks the search settled once it has settled and the main thread is idle', () => {
        const idleCallbacks: Array<() => void> = [];
        vi.stubGlobal('requestIdleCallback', (callback: () => void) => idleCallbacks.push(callback));
        vi.stubGlobal('cancelIdleCallback', vi.fn());

        const { markSearchSettled } = renderReport(true);
        expect(markSearchSettled).not.toHaveBeenCalled();

        act(() => idleCallbacks.forEach((callback) => callback()));
        expect(markSearchSettled).toHaveBeenCalledTimes(1);
    });

    it('falls back to a timer where idle callbacks are unavailable', () => {
        const { markSearchSettled } = renderReport(true);
        act(() => {
            vi.runAllTimers();
        });
        expect(markSearchSettled).toHaveBeenCalledTimes(1);
    });

    it('does not mark the search settled after unmounting', () => {
        const { markSearchSettled, unmount } = renderReport(true);
        unmount();
        act(() => {
            vi.runAllTimers();
        });
        expect(markSearchSettled).not.toHaveBeenCalled();
    });
});
