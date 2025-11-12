import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useInfiniteScroll } from '@components/components/InfiniteScrollList/useInfiniteScroll';

const flushPromises = () => new Promise(setImmediate);

// Mock IntersectionObserver
const mockIntersectionObserver = vi.fn((callback) => {
    const observerInstance = {
        observe: (element: Element) => {
            callback([{ isIntersecting: true, target: element }], observerInstance);
        },
        unobserve: vi.fn(),
        disconnect: vi.fn(),
    };
    return observerInstance;
});
global.IntersectionObserver = mockIntersectionObserver as any;

describe('useInfiniteScroll hook', () => {
    const pageSize = 3;

    function createFetchDataMock(dataBatches: unknown[][]) {
        let callCount = 0;
        return vi.fn().mockImplementation(() => {
            const result = dataBatches[callCount] || [];
            callCount++;
            return Promise.resolve(result);
        });
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('loads initial data on mount', async () => {
        const dataBatches = [[1, 2, 3]];
        const fetchData = createFetchDataMock(dataBatches);

        const { result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        expect(result.current.loading).toBe(true);
        expect(result.current.items).toEqual([]);

        await waitForNextUpdate();

        expect(fetchData).toHaveBeenCalledWith(0, pageSize);
        expect(result.current.items).toEqual([1, 2, 3]);
        expect(result.current.loading).toBe(false);
        expect(result.current.hasMore).toBe(true);
    });

    it('sets hasMore=false when fewer items than pageSize are returned', async () => {
        const dataBatches = [[1, 2]];
        const fetchData = createFetchDataMock(dataBatches);

        const { result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        await waitForNextUpdate();

        expect(result.current.items.length).toBe(2);
        expect(result.current.hasMore).toBe(false);
    });

    it('sets hasMore based on totalItemCount when provided', async () => {
        const totalItemCount = 5;
        const dataBatches = [
            [1, 2, 3],
            [4, 5],
        ];
        const fetchData = createFetchDataMock(dataBatches);

        const { result, waitForNextUpdate } = renderHook(() =>
            useInfiniteScroll({ fetchData, pageSize, totalItemCount }),
        );

        act(() => {
            result.current.observerRef.current = document.createElement('div');
        });

        await waitForNextUpdate();

        expect(result.current.items.length).toBe(5);
        expect(result.current.hasMore).toBe(false);
    });

    it('does not fetch more when already loading or hasMore is false', async () => {
        const dataBatches = [[1, 2, 3]];
        const fetchData = createFetchDataMock(dataBatches);

        const { result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        await waitForNextUpdate();

        // set hasMore=false to trigger early return
        act(() => {
            result.current.hasMore = false as any;
        });

        await flushPromises();

        expect(fetchData).toHaveBeenCalledTimes(1);
    });

    it('does not crash if fetchData returns non-array', async () => {
        const fetchData = vi.fn(() => Promise.resolve(null as any));
        const { result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        await waitForNextUpdate();

        expect(result.current.items).toEqual([]);
        expect(result.current.hasMore).toBe(true);
    });

    it('prepends a new item correctly and prevents duplicates', async () => {
        const dataBatches = [[1, 2, 3]];
        const fetchData = createFetchDataMock(dataBatches);

        const { result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        // Prepend before initial load
        act(() => {
            result.current.prependItem(0);
        });

        await waitForNextUpdate();

        expect(result.current.items).toEqual([0, 1, 2, 3]);

        // Attempt to prepend the same item again
        act(() => {
            result.current.prependItem(0);
        });

        expect(result.current.items).toEqual([0, 1, 2, 3]);
    });

    it('does not prepend null or undefined', async () => {
        const fetchData = vi.fn(() => Promise.resolve([1, 2]));
        const { result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        await waitForNextUpdate();

        act(() => {
            result.current.prependItem(undefined as any);
            result.current.prependItem(null as any);
        });

        expect(result.current.items).toEqual([1, 2]);
    });

    it('removes items correctly using predicate', async () => {
        const fetchData = vi.fn(() => Promise.resolve([1, 2, 3]));
        const { result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        await waitForNextUpdate();

        act(() => {
            result.current.removeItem((item) => item === 2);
        });

        expect(result.current.items).toEqual([1, 3]);
    });

    it('updates items correctly using predicate', async () => {
        const fetchData = vi.fn(() => Promise.resolve([1, 2, 3]));
        const { result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        await waitForNextUpdate();

        act(() => {
            result.current.updateItem(99, (item) => item === 2);
        });

        expect(result.current.items).toEqual([1, 99, 3]);
    });

    it('cleans up IntersectionObserver on unmount', async () => {
        const dataBatches = [[1, 2, 3]];
        const fetchData = createFetchDataMock(dataBatches);

        const unobserveMock = vi.fn();
        const disconnectMock = vi.fn();
        const observeMock = vi.fn(function (this: any, element: Element) {
            this.callback([{ isIntersecting: true, target: element }], this);
        });

        const mockIO = vi.fn(function (this: any, callback: IntersectionObserverCallback) {
            this.callback = callback;
            return {
                observe: observeMock.bind(this),
                unobserve: unobserveMock,
                disconnect: disconnectMock,
            };
        });

        global.IntersectionObserver = mockIO as any;

        const { unmount, result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        act(() => {
            result.current.observerRef.current = document.createElement('div');
        });

        await waitForNextUpdate();

        unmount();

        expect(unobserveMock).toHaveBeenCalled();
        expect(disconnectMock).toHaveBeenCalled();
    });

    it('exposes observerRef ref object', () => {
        const fetchData = vi.fn(() => Promise.resolve([]));
        const { result } = renderHook(() => useInfiniteScroll({ fetchData }));

        expect(result.current.observerRef).toBeDefined();
        expect(typeof result.current.observerRef).toBe('object');
        expect(result.current.observerRef.current).toBeNull();
    });

    it('resets items and startIndex on resetTrigger', async () => {
        const dataBatches = [[1, 2, 3]];
        const fetchData = createFetchDataMock(dataBatches);

        let trigger = 1;
        const { result, waitForNextUpdate, rerender } = renderHook(
            ({ resetTrigger }) => useInfiniteScroll({ fetchData, pageSize, resetTrigger }),
            { initialProps: { resetTrigger: trigger } },
        );

        await waitForNextUpdate();
        expect(result.current.items).toEqual([1, 2, 3]);

        // Trigger reset
        trigger = 2;
        rerender({ resetTrigger: trigger });
        expect(result.current.items).toEqual([]);
        expect(result.current.hasMore).toBe(true);
        expect(result.current.loading).toBe(false);
    });
});
