import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useInfiniteScroll } from '@components/components/InfiniteScrollList/useInfiniteScroll';

const flushPromises = () => new Promise(setImmediate);

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

    it('sets hasMore=false when less than pageSize items are fetched and no totalItemCount provided', async () => {
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

    it('does not load more data if already loading or hasMore is false', async () => {
        const dataBatches = [[1, 2, 3]];
        const fetchData = createFetchDataMock(dataBatches);

        const { result, waitForNextUpdate } = renderHook(() => useInfiniteScroll({ fetchData, pageSize }));

        await waitForNextUpdate();

        expect(result.current.loading).toBe(false);
        expect(result.current.hasMore).toBe(true);

        act(() => {
            result.current.observerRef.current = document.createElement('div');
        });

        await flushPromises();

        expect(fetchData).toHaveBeenCalledTimes(1);
        expect(fetchData).toHaveBeenCalledTimes(1);
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

        expect(observeMock).toHaveBeenCalled();

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
});
