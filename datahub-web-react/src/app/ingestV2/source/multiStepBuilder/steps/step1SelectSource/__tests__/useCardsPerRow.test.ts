import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useCardsPerRow } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/useCardsPerRow';

class MockResizeObserver {
    callback: ResizeObserverCallback;

    constructor(cb: ResizeObserverCallback) {
        this.callback = cb;
    }

    observe = vi.fn();

    disconnect = vi.fn();

    triggerResize(entries: any = []) {
        this.callback(entries, this as any);
    }
}

describe('useCardsPerRow', () => {
    let resizeObserverMock: MockResizeObserver;
    let resizeHandler: (() => void) | null = null;

    beforeEach(() => {
        resizeObserverMock = new MockResizeObserver(() => {});
        (global as any).ResizeObserver = vi.fn().mockImplementation((cb) => {
            resizeObserverMock = new MockResizeObserver(cb);
            return resizeObserverMock;
        });

        resizeHandler = null;

        vi.spyOn(window, 'addEventListener').mockImplementation((event, handler) => {
            if (event === 'resize') {
                resizeHandler = handler as any;
            }
        });

        vi.spyOn(window, 'removeEventListener').mockImplementation((event, handler) => {
            if (event === 'resize' && resizeHandler === handler) {
                resizeHandler = null;
            }
        });
    });

    afterEach(() => {
        vi.restoreAllMocks();
        resizeHandler = null;
    });

    function setup(width: number) {
        const element = {} as HTMLElement;

        Object.defineProperty(element, 'clientWidth', {
            get: () => width,
            configurable: true,
        });

        return { current: element };
    }

    it('should return minColumns initially when container has no width', () => {
        const ref = setup(0);

        const { result } = renderHook(() => useCardsPerRow(ref, 300, 16));

        expect(result.current).toBe(1);
    });

    it('should compute correct number of columns based on container width', () => {
        const ref = setup(1000);

        const { result } = renderHook(() => useCardsPerRow(ref, 300, 16, 1));

        expect(result.current).toBe(3);
    });

    it('should never return columns below minColumns', () => {
        const ref = setup(200);

        const { result } = renderHook(() => useCardsPerRow(ref, 300, 16, 2));

        expect(result.current).toBe(2);
    });

    it('should update columns when ResizeObserver triggers a resize event', () => {
        let width = 1000;
        const ref = setup(width);

        const { result } = renderHook(() => useCardsPerRow(ref, 300, 16, 1));

        expect(result.current).toBe(3);

        width = 1400;
        Object.defineProperty(ref.current, 'clientWidth', {
            get: () => width,
            configurable: true,
        });

        act(() => {
            resizeObserverMock.triggerResize();
        });

        expect(result.current).toBe(4);
    });

    it('should fall back to window.resize when ResizeObserver is unavailable', () => {
        (global as any).ResizeObserver = undefined;

        const ref = setup(900);
        const { result } = renderHook(() => useCardsPerRow(ref, 300, 16, 1));

        expect(result.current).toBe(2);
        expect(resizeHandler).toBeDefined();

        Object.defineProperty(ref.current, 'clientWidth', {
            get: () => 1300,
            configurable: true,
        });

        act(() => {
            resizeHandler?.();
        });

        expect(result.current).toBe(4);
    });

    it('should clean up ResizeObserver when unmounted', () => {
        const ref = setup(1000);

        const { unmount } = renderHook(() => useCardsPerRow(ref, 300, 16, 1));

        expect(resizeObserverMock.observe).toHaveBeenCalled();

        unmount();

        expect(resizeObserverMock.disconnect).toHaveBeenCalled();
    });

    it('should clean up window resize listener on unmount when ResizeObserver is unavailable', () => {
        (global as any).ResizeObserver = undefined;

        const ref = setup(1000);

        const { unmount } = renderHook(() => useCardsPerRow(ref, 300, 16, 1));

        unmount();

        expect(resizeHandler).toBeNull();
    });

    it('should recompute when cardMinWidth changes', () => {
        const width = 1000;
        const ref = setup(width);

        const { rerender, result } = renderHook(({ w }) => useCardsPerRow(ref, w, 16, 1), { initialProps: { w: 300 } });

        expect(result.current).toBe(3);

        rerender({ w: 200 });

        expect(result.current).toBe(4);
    });

    it('should recompute when gap changes', () => {
        const ref = setup(1000);

        const { rerender, result } = renderHook(({ gap }) => useCardsPerRow(ref, 300, gap, 1), {
            initialProps: { gap: 16 },
        });

        expect(result.current).toBe(3);

        rerender({ gap: 32 });

        expect(result.current).toBe(3);
    });

    it('should handle very large container widths', () => {
        const ref = setup(5000);

        const { result } = renderHook(() => useCardsPerRow(ref, 300, 16, 1));

        expect(result.current).toBe(15);
    });

    it('should return initial minColumns and exit early when containerRef.current is null', () => {
        const ref = { current: null } as any;

        const { result } = renderHook(() => useCardsPerRow(ref, 300, 16, 2));

        expect(result.current).toBe(2);
    });
});
