/* eslint-disable max-classes-per-file */
import { act, waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useVisibilityObserver } from '@app/entityV2/summary/documentation/useVisibilityObserver';

class MockResizeObserver {
    cb: ResizeObserverCallback;

    elements: Element[] = [];

    constructor(cb: ResizeObserverCallback) {
        this.cb = cb;
    }

    observe = (el: Element) => {
        this.elements.push(el);
    };

    disconnect = vi.fn();

    unobserve = vi.fn();

    triggerResize = () => {
        const entries = this.elements.map((el) => ({
            target: el,
            contentRect: {} as DOMRect,
            borderBoxSize: [],
            contentBoxSize: [],
        }));
        this.cb(entries as any, this as unknown as ResizeObserver);
    };
}

// Mock IntersectionObserver
class MockIntersectionObserver {
    cb: IntersectionObserverCallback;

    elements: Element[] = [];

    constructor(cb: IntersectionObserverCallback) {
        this.cb = cb;
    }

    observe = (el: Element) => {
        this.elements.push(el);
    };

    disconnect = vi.fn();

    unobserve = vi.fn();

    triggerIntersect = (isIntersecting: boolean) => {
        const entries = this.elements.map((el) => ({
            isIntersecting,
            target: el,
            boundingClientRect: {} as DOMRect,
            intersectionRatio: isIntersecting ? 1 : 0,
            intersectionRect: {} as DOMRect,
            rootBounds: {} as DOMRect,
            time: Date.now(),
        }));
        this.cb(entries, this as unknown as IntersectionObserver);
    };
}

// helper to mock scrollHeight
const setScrollHeight = (el: HTMLElement, value: number) => {
    Object.defineProperty(el, 'scrollHeight', { value, configurable: true });
};

describe('useVisibilityObserver', () => {
    let element: HTMLDivElement;
    let mockIntersectionObserver: MockIntersectionObserver;
    let mockResizeObserver: MockResizeObserver;

    beforeEach(() => {
        element = document.createElement('div');
        document.body.appendChild(element);

        mockIntersectionObserver = new MockIntersectionObserver(() => {});
        mockResizeObserver = new MockResizeObserver(() => {});

        global.IntersectionObserver = vi.fn((cb) => {
            mockIntersectionObserver = new MockIntersectionObserver(cb);
            return mockIntersectionObserver;
        }) as any;

        global.ResizeObserver = vi.fn((cb) => {
            mockResizeObserver = new MockResizeObserver(cb);
            return mockResizeObserver;
        }) as any;
    });

    afterEach(() => {
        document.body.innerHTML = '';
        vi.restoreAllMocks();
    });

    it('should return default values when no element is attached', () => {
        const { result } = renderHook(() => useVisibilityObserver(100));
        expect(result.current.hasMore).toBe(false);
        expect(result.current.elementRef.current).toBe(null);
    });

    it('should detect when element is taller than maxViewHeight', async () => {
        const { result, rerender } = renderHook(({ maxHeight }) => useVisibilityObserver(maxHeight), {
            initialProps: { maxHeight: 0 },
        });

        act(() => {
            result.current.elementRef.current = element;
            setScrollHeight(element, 200);
        });

        rerender({ maxHeight: 100 });

        await waitFor(() => {
            expect(result.current.hasMore).toBe(true);
        });
    });

    it('should detect when element is shorter than maxViewHeight', async () => {
        const { result, rerender } = renderHook(({ maxHeight }) => useVisibilityObserver(maxHeight), {
            initialProps: { maxHeight: 0 },
        });

        act(() => {
            result.current.elementRef.current = element;
            setScrollHeight(element, 100);
        });

        rerender({ maxHeight: 300 });

        await waitFor(() => {
            expect(result.current.hasMore).toBe(false);
        });
    });

    it('should update hasMore when intersection occurs', async () => {
        const { result, rerender } = renderHook(({ maxHeight }) => useVisibilityObserver(maxHeight), {
            initialProps: { maxHeight: 0 },
        });

        act(() => {
            result.current.elementRef.current = element;
            setScrollHeight(element, 200);
        });

        rerender({ maxHeight: 150 });

        await waitFor(() => {
            expect(result.current.hasMore).toBe(true);
        });

        act(() => {
            setScrollHeight(element, 100);
            mockIntersectionObserver.triggerIntersect(true);
        });

        await waitFor(() => {
            expect(result.current.hasMore).toBe(false);
        });
    });

    it('should respond to dependency changes and recreate observer', async () => {
        let depValue = 0;
        const { result, rerender } = renderHook(({ dep }) => useVisibilityObserver(120, [dep]), {
            initialProps: { dep: depValue },
        });

        act(() => {
            result.current.elementRef.current = element;
            setScrollHeight(element, 200);
        });

        depValue = 1;
        rerender({ dep: depValue });

        await waitFor(() => {
            expect(result.current.hasMore).toBe(true);
        });

        depValue = 2;
        rerender({ dep: depValue });

        expect((global.IntersectionObserver as any).mock.calls.length).toBeGreaterThan(1);
    });

    it('should clean up both observers on unmount', async () => {
        const { result, unmount, rerender } = renderHook(({ maxHeight }) => useVisibilityObserver(maxHeight), {
            initialProps: { maxHeight: 0 },
        });

        act(() => {
            result.current.elementRef.current = element;
            setScrollHeight(element, 200);
        });

        rerender({ maxHeight: 100 });

        await waitFor(() => {
            expect(mockIntersectionObserver.elements.length).toBeGreaterThan(0);
            expect(mockResizeObserver.elements.length).toBeGreaterThan(0);
        });

        unmount();
        expect(mockIntersectionObserver.disconnect).toHaveBeenCalled();
        expect(mockResizeObserver.disconnect).toHaveBeenCalled();
    });

    it('should update hasMore when element size changes through ResizeObserver', async () => {
        const { result, rerender } = renderHook(({ maxHeight }) => useVisibilityObserver(maxHeight), {
            initialProps: { maxHeight: 0 },
        });

        act(() => {
            result.current.elementRef.current = element;
            setScrollHeight(element, 100);
        });

        rerender({ maxHeight: 300 });

        await waitFor(() => {
            expect(result.current.hasMore).toBe(false);
        });

        // Simulate a resize by changing scrollHeight and triggering the resize observer
        act(() => {
            setScrollHeight(element, 400);
            // Trigger the resize observer to detect the change
            mockResizeObserver.triggerResize();
        });

        await waitFor(() => {
            expect(result.current.hasMore).toBe(true);
        });
    });
});
