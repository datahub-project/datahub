import { act, waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useVisibilityObserver } from '@app/entityV2/summary/documentation/useVisibilityObserver';

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

// @ts-expect-error override global (will be replaced per test in beforeEach)
global.IntersectionObserver = MockIntersectionObserver;

// helper to mock scrollHeight
const setScrollHeight = (el: HTMLElement, value: number) => {
    Object.defineProperty(el, 'scrollHeight', { value, configurable: true });
};

describe('useVisibilityObserver', () => {
    let element: HTMLDivElement;
    let mockObserver: MockIntersectionObserver;

    beforeEach(() => {
        element = document.createElement('div');
        document.body.appendChild(element);

        mockObserver = new MockIntersectionObserver(() => {});
        // @ts-expect-error override with vi.fn factory
        global.IntersectionObserver = vi.fn((cb) => {
            mockObserver = new MockIntersectionObserver(cb);
            return mockObserver;
        });
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
            mockObserver.triggerIntersect(true);
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

    it('should clean up observer on unmount', async () => {
        const { result, unmount, rerender } = renderHook(({ maxHeight }) => useVisibilityObserver(maxHeight), {
            initialProps: { maxHeight: 0 },
        });

        act(() => {
            result.current.elementRef.current = element;
            setScrollHeight(element, 200);
        });

        rerender({ maxHeight: 100 });

        await waitFor(() => {
            expect(mockObserver.elements.length).toBeGreaterThan(0);
        });

        unmount();
        expect(mockObserver.disconnect).toHaveBeenCalled();
    });
});
