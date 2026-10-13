/**
 * Reports every observed element as on-screen, immediately.
 *
 * Alchemy selects only mount their dropdown once `useIsVisible` says they are visible, and the
 * global IntersectionObserver stub in `setupTests.ts` never invokes its callback — so without this
 * a select renders its label and nothing else, and no option is ever reachable in a test.
 *
 * Call it from `beforeEach` in tests that open a select.
 */
export function mockVisibilityObserver(): void {
    vi.stubGlobal(
        'IntersectionObserver',
        vi.fn((callback: IntersectionObserverCallback) => ({
            observe: vi.fn((element: Element) =>
                callback(
                    [{ isIntersecting: true, target: element } as IntersectionObserverEntry],
                    {} as IntersectionObserver,
                ),
            ),
            unobserve: vi.fn(),
            disconnect: vi.fn(),
            takeRecords: vi.fn(() => []),
        })),
    );
}
