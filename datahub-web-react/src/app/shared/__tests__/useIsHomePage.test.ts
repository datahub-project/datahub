import { renderHook } from '@testing-library/react-hooks';

import { useIsHomePage } from '@app/shared/useIsHomePage';

// Mock react-router
const mockUseLocation = vi.fn();
vi.mock('react-router', () => ({
    useLocation: () => mockUseLocation(),
}));

describe('useIsHomePage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('should return true when pathname is "/"', () => {
        mockUseLocation.mockReturnValue({ pathname: '/' });

        const { result } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(true);
    });

    it('should return false when pathname is not "/"', () => {
        mockUseLocation.mockReturnValue({ pathname: '/search' });

        const { result } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(false);
    });

    it('should return false for nested paths', () => {
        mockUseLocation.mockReturnValue({ pathname: '/dataset/urn:li:dataset:123' });

        const { result } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(false);
    });

    it('should return false for paths with query parameters', () => {
        mockUseLocation.mockReturnValue({ pathname: '/search?q=test' });

        const { result } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(false);
    });

    it('should return false for paths with hash fragments', () => {
        mockUseLocation.mockReturnValue({ pathname: '/dashboard#metrics' });

        const { result } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(false);
    });

    it('should return false for empty pathname', () => {
        mockUseLocation.mockReturnValue({ pathname: '' });

        const { result } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(false);
    });

    it('should return false for undefined pathname', () => {
        mockUseLocation.mockReturnValue({ pathname: undefined });

        const { result } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(false);
    });

    it('should handle pathname with trailing slash correctly', () => {
        mockUseLocation.mockReturnValue({ pathname: '//' });

        const { result } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(false);
    });

    it('should memoize the result and not recompute when pathname stays the same', () => {
        mockUseLocation.mockReturnValue({ pathname: '/' });

        const { result, rerender } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(true);

        // Force a rerender without changing the pathname
        rerender();

        expect(result.current).toBe(true);
        expect(mockUseLocation).toHaveBeenCalledTimes(2); // Called on mount and rerender
    });

    it('should recompute when pathname changes', () => {
        // Start with home page
        mockUseLocation.mockReturnValue({ pathname: '/' });

        const { result, rerender } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(true);

        // Change to different page
        mockUseLocation.mockReturnValue({ pathname: '/search' });
        rerender();

        expect(result.current).toBe(false);

        // Change back to home page
        mockUseLocation.mockReturnValue({ pathname: '/' });
        rerender();

        expect(result.current).toBe(true);
    });

    it('should handle case sensitivity correctly', () => {
        mockUseLocation.mockReturnValue({ pathname: '/HOME' });

        const { result } = renderHook(() => useIsHomePage());

        expect(result.current).toBe(false);
    });

    it('should handle common route variations', () => {
        const testCases = [
            { pathname: '/browse', expected: false },
            { pathname: '/search', expected: false },
            { pathname: '/datasets', expected: false },
            { pathname: '/', expected: true },
            { pathname: '/settings', expected: false },
            { pathname: '/profile', expected: false },
            { pathname: '/lineage', expected: false },
            { pathname: '/glossary', expected: false },
            { pathname: '/domains', expected: false },
            { pathname: '/tags', expected: false },
            { pathname: '/onboarding', expected: false },
        ];

        testCases.forEach(({ pathname, expected }) => {
            mockUseLocation.mockReturnValue({ pathname });

            const { result } = renderHook(() => useIsHomePage());

            expect(result.current).toBe(expected);
        });
    });
});
