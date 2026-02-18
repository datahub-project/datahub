import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useUrlQueryParam } from '@app/shared/useUrlQueryParam';
import useUrlParamsPagination from '@app/sharedV2/pagination/useUrlParamsPagination';

// Mock useUrlQueryParam
vi.mock('@app/shared/useUrlQueryParam', () => ({
    useUrlQueryParam: vi.fn(),
}));

describe('useUrlParamsPagination', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        // Default mock implementation for useUrlQueryParam
        (useUrlQueryParam as any).mockImplementation((paramKey, defaultValue) => {
            if (paramKey === 'page') {
                return { value: '1', setValue: vi.fn() };
            }
            // Add other parameters if needed
            return { value: defaultValue, setValue: vi.fn() };
        });
    });

    it('should initialize with default values when no URL param', () => {
        (useUrlQueryParam as any).mockImplementation((paramKey, defaultValue) => {
            if (paramKey === 'page') {
                return { value: '1', setValue: vi.fn() };
            }
            return { value: defaultValue, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useUrlParamsPagination(10));

        expect(result.current.page).toBe(1);
        expect(result.current.pageSize).toBe(10);
        expect(result.current.start).toBe(0);
        expect(result.current.count).toBe(10);
    });

    it('should initialize with URL param page value', () => {
        (useUrlQueryParam as any).mockImplementation((paramKey, defaultValue) => {
            if (paramKey === 'page') {
                return { value: '3', setValue: vi.fn() };
            }
            return { value: defaultValue, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useUrlParamsPagination(20));

        expect(result.current.page).toBe(3);
        expect(result.current.pageSize).toBe(20);
        expect(result.current.start).toBe(40); // (3-1) * 20
        expect(result.current.count).toBe(20);
    });

    it('should update both internal state and URL param when page is set', () => {
        const mockSetValue = vi.fn();
        (useUrlQueryParam as any).mockImplementation((paramKey, defaultValue) => {
            if (paramKey === 'page') {
                return { value: '1', setValue: mockSetValue };
            }
            return { value: defaultValue, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useUrlParamsPagination(10));

        act(() => {
            result.current.setPage(5);
        });

        expect(mockSetValue).toHaveBeenCalledWith('5');
        expect(result.current.page).toBe(5);
        expect(result.current.start).toBe(40); // (5-1) * 10
    });

    it('should handle page change correctly with different default page size', () => {
        const mockSetValue = vi.fn();
        (useUrlQueryParam as any).mockImplementation((paramKey, defaultValue) => {
            if (paramKey === 'page') {
                return { value: '2', setValue: mockSetValue };
            }
            return { value: defaultValue, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useUrlParamsPagination(25));

        expect(result.current.page).toBe(2);
        expect(result.current.pageSize).toBe(25);
        expect(result.current.start).toBe(25); // (2-1) * 25

        act(() => {
            result.current.setPage(4);
        });

        expect(mockSetValue).toHaveBeenCalledWith('4');
        expect(result.current.page).toBe(4);
        expect(result.current.start).toBe(75); // (4-1) * 25
    });
});
