import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it } from 'vitest';

import usePagination from '@app/sharedV2/pagination/usePagination';

describe('usePagination', () => {
    it('should initialize with default page 1 and default page size', () => {
        const { result } = renderHook(() => usePagination());

        expect(result.current.page).toBe(1);
        expect(result.current.pageSize).toBe(10); // default fallback
        expect(result.current.start).toBe(0);
        expect(result.current.count).toBe(10);
    });

    it('should initialize with provided default page size', () => {
        const { result } = renderHook(() => usePagination(25));

        expect(result.current.page).toBe(1);
        expect(result.current.pageSize).toBe(25);
        expect(result.current.start).toBe(0);
        expect(result.current.count).toBe(25);
    });

    it('should initialize with provided initial page', () => {
        const { result } = renderHook(() => usePagination(10, 3));

        expect(result.current.page).toBe(3);
        expect(result.current.pageSize).toBe(10);
        expect(result.current.start).toBe(20); // (3-1) * 10
        expect(result.current.count).toBe(10);
    });

    it('should update page correctly', () => {
        const { result } = renderHook(() => usePagination(10, 1));

        act(() => {
            result.current.setPage(3);
        });

        expect(result.current.page).toBe(3);
        expect(result.current.start).toBe(20); // (3-1) * 10
    });

    it('should update page size correctly', () => {
        const { result } = renderHook(() => usePagination(10, 1));

        act(() => {
            result.current.setPageSize(20);
        });

        expect(result.current.pageSize).toBe(20);
        expect(result.current.count).toBe(20);
        expect(result.current.start).toBe(0); // Since page is still 1, start should be (1-1)*20 = 0
    });

    it('should calculate start correctly based on page and page size', () => {
        const { result } = renderHook(() => usePagination(5, 4));

        expect(result.current.start).toBe(15); // (4-1) * 5 = 15

        act(() => {
            result.current.setPage(2);
        });

        expect(result.current.start).toBe(5); // (2-1) * 5 = 5
    });
});
