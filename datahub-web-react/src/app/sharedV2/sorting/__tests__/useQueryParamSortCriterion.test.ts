import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useQueryParamSortCriterion } from '@app/sharedV2/sorting/useQueryParamSortCriterion';

import { SortOrder } from '@types';

// Mock useUrlQueryParam
const mockUseUrlQueryParam = vi.fn();

vi.mock('@app/shared/useUrlQueryParam', () => ({
    useUrlQueryParam: (...args) => mockUseUrlQueryParam(...args),
}));

describe('useQueryParamSortCriterion', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUseUrlQueryParam.mockReset();
    });

    it('should initialize with undefined when no URL params', () => {
        mockUseUrlQueryParam.mockImplementation((paramKey) => {
            if (paramKey === 'sort_field') {
                return { value: undefined, setValue: vi.fn() };
            }
            if (paramKey === 'order') {
                return { value: undefined, setValue: vi.fn() };
            }
            return { value: undefined, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useQueryParamSortCriterion());

        expect(result.current.sort).toBeUndefined();
    });

    it('should initialize with sort criterion from URL params', () => {
        mockUseUrlQueryParam.mockImplementation((paramKey) => {
            if (paramKey === 'sort_field') {
                return { value: 'name', setValue: vi.fn() };
            }
            if (paramKey === 'order') {
                return { value: SortOrder.Ascending, setValue: vi.fn() };
            }
            return { value: undefined, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useQueryParamSortCriterion());

        expect(result.current.sort).toEqual({
            field: 'name',
            sortOrder: SortOrder.Ascending,
        });
    });

    it('should handle descending order', () => {
        mockUseUrlQueryParam.mockImplementation((paramKey) => {
            if (paramKey === 'sort_field') {
                return { value: 'date', setValue: vi.fn() };
            }
            if (paramKey === 'order') {
                return { value: SortOrder.Descending, setValue: vi.fn() };
            }
            return { value: undefined, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useQueryParamSortCriterion());

        expect(result.current.sort).toEqual({
            field: 'date',
            sortOrder: SortOrder.Descending,
        });
    });

    it('should handle default value', () => {
        const defaultValue = { field: 'id', sortOrder: SortOrder.Descending };
        mockUseUrlQueryParam.mockImplementation((paramKey, defaultValueParam) => {
            if (paramKey === 'sort_field') {
                return { value: defaultValueParam, setValue: vi.fn() };
            }
            if (paramKey === 'order') {
                return { value: defaultValueParam, setValue: vi.fn() };
            }
            return { value: undefined, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useQueryParamSortCriterion(defaultValue));

        expect(result.current.sort).toEqual({
            field: 'id',
            sortOrder: SortOrder.Descending,
        });
    });

    it('should update URL params when sort is set', () => {
        const mockSetField = vi.fn();
        const mockSetOrder = vi.fn();

        mockUseUrlQueryParam.mockImplementation((paramKey) => {
            if (paramKey === 'sort_field') {
                return { value: 'name', setValue: mockSetField };
            }
            if (paramKey === 'order') {
                return { value: SortOrder.Ascending, setValue: mockSetOrder };
            }
            return { value: undefined, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useQueryParamSortCriterion());

        const newSort = { field: 'date', sortOrder: SortOrder.Descending };

        act(() => {
            result.current.setSort(newSort);
        });

        expect(mockSetField).toHaveBeenCalledWith('date');
        expect(mockSetOrder).toHaveBeenCalledWith(SortOrder.Descending);
        expect(result.current.sort).toEqual(newSort);
    });

    it('should clear URL params when sort is set to undefined', () => {
        const mockSetField = vi.fn();
        const mockSetOrder = vi.fn();

        mockUseUrlQueryParam.mockImplementation((paramKey) => {
            if (paramKey === 'sort_field') {
                return { value: 'name', setValue: mockSetField };
            }
            if (paramKey === 'order') {
                return { value: SortOrder.Ascending, setValue: mockSetOrder };
            }
            return { value: undefined, setValue: vi.fn() };
        });

        const { result } = renderHook(() => useQueryParamSortCriterion());

        act(() => {
            result.current.setSort(undefined);
        });

        expect(mockSetField).toHaveBeenCalledWith('');
        expect(mockSetOrder).toHaveBeenCalledWith('');
        expect(result.current.sort).toBeUndefined();
    });
});
