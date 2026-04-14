import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { updateUrlParam } from '@app/shared/updateUrlParam';
import { useUrlQueryParam } from '@app/shared/useUrlQueryParam';

// Mock updateUrlParam
vi.mock('@app/shared/updateUrlParam', () => ({
    updateUrlParam: vi.fn(),
}));

// Mock history and location for useUrlQueryParam
const mockHistory = {
    replace: vi.fn(),
};

const mockLocation = {
    pathname: '/test',
    search: '?filter=test',
    state: {},
};

// Mock the react-router hooks
vi.mock('react-router', () => ({
    useHistory: () => mockHistory,
    useLocation: () => mockLocation,
}));

describe('useUrlQueryParam', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockLocation.search = '?filter=test';
    });

    it('should return default value when URL param is not present', () => {
        mockLocation.search = '?other=value';
        const defaultValue = 'default-value';

        const { result } = renderHook(() => useUrlQueryParam('filter', defaultValue));

        expect(result.current.value).toBe(defaultValue);
    });

    it('should return URL param value when present', () => {
        mockLocation.search = '?filter=custom-value&other=value';
        const defaultValue = 'default-value';

        const { result } = renderHook(() => useUrlQueryParam('filter', defaultValue));

        expect(result.current.value).toBe('custom-value');
    });

    it('should set URL param when setValue is called', () => {
        const { result } = renderHook(() => useUrlQueryParam('filter', 'default'));

        act(() => {
            result.current.setValue('new-value');
        });

        expect(updateUrlParam).toHaveBeenCalledWith(mockHistory, 'filter', 'new-value', mockLocation.state);
    });

    it('should set URL param with default value initially if not present', () => {
        mockLocation.search = '?other=value';
        const defaultValue = 'initial-value';

        renderHook(() => useUrlQueryParam('filter', defaultValue));

        expect(updateUrlParam).toHaveBeenCalledWith(mockHistory, 'filter', defaultValue, mockLocation.state);
    });

    it('should not set URL param if value already exists in URL', () => {
        mockLocation.search = '?filter=existing-value';
        const defaultValue = 'default-value';

        renderHook(() => useUrlQueryParam('filter', defaultValue));

        expect(updateUrlParam).not.toHaveBeenCalled();
    });
});
