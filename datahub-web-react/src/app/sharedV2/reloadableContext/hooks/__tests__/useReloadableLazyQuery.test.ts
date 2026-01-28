import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import * as ReloadableContext from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { useReloadableLazyQuery } from '@app/sharedV2/reloadableContext/hooks/useReloadableLazyQuery';

describe('useReloadableLazyQuery', () => {
    const useReloadableContextSpy = vi.spyOn(ReloadableContext, 'useReloadableContext');

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should use "cache-and-network" fetch policy when the query needs to be reloaded', () => {
        const mockExecute = vi.fn();
        const mockLazyQueryHook = vi.fn().mockReturnValue([mockExecute, { loading: false, error: null }]);
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => true,
            markAsReloaded: () => {},
            reloadByKeyType: () => {},
        });

        const { result } = renderHook(() =>
            useReloadableLazyQuery(mockLazyQueryHook, { type: 'test', id: '1' }, { fetchPolicy: 'cache-first' }),
        );

        act(() => {
            result.current[0]();
        });

        expect(mockExecute).toHaveBeenCalledWith({ fetchPolicy: 'cache-and-network' });
    });

    it('should use the default fetch policy when the query does not need to be reloaded', () => {
        const mockExecute = vi.fn();
        const mockLazyQueryHook = vi.fn().mockReturnValue([mockExecute, { loading: false, error: null }]);
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => false,
            markAsReloaded: () => {},
            reloadByKeyType: () => {},
        });

        const { result } = renderHook(() =>
            useReloadableLazyQuery(mockLazyQueryHook, { type: 'test', id: '1' }, { fetchPolicy: 'cache-first' }),
        );

        act(() => {
            result.current[0]();
        });

        expect(mockExecute).toHaveBeenCalledWith({ fetchPolicy: 'cache-first' });
    });

    it('should call the reloaded function when the query is successful', () => {
        const markAsReloadedMock = vi.fn();
        const mockLazyQueryHook = vi.fn().mockReturnValue([vi.fn(), { loading: false, error: null }]);
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => true,
            markAsReloaded: markAsReloadedMock,
            reloadByKeyType: () => {},
        });

        renderHook(() => useReloadableLazyQuery(mockLazyQueryHook, { type: 'test', id: '1' }, {}));

        expect(markAsReloadedMock).toHaveBeenCalledWith('test', '1');
    });

    it('should not call the reloaded function when the query is loading', () => {
        const markAsReloadedMock = vi.fn();
        const mockLazyQueryHook = vi.fn().mockReturnValue([vi.fn(), { loading: true, error: null }]);
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => true,
            markAsReloaded: markAsReloadedMock,
            reloadByKeyType: () => {},
        });

        renderHook(() => useReloadableLazyQuery(mockLazyQueryHook, { type: 'test', id: '1' }, {}));

        expect(markAsReloadedMock).not.toHaveBeenCalled();
    });

    it('should not call the reloaded function when the query has an error', () => {
        const markAsReloadedMock = vi.fn();
        const mockLazyQueryHook = vi
            .fn()
            .mockReturnValue([vi.fn(), { loading: false, error: new Error('test error') }]);
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => true,
            markAsReloaded: markAsReloadedMock,
            reloadByKeyType: () => {},
        });

        renderHook(() => useReloadableLazyQuery(mockLazyQueryHook, { type: 'test', id: '1' }, {}));

        expect(markAsReloadedMock).not.toHaveBeenCalled();
    });
});
