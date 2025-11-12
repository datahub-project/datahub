import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import * as ReloadableContext from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { useReloadableQuery } from '@app/sharedV2/reloadableContext/hooks/useReloadableQuery';

describe('useReloadableQuery', () => {
    const useReloadableContextSpy = vi.spyOn(ReloadableContext, 'useReloadableContext');

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should use "cache-and-network" fetch policy when the query needs to be reloaded', () => {
        const mockQueryHook = vi.fn().mockReturnValue({ loading: false, error: null });
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => true,
            markAsReloaded: () => {},
            reloadByKeyType: () => {},
        });

        renderHook(() => useReloadableQuery(mockQueryHook, { type: 'test', id: '1' }, { fetchPolicy: 'cache-first' }));

        expect(mockQueryHook).toHaveBeenCalledWith({ fetchPolicy: 'cache-and-network' });
    });

    it('should use the default fetch policy when the query does not need to be reloaded', () => {
        const mockQueryHook = vi.fn().mockReturnValue({ loading: false, error: null });
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => false,
            markAsReloaded: () => {},
            reloadByKeyType: () => {},
        });

        renderHook(() => useReloadableQuery(mockQueryHook, { type: 'test', id: '1' }, { fetchPolicy: 'cache-first' }));

        expect(mockQueryHook).toHaveBeenCalledWith({ fetchPolicy: 'cache-first' });
    });

    it('should call the reloaded function when the query is successful', () => {
        const markAsReloadedMock = vi.fn();
        const mockQueryHook = vi.fn().mockReturnValue({ loading: false, error: null });
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => true,
            markAsReloaded: markAsReloadedMock,
            reloadByKeyType: () => {},
        });

        renderHook(() => useReloadableQuery(mockQueryHook, { type: 'test', id: '1' }, {}));

        expect(markAsReloadedMock).toHaveBeenCalledWith('test', '1');
    });

    it('should not call the reloaded function when the query is loading', () => {
        const markAsReloadedMock = vi.fn();
        const mockQueryHook = vi.fn().mockReturnValue({ loading: true, error: null });
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => true,
            markAsReloaded: markAsReloadedMock,
            reloadByKeyType: () => {},
        });

        renderHook(() => useReloadableQuery(mockQueryHook, { type: 'test', id: '1' }, {}));

        expect(markAsReloadedMock).not.toHaveBeenCalled();
    });

    it('should not call the reloaded function when the query has an error', () => {
        const markAsReloadedMock = vi.fn();
        const mockQueryHook = vi.fn().mockReturnValue({ loading: false, error: new Error('test error') });
        useReloadableContextSpy.mockReturnValue({
            shouldBeReloaded: () => true,
            markAsReloaded: markAsReloadedMock,
            reloadByKeyType: () => {},
        });

        renderHook(() => useReloadableQuery(mockQueryHook, { type: 'test', id: '1' }, {}));

        expect(markAsReloadedMock).not.toHaveBeenCalled();
    });
});
