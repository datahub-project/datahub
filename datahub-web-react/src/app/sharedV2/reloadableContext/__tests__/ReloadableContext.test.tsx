import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { vi } from 'vitest';

import { ReloadableProvider } from '@app/sharedV2/reloadableContext/ReloadableContext';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';

describe('ReloadableContext', () => {
    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should return the default context values', () => {
        const { result } = renderHook(() => useReloadableContext());

        expect(result.current.shouldBeReloaded('any')).toBe(false);
        expect(result.current.reloaded('any')).toBe(undefined);
        expect(result.current.reloadByKeyType(['any'])).toBe(undefined);
    });

    it('should allow a component to be reloaded', () => {
        const wrapper = ({ children }) => <ReloadableProvider>{children}</ReloadableProvider>;
        const { result } = renderHook(() => useReloadableContext(), { wrapper });

        act(() => {
            result.current.reloaded('testKey');
        });

        expect(result.current.shouldBeReloaded('testKey')).toBe(false);
    });

    it('should indicate a component should be reloaded', () => {
        const wrapper = ({ children }) => <ReloadableProvider>{children}</ReloadableProvider>;
        const { result } = renderHook(() => useReloadableContext(), { wrapper });

        expect(result.current.shouldBeReloaded('testKey')).toBe(true);
    });

    it('should reload components by key type', () => {
        const wrapper = ({ children }) => <ReloadableProvider>{children}</ReloadableProvider>;
        const { result } = renderHook(() => useReloadableContext(), { wrapper });

        act(() => {
            result.current.reloaded('testKey', '1');
            result.current.reloaded('testKey', '2');
        });

        expect(result.current.shouldBeReloaded('testKey', '1')).toBe(false);
        expect(result.current.shouldBeReloaded('testKey', '2')).toBe(false);

        act(() => {
            result.current.reloadByKeyType(['testKey']);
        });

        expect(result.current.shouldBeReloaded('testKey', '1')).toBe(true);
        expect(result.current.shouldBeReloaded('testKey', '2')).toBe(true);
    });

    it('should reload components by key type with a delay', () => {
        vi.useFakeTimers();
        const wrapper = ({ children }) => <ReloadableProvider>{children}</ReloadableProvider>;
        const { result } = renderHook(() => useReloadableContext(), { wrapper });

        act(() => {
            result.current.reloaded('testKey', '1');
        });

        expect(result.current.shouldBeReloaded('testKey', '1')).toBe(false);

        act(() => {
            result.current.reloadByKeyType(['testKey'], 1000);
        });

        expect(result.current.shouldBeReloaded('testKey', '1')).toBe(false);

        act(() => {
            vi.advanceTimersByTime(1000);
        });

        expect(result.current.shouldBeReloaded('testKey', '1')).toBe(true);
        vi.useRealTimers();
    });
});
