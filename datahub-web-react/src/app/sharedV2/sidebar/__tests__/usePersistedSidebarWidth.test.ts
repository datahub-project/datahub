import { act } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import usePersistedSidebarWidth from '@app/sharedV2/sidebar/usePersistedSidebarWidth';
import { renderHook } from '@utils/test-utils/renderHook';

const STORAGE_KEY = 'test.sidebarWidth';
const OPTIONS = { storageKey: STORAGE_KEY, minWidth: 320, maxWidth: 800, defaultRatio: 0.3 };

describe('usePersistedSidebarWidth', () => {
    afterEach(() => {
        localStorage.clear();
        vi.restoreAllMocks();
    });

    it('clamps and saves a dragged width', () => {
        const { result } = renderHook(() => usePersistedSidebarWidth(OPTIONS));

        act(() => result.current.setWidth(5000));
        expect(result.current.width).toBe(800);
        expect(localStorage.getItem(STORAGE_KEY)).toBe('800');

        act(() => result.current.setWidth(10));
        expect(result.current.width).toBe(320);
    });

    it('restores a saved width and re-clamps it on read', () => {
        localStorage.setItem(STORAGE_KEY, '450');
        expect(renderHook(() => usePersistedSidebarWidth(OPTIONS)).result.current.width).toBe(450);

        localStorage.setItem(STORAGE_KEY, '2000');
        expect(renderHook(() => usePersistedSidebarWidth(OPTIONS)).result.current.width).toBe(800);
    });

    it('falls back to the default width when storage is unusable', () => {
        localStorage.setItem(STORAGE_KEY, 'not-a-number');
        const expected = Math.min(Math.max(window.innerWidth * 0.3, 320), 800);
        expect(renderHook(() => usePersistedSidebarWidth(OPTIONS)).result.current.width).toBe(expected);

        vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
            throw new Error('blocked');
        });
        vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
            throw new Error('blocked');
        });
        const { result } = renderHook(() => usePersistedSidebarWidth(OPTIONS));
        expect(result.current.width).toBe(expected);
        act(() => result.current.setWidth(500));
        expect(result.current.width).toBe(500);
    });

    it('keeps the width in memory only when no storage key is given', () => {
        const setItem = vi.spyOn(Storage.prototype, 'setItem');
        const { result } = renderHook(() => usePersistedSidebarWidth({ ...OPTIONS, storageKey: undefined }));

        act(() => result.current.setWidth(500));

        expect(result.current.width).toBe(500);
        expect(setItem).not.toHaveBeenCalled();
    });
});
