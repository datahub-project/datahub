import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';
import { THEME_DARK_MODE_FLAG, loadIsDarkMode, useIsDarkMode } from '@app/theme/useIsDarkMode';

vi.mock('@app/sharedV2/hooks/useFeatureFlag', async () => {
    const actual = await vi.importActual<typeof import('@app/sharedV2/hooks/useFeatureFlag')>(
        '@app/sharedV2/hooks/useFeatureFlag',
    );
    return {
        ...actual,
        useFeatureFlag: vi.fn(() => true),
    };
});

// Helpers

const DARK_MODE_KEY = 'isDarkModeEnabled';
const DARK_MODE_CHANGE_EVENT = 'datahub-darkmode-change';

// Setup

beforeEach(() => {
    localStorage.clear();
    vi.mocked(useFeatureFlag).mockReturnValue(true);
});

afterEach(() => {
    localStorage.clear();
});

// loadIsDarkMode

describe('loadIsDarkMode', () => {
    it('returns false when localStorage has no entry', () => {
        expect(loadIsDarkMode()).toBe(false);
    });

    it('returns false when dark mode is preferred but the feature flag is off', () => {
        localStorage.setItem(DARK_MODE_KEY, 'true');
        expect(loadIsDarkMode()).toBe(false);
    });

    it('returns true when localStorage value is "true" and the feature flag is on', () => {
        localStorage.setItem(THEME_DARK_MODE_FLAG, 'true');
        localStorage.setItem(DARK_MODE_KEY, 'true');
        expect(loadIsDarkMode()).toBe(true);
    });

    it('returns false when localStorage value is "false"', () => {
        localStorage.setItem(THEME_DARK_MODE_FLAG, 'true');
        localStorage.setItem(DARK_MODE_KEY, 'false');
        expect(loadIsDarkMode()).toBe(false);
    });
});

// Initial state

describe('useIsDarkMode – initial state', () => {
    it('defaults to false when localStorage is empty', () => {
        const { result } = renderHook(() => useIsDarkMode());
        expect(result.current[0]).toBe(false);
    });

    it('reads true from localStorage on mount', () => {
        localStorage.setItem(DARK_MODE_KEY, 'true');
        const { result } = renderHook(() => useIsDarkMode());
        expect(result.current[0]).toBe(true);
    });

    it('reads false from localStorage on mount', () => {
        localStorage.setItem(DARK_MODE_KEY, 'false');
        const { result } = renderHook(() => useIsDarkMode());
        expect(result.current[0]).toBe(false);
    });
});

// toggleDarkMode

describe('useIsDarkMode – toggleDarkMode', () => {
    it('toggles from false to true', () => {
        const { result } = renderHook(() => useIsDarkMode());
        act(() => result.current[1]());
        expect(result.current[0]).toBe(true);
    });

    it('toggles from true to false', () => {
        localStorage.setItem(DARK_MODE_KEY, 'true');
        const { result } = renderHook(() => useIsDarkMode());
        act(() => result.current[1]());
        expect(result.current[0]).toBe(false);
    });

    it('toggles back and forth correctly', () => {
        const { result } = renderHook(() => useIsDarkMode());
        act(() => result.current[1]());
        expect(result.current[0]).toBe(true);
        act(() => result.current[1]());
        expect(result.current[0]).toBe(false);
        act(() => result.current[1]());
        expect(result.current[0]).toBe(true);
    });

    it('persists the new value to localStorage after toggle', () => {
        const { result } = renderHook(() => useIsDarkMode());
        act(() => result.current[1]());
        expect(localStorage.getItem(DARK_MODE_KEY)).toBe('true');
    });

    it('persists false to localStorage when toggling back to light', () => {
        localStorage.setItem(DARK_MODE_KEY, 'true');
        const { result } = renderHook(() => useIsDarkMode());
        act(() => result.current[1]());
        expect(localStorage.getItem(DARK_MODE_KEY)).toBe('false');
    });

    it('dispatches the custom window event on toggle', () => {
        const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
        const { result } = renderHook(() => useIsDarkMode());
        act(() => result.current[1]());
        expect(dispatchSpy).toHaveBeenCalledOnce();
        expect(dispatchSpy.mock.calls[0][0]).toBeInstanceOf(Event);
        expect((dispatchSpy.mock.calls[0][0] as Event).type).toBe(DARK_MODE_CHANGE_EVENT);
    });

    it('returns a stable toggle reference across renders', () => {
        const { result, rerender } = renderHook(() => useIsDarkMode());
        const first = result.current[1];
        rerender();
        expect(result.current[1]).toBe(first);
    });
});

// Cross-instance sync via custom event

describe('useIsDarkMode – cross-instance sync', () => {
    it('syncs a second hook instance when the first toggles', () => {
        const hook1 = renderHook(() => useIsDarkMode());
        const hook2 = renderHook(() => useIsDarkMode());

        act(() => hook1.result.current[1]());

        // hook2 should have picked up the change via the window event
        expect(hook2.result.current[0]).toBe(true);
    });

    it('syncs multiple instances simultaneously', () => {
        const hooks = [
            renderHook(() => useIsDarkMode()),
            renderHook(() => useIsDarkMode()),
            renderHook(() => useIsDarkMode()),
        ];

        act(() => hooks[0].result.current[1]());

        hooks.forEach((h) => expect(h.result.current[0]).toBe(true));
    });

    it('reflects localStorage value when sync event fires', () => {
        const { result } = renderHook(() => useIsDarkMode());

        // Manually update localStorage and fire the event (simulates another instance toggling)
        act(() => {
            localStorage.setItem(DARK_MODE_KEY, 'true');
            window.dispatchEvent(new Event(DARK_MODE_CHANGE_EVENT));
        });

        expect(result.current[0]).toBe(true);
    });

    it('removes the sync event listener on unmount', () => {
        const removeEventListenerSpy = vi.spyOn(window, 'removeEventListener');
        const { unmount } = renderHook(() => useIsDarkMode());
        unmount();
        expect(removeEventListenerSpy).toHaveBeenCalledWith(DARK_MODE_CHANGE_EVENT, expect.any(Function));
    });
});

describe('useIsDarkMode – feature flag', () => {
    it('stays in light mode when the feature flag is off even if preference is dark', () => {
        vi.mocked(useFeatureFlag).mockReturnValue(false);
        localStorage.setItem(DARK_MODE_KEY, 'true');

        const { result } = renderHook(() => useIsDarkMode());

        expect(result.current[0]).toBe(false);
    });

    it('does not persist a toggle when the feature flag is off', () => {
        vi.mocked(useFeatureFlag).mockReturnValue(false);
        const { result } = renderHook(() => useIsDarkMode());

        act(() => result.current[1]());

        expect(result.current[0]).toBe(false);
        expect(localStorage.getItem(DARK_MODE_KEY)).toBeNull();
    });
});
