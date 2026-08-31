import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { loadFromLocalStorage, useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';
import { useAppConfig } from '@app/useAppConfig';

// Mocks
const localStorageMock = (() => {
    let store: Record<string, string> = {};
    return {
        getItem: (key: string) => store[key] ?? null,
        setItem: (key: string, value: string) => {
            store[key] = value;
        },
        clear: () => {
            store = {};
        },
    };
})();
Object.defineProperty(window, 'localStorage', { value: localStorageMock });

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

describe('useFeatureFlag', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        localStorage.clear();
    });

    it('should return flag value from appConfig and set localStorage when config loaded', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: { featureFlags: { myFlag: true } },
        });

        const { result } = renderHook(() => useFeatureFlag('myFlag'));

        expect(result.current).toBe(true);
        expect(localStorage.getItem('myFlag')).toBe('true');
    });

    it('should not set localStorage if value is unchanged', () => {
        localStorage.setItem('myFlag', 'true');

        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: { featureFlags: { myFlag: true } },
        });

        const setItemSpy = vi.spyOn(localStorage, 'setItem');

        renderHook(() => useFeatureFlag('myFlag'));

        expect(setItemSpy).not.toHaveBeenCalled();
    });

    it('should return value from localStorage if config not loaded', () => {
        localStorage.setItem('myFlag', 'true');

        (useAppConfig as any).mockReturnValue({
            loaded: false,
            config: { featureFlags: { myFlag: false } },
        });

        const { result } = renderHook(() => useFeatureFlag('myFlag'));

        expect(result.current).toBe(true);
    });

    it('should return false if localStorage has no value and config not loaded', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: false,
            config: { featureFlags: {} },
        });

        const { result } = renderHook(() => useFeatureFlag('myFlag'));

        expect(result.current).toBe(false);
    });

    it('should return false if feature flag is undefined in loaded config', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: { featureFlags: {} },
        });

        const { result } = renderHook(() => useFeatureFlag('myFlag'));
        expect(result.current).toBe(undefined);
    });

    it('should store false value in localStorage if feature flag is false and config loaded', () => {
        (useAppConfig as any).mockReturnValue({
            loaded: true,
            config: { featureFlags: { myFlag: false } },
        });

        renderHook(() => useFeatureFlag('myFlag'));
        expect(localStorage.getItem('myFlag')).toBe('false');
    });
});

describe('loadFromLocalStorage', () => {
    beforeEach(() => {
        localStorage.clear();
    });

    it('should return true if localStorage value is true', () => {
        localStorage.setItem('someFlag', 'true');
        expect(loadFromLocalStorage('someFlag')).toBe(true);
    });

    it('should return false if localStorage value is false', () => {
        localStorage.setItem('someFlag', 'false');
        expect(loadFromLocalStorage('someFlag')).toBe(false);
    });

    it('should return false if localStorage has no key', () => {
        expect(loadFromLocalStorage('someFlag')).toBe(false);
    });
});
