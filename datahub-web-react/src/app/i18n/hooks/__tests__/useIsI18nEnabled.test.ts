import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useIsI18nEnabled } from '@app/i18n/hooks/useIsI18nEnabled';
import { useAppConfig } from '@app/useAppConfig';

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

describe('useIsI18nEnabled', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        localStorage.clear();
    });

    it('should return true when i18nEnabled flag is true and config is loaded', () => {
        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: true,
            config: { featureFlags: { i18nEnabled: true } },
        });

        const { result } = renderHook(() => useIsI18nEnabled());

        expect(result.current).toBe(true);
    });

    it('should return false when i18nEnabled flag is false and config is loaded', () => {
        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: true,
            config: { featureFlags: { i18nEnabled: false } },
        });

        const { result } = renderHook(() => useIsI18nEnabled());

        expect(result.current).toBe(false);
    });

    it('should return cached localStorage value when config is not yet loaded', () => {
        localStorage.setItem('i18nEnabled', 'true');

        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: false,
            config: { featureFlags: { i18nEnabled: false } },
        });

        const { result } = renderHook(() => useIsI18nEnabled());

        expect(result.current).toBe(true);
    });

    it('should return false when config is not loaded and localStorage has no value', () => {
        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: false,
            config: { featureFlags: {} },
        });

        const { result } = renderHook(() => useIsI18nEnabled());

        expect(result.current).toBe(false);
    });

    it('should persist the flag value to localStorage when config is loaded', () => {
        (useAppConfig as ReturnType<typeof vi.fn>).mockReturnValue({
            loaded: true,
            config: { featureFlags: { i18nEnabled: true } },
        });

        renderHook(() => useIsI18nEnabled());

        expect(localStorage.getItem('i18nEnabled')).toBe('true');
    });
});
