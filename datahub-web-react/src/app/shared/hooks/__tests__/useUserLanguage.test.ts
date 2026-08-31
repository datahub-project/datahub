import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { useUserLanguage } from '@app/shared/hooks/useUserLanguage';

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

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

const mockUserContext = (loaded: boolean, language?: string | null) => {
    (useUserContext as ReturnType<typeof vi.fn>).mockReturnValue({
        loaded,
        user: language !== undefined ? { settings: { locale: { language } } } : null,
    });
};

describe('useUserLanguage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        localStorage.clear();
    });

    it('should return language from user context when loaded', () => {
        mockUserContext(true, 'en');

        const { result } = renderHook(() => useUserLanguage());

        expect(result.current).toBe('en');
    });

    it('should return null when loaded but no language is set', () => {
        mockUserContext(true, null);

        const { result } = renderHook(() => useUserLanguage());

        expect(result.current).toBeNull();
    });

    it('should persist language to localStorage when loaded', () => {
        mockUserContext(true, 'de');

        renderHook(() => useUserLanguage());

        expect(localStorage.getItem('userLanguage')).toBe('"de"');
    });

    it('should persist null to localStorage when language is not set', () => {
        mockUserContext(true, null);

        renderHook(() => useUserLanguage());

        expect(localStorage.getItem('userLanguage')).toBe('null');
    });

    it('should not write to localStorage if value has not changed', () => {
        localStorage.setItem('userLanguage', '"en"');
        mockUserContext(true, 'en');

        const setItemSpy = vi.spyOn(localStorage, 'setItem');
        renderHook(() => useUserLanguage());

        expect(setItemSpy).not.toHaveBeenCalled();
    });

    it('should return cached language from localStorage when not loaded', () => {
        localStorage.setItem('userLanguage', '"de"');
        mockUserContext(false);

        const { result } = renderHook(() => useUserLanguage());

        expect(result.current).toBe('de');
    });

    it('should return cached null from localStorage when not loaded', () => {
        localStorage.setItem('userLanguage', 'null');
        mockUserContext(false);

        const { result } = renderHook(() => useUserLanguage());

        expect(result.current).toBeNull();
    });

    it('should return undefined when not loaded and localStorage has no value', () => {
        mockUserContext(false);

        const { result } = renderHook(() => useUserLanguage());

        expect(result.current).toBeUndefined();
    });
});
