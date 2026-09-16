import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { USER_LANGUAGE_STORAGE_KEY } from '@app/shared/hooks/userLanguageStorage';
import { resolveInitialLanguage } from '@src/i18n/resolveInitialLanguage';

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

describe('resolveInitialLanguage', () => {
    beforeEach(() => {
        vi.unstubAllGlobals();
        localStorageMock.clear();
        Object.defineProperty(window, 'localStorage', { value: localStorageMock });
        vi.stubGlobal('navigator', { languages: ['en-US'], language: 'en-US' });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('returns English when i18n is not cached as enabled', () => {
        vi.stubGlobal('navigator', { languages: ['de-DE'], language: 'de-DE' });
        expect(resolveInitialLanguage()).toBe('en');
    });

    it('returns the cached user language when i18n is enabled', () => {
        localStorage.setItem('i18nEnabled', 'true');
        localStorage.setItem(USER_LANGUAGE_STORAGE_KEY, JSON.stringify('ja'));
        expect(resolveInitialLanguage()).toBe('ja');
    });

    it('returns the browser language when i18n is enabled and no user language is cached', () => {
        localStorage.setItem('i18nEnabled', 'true');
        vi.stubGlobal('navigator', { languages: ['fr-FR'], language: 'fr-FR' });
        expect(resolveInitialLanguage()).toBe('fr');
    });
});
