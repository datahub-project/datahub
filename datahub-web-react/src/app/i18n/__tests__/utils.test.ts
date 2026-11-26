import { afterEach, describe, expect, it, vi } from 'vitest';

import { LOCALE_MAP } from '@app/i18n/constants';
import {
    detectBrowserLanguage,
    getServedLanguage,
    isSupportedLanguage,
    pickEffectiveLanguage,
    resolveEffectiveLanguage,
} from '@app/i18n/utils';
import { SUPPORTED_LANGUAGES } from '@src/i18n/supportedLanguages';

describe('isSupportedLanguage', () => {
    it('returns true for supported languages', () => {
        expect(isSupportedLanguage('en')).toBe(true);
        expect(isSupportedLanguage('de')).toBe(true);
        expect(isSupportedLanguage('es')).toBe(true);
        expect(isSupportedLanguage('zh-CN')).toBe(true);
        expect(isSupportedLanguage('zh-TW')).toBe(true);
    });

    it('returns false for unsupported languages', () => {
        expect(isSupportedLanguage('unsupported')).toBe(false);
        expect(isSupportedLanguage('')).toBe(false);
        expect(isSupportedLanguage('EN')).toBe(false);
    });

    it('stays in sync with LOCALE_MAP', () => {
        expect([...SUPPORTED_LANGUAGES].sort()).toEqual(Object.keys(LOCALE_MAP).sort());
    });
});

describe('pickEffectiveLanguage', () => {
    it('returns the default language when i18n is disabled', () => {
        expect(
            pickEffectiveLanguage({
                i18nEnabled: false,
                userLanguage: 'de',
                browserLanguage: 'fr',
            }),
        ).toBe('en');
    });

    it('prefers an explicit supported user language', () => {
        expect(
            pickEffectiveLanguage({
                i18nEnabled: true,
                userLanguage: 'de',
                browserLanguage: 'fr',
            }),
        ).toBe('de');
    });

    it('uses the browser language when there is no supported user preference', () => {
        expect(
            pickEffectiveLanguage({
                i18nEnabled: true,
                userLanguage: null,
                browserLanguage: 'ja',
            }),
        ).toBe('ja');
    });

    it('falls back to the default when neither user nor browser language is supported', () => {
        expect(
            pickEffectiveLanguage({
                i18nEnabled: true,
                userLanguage: 'unsupported',
                browserLanguage: undefined,
            }),
        ).toBe('en');
    });
});

describe('detectBrowserLanguage', () => {
    afterEach(() => {
        vi.unstubAllGlobals();
    });

    function stubLanguages(languages: string[]) {
        vi.stubGlobal('navigator', { languages, language: languages[0] });
    }

    it('matches an exact supported locale', () => {
        stubLanguages(['sv']);
        expect(detectBrowserLanguage()).toBe('sv');
    });

    it('folds a region variant to its base language', () => {
        stubLanguages(['de-DE', 'de']);
        expect(detectBrowserLanguage()).toBe('de');
        stubLanguages(['fr-CA']);
        expect(detectBrowserLanguage()).toBe('fr');
    });

    it('maps any Portuguese variant to pt-BR', () => {
        stubLanguages(['pt-PT']);
        expect(detectBrowserLanguage()).toBe('pt-BR');
        stubLanguages(['pt-BR']);
        expect(detectBrowserLanguage()).toBe('pt-BR');
    });

    it('returns the first supported language in preference order', () => {
        // Use unsupported tags first so preference order is exercised past them.
        stubLanguages(['ko-KR', 'th-TH', 'fr-FR', 'de']);
        expect(detectBrowserLanguage()).toBe('fr');
    });

    it('maps Japanese region variants to ja', () => {
        stubLanguages(['ja-JP']);
        expect(detectBrowserLanguage()).toBe('ja');
        stubLanguages(['ja']);
        expect(detectBrowserLanguage()).toBe('ja');
    });

    it('maps Traditional Chinese tags to zh-TW', () => {
        stubLanguages(['zh-TW']);
        expect(detectBrowserLanguage()).toBe('zh-TW');
        stubLanguages(['zh-Hant']);
        expect(detectBrowserLanguage()).toBe('zh-TW');
        stubLanguages(['zh-HK']);
        expect(detectBrowserLanguage()).toBe('zh-TW');
        stubLanguages(['zh-MO']);
        expect(detectBrowserLanguage()).toBe('zh-TW');
    });

    it('maps Simplified Chinese tags to zh-CN', () => {
        stubLanguages(['zh-CN']);
        expect(detectBrowserLanguage()).toBe('zh-CN');
        stubLanguages(['zh-Hans']);
        expect(detectBrowserLanguage()).toBe('zh-CN');
        stubLanguages(['zh']);
        expect(detectBrowserLanguage()).toBe('zh-CN');
        stubLanguages(['zh-SG']);
        expect(detectBrowserLanguage()).toBe('zh-CN');
    });

    it('returns undefined when no preferred language is supported', () => {
        stubLanguages(['ko-KR', 'th-TH']);
        expect(detectBrowserLanguage()).toBeUndefined();
    });

    it('returns undefined when the browser exposes no languages', () => {
        stubLanguages([]);
        expect(detectBrowserLanguage()).toBeUndefined();
    });
});

describe('resolveEffectiveLanguage', () => {
    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('returns the default when i18n is disabled', () => {
        expect(resolveEffectiveLanguage(false, 'de')).toBe('en');
    });

    it('prefers an explicit supported user language', () => {
        vi.stubGlobal('navigator', { languages: ['fr'], language: 'fr' });
        expect(resolveEffectiveLanguage(true, 'pt-BR')).toBe('pt-BR');
    });

    it('falls back to the browser language when the user has no preference', () => {
        vi.stubGlobal('navigator', { languages: ['de-DE'], language: 'de-DE' });
        expect(resolveEffectiveLanguage(true, null)).toBe('de');
    });
});

describe('getServedLanguage', () => {
    afterEach(() => {
        vi.unstubAllGlobals();
        localStorage.removeItem('i18nEnabled');
        localStorage.removeItem('userLanguage');
    });

    it('reads the served locale from localStorage and the browser', () => {
        localStorage.setItem('i18nEnabled', 'true');
        localStorage.setItem('userLanguage', 'null');
        vi.stubGlobal('navigator', { languages: ['ja-JP'], language: 'ja-JP' });

        expect(getServedLanguage()).toBe('ja');
    });
});
