import { afterEach, describe, expect, it, vi } from 'vitest';

import { detectBrowserLanguage, isSupportedLanguage } from '@app/i18n/utils';

describe('isSupportedLanguage', () => {
    it('returns true for supported languages', () => {
        expect(isSupportedLanguage('en')).toBe(true);
        expect(isSupportedLanguage('de')).toBe(true);
        expect(isSupportedLanguage('es')).toBe(true);
    });

    it('returns false for unsupported languages', () => {
        expect(isSupportedLanguage('unsupported')).toBe(false);
        expect(isSupportedLanguage('')).toBe(false);
        expect(isSupportedLanguage('EN')).toBe(false);
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
        stubLanguages(['ja-JP', 'fr-FR', 'de']);
        expect(detectBrowserLanguage()).toBe('fr');
    });

    it('returns undefined when no preferred language is supported', () => {
        stubLanguages(['ja-JP', 'zh-CN']);
        expect(detectBrowserLanguage()).toBeUndefined();
    });

    it('returns undefined when the browser exposes no languages', () => {
        stubLanguages([]);
        expect(detectBrowserLanguage()).toBeUndefined();
    });
});
