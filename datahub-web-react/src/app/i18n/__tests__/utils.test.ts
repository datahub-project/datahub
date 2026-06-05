import { describe, expect, it } from 'vitest';

import { isSupportedLanguage } from '@app/i18n/utils';

describe('isSupportedLanguage', () => {
    it('returns true for supported languages', () => {
        expect(isSupportedLanguage('en')).toBe(true);
        expect(isSupportedLanguage('de')).toBe(true);
    });

    it('returns false for unsupported languages', () => {
        expect(isSupportedLanguage('unsupported')).toBe(false);
        expect(isSupportedLanguage('')).toBe(false);
        expect(isSupportedLanguage('EN')).toBe(false);
    });
});
