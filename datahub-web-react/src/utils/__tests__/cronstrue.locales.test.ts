import i18next from 'i18next';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { LOCALE_MAP } from '@app/i18n/constants';
import { cronToString } from '@utils/cronstrue';

// Unlike cronstrue.test.ts, this suite does NOT mock cronstrue: it exercises the real
// library so the side-effect `import 'cronstrue/locales/*'` lines in cronstrue.ts actually
// run. cronstrue silently falls back to English (with a console warning) for a locale it
// hasn't loaded, so a language added to LOCALE_MAP but never wired into cronstrue.ts would
// render its cron schedules in English. This guard iterates LOCALE_MAP — the single source
// of truth for supported languages — so a new language is covered automatically.
describe('cronstrue locale registration', () => {
    // Rich enough that every registered locale differs from English (it translates the
    // "At"/"only on"/weekday tokens). ponytail: relies on translation ≠ English for this
    // expression; if a future cronstrue locale ever renders it identically to English,
    // switch the check to spy on cronstrue's "could not be found" fallback warning instead.
    const EXPRESSION = '0 9 * * 1';
    const originalLanguage = i18next.language;

    const setLanguage = (lang: string) => {
        (i18next as any).language = lang;
    };

    let englishOutput: string;

    beforeAll(() => {
        setLanguage('en');
        englishOutput = cronToString(EXPRESSION);
    });

    afterAll(() => setLanguage(originalLanguage));

    const nonEnglishLanguages = Object.keys(LOCALE_MAP).filter((lang) => lang !== 'en');

    it.each(nonEnglishLanguages)('registers the cronstrue locale for %s', (lang) => {
        setLanguage(lang);
        expect(cronToString(EXPRESSION)).not.toBe(englishOutput);
    });
});
