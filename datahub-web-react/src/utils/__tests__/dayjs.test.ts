import { describe, expect, it } from 'vitest';

import { LOCALE_MAP } from '@app/i18n/constants';
import { DAYJS_LOCALE_LOADERS } from '@utils/dayjs';

// LOCALE_MAP is the source of truth for supported languages. Every non-English locale must
// have a dayjs loader keyed by its `dayjs` code, or its date/time formatting silently falls
// back to English. `en` is dayjs's built-in default and intentionally has no loader.
describe('dayjs locale registration', () => {
    const nonEnglishConfigs = Object.values(LOCALE_MAP).filter((config) => config.dayjs !== 'en');

    it.each(nonEnglishConfigs)('registers a dayjs loader for $lang ($dayjs)', (config) => {
        expect(Object.keys(DAYJS_LOCALE_LOADERS)).toContain(config.dayjs);
    });
});
