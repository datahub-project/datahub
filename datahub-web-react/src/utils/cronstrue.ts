import cronstrue from 'cronstrue';
import 'cronstrue/locales/de';
import i18next from 'i18next';

import { SupportedLanguage } from '@app/i18n/types';

type CronOptions = Exclude<Parameters<typeof cronstrue.toString>[1], undefined>;

// Word that cronstrue prepends to time in each language (e.g. "At 9:00 AM" / "Um 9:00 Uhr").
const TIME_PREFIXES: Partial<Record<SupportedLanguage, RegExp>> = {
    en: /^at /i,
    de: /^um /i,
};

export function cronToString(expression: string, options?: Omit<CronOptions, 'locale'>): string {
    return cronstrue.toString(expression, { ...options, locale: i18next.language });
}

export function removeTimePrefix(cronString: string): string {
    const prefix = TIME_PREFIXES[i18next.language];
    if (prefix) {
        return cronString.replace(prefix, '');
    }
    return cronString;
}
