import cronstrue from 'cronstrue';
import 'cronstrue/locales/de';
import 'cronstrue/locales/es';
import 'cronstrue/locales/fi';
import 'cronstrue/locales/fr';
import 'cronstrue/locales/hu';
import 'cronstrue/locales/it';
import 'cronstrue/locales/ja';
import 'cronstrue/locales/nb';
import 'cronstrue/locales/pt_BR';
import 'cronstrue/locales/sv';
import i18next from 'i18next';

import { SupportedLanguage } from '@app/i18n/types';

type CronOptions = Exclude<Parameters<typeof cronstrue.toString>[1], undefined>;

// cronstrue names its locale bundles with an underscore where BCP-47 uses a hyphen.
const CRONSTRUE_LOCALE: Partial<Record<SupportedLanguage, string>> = {
    'pt-BR': 'pt_BR',
};

// Word that cronstrue prepends to time in each language (e.g. "At 9:00 AM" / "Um 9:00 Uhr").
const TIME_PREFIXES: Partial<Record<SupportedLanguage, RegExp>> = {
    en: /^at /i,
    de: /^um /i,
    es: /^a las /i,
    fi: /^klo /i,
    fr: /^à /i,
    hu: /^ekkor: /i,
    it: /^alle /i,
    nb: /^kl\.? ?/i,
    'pt-BR': /^às /i,
    sv: /^kl\.? ?/i,
};

export function cronToString(expression: string, options?: Omit<CronOptions, 'locale'>): string {
    const lang = i18next.language as SupportedLanguage;
    return cronstrue.toString(expression, { ...options, locale: CRONSTRUE_LOCALE[lang] ?? lang });
}

export function removeTimePrefix(cronString: string): string {
    const prefix = TIME_PREFIXES[i18next.language];
    if (prefix) {
        return cronString.replace(prefix, '');
    }
    return cronString;
}
