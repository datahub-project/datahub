import { SelectOption } from '@components';

import { LocaleConfig, SupportedLanguage } from '@app/i18n/types';

export const EN_LOCALE_CONFIG: LocaleConfig = {
    lang: 'en',
    dayjs: 'en',
    label: 'English',
};

export const DE_LOCALE_CONFIG: LocaleConfig = {
    lang: 'de',
    dayjs: 'de',
    label: 'Deutsch',
};

export const ES_LOCALE_CONFIG: LocaleConfig = {
    lang: 'es',
    dayjs: 'es',
    label: 'Español (Beta)',
};

export const PT_BR_LOCALE_CONFIG: LocaleConfig = {
    lang: 'pt-BR',
    dayjs: 'pt-br',
    label: 'Português (Brasil) (Beta)',
};

export const FR_LOCALE_CONFIG: LocaleConfig = {
    lang: 'fr',
    dayjs: 'fr',
    label: 'Français (Beta)',
};

export const IT_LOCALE_CONFIG: LocaleConfig = {
    lang: 'it',
    dayjs: 'it',
    label: 'Italiano (Beta)',
};

export const SV_LOCALE_CONFIG: LocaleConfig = {
    lang: 'sv',
    dayjs: 'sv',
    label: 'Svenska (Beta)',
};

export const LOCALE_MAP: Record<SupportedLanguage, LocaleConfig> = {
    en: EN_LOCALE_CONFIG,
    de: DE_LOCALE_CONFIG,
    es: ES_LOCALE_CONFIG,
    'pt-BR': PT_BR_LOCALE_CONFIG,
    fr: FR_LOCALE_CONFIG,
    it: IT_LOCALE_CONFIG,
    sv: SV_LOCALE_CONFIG,
};

export const LANGUAGE_OPTIONS: SelectOption[] = [
    EN_LOCALE_CONFIG,
    DE_LOCALE_CONFIG,
    ES_LOCALE_CONFIG,
    PT_BR_LOCALE_CONFIG,
    FR_LOCALE_CONFIG,
    IT_LOCALE_CONFIG,
    SV_LOCALE_CONFIG,
].map((localeConfig) => ({
    value: localeConfig.lang,
    label: localeConfig.label,
}));

export const DEFAULT_LANGUAGE: SupportedLanguage = 'en';
