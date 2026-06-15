import { SelectOption } from '@components';
import deDE from 'antd/lib/locale/de_DE';
import enUS from 'antd/lib/locale/en_US';
import esES from 'antd/lib/locale/es_ES';
import ptBR from 'antd/lib/locale/pt_BR';

import { LocaleConfig, SupportedLanguage } from '@app/i18n/types';

export const EN_LOCALE_CONFIG: LocaleConfig = {
    lang: 'en',
    antd: enUS,
    dayjs: 'en',
    label: 'English',
};

export const DE_LOCALE_CONFIG: LocaleConfig = {
    lang: 'de',
    antd: deDE,
    dayjs: 'de',
    label: 'Deutsch',
};

export const ES_LOCALE_CONFIG: LocaleConfig = {
    lang: 'es',
    antd: esES,
    dayjs: 'es',
    label: 'Español (Beta)',
};

export const PT_BR_LOCALE_CONFIG: LocaleConfig = {
    lang: 'pt-BR',
    antd: ptBR,
    dayjs: 'pt-br',
    label: 'Português (Brasil)',
};

export const LOCALE_MAP: Record<SupportedLanguage, LocaleConfig> = {
    en: EN_LOCALE_CONFIG,
    de: DE_LOCALE_CONFIG,
    es: ES_LOCALE_CONFIG,
    'pt-BR': PT_BR_LOCALE_CONFIG,
};

export const LANGUAGE_OPTIONS: SelectOption[] = [
    EN_LOCALE_CONFIG,
    DE_LOCALE_CONFIG,
    ES_LOCALE_CONFIG,
    PT_BR_LOCALE_CONFIG,
].map((localeConfig) => ({
    value: localeConfig.lang,
    label: localeConfig.label,
}));

export const DEFAULT_LANGUAGE: SupportedLanguage = 'en';
