import { SelectOption } from '@components';
import deDE from 'antd/lib/locale/de_DE';
import enUS from 'antd/lib/locale/en_US';
import esES from 'antd/lib/locale/es_ES';
import fiFI from 'antd/lib/locale/fi_FI';
import frFR from 'antd/lib/locale/fr_FR';
import huHU from 'antd/lib/locale/hu_HU';
import itIT from 'antd/lib/locale/it_IT';
import jaJP from 'antd/lib/locale/ja_JP';
import nbNO from 'antd/lib/locale/nb_NO';
import ptBR from 'antd/lib/locale/pt_BR';
import ruRU from 'antd/lib/locale/ru_RU';
import svSE from 'antd/lib/locale/sv_SE';
import zhCN from 'antd/lib/locale/zh_CN';
import zhTW from 'antd/lib/locale/zh_TW';

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
    label: 'Português (Brasil) (Beta)',
};

export const FR_LOCALE_CONFIG: LocaleConfig = {
    lang: 'fr',
    antd: frFR,
    dayjs: 'fr',
    label: 'Français (Beta)',
};

export const IT_LOCALE_CONFIG: LocaleConfig = {
    lang: 'it',
    antd: itIT,
    dayjs: 'it',
    label: 'Italiano (Beta)',
};

export const NB_LOCALE_CONFIG: LocaleConfig = {
    lang: 'nb',
    antd: nbNO,
    dayjs: 'nb',
    label: 'Norsk bokmål (Beta)',
};

export const SV_LOCALE_CONFIG: LocaleConfig = {
    lang: 'sv',
    antd: svSE,
    dayjs: 'sv',
    label: 'Svenska (Beta)',
};

export const HU_LOCALE_CONFIG: LocaleConfig = {
    lang: 'hu',
    antd: huHU,
    dayjs: 'hu',
    label: 'Magyar (Beta)',
};

export const FI_LOCALE_CONFIG: LocaleConfig = {
    lang: 'fi',
    antd: fiFI,
    dayjs: 'fi',
    label: 'Suomi (Beta)',
};

export const JA_LOCALE_CONFIG: LocaleConfig = {
    lang: 'ja',
    antd: jaJP,
    dayjs: 'ja',
    label: '日本語 (Beta)',
};

export const ZH_CN_LOCALE_CONFIG: LocaleConfig = {
    lang: 'zh-CN',
    antd: zhCN,
    dayjs: 'zh-cn',
    label: '简体中文',
};

export const ZH_TW_LOCALE_CONFIG: LocaleConfig = {
    lang: 'zh-TW',
    antd: zhTW,
    dayjs: 'zh-tw',
    label: '繁體中文 (Beta)',
};

export const RU_LOCALE_CONFIG: LocaleConfig = {
    lang: 'ru',
    antd: ruRU,
    dayjs: 'ru',
    label: 'Русский (Beta)',
};

export const LOCALE_MAP: Record<SupportedLanguage, LocaleConfig> = {
    en: EN_LOCALE_CONFIG,
    de: DE_LOCALE_CONFIG,
    es: ES_LOCALE_CONFIG,
    'pt-BR': PT_BR_LOCALE_CONFIG,
    fr: FR_LOCALE_CONFIG,
    it: IT_LOCALE_CONFIG,
    nb: NB_LOCALE_CONFIG,
    sv: SV_LOCALE_CONFIG,
    hu: HU_LOCALE_CONFIG,
    fi: FI_LOCALE_CONFIG,
    ja: JA_LOCALE_CONFIG,
    'zh-CN': ZH_CN_LOCALE_CONFIG,
    'zh-TW': ZH_TW_LOCALE_CONFIG,
    ru: RU_LOCALE_CONFIG,
};

// Derived from LOCALE_MAP so a new language shows up in the picker automatically — no hand-kept
// list to drift. Object key order is insertion order, matching LOCALE_MAP's declared order.
export const LANGUAGE_OPTIONS: SelectOption[] = Object.values(LOCALE_MAP).map((localeConfig) => ({
    value: localeConfig.lang,
    label: localeConfig.label,
}));

export { DEFAULT_LANGUAGE } from '@src/i18n/supportedLanguages';
