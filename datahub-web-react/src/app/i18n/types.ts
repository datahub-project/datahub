import { Locale } from 'antd/lib/locale-provider';

export type SupportedLanguage = 'en' | 'de' | 'pt-BR';

export type LocaleConfig = {
    lang: SupportedLanguage;
    label: string;
    antd: Locale;
    dayjs: string;
};
