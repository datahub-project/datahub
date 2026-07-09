export type SupportedLanguage = 'en' | 'de' | 'es' | 'pt-BR' | 'fr' | 'it' | 'sv';

export type LocaleConfig = {
    lang: SupportedLanguage;
    label: string;
    dayjs: string;
};
