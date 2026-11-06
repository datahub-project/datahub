import type { Locale } from 'antd/es/locale';

import type { SupportedLanguage } from '@src/i18n/supportedLanguages';

export type { SupportedLanguage };

export type LocaleConfig = {
    lang: SupportedLanguage;
    label: string;
    antd: Locale;
    dayjs: string;
};
