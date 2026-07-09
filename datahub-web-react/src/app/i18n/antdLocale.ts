import type { Locale } from 'antd/lib/locale-provider';

import { SupportedLanguage } from '@app/i18n/types';

// antd ships one Locale object per language. Importing them all eagerly pulls every language's
// antd locale data into the main chunk; loading them dynamically keeps all but the active
// language out of the main bundle — each chunk is fetched only when that language is selected.
// Mirrors DAYJS_LOCALE_LOADERS in utils/dayjs.ts. Keep keys in sync with LOCALE_MAP in constants.
const ANTD_LOCALE_LOADERS: Record<SupportedLanguage, () => Promise<{ default: Locale }>> = {
    en: () => import('antd/lib/locale/en_US'),
    de: () => import('antd/lib/locale/de_DE'),
    es: () => import('antd/lib/locale/es_ES'),
    'pt-BR': () => import('antd/lib/locale/pt_BR'),
    fr: () => import('antd/lib/locale/fr_FR'),
    it: () => import('antd/lib/locale/it_IT'),
    sv: () => import('antd/lib/locale/sv_SE'),
};

/**
 * Loads and returns antd's Locale for the given language. The chunk is fetched on demand; callers
 * should render antd's built-in English default until this resolves.
 */
export async function loadAntdLocale(language: SupportedLanguage): Promise<Locale> {
    const { default: locale } = await ANTD_LOCALE_LOADERS[language]();
    return locale;
}
