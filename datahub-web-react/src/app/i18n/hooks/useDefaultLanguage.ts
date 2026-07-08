import { DEFAULT_LANGUAGE } from '@app/i18n/constants';
import { SupportedLanguage } from '@app/i18n/types';
import { isSupportedLanguage } from '@app/i18n/utils';
import { useAppConfig } from '@app/useAppConfig';

/**
 * The UI-wide default language for users who have not selected a supported language of their own.
 * Sourced from the `I18N_DEFAULT_LOCALE` env var (surfaced via appConfig). Falls back to English
 * when the configured value is unset or not a supported locale — GMS logs a warning for an
 * unsupported value, and this is the authoritative client-side guard.
 */
export function useDefaultLanguage(): SupportedLanguage {
    const configuredLocale = useAppConfig().config?.featureFlags?.i18nDefaultLocale;
    return configuredLocale && isSupportedLanguage(configuredLocale) ? configuredLocale : DEFAULT_LANGUAGE;
}
