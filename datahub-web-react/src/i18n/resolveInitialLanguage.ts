import { readCachedUserLanguage } from '@app/shared/hooks/userLanguageStorage';

import { detectBrowserLanguage, pickEffectiveLanguage, SupportedLanguage } from '@src/i18n/supportedLanguages';

const I18N_ENABLED_FLAG_KEY = 'i18nEnabled';

function readCachedI18nEnabled(): boolean {
    return localStorage.getItem(I18N_ENABLED_FLAG_KEY) === 'true';
}

/**
 * Language to load at i18n init, before React/app config is available. Mirrors
 * `useEffectiveLanguage` using the same localStorage caches that hook writes after the first load.
 */
export function resolveInitialLanguage(): SupportedLanguage {
    return pickEffectiveLanguage({
        i18nEnabled: readCachedI18nEnabled(),
        userLanguage: readCachedUserLanguage(),
        browserLanguage: detectBrowserLanguage(),
    });
}
