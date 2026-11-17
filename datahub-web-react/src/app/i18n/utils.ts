import { readCachedUserLanguage } from '@app/sharedV2/hooks/userLanguageStorage';
import {
    SupportedLanguage,
    detectBrowserLanguage,
    isSupportedLanguage,
    pickEffectiveLanguage,
} from '@src/i18n/supportedLanguages';

export { detectBrowserLanguage, isSupportedLanguage, pickEffectiveLanguage };

const I18N_ENABLED_STORAGE_KEY = 'i18nEnabled';

/**
 * Locale the UI will actually render — same rules as `useEffectiveLanguage`.
 * Browser preference (`navigator.language`) is a different signal.
 */
export function resolveEffectiveLanguage(
    i18nEnabled: boolean,
    userLanguage: string | null | undefined,
    defaultLanguage?: SupportedLanguage,
): SupportedLanguage {
    return pickEffectiveLanguage({
        i18nEnabled,
        userLanguage,
        browserLanguage: detectBrowserLanguage(),
        defaultLanguage,
    });
}

function loadUserLanguageFromStorage(): string | null | undefined {
    // getServedLanguage runs on every analytics event, so a malformed cached value must degrade to
    // "no preference" rather than throwing inside the analytics path.
    try {
        return readCachedUserLanguage();
    } catch {
        return undefined;
    }
}

/** Served UI locale for analytics (and anything else outside React). */
export function getServedLanguage(): SupportedLanguage {
    return resolveEffectiveLanguage(
        localStorage.getItem(I18N_ENABLED_STORAGE_KEY) === 'true',
        loadUserLanguageFromStorage(),
    );
}
