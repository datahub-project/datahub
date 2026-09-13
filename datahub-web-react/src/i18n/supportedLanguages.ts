export const DEFAULT_LANGUAGE = 'en' as const;

export const SUPPORTED_LANGUAGES = [
    'en',
    'de',
    'es',
    'pt-BR',
    'fr',
    'it',
    'nb',
    'sv',
    'hu',
    'fi',
    'ja',
    'zh-CN',
] as const;

export type SupportedLanguage = (typeof SUPPORTED_LANGUAGES)[number];

const SUPPORTED_SET = new Set<string>(SUPPORTED_LANGUAGES);

export function isSupportedLanguage(lang: string): lang is SupportedLanguage {
    return SUPPORTED_SET.has(lang);
}

export function lookupSupportedLanguage(code: string): SupportedLanguage | undefined {
    // Cast after the runtime membership check so companion-locale codes (e.g. zh-TW on the
    // zh-CN-only PR) type-check without being present in the SupportedLanguage union yet.
    return SUPPORTED_SET.has(code) ? (code as SupportedLanguage) : undefined;
}

export function pickEffectiveLanguage({
    i18nEnabled,
    userLanguage,
    browserLanguage,
    defaultLanguage = DEFAULT_LANGUAGE,
}: {
    i18nEnabled: boolean;
    userLanguage: string | null | undefined;
    browserLanguage: SupportedLanguage | undefined;
    defaultLanguage?: SupportedLanguage;
}): SupportedLanguage {
    if (!i18nEnabled) return defaultLanguage;
    if (userLanguage && isSupportedLanguage(userLanguage)) return userLanguage;
    return browserLanguage ?? defaultLanguage;
}

/**
 * Best-effort match of the browser's preferred languages (`navigator.languages`, most-preferred
 * first) to a supported UI locale. Tries an exact, case-insensitive match first, then folds region
 * variants to their base language (e.g. `de-DE` -> `de`, `fr-CA` -> `fr`, and `pt-PT` -> `pt-BR` as
 * the only Portuguese variant). Returns `undefined` when nothing matches, so callers can fall back
 * to the default language.
 */
export function detectBrowserLanguage(): SupportedLanguage | undefined {
    const preferred = typeof navigator !== 'undefined' ? (navigator.languages ?? [navigator.language]) : [];

    const matchTag = (tag: string | undefined): SupportedLanguage | undefined => {
        if (!tag) return undefined;
        const lower = tag.toLowerCase();
        const base = lower.split('-')[0];
        if (base === 'zh') {
            if (lower.includes('hant') || lower === 'zh-tw' || lower === 'zh-hk' || lower === 'zh-mo') {
                // Prefer zh-TW when registered (companion PR); never fold Traditional to zh-CN.
                return lookupSupportedLanguage('zh-TW');
            }
            // zh-Hans, zh-CN, zh-SG, bare zh
            return lookupSupportedLanguage('zh-CN');
        }
        return (
            SUPPORTED_LANGUAGES.find((locale) => locale.toLowerCase() === lower) ??
            SUPPORTED_LANGUAGES.find((locale) => locale.toLowerCase().split('-')[0] === base)
        );
    };

    return preferred.map(matchTag).find((match): match is SupportedLanguage => match !== undefined);
}
