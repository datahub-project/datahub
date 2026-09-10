import { LOCALE_MAP } from '@app/i18n/constants';
import { SupportedLanguage } from '@app/i18n/types';

export function isSupportedLanguage(lang: string): lang is SupportedLanguage {
    return lang in LOCALE_MAP;
}

function lookupSupportedLanguage(code: string): SupportedLanguage | undefined {
    // Cast after the runtime membership check so companion-locale codes (e.g. zh-TW on the
    // zh-CN-only PR) type-check without being present in the SupportedLanguage union yet.
    return code in LOCALE_MAP ? (code as SupportedLanguage) : undefined;
}

/**
 * Best-effort match of the browser's preferred languages (`navigator.languages`, most-preferred
 * first) to a supported UI locale. Tries an exact, case-insensitive match first, then folds region
 * variants to their base language (e.g. `de-DE` -> `de`, `fr-CA` -> `fr`, and `pt-PT` -> `pt-BR` as
 * the only Portuguese variant). Returns `undefined` when nothing matches, so callers can fall back
 * to the default language.
 */
export function detectBrowserLanguage(): SupportedLanguage | undefined {
    const supported = Object.keys(LOCALE_MAP) as SupportedLanguage[];
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
        // Exact (case-insensitive) match first, then fold region variants to their base language.
        return (
            supported.find((locale) => locale.toLowerCase() === lower) ??
            supported.find((locale) => locale.toLowerCase().split('-')[0] === base)
        );
    };

    // navigator.languages is ordered most-preferred first; take the first tag that maps to a locale.
    return preferred.map(matchTag).find((match): match is SupportedLanguage => match !== undefined);
}
