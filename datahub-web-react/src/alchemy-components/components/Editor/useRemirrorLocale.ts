import { type I18n, i18n as remirrorI18n } from '@remirror/i18n';
import * as remirrorPlurals from '@remirror/i18n/plurals';
import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { REMIRROR_LOCALE_LOADERS } from '@src/i18n/remirror';

// Languages whose Remirror built-in labels we localize. Any other app language falls back
// to English (Remirror's built-in) so its labels never render as raw message ids.
const REMIRROR_SUPPORTED_LOCALES = ['en', ...Object.keys(REMIRROR_LOCALE_LOADERS)];

// Remirror's built-in labels use its own Lingui i18n, which ships English only. Load our
// supplementary locale bundle (e.g. German) into that shared instance the first time its
// language is activated — values may be plain strings or ICU messages, which Lingui compiles
// at render time. Plural messages also need the locale's plural rules.
async function loadRemirrorLocale(locale: string): Promise<void> {
    const loader = REMIRROR_LOCALE_LOADERS[locale];
    // `en` has no loader (built in); a bundle is loaded at most once.
    if (!loader || remirrorI18n.messages[locale]) {
        return;
    }
    const { default: messages } = await loader();
    // Lingui keys plural rules by language only (`pt`, `zh`), so a region-tagged locale such as
    // `pt-BR` or `zh-TW` must fall back to its primary subtag. Without this, Lingui warns and
    // resolves every plural to the `other` branch (e.g. pt-BR renders "1 linhas").
    const pluralsByLocale = remirrorPlurals as Record<string, ((n: number, ord?: boolean) => string) | undefined>;
    const plurals = pluralsByLocale[locale] ?? pluralsByLocale[locale.split('-')[0]];
    if (plurals) {
        remirrorI18n.loadLocaleData(locale, { plurals });
    }
    remirrorI18n.load(locale, messages);
}

// Prefer the full BCP-47 tag (e.g. zh-TW, pt-BR) before falling back to the primary subtag, so a
// region-specific bundle is never skipped in favour of a generic one.
function resolveRemirrorLocale(language: string): string {
    if (REMIRROR_SUPPORTED_LOCALES.includes(language)) {
        return language;
    }
    const primarySubtag = language.split('-')[0];
    return REMIRROR_SUPPORTED_LOCALES.includes(primarySubtag) ? primarySubtag : 'en';
}

/**
 * Resolves the Remirror locale for the active app language and loads its bundle.
 *
 * Spread the result onto `<Remirror>` rather than wrapping the tree in `<I18nProvider>`:
 * `<Remirror>` renders its own `I18nProvider` from these props, so an outer provider is
 * shadowed and the editor's built-in labels stay English.
 */
export default function useRemirrorLocale(): { i18n: I18n; locale: string } {
    const { i18n: appI18n } = useTranslation();
    const locale = resolveRemirrorLocale(appI18n.resolvedLanguage || appI18n.language || 'en');
    // Only switch once the bundle is loaded, so labels never flash raw message ids. Until then
    // the editor stays on English (Remirror's built-in).
    const [activeLocale, setActiveLocale] = useState('en');

    useEffect(() => {
        let cancelled = false;
        loadRemirrorLocale(locale).then(() => {
            if (cancelled) {
                return;
            }
            remirrorI18n.activate(locale);
            setActiveLocale(locale);
        });
        return () => {
            cancelled = true;
        };
    }, [locale]);

    return { i18n: remirrorI18n, locale: activeLocale };
}
