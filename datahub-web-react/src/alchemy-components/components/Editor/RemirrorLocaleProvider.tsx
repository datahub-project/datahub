import { i18n as remirrorI18n } from '@remirror/i18n';
import * as remirrorPlurals from '@remirror/i18n/plurals';
import { I18nProvider } from '@remirror/react';
import React, { useEffect, useState } from 'react';
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
    const plurals = (remirrorPlurals as Record<string, ((n: number, ord?: boolean) => string) | undefined>)[locale];
    if (plurals) {
        remirrorI18n.loadLocaleData(locale, { plurals });
    }
    remirrorI18n.load(locale, messages);
}

type Props = {
    children: React.ReactNode;
};

/**
 * Shares DataHub's Remirror translations with any Remirror editor. Wrap a `<Remirror>` tree
 * with this so the editor's built-in labels follow the active app language, falling back to
 * English for languages we don't ship a bundle for.
 */
export default function RemirrorLocaleProvider({ children }: Props) {
    const { i18n: appI18n } = useTranslation();
    const appLanguage = (appI18n.resolvedLanguage || appI18n.language || 'en').split('-')[0];
    const locale = REMIRROR_SUPPORTED_LOCALES.includes(appLanguage) ? appLanguage : 'en';
    // Only activate once the bundle is loaded, so labels never flash raw message ids. Until then
    // the provider stays on English (Remirror's built-in).
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

    return (
        <I18nProvider i18n={remirrorI18n} locale={activeLocale}>
            {children}
        </I18nProvider>
    );
}
