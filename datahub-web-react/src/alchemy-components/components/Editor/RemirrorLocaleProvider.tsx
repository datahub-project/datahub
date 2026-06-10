import { i18n as remirrorI18n } from '@remirror/i18n';
import * as remirrorPlurals from '@remirror/i18n/plurals';
import { I18nProvider } from '@remirror/react';
import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';

import { REMIRROR_LOCALE_MESSAGES } from '@src/i18n/remirror';

// Remirror's built-in labels use its own Lingui i18n, which ships English only. Load our
// supplementary locale bundles (e.g. German) into that shared instance once — values may
// be plain strings or ICU messages, which Lingui compiles at render time. Plural messages
// also need the locale's plural rules.
Object.entries(REMIRROR_LOCALE_MESSAGES).forEach(([locale, messages]) => {
    const plurals = (remirrorPlurals as Record<string, ((n: number, ord?: boolean) => string) | undefined>)[locale];
    if (plurals) {
        remirrorI18n.loadLocaleData(locale, { plurals });
    }
    remirrorI18n.load(locale, messages);
});

// Languages whose Remirror built-in labels we localize. Any other app language falls back
// to English (Remirror's built-in) so its labels never render as raw message ids.
const REMIRROR_SUPPORTED_LOCALES = ['en', ...Object.keys(REMIRROR_LOCALE_MESSAGES)];

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
    useEffect(() => {
        remirrorI18n.activate(locale);
    }, [locale]);

    return (
        <I18nProvider i18n={remirrorI18n} locale={locale}>
            {children}
        </I18nProvider>
    );
}
