import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

import { localeBundleBackend } from '@src/i18n/localeBackend';
import { NAMESPACES } from '@src/i18n/namespaces';
import { resolveInitialLanguage } from '@src/i18n/resolveInitialLanguage';

export { NAMESPACES };

// Init is intentionally not awaited. Blocking module evaluation on the locale fetch delays the
// app's first render — including the unauthenticated redirect in ProtectedRoute — by a network
// round trip. Every namespace read below resolves from one shared bundle request (localeBackend),
// so the active language still costs a single fetch.
i18n.use(localeBundleBackend)
    .use(initReactI18next)
    .init({
        lng: resolveInitialLanguage(),
        // Every locale trails English by a few percent of its keys, so fall back to English
        // rather than rendering the key name itself. That costs one extra bundle for non-English
        // users, which is cheap now that a language is a single request.
        fallbackLng: 'en',
        load: 'currentOnly',
        ns: NAMESPACES,
        interpolation: { escapeValue: false },
    });

export default i18n;
