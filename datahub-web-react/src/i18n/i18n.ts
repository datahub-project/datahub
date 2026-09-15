import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import { coreNamespaces, namespacesByGroup } from 'virtual:i18n-locale-loaders';

import { I18N_LOCALE_UPDATE_EVENT } from '@src/i18n/i18nVirtualModules';
import { evictLocaleGroup, localeBundleBackend } from '@src/i18n/localeBackend';
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
        // Don't fetch English (or any other language) as a fallback resource pack. Missing keys
        // stay missing until that language is actually selected.
        fallbackLng: false,
        load: 'currentOnly',
        ns: coreNamespaces,
        interpolation: { escapeValue: false },
    });

if (import.meta.hot) {
    import.meta.hot.on(I18N_LOCALE_UPDATE_EVENT, ({ lng, group }: { lng: string; group: string }) => {
        evictLocaleGroup(lng, group);
        i18n.reloadResources(lng, namespacesByGroup[group]).catch(() => undefined);
    });
}

export default i18n;
