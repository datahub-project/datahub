import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

import { I18N_LOCALE_UPDATE_EVENT } from '@src/i18n/i18nVirtualModules';
import { evictLocaleBundle, loadLocaleBundle, localeBundleBackend } from '@src/i18n/localeBackend';
import { NAMESPACES } from '@src/i18n/namespaces';
import { resolveInitialLanguage } from '@src/i18n/resolveInitialLanguage';

export { NAMESPACES };

const lng = resolveInitialLanguage();
const bundle = await loadLocaleBundle(lng);

i18n.use(localeBundleBackend)
    .use(initReactI18next)
    .init({
        lng,
        // Don't fetch English (or any other language) as a fallback resource pack. Missing keys
        // stay missing until that language is actually selected.
        fallbackLng: false,
        load: 'currentOnly',
        ns: NAMESPACES,
        resources: { [lng]: bundle },
        partialBundledLanguages: true,
        interpolation: { escapeValue: false },
    });

if (import.meta.hot) {
    import.meta.hot.on(I18N_LOCALE_UPDATE_EVENT, ({ lng: updatedLng }: { lng: string }) => {
        evictLocaleBundle(updatedLng);
        void i18n.reloadResources(updatedLng, [...NAMESPACES]);
    });
}

export default i18n;
