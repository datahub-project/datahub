import i18n from 'i18next';
import resourcesToBackend from 'i18next-resources-to-backend';
import { initReactI18next } from 'react-i18next';

// Eager-bundle only English so the default (and dominant) Storybook view renders real
// strings synchronously with no key-flash and no hardcoded namespace list to maintain.
// Every other language is lazy-loaded on demand (see below), keeping ~3.5MB of non-English
// translations out of the preview entry bundle. Namespaces without a translation for the
// active language fall back to the already-loaded English text via `fallbackLng`.
const enModules = import.meta.glob('../src/i18n/locales/en/*.json', { eager: true, import: 'default' });

const enResources: Record<string, unknown> = {};
for (const path in enModules) {
    // path looks like `../src/i18n/locales/en/<namespace>.json`
    const match = path.match(/\/en\/([^/]+)\.json$/);
    if (match) {
        const [, namespace] = match;
        enResources[namespace] = enModules[path];
    }
}

// Lazy tier: mirror the app (`src/i18n/i18n.ts`). Vite code-splits each `<lng>/<ns>.json`
// into its own chunk, fetched by i18next only when a story requests it for the active
// language. `partialBundledLanguages` is required so i18next uses this backend for any
// language/namespace absent from `resources` — without it, the bundled `en` would suppress
// all backend loads and non-English languages would never localize.
//
// `ns` stays empty (same as the app): listing every namespace here would still trigger a
// full load storm when switching away from English. useTranslation() loads on demand.
i18n.use(resourcesToBackend((lng: string, ns: string) => import(`../src/i18n/locales/${lng}/${ns}.json`)));

i18n.use(initReactI18next).init({
    lng: 'en',
    fallbackLng: 'en',
    ns: [],
    defaultNS: false,
    resources: { en: enResources },
    partialBundledLanguages: true,
    interpolation: { escapeValue: false },
    react: { useSuspense: false },
});

export default i18n;
