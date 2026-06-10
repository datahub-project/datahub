import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

// Eagerly bundle every namespace for every language so stories render real strings
// instead of raw keys, and the toolbar language switcher works offline.
// (The app lazy-loads these via a backend; Storybook has no backend, so we load them
// synchronously.) Namespaces without a translation for a given language fall back to en.
const modules = import.meta.glob('../src/i18n/locales/*/*.json', { eager: true, import: 'default' });

const resources: Record<string, Record<string, unknown>> = {};
for (const path in modules) {
    // path looks like `../src/i18n/locales/<lng>/<namespace>.json`
    const match = path.match(/\/locales\/([^/]+)\/([^/]+)\.json$/);
    if (match) {
        const [, lng, namespace] = match;
        resources[lng] = resources[lng] || {};
        resources[lng][namespace] = modules[path];
    }
}

const namespaces = Array.from(new Set(Object.values(resources).flatMap((nsMap) => Object.keys(nsMap))));

i18n.use(initReactI18next).init({
    lng: 'en',
    fallbackLng: 'en',
    ns: namespaces,
    resources,
    interpolation: { escapeValue: false },
    react: { useSuspense: false },
});

export default i18n;
