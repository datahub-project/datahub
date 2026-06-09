import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

// Eagerly bundle every English namespace so stories render real strings instead of raw keys.
// (The app lazy-loads these via a backend; Storybook has no backend, so we load them synchronously.)
const modules = import.meta.glob('../src/i18n/locales/en/*.json', { eager: true, import: 'default' });

const enResources: Record<string, unknown> = {};
for (const path in modules) {
    const namespace = path.split('/').pop()?.replace('.json', '');
    if (namespace) {
        enResources[namespace] = modules[path];
    }
}

i18n.use(initReactI18next).init({
    lng: 'en',
    fallbackLng: 'en',
    ns: Object.keys(enResources),
    resources: { en: enResources },
    interpolation: { escapeValue: false },
    react: { useSuspense: false },
});

export default i18n;
