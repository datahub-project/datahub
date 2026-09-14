import i18n from 'i18next';
import HttpBackend, { HttpBackendOptions } from 'i18next-http-backend';
import resourcesToBackend from 'i18next-resources-to-backend';
import { initReactI18next } from 'react-i18next';

import { NAMESPACES } from '@src/i18n/namespaces';

export { NAMESPACES };

if (import.meta.env.DEV) {
    const { HMRPlugin } = await import('i18next-hmr/plugin');
    i18n.use(new HMRPlugin({ vite: { client: true } })).use(HttpBackend);
} else {
    i18n.use(resourcesToBackend((lng: string, ns: string) => import(`./locales/${lng}/${ns}.json`)));
}

i18n.use(initReactI18next).init({
    fallbackLng: 'en',
    ns: NAMESPACES,
    ...(import.meta.env.DEV && {
        backend: { loadPath: '/assets/locales/{{lng}}/{{ns}}.json' } satisfies HttpBackendOptions,
    }),
    interpolation: { escapeValue: false },
});

export default i18n;
