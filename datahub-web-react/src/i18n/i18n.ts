import i18n from 'i18next';
import HttpBackend, { HttpBackendOptions } from 'i18next-http-backend';
import resourcesToBackend from 'i18next-resources-to-backend';
import { initReactI18next } from 'react-i18next';

export const NAMESPACES = [
    'common.actions',
    'common.feedback',
    'common.labels',
    'entity.identity',
    'entity.ownership',
    'entity.profile.documentation',
    'entity.profile.incident',
    'entity.profile.validations',
    'entity.profile.access',
    'entity.profile.queries',
    'entity.profile.schema',
    'entity.profile.stats',
    'entity.profile.view',
    'entity.views',
    'governance.domain',
    'governance.glossary',
    'governance.structured-properties',
    'home.v2',
    'home.v3',
    'misc',
    'modules',
    'settings.features',
    'settings.page',
    'settings.permissions',
    'settings.posts',
    'settings.preferences',
    'settings.tokens',
    'shared.query-builder',
] as const;

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
