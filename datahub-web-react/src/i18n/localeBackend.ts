import localeLoaders, { namespaceGroups } from 'virtual:i18n-locale-loaders';

import type { LocaleBundle } from '@src/i18n/i18nVirtualModules';

const inflight = new Map<string, Promise<LocaleBundle>>();

export function loadLocaleGroup(lng: string, group: string): Promise<LocaleBundle> {
    const cacheKey = `${lng}:${group}`;
    const cached = inflight.get(cacheKey);
    if (cached) return cached;

    const loader = localeLoaders[lng]?.[group];
    if (!loader) return Promise.reject(new Error(`Missing i18n locale bundle for "${cacheKey}"`));

    const pending = loader().then(
        (mod) => mod.default,
        (error) => {
            inflight.delete(cacheKey);
            throw error;
        },
    );
    inflight.set(cacheKey, pending);
    return pending;
}

export function evictLocaleGroup(lng: string, group: string): void {
    inflight.delete(`${lng}:${group}`);
}

export function clearLocaleBundleCache(): void {
    inflight.clear();
}

export const localeBundleBackend = {
    type: 'backend' as const,
    read(lng: string, ns: string, callback: (error: unknown, data: false | Record<string, unknown>) => void): void {
        const group = namespaceGroups[ns];
        if (!group) {
            callback(new Error(`Missing i18n namespace group for "${ns}"`), false);
            return;
        }
        loadLocaleGroup(lng, group)
            .then((bundle) => {
                callback(null, bundle[ns] ?? {});
            })
            .catch((error) => callback(error, false));
    },
};
