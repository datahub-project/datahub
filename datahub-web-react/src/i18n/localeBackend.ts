import type { LocaleBundle } from '@src/i18n/i18nVirtualModules';
import localeLoaders from 'virtual:i18n-locale-loaders';

const inflight = new Map<string, Promise<LocaleBundle>>();

export function loadLocaleBundle(lng: string): Promise<LocaleBundle> {
    const cached = inflight.get(lng);
    if (cached) return cached;

    const loader = localeLoaders[lng];
    const pending = loader
        ? loader().then((mod) => mod.default)
        : Promise.reject(new Error(`Missing i18n locale bundle for "${lng}"`));
    inflight.set(lng, pending);
    return pending;
}

export function evictLocaleBundle(lng: string): void {
    inflight.delete(lng);
}

export function clearLocaleBundleCache(): void {
    inflight.clear();
}

export const localeBundleBackend = {
    type: 'backend' as const,
    read(
        lng: string,
        ns: string,
        callback: (error: unknown, data: false | Record<string, unknown>) => void,
    ): void {
        loadLocaleBundle(lng)
            .then((bundle) => {
                callback(null, bundle[ns] ?? {});
            })
            .catch((error) => callback(error, false));
    },
};
