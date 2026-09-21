import localeLoaders from 'virtual:i18n-locale-loaders';

import type { LocaleBundle } from '@src/i18n/i18nVirtualModules';

const inflight = new Map<string, Promise<LocaleBundle>>();
const failed = new Set<string>();

export function loadLocaleBundle(lng: string): Promise<LocaleBundle> {
    const cached = inflight.get(lng);
    if (cached) return cached;

    const loader = localeLoaders[lng];
    if (!loader) return Promise.reject(new Error(`Missing i18n locale bundle for "${lng}"`));

    const pending = loader().then(
        (mod) => {
            failed.delete(lng);
            return mod.default;
        },
        (error) => {
            inflight.delete(lng);
            failed.add(lng);
            throw error;
        },
    );
    inflight.set(lng, pending);
    return pending;
}

/** Whether the last attempt to load this language's bundle failed, so callers can retry it. */
export function hasLocaleBundleFailed(lng: string): boolean {
    return failed.has(lng);
}

export function clearLocaleBundleCache(): void {
    inflight.clear();
    failed.clear();
}

export const localeBundleBackend = {
    type: 'backend' as const,
    read(lng: string, ns: string, callback: (error: unknown, data: boolean | Record<string, unknown>) => void): void {
        loadLocaleBundle(lng)
            .then((bundle) => {
                callback(null, bundle[ns] ?? {});
            })
            // The second argument is i18next's retry flag: it re-reads a namespace only when the
            // backend reports one alongside the error, and it leaves the namespace recoverable
            // instead of permanently failed, so a later language change can load it.
            .catch((error) => callback(error, true));
    },
};
