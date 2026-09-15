import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const enLoader = vi.fn(async () => ({
    default: {
        alchemy: { save: 'Save' },
        auth: { login: 'Log in' },
    },
}));
const deLoader = vi.fn(async () => ({
    default: {
        alchemy: { save: 'Speichern' },
    },
}));
const ingestionLoader = vi.fn(async () => ({
    default: {
        ingestion: { run: 'Run' },
    },
}));

vi.mock('virtual:i18n-locale-loaders', () => ({
    namespaceGroups: {
        alchemy: 'core',
        auth: 'core',
        ingestion: 'ingestion',
    },
    default: {
        en: {
            core: () => enLoader(),
            ingestion: () => ingestionLoader(),
        },
        de: {
            core: () => deLoader(),
        },
    },
}));

const { clearLocaleBundleCache, loadLocaleGroup, localeBundleBackend } = await import('@src/i18n/localeBackend');

function readNamespace(lng: string, ns: string): Promise<false | Record<string, unknown>> {
    return new Promise((resolve, reject) => {
        localeBundleBackend.read(lng, ns, (error, data) => {
            if (error) reject(error);
            else resolve(data);
        });
    });
}

describe('localeBackend', () => {
    beforeEach(() => {
        clearLocaleBundleCache();
        enLoader.mockClear();
        deLoader.mockClear();
        ingestionLoader.mockClear();
    });

    afterEach(() => {
        clearLocaleBundleCache();
    });

    it('loads a language group once and serves every namespace from it', async () => {
        await expect(readNamespace('en', 'alchemy')).resolves.toEqual({ save: 'Save' });
        await expect(readNamespace('en', 'auth')).resolves.toEqual({ login: 'Log in' });

        expect(enLoader).toHaveBeenCalledTimes(1);
        expect(deLoader).not.toHaveBeenCalled();
    });

    it('does not load English when reading a non-English language', async () => {
        await expect(readNamespace('de', 'alchemy')).resolves.toEqual({ save: 'Speichern' });

        expect(deLoader).toHaveBeenCalledTimes(1);
        expect(enLoader).not.toHaveBeenCalled();
    });

    it('loads a feature group without loading the core group', async () => {
        await expect(readNamespace('en', 'ingestion')).resolves.toEqual({ run: 'Run' });

        expect(ingestionLoader).toHaveBeenCalledTimes(1);
        expect(enLoader).not.toHaveBeenCalled();
    });

    it('shares the in-flight promise across loadLocaleGroup callers', async () => {
        const [first, second] = await Promise.all([loadLocaleGroup('en', 'core'), loadLocaleGroup('en', 'core')]);
        expect(first).toBe(second);
        expect(enLoader).toHaveBeenCalledTimes(1);
    });

    it('retries a language after a transient load failure', async () => {
        enLoader.mockRejectedValueOnce(new Error('network error'));

        await expect(loadLocaleGroup('en', 'core')).rejects.toThrow('network error');
        await expect(loadLocaleGroup('en', 'core')).resolves.toEqual({
            alchemy: { save: 'Save' },
            auth: { login: 'Log in' },
        });
        expect(enLoader).toHaveBeenCalledTimes(2);
    });

    it('rejects unsupported languages without caching the failure', async () => {
        await expect(loadLocaleGroup('unsupported', 'core')).rejects.toThrow();
        await expect(loadLocaleGroup('unsupported', 'core')).rejects.toThrow();
    });

    it('rejects namespaces missing from the group manifest', async () => {
        await expect(readNamespace('en', 'missing')).rejects.toThrow();
        expect(enLoader).not.toHaveBeenCalled();
    });
});
