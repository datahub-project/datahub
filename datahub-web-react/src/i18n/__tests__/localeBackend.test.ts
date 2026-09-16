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

vi.mock('virtual:i18n-locale-loaders', () => ({
    default: {
        en: () => enLoader(),
        de: () => deLoader(),
    },
}));

const { clearLocaleBundleCache, hasLocaleBundleFailed, loadLocaleBundle, localeBundleBackend } = await import(
    '@src/i18n/localeBackend'
);

function readNamespace(lng: string, ns: string): Promise<boolean | Record<string, unknown>> {
    return new Promise((resolve, reject) => {
        localeBundleBackend.read(lng, ns, (error, data) => {
            if (error) reject(error);
            else resolve(data);
        });
    });
}

function readNamespaceResult(lng: string, ns: string): Promise<{ error: unknown; data: unknown }> {
    return new Promise((resolve) => {
        localeBundleBackend.read(lng, ns, (error, data) => resolve({ error, data }));
    });
}

describe('localeBackend', () => {
    beforeEach(() => {
        clearLocaleBundleCache();
        enLoader.mockClear();
        deLoader.mockClear();
    });

    afterEach(() => {
        clearLocaleBundleCache();
    });

    it('loads a language bundle once and serves every namespace from it', async () => {
        await expect(readNamespace('en', 'alchemy')).resolves.toEqual({ save: 'Save' });
        await expect(readNamespace('en', 'auth')).resolves.toEqual({ login: 'Log in' });
        await expect(readNamespace('en', 'missing')).resolves.toEqual({});

        expect(enLoader).toHaveBeenCalledTimes(1);
        expect(deLoader).not.toHaveBeenCalled();
    });

    it('does not load English when reading a non-English language', async () => {
        await expect(readNamespace('de', 'alchemy')).resolves.toEqual({ save: 'Speichern' });

        expect(deLoader).toHaveBeenCalledTimes(1);
        expect(enLoader).not.toHaveBeenCalled();
    });

    it('shares the in-flight promise across loadLocaleBundle callers', async () => {
        const [first, second] = await Promise.all([loadLocaleBundle('en'), loadLocaleBundle('en')]);
        expect(first).toBe(second);
        expect(enLoader).toHaveBeenCalledTimes(1);
    });

    it('retries a language after a transient load failure', async () => {
        enLoader.mockRejectedValueOnce(new Error('network error'));

        await expect(loadLocaleBundle('en')).rejects.toThrow('network error');
        await expect(loadLocaleBundle('en')).resolves.toEqual({
            alchemy: { save: 'Save' },
            auth: { login: 'Log in' },
        });
        expect(enLoader).toHaveBeenCalledTimes(2);
    });

    it('rejects unsupported languages without caching the failure', async () => {
        await expect(loadLocaleBundle('unsupported')).rejects.toThrow();
        await expect(loadLocaleBundle('unsupported')).rejects.toThrow();
    });

    it("asks i18next to retry a failed read so the namespace doesn't stay permanently failed", async () => {
        enLoader.mockRejectedValueOnce(new Error('network error'));

        const { error, data } = await readNamespaceResult('en', 'alchemy');

        expect(error).toBeInstanceOf(Error);
        expect(data).toBe(true);
    });

    it('reports a failed language so callers can re-request it', async () => {
        enLoader.mockRejectedValueOnce(new Error('network error'));

        await expect(loadLocaleBundle('en')).rejects.toThrow('network error');
        expect(hasLocaleBundleFailed('en')).toBe(true);

        await expect(loadLocaleBundle('en')).resolves.toMatchObject({ alchemy: { save: 'Save' } });
        expect(hasLocaleBundleFailed('en')).toBe(false);
    });
});
