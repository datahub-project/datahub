import fs from 'fs';
import i18next from 'i18next';
import resourcesToBackend from 'i18next-resources-to-backend';
import path from 'path';
import { fileURLToPath } from 'url';
import { describe, expect, it } from 'vitest';

import { INITIAL_NAMESPACES, NAMESPACES } from '@src/i18n/namespaces';

const LOCALES_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../locales');

// Representative home-shell namespaces (matches useTranslation call sites on home V3).
const HOME_FIRST_PAINT = [
    'modules',
    'home.v2',
    'home.v3',
    'search',
    'common.actions',
    'common.labels',
    'misc',
] as const;

function loadResource(lng: string, ns: string): Record<string, string> {
    return JSON.parse(fs.readFileSync(path.join(LOCALES_DIR, lng, `${ns}.json`), 'utf8'));
}

async function measureLoads(initNs: readonly string[], loadAfterInit: readonly string[] = []) {
    const loads: string[] = [];
    const instance = i18next.createInstance();

    await instance
        .use(
            resourcesToBackend((lng: string, ns: string) => {
                loads.push(`${lng}/${ns}`);
                return Promise.resolve(loadResource(lng, ns));
            }),
        )
        .init({
            lng: 'en',
            fallbackLng: 'en',
            ns: [...initNs],
            defaultNS: false,
            initImmediate: false,
            interpolation: { escapeValue: false },
        });

    const afterInit = loads.length;
    if (loadAfterInit.length) {
        await instance.loadNamespaces([...loadAfterInit]);
    }

    return { instance, loads, afterInit, afterAll: loads.length };
}

describe('i18n bootstrap namespaces', () => {
    it('does not eagerly register namespaces for init-time loading', () => {
        expect(INITIAL_NAMESPACES).toEqual([]);
    });

    it('still exports the full namespace registry for tooling and parity checks', () => {
        expect(NAMESPACES.length).toBeGreaterThan(0);
        expect(NAMESPACES).toContain('common.actions');
        expect(NAMESPACES).toContain('home.v3');
    });
});

describe('i18n lazy namespace loading (before/after)', () => {
    it('loads zero namespaces on init vs all namespaces previously', async () => {
        const before = await measureLoads(NAMESPACES);
        const after = await measureLoads(INITIAL_NAMESPACES);

        expect(before.afterInit).toBe(NAMESPACES.length);
        expect(after.afterInit).toBe(0);
        expect(after.afterInit).toBeLessThan(before.afterInit);
    });

    it('loads only requested namespaces for home first paint and resolves translations', async () => {
        const { instance, afterInit, afterAll, loads } = await measureLoads(INITIAL_NAMESPACES, HOME_FIRST_PAINT);

        expect(afterInit).toBe(0);
        expect(afterAll).toBe(HOME_FIRST_PAINT.length);
        expect(loads).toEqual(HOME_FIRST_PAINT.map((ns) => `en/${ns}`));

        expect(instance.t('home.v2:greeting.morning')).toBe('Good morning');
        expect(instance.t('modules:yourAssets.moduleName')).toBe('Your Assets');
        expect(instance.t('search:advancedFilter.selectOwners')).toBe('Select Owners');

        // Deferred feature namespaces stay unloaded until navigated to.
        expect(instance.t('settings.page:logOut')).toBe('logOut');
        expect(loads.some((l) => l.includes('settings.page'))).toBe(false);
        expect(loads.some((l) => l.includes('ingestion'))).toBe(false);
    });

    it('loads deferred namespaces on demand and only those languages on changeLanguage', async () => {
        const { instance, loads } = await measureLoads(INITIAL_NAMESPACES, HOME_FIRST_PAINT);

        await instance.loadNamespaces(['settings.page', 'ingestion']);
        expect(instance.t('settings.page:logOut')).toBe('Log Out');
        expect(instance.t('ingestion:assets.title')).toBe('Assets');

        const beforeLang = loads.length;
        await instance.changeLanguage('de');
        const deLoads = loads.slice(beforeLang);

        expect(deLoads.sort()).toEqual(
            [...HOME_FIRST_PAINT, 'settings.page', 'ingestion'].map((ns) => `de/${ns}`).sort(),
        );
        expect(deLoads.length).toBeLessThan(NAMESPACES.length);
        expect(instance.t('home.v2:greeting.morning')).toBe('Guten Morgen');
        expect(instance.t('settings.page:logOut')).toBe('Abmelden');
    });
});
