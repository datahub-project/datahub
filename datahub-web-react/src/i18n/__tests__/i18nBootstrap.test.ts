import { describe, expect, it, vi } from 'vitest';

// A loader that never settles. If i18n.ts awaited the locale bundle at module scope, importing
// it would hang here — which is what delayed the app's first render (and the unauthenticated
// redirect in ProtectedRoute) by a network round trip.
const neverSettles = new Promise<{ default: Record<string, Record<string, unknown>> }>(() => {});

vi.mock('virtual:i18n-locale-loaders', () => ({
    coreNamespaces: ['alchemy'],
    namespaceGroups: { alchemy: 'core' },
    namespacesByGroup: { core: ['alchemy'] },
    default: {
        en: {
            core: () => neverSettles,
        },
    },
}));

describe('i18n bootstrap', () => {
    it('initializes without waiting for the locale bundle to load', async () => {
        const { default: i18n } = await import('@src/i18n/i18n');

        expect(i18n.language).toBe('en');
        expect(i18n.options.ns).toEqual(['alchemy']);
    });
});
