import { beforeEach, describe, expect, it, vi } from 'vitest';

let store: Record<string, string> = {};
Object.defineProperty(window, 'localStorage', {
    value: {
        getItem: (key: string) => store[key] ?? null,
        setItem: (key: string, value: string) => {
            store[key] = value;
        },
        clear: () => {
            store = {};
        },
    },
});

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

// The refs are initialised at module load, so each case needs a fresh module instance.
async function loadFlagsModule() {
    vi.resetModules();
    return import('@app/appConfig/UpdateGlobalFlags');
}

describe('global flag refs', () => {
    beforeEach(() => {
        localStorage.clear();
        vi.restoreAllMocks();
    });

    it('default to false when nothing has been persisted', async () => {
        const { showSeparateSiblingsRef, hideLineageInSearchCardsRef } = await loadFlagsModule();

        expect(showSeparateSiblingsRef.current).toBe(false);
        expect(hideLineageInSearchCardsRef.current).toBe(false);
    });

    it('seed from the values persisted by the previous page load', async () => {
        localStorage.setItem('showSeparateSiblings', 'true');
        localStorage.setItem('hideLineageInSearchCards', 'true');

        const { showSeparateSiblingsRef, hideLineageInSearchCardsRef } = await loadFlagsModule();

        expect(showSeparateSiblingsRef.current).toBe(true);
        expect(hideLineageInSearchCardsRef.current).toBe(true);
    });

    it('fall back to false when storage is unavailable', async () => {
        vi.spyOn(localStorage, 'getItem').mockImplementation(() => {
            throw new Error('storage disabled');
        });

        const { showSeparateSiblingsRef, hideLineageInSearchCardsRef } = await loadFlagsModule();

        expect(showSeparateSiblingsRef.current).toBe(false);
        expect(hideLineageInSearchCardsRef.current).toBe(false);
    });
});
