import { beforeAll } from 'vitest';

/**
 * Polyfill localStorage for jsdom test environment.
 * This is needed because CustomThemeProvider calls localStorage.getItem
 * and jsdom may not provide it in all configurations.
 */
beforeAll(() => {
    if (!globalThis.localStorage || typeof globalThis.localStorage.getItem !== 'function') {
        const store: Record<string, string> = {};
        Object.defineProperty(globalThis, 'localStorage', {
            value: {
                getItem: (key: string) => store[key] ?? null,
                setItem: (key: string, value: string) => {
                    store[key] = value;
                },
                removeItem: (key: string) => {
                    delete store[key];
                },
                clear: () => {
                    Object.keys(store).forEach((k) => delete store[k]);
                },
                get length() {
                    return Object.keys(store).length;
                },
                key: (index: number) => Object.keys(store)[index] ?? null,
            },
            writable: true,
        });
    }
});
