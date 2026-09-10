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

    if (!File.prototype.text) {
        File.prototype.text = function text(this: File) {
            return new Promise<string>((resolve, reject) => {
                const reader = new FileReader();
                reader.onload = () => resolve(String(reader.result ?? ''));
                reader.onerror = () => reject(reader.error);
                reader.readAsText(this);
            });
        };
    }

    if (!File.prototype.arrayBuffer) {
        File.prototype.arrayBuffer = function arrayBuffer(this: File) {
            return new Promise<ArrayBuffer>((resolve, reject) => {
                const reader = new FileReader();
                reader.onload = () => resolve(reader.result as ArrayBuffer);
                reader.onerror = () => reject(reader.error);
                reader.readAsArrayBuffer(this);
            });
        };
    }
});
