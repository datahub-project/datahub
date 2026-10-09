import type { ComponentType } from 'react';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type PhosphorIconComponent = ComponentType<any>;
export type PhosphorIconsModule = Record<string, PhosphorIconComponent | unknown>;

type PhosphorIconsImporter = () => Promise<PhosphorIconsModule>;

let phosphorIconsPromise: Promise<PhosphorIconsModule> | null = null;

const defaultImporter: PhosphorIconsImporter = () =>
    // eslint-disable-next-line rulesdir/no-phosphor-generic-imports -- picker needs one shared async chunk
    import('@phosphor-icons/react').then((mod) => mod as PhosphorIconsModule);

let importer: PhosphorIconsImporter = defaultImporter;

/**
 * Load the full Phosphor icon set in one async chunk for the icon picker.
 *
 * Do NOT use per-icon CSR lazy chunks here: scrolling the grid would request
 * hundreds of modules, blank the UI, and often trigger a Vite full reload / tab crash.
 */
export function loadPhosphorIcons(): Promise<PhosphorIconsModule> {
    if (!phosphorIconsPromise) {
        phosphorIconsPromise = importer().catch((error) => {
            // Clear so a later mount can retry after a transient chunk/network failure.
            phosphorIconsPromise = null;
            throw error;
        });
    }
    return phosphorIconsPromise;
}

/** @internal Clears the shared promise / importer override for unit tests. */
export function resetPhosphorIconsCacheForTests(nextImporter?: PhosphorIconsImporter): void {
    phosphorIconsPromise = null;
    importer = nextImporter ?? defaultImporter;
}
