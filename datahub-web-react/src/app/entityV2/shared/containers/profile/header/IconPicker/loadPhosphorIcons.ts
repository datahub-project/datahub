import type { ComponentType } from 'react';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type PhosphorIconComponent = ComponentType<any>;
export type PhosphorIconsModule = Record<string, PhosphorIconComponent | unknown>;

let phosphorIconsPromise: Promise<PhosphorIconsModule> | null = null;

/**
 * Load the full Phosphor icon set in one async chunk for the icon picker.
 *
 * Do NOT use per-icon CSR lazy chunks here: scrolling the grid would request
 * hundreds of modules, blank the UI, and often trigger a Vite full reload / tab crash.
 */
export function loadPhosphorIcons(): Promise<PhosphorIconsModule> {
    if (!phosphorIconsPromise) {
        // eslint-disable-next-line rulesdir/no-phosphor-generic-imports -- picker needs one shared async chunk
        phosphorIconsPromise = import('@phosphor-icons/react').then((mod) => mod as PhosphorIconsModule);
    }
    return phosphorIconsPromise;
}
