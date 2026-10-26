import type { Plugin } from 'vite';

/**
 * Gives the MFE lazy icon stubs their own copy of each Phosphor icon module.
 *
 * `iconLoader.ts` globs `lazy-icons/*.ts`, so every stub is a dynamic entry. Without
 * this plugin a stub and the app share `@phosphor-icons/react/dist/csr/<Icon>.mjs`
 * and its `defs/<Icon>.mjs`, which makes each icon the app imports its own chunk:
 * Rollup cannot put a dynamic entry's code into a page chunk. A logged-in page then
 * requested one file per icon. With a separate copy for the stubs, the icons the app
 * imports are split like any other module and land in the page chunks that use them.
 *
 * The stub copy is the same file with a query suffix, so the MFE icon chunk is
 * unchanged apart from its hash. `lib/IconBase.mjs` and the icon context stay shared.
 */

export const LAZY_ICON_QUERY = '?lazy-icon';

const LAZY_ICON_STUB = /\/mfeframework\/lazy-icons\/[^/]+\.ts$/;
// Bare, or already an absolute path when a resolve.alias anchors `@phosphor-icons/react`.
const PHOSPHOR_CSR_ICON = /(?:^|\/)@phosphor-icons\/react\/dist\/csr\/[^/]+$/;
const PHOSPHOR_ICON_DEFS = /^\.\.\/defs\//;

/** True when `source` imported from `importer` should resolve to the stub-only copy. */
export function isLazyIconCopyImport(source: string, importer: string | undefined): boolean {
    if (!importer) return false;
    if (LAZY_ICON_STUB.test(importer)) return PHOSPHOR_CSR_ICON.test(source);
    return importer.endsWith(LAZY_ICON_QUERY) && PHOSPHOR_ICON_DEFS.test(source);
}

export function lazyIconCopiesPlugin(): Plugin {
    return {
        name: 'lazy-icon-copies',
        apply: 'build',
        enforce: 'pre',
        async resolveId(source, importer, options) {
            if (!importer || !isLazyIconCopyImport(source, importer)) return null;
            const resolved = await this.resolve(source, importer.replace(LAZY_ICON_QUERY, ''), {
                ...options,
                skipSelf: true,
            });
            if (!resolved || resolved.external) return null;
            return `${resolved.id}${LAZY_ICON_QUERY}`;
        },
    };
}
