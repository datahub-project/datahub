import { describe, expect, it } from 'vitest';

// eslint-disable-next-line import-alias/import-alias -- plugin tests sit next to the plugin, outside src aliases
import { LAZY_ICON_QUERY, isLazyIconCopyImport, lazyIconCopiesPlugin } from './lazyIconCopiesPlugin';

const STUB = '/repo/datahub-web-react/src/app/mfeframework/lazy-icons/ArrowUp.ts';
const CSR = '/repo/node_modules/@phosphor-icons/react/dist/csr/ArrowUp.mjs';
const DEFS = '/repo/node_modules/@phosphor-icons/react/dist/defs/ArrowUp.mjs';

type ResolveId = (
    this: { resolve: (source: string, importer: string) => Promise<{ id: string; external?: boolean } | null> },
    source: string,
    importer: string | undefined,
    options: Record<string, unknown>,
) => Promise<string | null>;

function resolveWith(resolved: Record<string, string>) {
    const resolveId = lazyIconCopiesPlugin().resolveId as unknown as ResolveId;
    const calls: Array<[string, string]> = [];
    const context = {
        resolve: async (source: string, importer: string) => {
            calls.push([source, importer]);
            return resolved[source] ? { id: resolved[source] } : null;
        },
    };
    return {
        calls,
        resolve: (source: string, importer: string | undefined) => resolveId.call(context, source, importer, {}),
    };
}

describe('isLazyIconCopyImport', () => {
    it('copies the CSR icon a lazy icon stub re-exports, and that copy’s defs', () => {
        expect(isLazyIconCopyImport('@phosphor-icons/react/dist/csr/ArrowUp', STUB)).toBe(true);
        expect(isLazyIconCopyImport('../defs/ArrowUp.mjs', `${CSR}${LAZY_ICON_QUERY}`)).toBe(true);
    });

    it('matches a stub import that a resolve.alias already made absolute', () => {
        expect(isLazyIconCopyImport('/repo/node_modules/@phosphor-icons/react/dist/csr/ArrowUp', STUB)).toBe(true);
    });

    it('leaves app imports and shared icon internals alone', () => {
        expect(isLazyIconCopyImport('@phosphor-icons/react/dist/csr/ArrowUp', '/repo/src/app/Nav.tsx')).toBe(false);
        expect(isLazyIconCopyImport('../defs/ArrowUp.mjs', CSR)).toBe(false);
        expect(isLazyIconCopyImport('../lib/IconBase.mjs', `${CSR}${LAZY_ICON_QUERY}`)).toBe(false);
        expect(isLazyIconCopyImport('react', STUB)).toBe(false);
        expect(isLazyIconCopyImport('@phosphor-icons/react/dist/csr/ArrowUp', undefined)).toBe(false);
    });
});

describe('lazyIconCopiesPlugin', () => {
    it('resolves the stub import and the defs import to query-suffixed copies', async () => {
        const { calls, resolve } = resolveWith({
            '@phosphor-icons/react/dist/csr/ArrowUp': CSR,
            '../defs/ArrowUp.mjs': DEFS,
        });

        expect(await resolve('@phosphor-icons/react/dist/csr/ArrowUp', STUB)).toBe(`${CSR}${LAZY_ICON_QUERY}`);
        expect(await resolve('../defs/ArrowUp.mjs', `${CSR}${LAZY_ICON_QUERY}`)).toBe(`${DEFS}${LAZY_ICON_QUERY}`);
        // The copy resolves its relative imports from the real file's location.
        expect(calls[1]).toEqual(['../defs/ArrowUp.mjs', CSR]);
    });

    it('defers to the default resolver for everything else', async () => {
        const { calls, resolve } = resolveWith({});

        expect(await resolve('@phosphor-icons/react/dist/csr/ArrowUp', '/repo/src/app/Nav.tsx')).toBeNull();
        expect(await resolve('@phosphor-icons/react/dist/csr/Missing', STUB)).toBeNull();
        expect(calls).toEqual([['@phosphor-icons/react/dist/csr/Missing', STUB]]);
    });
});
