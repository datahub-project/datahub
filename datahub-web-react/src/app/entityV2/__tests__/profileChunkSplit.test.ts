import { readFileSync } from 'fs';
import { dirname, join, resolve } from 'path';
import { fileURLToPath } from 'url';
import { describe, expect, it } from 'vitest';

const srcRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');

const ALIASES: Array<[string, string]> = [
    ['@app/', join(srcRoot, 'app')],
    ['@src/', srcRoot],
    ['@components/', join(srcRoot, 'alchemy-components')],
    ['@conf/', join(srcRoot, 'conf')],
    ['@graphql/', join(srcRoot, 'graphql')],
    ['@utils/', join(srcRoot, 'utils')],
    ['@providers/', join(srcRoot, 'providers')],
];

const EXTENSIONS = ['.tsx', '.ts', '.jsx', '.js'];

// Entity classes may import these. Everything else under a profile, tab, embed, or lineage
// path has to stay behind a dynamic import, including files that are not in a fixed list.
const PROFILE_IMPORT_ALLOWLIST = [
    'entityData',
    'lazyEntityProfile',
    'profileChunks',
    'RelatedTermTypes',
    'useGetColumnTabCount',
    'useGlossaryRelatedAssetsTabCount',
    'lineageV3/types',
    'lineageV3/utils/lineageUtils',
];

const PROFILE_IMPORT_MARKERS = [
    '/profile/',
    '/containers/profile/',
    '/shared/tabs/',
    '/shared/embed/',
    '/shared/sidebarSection/',
    '/lineageV3/',
];

function isForbiddenProfileImport(spec: string): boolean {
    if (PROFILE_IMPORT_ALLOWLIST.some((allowed) => spec.includes(allowed))) {
        return false;
    }
    return PROFILE_IMPORT_MARKERS.some((marker) => spec.includes(marker));
}

function firstExisting(candidates: Array<string | undefined>): string | undefined {
    return candidates.find((candidate) => {
        if (!candidate) {
            return false;
        }
        try {
            readFileSync(candidate);
            return true;
        } catch {
            return false;
        }
    });
}

function resolveImport(spec: string, fromFile: string): string | undefined {
    if (spec === '@types' || spec.startsWith('@types/')) {
        return join(srcRoot, 'types.generated.ts');
    }

    let base: string | undefined;
    if (spec.startsWith('.')) {
        base = resolve(dirname(fromFile), spec);
    } else {
        const alias = ALIASES.find(([prefix]) => spec.startsWith(prefix));
        if (alias) {
            base = join(alias[1], spec.slice(alias[0].length));
        }
    }
    if (!base) {
        return undefined;
    }
    const resolvedBase = base;

    return firstExisting([
        ...EXTENSIONS.map((extension) => `${resolvedBase}${extension}`),
        ...EXTENSIONS.map((extension) => join(resolvedBase, `index${extension}`)),
    ]);
}

function staticImportSpecs(source: string): string[] {
    const withoutDynamic = source.replace(/import\s*\(/g, 'dynamic(');
    const specs: string[] = [];
    withoutDynamic.replace(/from\s+['"]([^'"]+)['"]|import\s+['"]([^'"]+)['"]/g, (full, fromSpec, bareSpec) => {
        const spec = fromSpec || bareSpec;
        if (spec) {
            specs.push(spec);
        }
        return full;
    });
    return specs;
}

describe('logged-in shell bundle', () => {
    it('does not statically import profile UI from entity definitions', () => {
        const registryFile = join(srcRoot, 'app/buildEntityRegistryV2.ts');
        const entityFiles = staticImportSpecs(readFileSync(registryFile, 'utf8'))
            .map((spec) => resolveImport(spec, registryFile))
            .filter((file): file is string => Boolean(file));
        const leaked = entityFiles.flatMap((file) =>
            staticImportSpecs(readFileSync(file, 'utf8'))
                .filter(isForbiddenProfileImport)
                .map((spec) => `${file.slice(srcRoot.length + 1)} imports ${spec}`),
        );
        expect(leaked).toEqual([]);
    });
});
