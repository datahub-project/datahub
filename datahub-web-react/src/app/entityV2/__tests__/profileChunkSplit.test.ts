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

// Profile UI. The eager shell may import the lazy wrapper, not these modules.
const DEFERRED = [
    'app/entityV2/shared/containers/profile/EntityProfile.tsx',
    'app/entityV2/shared/tabs/Lineage/LineageTab.tsx',
    'app/entityV2/shared/tabs/Dataset/Schema/SchemaTab.tsx',
    'app/entityV2/shared/tabs/Documentation/DocumentationTab.tsx',
    'app/entityV2/shared/embed/EmbeddedProfile.tsx',
    'app/lineageV3/LineageGraph.tsx',
    'app/entityV2/shared/containers/profile/utils.tsx',
];

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

    return firstExisting([
        ...EXTENSIONS.map((extension) => `${base}${extension}`),
        ...EXTENSIONS.map((extension) => join(base, `index${extension}`)),
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

function visitEager(file: string, seen: Set<string>): void {
    if (seen.has(file)) {
        return;
    }
    seen.add(file);
    let source = '';
    try {
        source = readFileSync(file, 'utf8');
    } catch {
        return;
    }
    staticImportSpecs(source).forEach((spec) => {
        const next = resolveImport(spec, file);
        if (next) {
            visitEager(next, seen);
        }
    });
}

describe('logged-in shell bundle', () => {
    it('does not statically import entity profile UI', () => {
        const seen = new Set<string>();
        visitEager(join(srcRoot, 'index.tsx'), seen);
        const relative = new Set([...seen].map((file) => file.slice(srcRoot.length + 1)));
        expect(DEFERRED.filter((file) => relative.has(file))).toEqual([]);
    });
});
