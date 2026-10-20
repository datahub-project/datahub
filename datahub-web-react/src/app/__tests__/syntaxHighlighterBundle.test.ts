import { readFileSync } from 'fs';
import { dirname, join, resolve } from 'path';
import { fileURLToPath } from 'url';
import { describe, expect, it } from 'vitest';

const srcRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../..');

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

const ENTRIES = [
    'app/AuthenticatedApp.tsx',
    'app/homeV2/HomePage.tsx',
    'app/homeV3/HomePage.tsx',
    'app/searchV2/SearchPage.tsx',
    'app/entityV2/EntityPage.tsx',
    'app/entityV2/shared/containers/profile/EntityProfile.tsx',
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
    const resolvedBase = base;

    return firstExisting([
        ...EXTENSIONS.map((extension) => `${resolvedBase}${extension}`),
        ...EXTENSIONS.map((extension) => join(resolvedBase, `index${extension}`)),
    ]);
}

function staticImportSpecs(source: string): string[] {
    const withoutTypeImports = source
        .split('\n')
        .filter((line) => !line.trim().startsWith('import type'))
        .join('\n');
    const withoutDynamic = withoutTypeImports.replace(/import\s*\(/g, 'dynamic(');
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

function isHeavySyntaxImport(spec: string): boolean {
    return spec === 'react-syntax-highlighter' || spec === 'refractor' || spec === 'sql-formatter';
}

function staticClosure(entry: string): Map<string, string[]> {
    const files = new Map<string, string[]>();
    const pending = [join(srcRoot, entry)];
    while (pending.length > 0) {
        const file = pending.pop();
        if (file && !files.has(file)) {
            let source: string | undefined;
            try {
                source = readFileSync(file, 'utf8');
            } catch {
                source = undefined;
            }
            if (source) {
                const specs = staticImportSpecs(source);
                files.set(file, specs);
                specs.forEach((spec) => {
                    const resolved = resolveImport(spec, file);
                    if (resolved && !files.has(resolved)) {
                        pending.push(resolved);
                    }
                });
            }
        }
    }
    return files;
}

describe('syntax highlighter chunks', () => {
    it('keeps the full catalogs off the shell, home, search, and entity graphs', () => {
        const closure = new Map<string, string[]>();
        ENTRIES.forEach((entry) => {
            staticClosure(entry).forEach((specs, file) => {
                closure.set(file, specs);
            });
        });

        const leaked = [...closure.entries()].flatMap(([file, specs]) =>
            specs.filter(isHeavySyntaxImport).map((spec) => `${file.slice(srcRoot.length + 1)} imports ${spec}`),
        );

        expect(leaked).toEqual([]);
        expect([...closure.keys()].some((file) => file.endsWith('CodeBlock/highlighter.ts'))).toBe(true);
        expect([...closure.keys()].some((file) => file.endsWith('Editor/editorCodeLanguages.ts'))).toBe(false);
    });

    it('loads the full grammar catalog from the lazy editor', () => {
        const editor = readFileSync(
            join(srcRoot, 'alchemy-components/components/Editor/editorCodeLanguages.ts'),
            'utf8',
        );
        const editorShell = readFileSync(join(srcRoot, 'alchemy-components/components/Editor/Editor.tsx'), 'utf8');
        const highlighter = readFileSync(
            join(srcRoot, 'alchemy-components/components/CodeBlock/highlighter.ts'),
            'utf8',
        );
        const formatCode = readFileSync(join(srcRoot, 'alchemy-components/components/CodeBlock/formatCode.ts'), 'utf8');

        expect(editor).toMatch(/from\s+['"]refractor['"]/);
        expect(editorShell).toMatch(/import\('\.\/EditorImpl'\)/);
        expect(editorShell).not.toMatch(/from\s+['"]\.\/EditorImpl['"]/);
        expect(highlighter).toMatch(/prism-light/);
        expect(highlighter).not.toMatch(/from\s+['"]refractor['"]/);
        expect(highlighter).not.toMatch(/from\s+['"]react-syntax-highlighter['"]/);
        expect(formatCode).toMatch(/import\('sql-formatter'\)/);
        expect(formatCode).not.toMatch(/from\s+['"]sql-formatter['"]/);
    });
});
