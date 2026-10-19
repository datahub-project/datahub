import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';

const ROOT = path.resolve(__dirname, '../../..');
const SRC = path.join(ROOT, 'src');

const ALIASES: ReadonlyArray<readonly [string, string]> = [
    ['@components/', 'src/alchemy-components/'],
    ['@components', 'src/alchemy-components'],
    ['@app/', 'src/app/'],
    ['@conf/', 'src/conf/'],
    ['@conf', 'src/conf'],
    ['@graphql/', 'src/graphql/'],
    ['@graphql-mock/', 'src/graphql-mock/'],
    ['@providers/', 'src/providers/'],
    ['@utils/', 'src/utils/'],
    ['@src/', 'src/'],
    ['@types', 'src/types.generated.ts'],
];

function walk(dir: string, out: string[] = []): string[] {
    fs.readdirSync(dir, { withFileTypes: true }).forEach((entry) => {
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) {
            if (entry.name !== 'node_modules' && entry.name !== '__tests__') walk(full, out);
        } else if (
            /\.(ts|tsx)$/.test(entry.name) &&
            !entry.name.endsWith('.d.ts') &&
            !/\.(test|stories|mock)\./.test(entry.name)
        ) {
            out.push(full);
        }
    });
    return out;
}

function resolveSpec(fromFile: string, spec: string): string | null {
    if (!spec || spec.startsWith('data:') || spec.includes('?')) return null;
    let base: string;
    if (spec.startsWith('.')) {
        base = path.resolve(path.dirname(fromFile), spec);
    } else {
        const alias = ALIASES.find(([prefix]) => spec === prefix.replace(/\/$/, '') || spec.startsWith(prefix));
        if (!alias) return null;
        const [prefix, dest] = alias;
        const rest = spec.slice(prefix.length);
        base = path.join(ROOT, dest + rest);
    }
    const candidates = [
        base,
        `${base}.ts`,
        `${base}.tsx`,
        `${base}.js`,
        `${base}.jsx`,
        `${base}.acryl.ts`,
        `${base}.acryl.tsx`,
        path.join(base, 'index.ts'),
        path.join(base, 'index.tsx'),
    ];
    return candidates.find((candidate) => fs.existsSync(candidate) && fs.statSync(candidate).isFile()) ?? null;
}

function stripComments(source: string): string {
    return source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/.*$/gm, '$1');
}

function parseFile(file: string): { staticSpecs: Set<string>; dynamicSpecs: Set<string> } {
    const text = stripComments(fs.readFileSync(file, 'utf8'));
    const staticSpecs = new Set<string>();
    const dynamicSpecs = new Set<string>();
    const blockRe = /\b(import|export)\s+type\b|\b(import|export)\b[\s\S]*?\bfrom\s+['"]([^'"]+)['"]/g;
    let match = blockRe.exec(text);
    while (match) {
        if (!match[1] && match[3]) staticSpecs.add(match[3]);
        match = blockRe.exec(text);
    }
    // `import '@src/i18n/i18n'` has no `from` clause. It is still a runtime edge.
    const sideEffectRe = /\bimport\s+['"]([^'"]+)['"]/g;
    match = sideEffectRe.exec(text);
    while (match) {
        if (match[1]) staticSpecs.add(match[1]);
        match = sideEffectRe.exec(text);
    }
    const dynamicRe = /\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)/g;
    match = dynamicRe.exec(text);
    while (match) {
        if (match[1]) dynamicSpecs.add(match[1]);
        match = dynamicRe.exec(text);
    }
    return { staticSpecs, dynamicSpecs };
}

function cyclesCrossingDynamicImports(): string[] {
    const files = walk(SRC);
    const staticEdges = new Map<string, string[]>();
    const dynamicEdges: Array<[string, string, string]> = [];

    files.forEach((file) => {
        const { staticSpecs, dynamicSpecs } = parseFile(file);
        const staticTargets: string[] = [];
        staticSpecs.forEach((spec) => {
            const resolved = resolveSpec(file, spec);
            if (resolved && resolved !== file) staticTargets.push(resolved);
        });
        staticEdges.set(file, staticTargets);
        dynamicSpecs.forEach((spec) => {
            const resolved = resolveSpec(file, spec);
            if (resolved && resolved !== file) dynamicEdges.push([file, resolved, spec]);
        });
    });

    const allEdges = new Map<string, string[]>();
    const add = (from: string, to: string) => {
        const next = allEdges.get(from);
        if (next) next.push(to);
        else allEdges.set(from, [to]);
    };
    staticEdges.forEach((targets, from) => targets.forEach((to) => add(from, to)));
    dynamicEdges.forEach(([from, to]) => add(from, to));

    const pathFrom = (start: string, goal: string): string[] | null => {
        const queue = [start];
        const prev = new Map<string, string | null>([[start, null]]);
        while (queue.length) {
            const node = queue.shift();
            if (!node) break;
            if (node === goal) {
                const out: string[] = [];
                let cursor: string | null = node;
                while (cursor) {
                    out.push(cursor);
                    cursor = prev.get(cursor) ?? null;
                }
                return out.reverse();
            }
            (allEdges.get(node) || []).forEach((next) => {
                if (!prev.has(next)) {
                    prev.set(next, node);
                    queue.push(next);
                }
            });
        }
        return null;
    };

    return dynamicEdges.flatMap(([from, to, spec]) => {
        const back = pathFrom(to, from);
        if (!back) return [];
        const cycle = [from, ...back].map((file) => path.relative(ROOT, file));
        return [`${path.relative(ROOT, from)} -> ${spec}\n  ${cycle.join('\n  ')}`];
    });
}

describe('dynamic import boundaries', () => {
    it('records a side-effect import as a static edge', () => {
        const parsed = parseFile(path.join(SRC, 'index.tsx'));
        expect(parsed.staticSpecs.has('@src/i18n/i18n')).toBe(true);
        expect(parsed.staticSpecs.has('@utils/dayjs')).toBe(true);
    });

    it('has no import cycle that crosses a dynamic import()', () => {
        const crossing = cyclesCrossingDynamicImports();
        if (crossing.length > 0) {
            throw new Error(
                `A cycle through import() is a chunk cycle. modulepreload can evaluate either side first and throw ReferenceError on the route that loads that chunk.\n${crossing.join('\n\n')}`,
            );
        }
        expect(crossing).toEqual([]);
    });
});
