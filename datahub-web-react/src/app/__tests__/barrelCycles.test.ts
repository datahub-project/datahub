import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';

/**
 * Rollup splits a component and the barrel that reexports it into different
 * chunks when the component's static graph also reaches that barrel. The live
 * binding is then undefined at runtime (TypeError on .includes / .displayName).
 * Named manualChunks used to hide this by forcing one chunk. With default
 * splitting, the cycle has to be gone.
 */
const WEB_REACT_ROOT = path.resolve(__dirname, '../../..');
const SRC = path.join(WEB_REACT_ROOT, 'src');

const ALIASES: Array<[string, string]> = [
    ['@components', path.join(SRC, 'alchemy-components')],
    ['@app', path.join(SRC, 'app')],
    ['@conf', path.join(SRC, 'conf')],
    ['@graphql-mock', path.join(SRC, 'graphql-mock')],
    ['@graphql', path.join(SRC, 'graphql')],
    ['@images', path.join(SRC, 'images')],
    ['@providers', path.join(SRC, 'providers')],
    ['@utils', path.join(SRC, 'utils')],
    ['@src', SRC],
    ['@types', path.join(SRC, 'types.generated.ts')],
];

function resolveSpecifier(fromFile: string, spec: string): string | null {
    if (spec.startsWith('.')) return path.resolve(path.dirname(fromFile), spec);
    let resolved: string | null = null;
    ALIASES.forEach(([alias, target]) => {
        if (resolved) return;
        if (spec === alias) resolved = target;
        else if (spec.startsWith(`${alias}/`)) resolved = path.join(target, spec.slice(alias.length + 1));
    });
    return resolved;
}

function materialize(abs: string): string | null {
    const file = ['', '.ts', '.tsx', '.js', '.jsx']
        .map((ext) => abs + ext)
        .find((candidate) => fs.existsSync(candidate) && fs.statSync(candidate).isFile());
    if (file) return file;
    if (!fs.existsSync(abs) || !fs.statSync(abs).isDirectory()) return null;
    return (
        ['index.ts', 'index.tsx', 'index.js']
            .map((name) => path.join(abs, name))
            .find((candidate) => fs.existsSync(candidate)) || null
    );
}

function staticImports(file: string): string[] {
    const text = fs.readFileSync(file, 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
    const specs: string[] = [];
    const re = /(?:^|\n)\s*(import|export)\s+(type\s+)?[^'";]*?from\s+['"]([^'"]+)['"]/g;
    let match = re.exec(text);
    while (match) {
        if (!match[2]) specs.push(match[3]);
        match = re.exec(text);
    }
    return specs;
}

function walk(dir: string, out: Map<string, string[]>): void {
    fs.readdirSync(dir, { withFileTypes: true }).forEach((ent) => {
        if (ent.name === 'node_modules' || ent.name === '.docs') return;
        const full = path.join(dir, ent.name);
        if (ent.isDirectory()) {
            walk(full, out);
            return;
        }
        if (!/\.(ts|tsx)$/.test(ent.name) || ent.name.endsWith('.d.ts')) return;
        out.set(
            full,
            staticImports(full)
                .map((spec) => materialize(resolveSpecifier(full, spec) || ''))
                .filter((resolved): resolved is string => resolved != null),
        );
    });
}

function shortest(graph: Map<string, string[]>, start: string, goal: string): string[] | null {
    const queue: string[][] = [[start]];
    const seen = new Set([start]);
    while (queue.length) {
        const current = queue.shift();
        if (!current) break;
        const edges = graph.get(current[current.length - 1]) || [];
        for (let i = 0; i < edges.length; i += 1) {
            const next = edges[i];
            if (!seen.has(next)) {
                const pathNodes = current.concat(next);
                if (next === goal) return pathNodes;
                seen.add(next);
                queue.push(pathNodes);
            }
        }
    }
    return null;
}

describe('unnamed chunks: barrel cycles stay in one initialization order', () => {
    it('does not let a reexported component depend on its own barrel', () => {
        const graph = new Map<string, string[]>();
        walk(SRC, graph);

        const cycles: string[] = [];
        const visitIndexes = (dir: string) => {
            fs.readdirSync(dir, { withFileTypes: true }).forEach((ent) => {
                const full = path.join(dir, ent.name);
                if (ent.isDirectory()) {
                    visitIndexes(full);
                    return;
                }
                if (!/^index\.tsx?$/.test(ent.name)) return;
                const text = fs.readFileSync(full, 'utf8');
                const re = /export\s+(?:type\s+)?(?:\{[^}]*\}|\*)\s+from\s+['"](\.[^'"]+)['"]/g;
                let match = re.exec(text);
                while (match) {
                    const target = materialize(resolveSpecifier(full, match[1]) || '');
                    if (target && graph.has(target)) {
                        const cycle = shortest(graph, target, full);
                        if (cycle) cycles.push(cycle.map((file) => path.relative(SRC, file)).join(' -> '));
                    }
                    match = re.exec(text);
                }
            });
        };
        visitIndexes(path.join(SRC, 'alchemy-components/components'));

        expect(cycles).toEqual([]);
    });
});
