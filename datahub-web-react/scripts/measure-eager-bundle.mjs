#!/usr/bin/env node
/**
 * Gzip size of the static import closure of the Vite entry.
 * Profile UI should be dynamic imports, so it is not part of this closure.
 *
 *   node scripts/measure-eager-bundle.mjs [--check-split]
 */
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const manifestPath = join(root, 'dist/.vite/manifest.json');
const checkSplit = process.argv.includes('--check-split');

// These modules are profile UI. Logged-in search and home must not download them
// before first paint. lazyEntityProfile.tsx is the small eager wrapper, not the profile.
const FORBIDDEN_SUFFIXES = [
    '/entityV2/shared/containers/profile/EntityProfile.tsx',
    '/entityV2/shared/tabs/Lineage/LineageTab.tsx',
    '/entityV2/shared/tabs/Dataset/Schema/SchemaTab.tsx',
    '/entityV2/shared/tabs/Documentation/DocumentationTab.tsx',
    '/entityV2/shared/embed/EmbeddedProfile.tsx',
    '/lineageV3/LineageGraph.tsx',
    '/entityV2/shared/containers/profile/utils.tsx',
];

function gzipBytes(filePath) {
    return gzipSync(readFileSync(filePath), { level: 6 }).length;
}

function loadManifest() {
    if (!existsSync(manifestPath)) {
        throw new Error(`Missing Vite manifest at ${manifestPath}. Run vite build first.`);
    }
    return JSON.parse(readFileSync(manifestPath, 'utf8'));
}

function entryKey(manifest) {
    const entries = Object.keys(manifest).filter((key) => manifest[key].isEntry);
    const index = entries.find(
        (key) => key === 'index.html' || key.endsWith('/index.html') || key.endsWith('index.tsx') || key.endsWith('index.ts'),
    );
    if (!index) {
        throw new Error(`No index entry in manifest. Entries: ${entries.join(', ')}`);
    }
    return index;
}

function staticClosure(manifest, rootKey) {
    const seen = new Set();
    const walk = (key) => {
        if (!key || seen.has(key) || !manifest[key]) {
            return;
        }
        seen.add(key);
        for (const child of manifest[key].imports || []) {
            walk(child);
        }
    };
    walk(rootKey);
    return seen;
}

function isForbidden(key) {
    if (key.endsWith('/lazyEntityProfile.tsx') || key.endsWith('/lazyEntityProfile.ts')) {
        return false;
    }
    return FORBIDDEN_SUFFIXES.some((suffix) => key.endsWith(suffix));
}

const manifest = loadManifest();
const entry = entryKey(manifest);
const closure = staticClosure(manifest, entry);
const files = [];
for (const key of closure) {
    const item = manifest[key];
    for (const rel of [item.file, ...(item.css || [])]) {
        if (!rel) continue;
        const abs = join(root, 'dist', rel);
        if (!existsSync(abs)) {
            throw new Error(`Missing bundle artifact ${rel} referenced by ${key}`);
        }
        files.push({ key, rel, gzip: gzipBytes(abs), bytes: readFileSync(abs).length });
    }
}

const byRel = new Map();
for (const file of files) {
    if (!byRel.has(file.rel)) byRel.set(file.rel, file);
}
const unique = [...byRel.values()];
const js = unique.filter((file) => file.rel.endsWith('.js'));
const staticClosureGzip = js.reduce((sum, file) => sum + file.gzip, 0);
const entryItem = manifest[entry];
const entryGzip = gzipBytes(join(root, 'dist', entryItem.file));
const forbiddenInClosure = [...closure].filter(isForbidden).sort();

const report = {
    entry,
    entryFile: entryItem.file,
    entryGzip,
    staticClosureGzip,
    staticJsFiles: js.length,
    forbiddenInClosure,
    largestJs: js
        .slice()
        .sort((a, b) => b.gzip - a.gzip)
        .slice(0, 12)
        .map((file) => ({ file: file.rel, gzip: file.gzip })),
};

process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);

if (checkSplit && forbiddenInClosure.length > 0) {
    process.stderr.write(
        `Profile UI is still in the eager static closure:\n${forbiddenInClosure.join('\n')}\n`,
    );
    process.exit(1);
}
