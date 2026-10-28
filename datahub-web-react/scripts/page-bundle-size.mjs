// Logged-in page gzip for the Frontend Preview comment.
// index.html is the login module graph. The shell, /search, and home rows walk
// the Vite manifest and do not follow dynamicImports.
//
// Gzip matches eager-budget.mjs (Node zlib level 9). Without --check this script
// only reports. --check compares shell and /search to page-budget.json: byte
// over/under is a non-blocking warning; a missing route fails the job.

import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

import {
    budgetExitCode,
    classifyGzipDelta,
    emptyFindings,
    gzipSize,
    printBudgetFindings,
} from './eager-budget.mjs';

// Page modules are Vite manifest `src` values (project-relative), not hashed files.
const AUTHENTICATED_APP_SRC = 'src/app/AuthenticatedApp.tsx';
const SEARCH_ROUTES_SRC = 'src/app/SearchRoutes.tsx';
const SEARCH_PAGE_SRC = 'src/app/searchV2/SearchPage.tsx';
const HOME_PAGE_SRC = 'src/app/homeV3/HomePage.tsx';
const APP_PROVIDERS_SRC = 'src/app/AppProviders.tsx';

export const PAGE_ROWS = [
    { id: 'indexHtml', label: 'index.html (/login)' },
    { id: 'shell', label: 'Logged-in shell' },
    { id: 'search', label: '/search' },
    { id: 'home', label: '/ (home)' },
];

function normalizeAssetPath(assetPath) {
    return assetPath.replace(/^\.\//, '').replace(/^\//, '');
}

function srcMatches(actual, expected) {
    if (!actual) return false;
    return actual.split('?')[0] === expected;
}

/** First `<script type="module" src>` in index.html. The manifest has no isEntry. */
export function moduleScriptSrc(html) {
    const scriptRe = /<script\b([^>]*)>/gi;
    let match;
    while ((match = scriptRe.exec(html)) !== null) {
        const attrs = match[1];
        if (!/\btype\s*=\s*["']module["']/i.test(attrs)) continue;
        const src = /\bsrc\s*=\s*["']([^"']+)["']/i.exec(attrs);
        if (src) return normalizeAssetPath(src[1]);
    }
    return null;
}

/**
 * Script and modulepreload JS, same set eager-budget.mjs records from index.html.
 * Used to confirm the manifest walk still matches today's login number.
 */
export function eagerJsFilesFromHtml(html) {
    const files = new Set();
    const add = (src) => files.add(normalizeAssetPath(src));
    for (const match of html.matchAll(/<script[^>]*\ssrc="([^"]+\.js)"/g)) add(match[1]);
    for (const match of html.matchAll(/<link[^>]*rel="modulepreload"[^>]*>/g)) {
        const href = match[0].match(/href="([^"]+\.js)"/);
        if (href) add(href[1]);
    }
    return files;
}

export function findManifestKeyByFile(manifest, file) {
    const wanted = normalizeAssetPath(file);
    for (const [key, chunk] of Object.entries(manifest)) {
        if (chunk && typeof chunk.file === 'string' && normalizeAssetPath(chunk.file) === wanted) return key;
    }
    return null;
}

export function findManifestKeyBySrc(manifest, src) {
    for (const [key, chunk] of Object.entries(manifest)) {
        if (!chunk) continue;
        if (srcMatches(chunk.src, src) || srcMatches(key, src)) return key;
    }
    return null;
}

// Some lazy parents (AuthenticatedApp, SearchRoutes) are shared chunks: the manifest
// key is `_Name-<hash>.js`, `src` is absent, and `name` is stable. `HomePage` is not
// unique across v2/v3, so page modules must be found by `src` and must not use this.
function findManifestKeyByUniqueName(manifest, name) {
    let found = null;
    for (const [key, chunk] of Object.entries(manifest)) {
        if (!chunk || chunk.name !== name) continue;
        if (typeof chunk.file !== 'string' || !chunk.file.endsWith('.js')) continue;
        if (found) return null;
        found = key;
    }
    return found;
}

function findRouteChunk(manifest, src, name) {
    const bySrc = src ? findManifestKeyBySrc(manifest, src) : null;
    if (bySrc) return bySrc;
    return name ? findManifestKeyByUniqueName(manifest, name) : null;
}

function resolveManifestKey(manifest, ref) {
    if (!ref) return null;
    if (manifest[ref]) return ref;
    return findManifestKeyByFile(manifest, ref);
}

/** Static `imports` only. `dynamicImports` stay out of the closure. */
export function staticClosureFiles(manifest, key, seen = new Set(), files = new Set()) {
    if (!key || seen.has(key)) return files;
    const chunk = manifest[key];
    if (!chunk) return files;
    seen.add(key);
    if (typeof chunk.file === 'string' && chunk.file.endsWith('.js')) {
        files.add(normalizeAssetPath(chunk.file));
    }
    for (const imported of chunk.imports || []) {
        staticClosureFiles(manifest, resolveManifestKey(manifest, imported), seen, files);
    }
    return files;
}

function unionFiles(manifest, keys) {
    if (keys.some((key) => !key)) return null;
    const seen = new Set();
    const files = new Set();
    for (const key of keys) staticClosureFiles(manifest, key, seen, files);
    return files;
}

function totalBytes(files, sizeOf) {
    let total = 0;
    for (const file of files) total += sizeOf(file);
    return total;
}

function singleChunkFile(manifest, src, name) {
    const key = findRouteChunk(manifest, src, name);
    if (!key) return null;
    const file = manifest[key].file;
    return typeof file === 'string' && file.endsWith('.js') ? normalizeAssetPath(file) : null;
}

/**
 * Gzip bytes of JS loaded before first paint.
 * `shell` / `search` / `home` are null when a route module is absent (older base builds).
 *
 * @param {Record<string, { file?: string, src?: string, name?: string, imports?: string[] }>} manifest
 * @param {string} entryFile `file` of the index.html module script
 * @param {(file: string) => number} sizeOf
 */
export function measureManifest(manifest, entryFile, sizeOf) {
    const entryKey = findManifestKeyByFile(manifest, entryFile);
    if (!entryKey) {
        throw new Error(`Module script ${entryFile} is not in the Vite manifest`);
    }

    const authKey = findRouteChunk(manifest, AUTHENTICATED_APP_SRC, 'AuthenticatedApp');
    const searchRoutesKey = findRouteChunk(manifest, SEARCH_ROUTES_SRC, 'SearchRoutes');
    // Page modules are found by src. `name` is HomePage for both homeV2 and homeV3.
    const searchPageKey = findRouteChunk(manifest, SEARCH_PAGE_SRC, null);
    const homeKey = findRouteChunk(manifest, HOME_PAGE_SRC, null);

    // Logged-in shell is the entry plus the AuthenticatedApp lazy step and static imports.
    // /search adds SearchRoutes and SearchPage. / adds homeV3. Other dynamicImports are ignored.
    const indexFiles = unionFiles(manifest, [entryKey]);
    const shellFiles = unionFiles(manifest, [entryKey, authKey]);
    const searchFiles = unionFiles(manifest, [entryKey, authKey, searchRoutesKey, searchPageKey]);
    const homeFiles = unionFiles(manifest, [entryKey, authKey, homeKey]);
    const appProvidersFile = singleChunkFile(manifest, APP_PROVIDERS_SRC, 'AppProviders');

    return {
        indexHtml: totalBytes(indexFiles, sizeOf),
        shell: shellFiles ? totalBytes(shellFiles, sizeOf) : null,
        search: searchFiles ? totalBytes(searchFiles, sizeOf) : null,
        home: homeFiles ? totalBytes(homeFiles, sizeOf) : null,
        appProviders: appProvidersFile ? sizeOf(appProvidersFile) : null,
        files: {
            indexHtml: [...indexFiles],
            shell: shellFiles ? [...shellFiles] : null,
            search: searchFiles ? [...searchFiles] : null,
            home: homeFiles ? [...homeFiles] : null,
        },
    };
}

export function measureDist(distDir) {
    const manifestPath = path.join(distDir, '.vite', 'manifest.json');
    const indexPath = path.join(distDir, 'index.html');
    if (!fs.existsSync(manifestPath)) {
        throw new Error(`No Vite manifest at ${manifestPath}`);
    }
    if (!fs.existsSync(indexPath)) {
        throw new Error(`No index.html at ${indexPath}`);
    }
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    const html = fs.readFileSync(indexPath, 'utf8');
    const entryFile = moduleScriptSrc(html);
    if (!entryFile) {
        throw new Error(`No module script in ${indexPath}`);
    }
    const measured = measureManifest(manifest, entryFile, (file) => gzipSize(path.join(distDir, file)));
    const eagerFiles = eagerJsFilesFromHtml(html);
    measured.indexHtmlEagerBytes = totalBytes(eagerFiles, (file) => gzipSize(path.join(distDir, file)));
    const manifestFiles = new Set(measured.files.indexHtml);
    measured.eagerMismatch = {
        onlyHtml: [...eagerFiles].filter((file) => !manifestFiles.has(file)),
        onlyManifest: [...manifestFiles].filter((file) => !eagerFiles.has(file)),
    };
    return measured;
}

function fmtBytes(bytes) {
    if (bytes == null) return 'n/a';
    if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(2)} MB`;
    return `${Math.round(bytes / 1024)} KB`;
}

function fmtDiff(prBytes, baseBytes) {
    if (prBytes == null || baseBytes == null) return 'n/a';
    const diff = prBytes - baseBytes;
    const sign = diff >= 0 ? '+' : '-';
    const abs = Math.abs(diff);
    const kb = Math.round(abs / 1024);
    const pct = baseBytes === 0 ? '0.0' : ((abs / baseBytes) * 100).toFixed(1);
    return `${sign}${kb} KB (${sign}${pct}%)`;
}

function iconFor(diff) {
    if (diff == null) return '➡️';
    if (diff > 2048) return '🔺';
    if (diff < -2048) return '🟢';
    return '➡️';
}

function isFiniteNumber(value) {
    return typeof value === 'number' && Number.isFinite(value);
}

const PAGE_BUDGET_IDS = ['shell', 'search'];

/**
 * Same slack / 10% cliff as the index.html eager budget.
 * Home is reported and not gated. Byte over/under past slack is soft until the cliff.
 * A missing route or broken baseline fails closed (hard).
 *
 * @param {{ shell: number | null, search: number | null }} measured
 * @param {{ shell: number, search: number }} baseline
 * @returns {import('./eager-budget.mjs').BudgetFindings}
 */
export function evaluatePageBudget(measured, baseline) {
    const findings = emptyFindings();
    for (const id of PAGE_BUDGET_IDS) {
        const actual = measured[id];
        const expected = baseline[id];
        if (!isFiniteNumber(expected) || !isFiniteNumber(actual)) {
            findings.hard.push(
                `page-budget.json ${id} must be a finite number, and the build must include that route. ` +
                    `Without it the size comparison does not run.`,
            );
            continue;
        }
        classifyGzipDelta({
            label: `Logged-in ${id} JS`,
            actual,
            expected,
            ratchetHint: `Update datahub-web-react/page-budget.json ${id} to ${actual} so the lower size becomes the new ceiling.`,
            findings,
        });
    }
    return findings;
}

export function formatBundleComment({ base, pr, baseSha }) {
    const short = (baseSha || 'unknown').slice(0, 7);
    // The title icon still tracks login. The table rows carry the logged-in diffs.
    const icon = iconFor(base.indexHtml == null || pr.indexHtml == null ? null : pr.indexHtml - base.indexHtml);
    const lines = [
        `## Initial JS bundle size ${icon}`,
        '',
        `| | Base (\`${short}\`) | This PR | Diff |`,
        '|---|---:|---:|---:|',
    ];
    for (const row of PAGE_ROWS) {
        lines.push(
            `| ${row.label} | ${fmtBytes(base[row.id])} | ${fmtBytes(pr[row.id])} | **${fmtDiff(pr[row.id], base[row.id])}** |`,
        );
    }
    lines.push(
        '',
        '> index.html is the login page; logged-in pages add the shell. Gzip of the JS loaded before first paint. Codecov reports total bundle including lazy chunks — these intentionally differ.',
    );
    return lines.join('\n');
}

function parseArgs(argv) {
    const args = { verbose: false };
    for (let i = 0; i < argv.length; i += 1) {
        const arg = argv[i];
        if (arg === '--verbose') {
            args.verbose = true;
        } else if (arg === '--check') {
            args.check = true;
        } else if (arg === '--dist' || arg === '--base-dist' || arg === '--pr-dist' || arg === '--base-sha') {
            args[arg.slice(2)] = argv[i + 1];
            i += 1;
        }
    }
    return args;
}

function summarize(measured) {
    const ids = ['indexHtml', 'shell', 'search', 'home', 'appProviders'];
    const bytes = {};
    const kib = {};
    for (const id of ids) {
        bytes[id] = measured[id];
        kib[id] = measured[id] == null ? null : Math.round(measured[id] / 1024);
    }
    const summary = {
        bytes,
        kib,
        indexHtmlEagerBytes: measured.indexHtmlEagerBytes ?? null,
        eagerMismatch: measured.eagerMismatch ?? null,
    };
    if (measured.verboseFiles) summary.files = measured.files;
    return summary;
}

function main() {
    const args = parseArgs(process.argv.slice(2));
    if (args['base-dist'] || args['pr-dist']) {
        if (!args['base-dist'] || !args['pr-dist'] || !args['base-sha']) {
            console.error('Usage: page-bundle-size.mjs --base-dist DIR --pr-dist DIR --base-sha SHA');
            process.exit(2);
        }
        const base = measureDist(args['base-dist']);
        const pr = measureDist(args['pr-dist']);
        for (const [label, measured] of [
            ['base', base],
            ['pr', pr],
        ]) {
            const mismatch = measured.eagerMismatch;
            if (mismatch && (mismatch.onlyHtml.length || mismatch.onlyManifest.length)) {
                console.error(
                    `${label} index.html manifest closure differs from script+modulepreload ` +
                        `(html-only ${mismatch.onlyHtml.length}, manifest-only ${mismatch.onlyManifest.length})`,
                );
            }
        }
        console.log(formatBundleComment({ base, pr, baseSha: args['base-sha'] }));
        return;
    }
    if (!args.dist) {
        console.error('Usage: page-bundle-size.mjs --dist DIR [--verbose]');
        process.exit(2);
    }
    const measured = measureDist(args.dist);
    if (args.verbose) measured.verboseFiles = true;
    const mismatch = measured.eagerMismatch;
    if (mismatch && (mismatch.onlyHtml.length || mismatch.onlyManifest.length)) {
        console.error(
            `index.html manifest closure is ${measured.indexHtml} bytes; script+modulepreload gzip is ${measured.indexHtmlEagerBytes}`,
        );
    }
    console.log(JSON.stringify(summarize(measured), null, 2));
    if (!args.check) return;
    const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
    const baseline = JSON.parse(fs.readFileSync(path.join(root, 'page-budget.json'), 'utf8'));
    const findings = evaluatePageBudget(measured, baseline);
    printBudgetFindings(findings);
    const exitCode = budgetExitCode(false, findings);
    if (exitCode !== 0) {
        process.exit(exitCode);
    }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    main();
}
