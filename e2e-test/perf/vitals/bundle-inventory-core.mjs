#!/usr/bin/env node
/**
 * Eager JS bundle inventory — attribution, reachability, and live-origin helpers.
 */
import fs from 'node:fs';
import http from 'node:http';
import https from 'node:https';
import path from 'node:path';
import zlib from 'node:zlib';

export const GZIP_LEVEL = 9;
export const APP_SOURCE = 'app-source';
const UNMAPPED = '[unmapped]';

export function gzipSize(buffer) {
    return zlib.gzipSync(buffer, { level: GZIP_LEVEL }).length;
}

// ---------------------------------------------------------------------------
// Layer 1: index.html + whole-file sizes
// ---------------------------------------------------------------------------

/** Script and modulepreload JS, in document order, de-duplicated. Same set as eager-budget.mjs. */
export function eagerJsRefs(parsed) {
    return [...new Set([...parsed.scripts, ...parsed.modulepreload].filter((s) => s.endsWith('.js')))];
}

/**
 * Resolve an asset ref the way the browser does: against `<base href>` when the
 * page has one. An unsubstituted `@basePath` placeholder is treated as `/`.
 */
export function resolveAssetUrl(pageUrl, html, ref) {
    if (/^https?:/.test(ref)) return ref;
    const baseMatch = /<base\b[^>]*\bhref="([^"]*)"/i.exec(html);
    let base = baseMatch?.[1] || '/';
    if (base.includes('@')) base = '/';
    return new URL(ref.replace(/^\.\//, ''), new URL(base, pageUrl)).href;
}

/** Bearer token only when the request stays on the measured origin. */
export function bearerFor(url, tokenOrigin, token) {
    if (!token || !tokenOrigin) return null;
    try {
        if (new URL(url).origin !== new URL(tokenOrigin).origin) return null;
    } catch {
        return null;
    }
    return `Bearer ${token}`;
}

export function parseIndexHtml(html) {
    const scripts = [];
    const scriptRe = /<script\b([^>]*)>/g;
    let m;
    while ((m = scriptRe.exec(html)) !== null) {
        const src = /\bsrc="([^"]+)"/.exec(m[1]);
        if (src) scripts.push(src[1]);
    }
    const modulepreload = [];
    const stylesheets = [];
    const linkRe = /<link\b([^>]*)>/g;
    while ((m = linkRe.exec(html)) !== null) {
        const rel = /\brel="([^"]+)"/.exec(m[1])?.[1];
        const href = /\bhref="([^"]+)"/.exec(m[1])?.[1];
        if (!href) continue;
        if (rel === 'modulepreload') modulepreload.push(href);
        if (rel === 'stylesheet') stylesheets.push(href);
    }
    return { scripts, modulepreload, stylesheets };
}

function assetName(ref) {
    return ref.replace(/^\.?\//, '').replace(/^assets\//, '');
}

function fileRecord(filePath) {
    const data = fs.readFileSync(filePath);
    return { file: path.basename(filePath), raw_bytes: data.length, gzip_bytes: gzipSize(data) };
}

function sum(records, key) {
    return records.reduce((acc, r) => acc + r[key], 0);
}

export function inventoryDist(distDir) {
    const indexHtml = fs.readFileSync(path.join(distDir, 'index.html'), 'utf8');
    const parsed = parseIndexHtml(indexHtml);
    const assetsDir = path.join(distDir, 'assets');
    const eagerJs = eagerJsRefs(parsed).map(assetName);
    const eagerCss = parsed.stylesheets.filter((s) => s.endsWith('.css')).map(assetName);
    const eagerSet = new Set(eagerJs);
    const allJs = fs
        .readdirSync(assetsDir)
        .filter((f) => f.endsWith('.js'))
        .sort();
    const asyncJs = allJs.filter((f) => !eagerSet.has(f));

    const eagerRecs = eagerJs.map((f) => fileRecord(path.join(assetsDir, f)));
    const cssRecs = eagerCss.map((f) => fileRecord(path.join(assetsDir, f)));
    const asyncRecs = asyncJs.map((f) => fileRecord(path.join(assetsDir, f)));

    return {
        index_html: {
            scripts: parsed.scripts,
            modulepreload: parsed.modulepreload,
            stylesheets: parsed.stylesheets,
        },
        eager_js: {
            files: eagerRecs,
            count: eagerRecs.length,
            raw_bytes: sum(eagerRecs, 'raw_bytes'),
            gzip_bytes: sum(eagerRecs, 'gzip_bytes'),
        },
        eager_css: {
            files: cssRecs,
            raw_bytes: sum(cssRecs, 'raw_bytes'),
            gzip_bytes: sum(cssRecs, 'gzip_bytes'),
        },
        async_js: {
            count: asyncRecs.length,
            raw_bytes: sum(asyncRecs, 'raw_bytes'),
            gzip_bytes: sum(asyncRecs, 'gzip_bytes'),
            largest: [...asyncRecs].sort((a, b) => b.gzip_bytes - a.gzip_bytes).slice(0, 40),
        },
        all_js: {
            count: allJs.length,
            raw_bytes: sum(eagerRecs, 'raw_bytes') + sum(asyncRecs, 'raw_bytes'),
            gzip_bytes: sum(eagerRecs, 'gzip_bytes') + sum(asyncRecs, 'gzip_bytes'),
        },
    };
}

// ---------------------------------------------------------------------------
// Layer 2: source-map attribution
// ---------------------------------------------------------------------------

const B64 = new Map(
    [...'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'].map((c, i) => [c, i]),
);

function readVlq(s, i) {
    let result = 0;
    let shift = 0;
    let pos = i;
    for (;;) {
        const digit = B64.get(s[pos]);
        if (digit === undefined) throw new Error(`bad VLQ char at ${pos}`);
        pos += 1;
        result |= (digit & 31) << shift;
        shift += 5;
        if ((digit & 32) === 0) break;
    }
    const negative = (result & 1) === 1;
    const value = result >>> 1;
    return [negative ? -value : value, pos];
}

/**
 * Collapse a module path to an attribution key: an npm package name
 * (scoped packages keep their scope), `app-source` for first-party code, or
 * a prefixed passthrough for anything else (virtual modules, plugin output).
 */
export function packageOf(rawSource) {
    // Rollup's commonjs plugin emits proxy ids as "\0<path>?commonjs-proxy"; the
    // path inside still identifies the package.
    const source = rawSource.replace(/^\0/, '').replace(/\?.*$/, '');
    if (source.startsWith('virtual:') || source.startsWith('[')) return `other:${rawSource}`;
    const idx = source.lastIndexOf('/node_modules/');
    let rest;
    if (idx !== -1) {
        rest = source.slice(idx + '/node_modules/'.length);
    } else if (source.startsWith('node_modules/')) {
        rest = source.slice('node_modules/'.length);
    } else {
        if (/(^|\/)src\//.test(source) || source.includes('/datahub_agents/') || source.startsWith('datahub_agents/')) {
            return APP_SOURCE;
        }
        return `other:${source}`;
    }
    const parts = rest.split('/');
    if (parts[0].startsWith('@') && parts.length > 1) return `${parts[0]}/${parts[1]}`;
    return parts[0];
}

/**
 * Second-level key for first-party code: `src/app/<area>`, `src/<dir>`, or
 * `datahub_agents/<agent>`. Used to explain where the `source` chunk's bytes
 * come from.
 */
export function appAreaOf(source) {
    const agents = /(?:^|\/)datahub_agents\/([^/]+)/.exec(source);
    if (agents) return `datahub_agents/${agents[1]}`;
    const m = /(?:^|\/)src\/(.*)$/.exec(source);
    if (!m) return 'src';
    const parts = m[1].split('/');
    if (parts[0] === 'app' && parts.length > 2) return `src/app/${parts[1]}`;
    if (parts.length > 1) return `src/${parts[0]}`;
    return 'src';
}

/**
 * Walk a chunk's source map and concatenate the generated bytes mapped to each
 * key. Returns byte blobs per key so the caller can gzip each blob once.
 * Vite's minified output is ASCII, so generated columns equal byte offsets.
 */
export function attributeChunk(code, sourceMap, keyFn = packageOf) {
    const text = code.toString('latin1');
    const lines = text.split('\n');
    const lineStarts = [];
    let off = 0;
    for (const line of lines) {
        lineStarts.push(off);
        off += line.length + 1;
    }
    const genOffset = (lineIdx, col) => {
        if (lineIdx >= lineStarts.length) return code.length;
        return lineStarts[lineIdx] + Math.min(col, lines[lineIdx].length);
    };

    const { sources, mappings } = sourceMap;
    const blobs = new Map(); // key -> Buffer[]
    const rawSizes = new Map();
    const sourceFiles = new Map(); // key -> Set<source>
    const push = (key, from, to, src) => {
        if (to <= from) return;
        const slice = code.subarray(from, to);
        if (!blobs.has(key)) {
            blobs.set(key, []);
            rawSizes.set(key, 0);
            sourceFiles.set(key, new Set());
        }
        blobs.get(key).push(slice);
        rawSizes.set(key, rawSizes.get(key) + slice.length);
        if (src) sourceFiles.get(key).add(src);
    };

    let genLine = 0;
    let genCol = 0;
    let srcIdx = 0;
    let prevOff = 0;
    let prevKey = null;
    let prevSrc = null;
    let segments = 0;
    let i = 0;
    const n = mappings.length;
    while (i < n) {
        const ch = mappings[i];
        if (ch === ';') {
            genLine += 1;
            genCol = 0;
            i += 1;
            continue;
        }
        if (ch === ',') {
            i += 1;
            continue;
        }
        let d;
        [d, i] = readVlq(mappings, i);
        genCol += d;
        let hasSrc = false;
        if (i < n && mappings[i] !== ',' && mappings[i] !== ';') {
            [d, i] = readVlq(mappings, i);
            srcIdx += d;
            if (i < n && mappings[i] !== ',' && mappings[i] !== ';') [, i] = readVlq(mappings, i); // src line
            if (i < n && mappings[i] !== ',' && mappings[i] !== ';') [, i] = readVlq(mappings, i); // src col
            if (i < n && mappings[i] !== ',' && mappings[i] !== ';') [, i] = readVlq(mappings, i); // name
            hasSrc = true;
        }
        const offNow = genOffset(genLine, genCol);
        if (prevKey !== null) push(prevKey, prevOff, offNow, prevSrc);
        if (hasSrc && srcIdx >= 0 && srcIdx < sources.length) {
            prevSrc = sources[srcIdx];
            prevKey = keyFn(prevSrc);
        } else {
            prevSrc = null;
            prevKey = UNMAPPED;
        }
        prevOff = offNow;
        segments += 1;
    }
    if (prevKey !== null && prevOff < code.length) push(prevKey, prevOff, code.length, prevSrc);

    const rows = new Map();
    for (const [key, parts] of blobs) {
        rows.set(key, {
            raw_bytes: rawSizes.get(key),
            gzip_bytes: gzipSize(Buffer.concat(parts)),
            source_file_count: sourceFiles.get(key).size,
            sources: sourceFiles.get(key),
            blob: Buffer.concat(parts),
        });
    }
    return { rows, segments, sourceCount: sources.length };
}

function rankCombined(perChunk, eagerGzipTotal, { sampleSources = 6, keepBlobs = false } = {}) {
    const combined = new Map();
    for (const [chunkFile, { rows }] of perChunk) {
        for (const [key, row] of rows) {
            if (!combined.has(key)) {
                combined.set(key, { blobs: [], raw_bytes: 0, sources: new Set(), chunks: new Set() });
            }
            const c = combined.get(key);
            c.blobs.push(row.blob);
            c.raw_bytes += row.raw_bytes;
            row.sources.forEach((s) => c.sources.add(s));
            if (row.gzip_bytes > 0) c.chunks.add(chunkFile);
        }
    }
    const ranked = [];
    for (const [key, c] of combined) {
        const blob = Buffer.concat(c.blobs);
        const gz = gzipSize(blob);
        ranked.push({
            package: key,
            gzip_bytes: gz,
            raw_bytes: c.raw_bytes,
            share_of_eager_gzip: eagerGzipTotal ? Number((gz / eagerGzipTotal).toFixed(6)) : 0,
            source_file_count: c.sources.size,
            chunks: [...c.chunks].sort(),
            sample_sources: [...c.sources].sort().slice(0, sampleSources).map(shortId),
            ...(keepBlobs ? { blob } : {}),
        });
    }
    ranked.sort((a, b) => b.gzip_bytes - a.gzip_bytes);
    return ranked;
}

export function attributeEager(distDir, eagerFiles) {
    const assetsDir = path.join(distDir, 'assets');
    const perChunkPkg = new Map();
    const perChunkArea = new Map();
    const chunkMeta = [];
    let eagerGzip = 0;
    for (const file of eagerFiles) {
        const jsPath = path.join(assetsDir, file);
        const mapPath = `${jsPath}.map`;
        if (!fs.existsSync(mapPath)) {
            const code = fs.readFileSync(jsPath);
            const gz = gzipSize(code);
            eagerGzip += gz;
            chunkMeta.push({ file, source_map: false, raw_bytes: code.length, gzip_bytes: gz });
            process.stderr.write(
                `bundle-inventory: no source map for eager file ${file}; counted in the eager total, omitted from the package table\n`,
            );
            continue;
        }
        const code = fs.readFileSync(jsPath);
        const map = JSON.parse(fs.readFileSync(mapPath, 'utf8'));
        const pkg = attributeChunk(code, map, packageOf);
        const area = attributeChunk(code, map, (s) => (packageOf(s) === APP_SOURCE ? appAreaOf(s) : '[node_modules]'));
        perChunkPkg.set(file, pkg);
        perChunkArea.set(file, area);
        const gz = gzipSize(code);
        eagerGzip += gz;
        chunkMeta.push({
            file,
            source_map: true,
            raw_bytes: code.length,
            gzip_bytes: gz,
            mapping_segments: pkg.segments,
            sources: pkg.sourceCount,
        });
    }

    const packages = rankCombined(perChunkPkg, eagerGzip);
    const appAreas = rankCombined(perChunkArea, eagerGzip).filter((r) => r.package !== '[node_modules]');

    const perChunk = {};
    for (const [file, { rows }] of perChunkPkg) {
        perChunk[file] = Object.fromEntries(
            [...rows.entries()]
                .sort((a, b) => b[1].gzip_bytes - a[1].gzip_bytes)
                .map(([key, row]) => [
                    key,
                    { raw_bytes: row.raw_bytes, gzip_bytes: row.gzip_bytes, source_file_count: row.source_file_count },
                ]),
        );
    }

    return {
        method:
            'Package gzip = Node zlib level 9 of the concatenation of the generated byte ranges a chunk source map ' +
            'attributes to that package, one blob per package across all eager chunks. Shares use the ' +
            'whole-file eager gzip total as denominator and do not sum to 1.0 (per-blob gzip pays its own ' +
            'header/dictionary cost).',
        chunks: chunkMeta,
        eager_whole_file_gzip_bytes: eagerGzip,
        sum_of_package_gzip_bytes: packages.reduce((a, r) => a + r.gzip_bytes, 0),
        packages,
        app_source_areas: appAreas,
        per_chunk: perChunk,
    };
}

// ---------------------------------------------------------------------------
// Layer 3: reachability from the Rollup module graph
// ---------------------------------------------------------------------------

function normaliseModuleId(id) {
    // Rollup ids can carry `?query` suffixes (e.g. svgr, ?url) — strip for attribution.
    const q = id.indexOf('?');
    return q === -1 ? id : id.slice(0, q);
}

export function shortId(id) {
    if (!id) return '—';
    const clean = normaliseModuleId(id);
    const nm = clean.lastIndexOf('/node_modules/');
    if (nm !== -1) return clean.slice(nm + '/node_modules/'.length);
    const src = clean.search(/(^|\/)(src|datahub_agents)\//);
    return src === -1 ? clean : clean.slice(src + (clean[src] === '/' ? 1 : 0));
}

/**
 * Reduce a module chain to the last first-party module followed by one hop
 * per distinct package, e.g.
 *   src/alchemy-components/components/CodeBlock/CodeBlock.tsx → react-syntax-highlighter → refractor
 */
export function collapseChain(chain) {
    const out = [];
    let lastApp = null;
    for (const id of chain) {
        if (/\.html$/.test(normaliseModuleId(id))) continue; // Rollup HTML entry, not a module
        const key = packageOf(normaliseModuleId(id));
        if (key === APP_SOURCE) {
            lastApp = id;
            continue;
        }
        if (lastApp) {
            out.push(shortId(lastApp));
            lastApp = null;
        }
        if (out[out.length - 1] !== key) out.push(key);
    }
    if (lastApp) out.push(shortId(lastApp));
    return out;
}

/**
 * Classify every module in the eager chunks as statically reachable from an
 * entry or reachable only through dynamic `import()`. Returns per-package
 * rendered-byte totals for both classes plus importer evidence.
 */
export function classifyReachability(graph, eagerChunkFiles) {
    const byId = new Map(graph.modules.map((m) => [m.id, m]));
    const dynamicImporters = new Map();
    const staticImporters = new Map();
    for (const m of graph.modules) {
        for (const target of m.importedIds) {
            if (!staticImporters.has(target)) staticImporters.set(target, []);
            staticImporters.get(target).push(m.id);
        }
        for (const target of m.dynamicallyImportedIds) {
            if (!dynamicImporters.has(target)) dynamicImporters.set(target, []);
            dynamicImporters.get(target).push(m.id);
        }
    }

    // BFS (not DFS) so `parent` yields a shortest static import chain from an
    // entry to each module; that chain is the "why is this eager" evidence.
    const staticReach = new Set();
    const parent = new Map();
    const depth = new Map();
    const queue = graph.modules.filter((m) => m.isEntry).map((m) => m.id);
    for (const id of queue) {
        staticReach.add(id);
        depth.set(id, 0);
    }
    let head = 0;
    while (head < queue.length) {
        const id = queue[head];
        head += 1;
        const m = byId.get(id);
        if (!m) continue;
        for (const next of m.importedIds) {
            if (staticReach.has(next)) continue;
            staticReach.add(next);
            parent.set(next, id);
            depth.set(next, depth.get(id) + 1);
            queue.push(next);
        }
    }
    const staticChain = (id) => {
        const chain = [id];
        let cur = id;
        while (parent.has(cur)) {
            cur = parent.get(cur);
            chain.push(cur);
        }
        return chain.reverse();
    };

    const eagerSet = new Set(eagerChunkFiles);
    const eagerChunks = graph.chunks.filter((c) => eagerSet.has(path.basename(c.fileName)));

    const perPackage = new Map();
    const dynamicOnlyModules = [];
    for (const chunk of eagerChunks) {
        for (const [id, rendered] of Object.entries(chunk.modules)) {
            const key = packageOf(normaliseModuleId(id));
            if (!perPackage.has(key)) {
                perPackage.set(key, {
                    package: key,
                    rendered_bytes: 0,
                    static_rendered_bytes: 0,
                    dynamic_only_rendered_bytes: 0,
                    module_count: 0,
                    dynamic_only_module_count: 0,
                    chunks: new Set(),
                    app_static_importers: new Set(),
                    app_dynamic_importers: new Set(),
                });
            }
            const row = perPackage.get(key);
            row.rendered_bytes += rendered;
            row.module_count += 1;
            row.chunks.add(path.basename(chunk.fileName));
            const isStatic = staticReach.has(id);
            if (isStatic) {
                row.static_rendered_bytes += rendered;
                const d = depth.get(id);
                if (row.shallowest === undefined || d < row.shallowest_depth) {
                    row.shallowest = id;
                    row.shallowest_depth = d;
                }
            } else {
                row.dynamic_only_rendered_bytes += rendered;
                row.dynamic_only_module_count += 1;
                dynamicOnlyModules.push({ id, package: key, chunk: path.basename(chunk.fileName), rendered });
            }
            for (const imp of staticImporters.get(id) ?? []) {
                if (packageOf(normaliseModuleId(imp)) === APP_SOURCE && key !== APP_SOURCE) {
                    row.app_static_importers.add(imp);
                }
            }
            for (const imp of dynamicImporters.get(id) ?? []) {
                if (packageOf(normaliseModuleId(imp)) === APP_SOURCE) row.app_dynamic_importers.add(imp);
            }
        }
    }

    // Walk the module graph "up" from a dynamic-only module to the first
    // dynamic import site, so the report can name the app module that owns it.
    const dynamicEntryFor = (startId) => {
        const seen = new Set();
        const stack = [startId];
        while (stack.length) {
            const id = stack.pop();
            if (seen.has(id)) continue;
            seen.add(id);
            const dyn = dynamicImporters.get(id);
            if (dyn && dyn.length) return { module: shortId(id), importers: dyn.slice(0, 3).map(shortId) };
            for (const parent of staticImporters.get(id) ?? []) stack.push(parent);
        }
        return null;
    };

    const packages = [...perPackage.values()]
        .map((row) => {
            const dynSample = dynamicOnlyModules
                .filter((m) => m.package === row.package)
                .sort((a, b) => b.rendered - a.rendered)
                .slice(0, 3)
                .map((m) => ({ module: shortId(m.id), rendered: m.rendered, dynamic_entry: dynamicEntryFor(m.id) }));
            return {
                package: row.package,
                rendered_bytes: row.rendered_bytes,
                static_rendered_bytes: row.static_rendered_bytes,
                static_chain: row.shallowest ? collapseChain(staticChain(row.shallowest)) : null,
                dynamic_only_rendered_bytes: row.dynamic_only_rendered_bytes,
                dynamic_only_share: row.rendered_bytes
                    ? Number((row.dynamic_only_rendered_bytes / row.rendered_bytes).toFixed(4))
                    : 0,
                module_count: row.module_count,
                dynamic_only_module_count: row.dynamic_only_module_count,
                chunks: [...row.chunks].sort(),
                app_static_importers: [...row.app_static_importers].sort().slice(0, 5).map(shortId),
                app_dynamic_importers: [...row.app_dynamic_importers].sort().slice(0, 5).map(shortId),
                dynamic_only_samples: dynSample,
            };
        })
        .sort((a, b) => b.rendered_bytes - a.rendered_bytes);

    return {
        method:
            'static = module is reachable from a Rollup entry through static imports only; dynamic-only = ' +
            'module sits in an eager chunk but every path from an entry crosses an import(). Bytes are ' +
            "Rollup's renderedLength, recorded before esbuild minify, so they are not minified bytes. " +
            "Gzip for a package's dynamic-only share is estimated proportionally in the Markdown.",
        entry_modules: graph.modules.filter((m) => m.isEntry).map((m) => shortId(m.id)),
        eager_chunks: eagerChunks.map((c) => c.fileName),
        static_reachable_module_count: staticReach.size,
        total_module_count: graph.modules.length,
        packages,
    };
}

// ---------------------------------------------------------------------------
// Live origin mode
// ---------------------------------------------------------------------------

/**
 * Raw HTTP GET that keeps the encoded body so the on-the-wire size is known even
 * when the server streams with chunked transfer encoding (no Content-Length).
 * `fetch` decompresses transparently and hides that number.
 */
function fetchBytes(url, { token, tokenOrigin, redirects = 5 } = {}) {
    return new Promise((resolve, reject) => {
        const headers = { 'accept-encoding': 'gzip', 'user-agent': 'datahub-bundle-inventory' };
        const bearer = bearerFor(url, tokenOrigin, token);
        if (bearer) headers.authorization = bearer;
        const req = (url.startsWith('https:') ? https : http).get(url, { headers }, (res) => {
            const status = res.statusCode ?? 0;
            if (status >= 300 && status < 400 && res.headers.location) {
                res.resume();
                if (redirects === 0) {
                    reject(new Error(`${url}: too many redirects`));
                    return;
                }
                resolve(
                    fetchBytes(new URL(res.headers.location, url).href, { token, tokenOrigin, redirects: redirects - 1 }),
                );
                return;
            }
            if (status < 200 || status >= 300) {
                res.resume();
                reject(new Error(`${url} -> HTTP ${status}`));
                return;
            }
            const parts = [];
            res.on('data', (d) => parts.push(d));
            res.on('error', reject);
            res.on('end', () => {
                const encoded = Buffer.concat(parts);
                const encoding = res.headers['content-encoding'] ?? null;
                let body = encoded;
                if (encoding === 'gzip') body = zlib.gunzipSync(encoded);
                else if (encoding === 'br') body = zlib.brotliDecompressSync(encoded);
                else if (encoding === 'deflate') body = zlib.inflateSync(encoded);
                resolve({
                    body,
                    finalUrl: url,
                    content_encoding: encoding,
                    transfer_bytes: encoded.length,
                    etag: res.headers.etag ?? null,
                    last_modified: res.headers['last-modified'] ?? null,
                });
            });
        });
        req.on('error', reject);
    });
}

export async function inventoryOrigin(origin, { token } = {}) {
    const base = origin.replace(/\/$/, '');
    const tokenOrigin = new URL(base).origin;
    const page = await fetchBytes(`${base}/login`, { token, tokenOrigin });
    const html = page.body.toString('utf8');
    const parsed = parseIndexHtml(html);
    const files = [];
    for (const ref of eagerJsRefs(parsed)) {
        const url = resolveAssetUrl(page.finalUrl, html, ref);
        const r = await fetchBytes(url, { token, tokenOrigin });
        files.push({
            file: assetName(ref),
            url,
            raw_bytes: r.body.length,
            gzip_bytes: gzipSize(r.body),
            transfer_bytes: r.transfer_bytes,
            content_encoding: r.content_encoding,
            etag: r.etag,
            last_modified: r.last_modified,
        });
    }
    // Play's /config is served anonymously and carries the deployed appVersion.
    let appVersion = null;
    try {
        const cfg = JSON.parse((await fetchBytes(`${base}/config`, { token, tokenOrigin })).body.toString('utf8'));
        appVersion = cfg?.config?.appVersion ?? null;
    } catch {
        appVersion = null;
    }
    return {
        origin: base,
        app_version: appVersion,
        document_url: page.finalUrl,
        index_html: { scripts: parsed.scripts, modulepreload: parsed.modulepreload, stylesheets: parsed.stylesheets },
        eager_js: {
            files,
            count: files.length,
            raw_bytes: sum(files, 'raw_bytes'),
            gzip_bytes: sum(files, 'gzip_bytes'),
            transfer_bytes: files.every((f) => f.transfer_bytes !== null) ? sum(files, 'transfer_bytes') : null,
        },
        note:
            'gzip_bytes is Node zlib level 9 of the decoded body (comparable to local builds); transfer_bytes is the ' +
            "server's Content-Length for the encoded response (what the browser downloads).",
    };
}

// ---------------------------------------------------------------------------
// Markdown
// ---------------------------------------------------------------------------

