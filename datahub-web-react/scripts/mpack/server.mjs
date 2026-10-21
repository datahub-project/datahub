// mpack server: sits in front of a Vite dev server and serves its modules to a service worker as
// content-addressed chunks (see README.md). Everything that is not a module passes through to Vite.
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';

import {
    affectedModules,
    barePath,
    changedPaths,
    chunkModules,
    injectBoot,
    isPackableUrl,
    isScriptContentType,
    merkleRoot,
    sha256,
    stripInlineSourceMap,
    viteWsToken,
} from './lib.mjs';

const SW_FILE = path.join(path.dirname(fileURLToPath(import.meta.url)), 'sw.js');
const HOP_BY_HOP = new Set(['connection', 'keep-alive', 'transfer-encoding', 'upgrade', 'te']);

// GET `url` from the dev server: { status, ctype, body }.
export function httpFetcher(viteUrl, agent = new http.Agent({ keepAlive: true, maxSockets: 64 })) {
    const { hostname, port } = new URL(viteUrl);
    return (url) =>
        new Promise((resolve, reject) => {
            http.get({ hostname, port, path: url, agent }, (r) => {
                const parts = [];
                r.on('data', (d) => parts.push(d));
                r.on('end', () =>
                    resolve({
                        status: r.statusCode,
                        ctype: r.headers['content-type'] || '',
                        body: Buffer.concat(parts),
                    }),
                );
            }).on('error', reject);
        });
}

export function createMpack({
    viteUrl,
    workDir,
    modules: initialModules = [],
    fetchModule = httpFetcher(viteUrl),
    WebSocketImpl = globalThis.WebSocket,
    log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a),
    learnDelayMs = 500,
}) {
    const vite = new URL(viteUrl);
    const chunkDir = path.join(workDir, 'chunks');
    fs.mkdirSync(chunkDir, { recursive: true });

    const order = [...initialModules]; // load order: the linearization chunks are cut from
    const mods = new Map(); // url -> { ctype, body, gz, hash }; only changed ones are re-read
    const listeners = new Set(); // open /__mpack/events streams
    let closed = false;
    let reconnectTimer = null;
    let pack = { root: '', chunks: [], byHash: new Map(), indexBr: zlib.brotliCompressSync('{}') };

    async function ingest(urls) {
        let next = 0;
        const worker = async () => {
            while (next < urls.length) {
                const url = urls[next++];
                const r = await fetchModule(url);
                if (r.status !== 200) {
                    mods.delete(url);
                    continue;
                }
                const body = isScriptContentType(r.ctype)
                    ? Buffer.from(stripInlineSourceMap(r.body.toString('utf8')))
                    : r.body;
                mods.set(url, { ctype: r.ctype, body, gz: zlib.gzipSync(body), hash: sha256(body) });
            }
        };
        await Promise.all(Array.from({ length: Math.min(64, urls.length) }, worker));
    }

    // Chunks and root from the cached modules; a chunk file is written only when its hash is new.
    function layout(why, t0) {
        const live = order.filter((url) => mods.has(url)).map((url) => ({ url, ...mods.get(url) }));
        const chunks = chunkModules(live);
        let fresh = 0;
        for (const c of chunks) {
            c.file = path.join(chunkDir, c.hash);
            if (!fs.existsSync(c.file)) {
                fs.writeFileSync(c.file, Buffer.concat(c.parts));
                fresh += 1;
            }
            delete c.parts;
        }
        const previous = pack.root;
        const root = merkleRoot(chunks.map((c) => c.hash));
        const index = JSON.stringify({ root, chunks: chunks.map(({ hash, mods: m }) => ({ hash, mods: m })) });
        pack = {
            root,
            chunks,
            byHash: new Map(chunks.map((c) => [c.hash, c])),
            indexBr: zlib.brotliCompressSync(index, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 4 } }),
        };
        if (root !== previous) for (const res of listeners) res.write(`data: ${root}\n\n`);
        log(
            `${why}: ${live.length} modules in ${chunks.length} chunks (${fresh} new), root ${root.slice(0, 12)}, ${Date.now() - t0} ms`,
        );
    }

    async function build() {
        const t0 = Date.now();
        await ingest(order);
        layout('build', t0);
    }

    // One rebuild at a time; changes that arrive meanwhile are batched into the next one.
    let running = null;
    const pending = new Set();
    async function rebuild(paths, why) {
        for (const p of paths) pending.add(p);
        if (running) return running;
        running = (async () => {
            while (pending.size) {
                const t0 = Date.now();
                const batch = new Set(pending);
                pending.clear();
                for (const p of batch) if (!order.some((u) => barePath(u) === p)) order.push(p);
                const bodies = new Map([...mods].map(([url, m]) => [url, m.body.toString('utf8')]));
                const hit = affectedModules(order, bodies, batch);
                await ingest([...hit]);
                layout(`${why} (${batch.size} path(s), ${hit.size} module(s) re-read)`, t0);
            }
        })();
        try {
            await running;
        } finally {
            running = null;
        }
        return undefined;
    }

    // Modules the browser fetches that the list lacks are learned and packed for the next load.
    const learning = new Set();
    let learnTimer = null;
    function learn(url) {
        if (mods.has(url) || learning.has(url) || !isPackableUrl(url)) return;
        learning.add(url);
        clearTimeout(learnTimer);
        learnTimer = setTimeout(async () => {
            const t0 = Date.now();
            const urls = [...learning];
            learning.clear();
            for (const u of urls) if (!order.includes(u)) order.push(u);
            await ingest(urls);
            layout(`learned ${urls.length} module(s)`, t0);
        }, learnDelayMs);
    }

    // Rebuild as soon as Vite reports a change: it has invalidated the module by then, so the re-read
    // gets new code, with no settle delay.
    async function watchVite({ retryMs = 1000 } = {}) {
        const client = await fetchModule('/@vite/client');
        const token = viteWsToken(client.body.toString('utf8'));
        const ws = new WebSocketImpl(`ws://${vite.host}/?token=${encodeURIComponent(token)}`, 'vite-hmr');
        ws.onopen = () => log('following Vite HMR events');
        // Vite restarts drop the socket; reconnect (with a fresh token) until mpack is closed.
        ws.onclose = () => {
            if (closed) return;
            log(`Vite HMR socket closed; reconnecting in ${retryMs} ms`);
            reconnectTimer = setTimeout(() => watchVite({ retryMs }).catch(() => ws.onclose()), retryMs);
        };
        ws.onmessage = (e) => {
            const message = JSON.parse(e.data);
            const paths = changedPaths(message);
            if (paths) rebuild(paths, 'vite update');
            else if (message.type === 'full-reload') build();
        };
        return ws;
    }

    function handle(req, res) {
        const url = req.url;
        if (url === '/__mpack/sw.js') {
            // The worker lives under /__mpack/ but must control the whole app.
            res.writeHead(200, {
                'content-type': 'text/javascript',
                'cache-control': 'no-cache',
                'service-worker-allowed': '/',
            });
            return fs.createReadStream(SW_FILE).pipe(res);
        }
        if (url === '/__mpack/root') {
            res.writeHead(200, { 'content-type': 'text/plain', 'cache-control': 'no-store' });
            return res.end(pack.root);
        }
        if (url === '/__mpack/index') {
            res.writeHead(200, {
                'content-type': 'application/json',
                'content-encoding': 'br',
                'cache-control': 'no-store',
            });
            return res.end(pack.indexBr);
        }
        if (url === '/__mpack/events') {
            res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' });
            res.write(`data: ${pack.root}\n\n`);
            listeners.add(res);
            req.on('close', () => listeners.delete(res));
            return undefined;
        }
        if (url.startsWith('/__mpack/c/')) {
            const chunk = pack.byHash.get(url.slice('/__mpack/c/'.length));
            if (!chunk) {
                res.writeHead(404);
                return res.end();
            }
            res.writeHead(200, {
                'content-type': 'application/octet-stream',
                'cache-control': 'public, max-age=31536000, immutable',
            });
            return fs.createReadStream(chunk.file).pipe(res);
        }
        return proxy(req, res);
    }

    function proxy(req, res) {
        const headers = { ...req.headers, host: vite.host };
        delete headers['accept-encoding'];
        const upstream = http.request(
            { hostname: vite.hostname, port: vite.port, method: req.method, path: req.url, headers },
            (r) => {
                const out = Object.fromEntries(Object.entries(r.headers).filter(([k]) => !HOP_BY_HOP.has(k)));
                const ctype = r.headers['content-type'] || '';
                if (r.statusCode === 200 && ctype.includes('text/html')) {
                    const parts = [];
                    r.on('data', (d) => parts.push(d));
                    r.on('end', () => {
                        const html = Buffer.from(injectBoot(Buffer.concat(parts).toString('utf8')));
                        delete out.etag;
                        res.writeHead(200, { ...out, 'content-length': html.length });
                        res.end(html);
                    });
                    return;
                }
                if (r.statusCode === 200 && req.method === 'GET' && isScriptContentType(ctype)) learn(req.url);
                res.writeHead(r.statusCode, out);
                r.pipe(res);
            },
        );
        upstream.on('error', (e) => {
            if (!res.headersSent) res.writeHead(502);
            res.end(String(e));
        });
        req.pipe(upstream);
    }

    return {
        build,
        rebuild,
        watchVite,
        handle,
        get root() {
            return pack.root;
        },
        get order() {
            return order;
        },
        close() {
            closed = true;
            clearTimeout(learnTimer);
            clearTimeout(reconnectTimer);
            for (const res of listeners) res.end();
            listeners.clear();
        },
    };
}
