/* mpack service worker (see README.md). Keeps dev modules in Cache Storage by content hash. On each
   page load it asks for the pack root (one tiny request); only when that changed does it fetch the
   chunk index and download the chunks it does not have, then serves every module from the cache. */
/* eslint-disable no-restricted-globals */

const CACHE = 'mpack-v1';
let byUrl = null; // url -> { hash, ctype }
let syncing = null; // the update started by the current navigation

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

async function readState(cache) {
    const r = await cache.match('/__mpack/state');
    return r ? r.json() : null;
}

async function sync() {
    const root = await (await fetch('/__mpack/root', { cache: 'no-store' })).text();
    const cache = await caches.open(CACHE);
    const held = await readState(cache);
    if (held && held.root === root) {
        byUrl = new Map(held.map);
        return;
    }
    const index = await (await fetch('/__mpack/index', { cache: 'no-store' })).json();
    const have = new Set(held ? held.chunks : []);
    const missing = index.chunks.filter((c) => !have.has(c.hash));
    let next = 0;
    const worker = async () => {
        while (next < missing.length) {
            const chunk = missing[next];
            next += 1;
            const buf = new Uint8Array(await (await fetch(`/__mpack/c/${chunk.hash}`)).arrayBuffer());
            let off = 0;
            await Promise.all(
                chunk.mods.map(([, hash, len, ctype]) => {
                    const part = buf.subarray(off, off + len);
                    off += len;
                    const body = new Blob([part]).stream().pipeThrough(new DecompressionStream('gzip'));
                    return new Response(body)
                        .arrayBuffer()
                        .then((ab) =>
                            cache.put(`/__mpack/h/${hash}`, new Response(ab, { headers: { 'content-type': ctype } })),
                        );
                }),
            );
        }
    };
    await Promise.all(Array.from({ length: Math.min(6, missing.length) }, worker));
    const map = index.chunks.flatMap((c) => c.mods.map(([url, hash, , ctype]) => [url, { hash, ctype }]));
    await cache.put(
        '/__mpack/state',
        new Response(JSON.stringify({ root: index.root, map, chunks: index.chunks.map((c) => c.hash) })),
    );
    byUrl = new Map(map);
}

const warn = (err) => console.warn('mpack: update failed, serving from the dev server', err);

async function respond(request, key) {
    // Wait for the update this navigation started before choosing a version, or a module that just
    // changed could be served from the previous map. A worker restarted mid-page starts one itself.
    if (!syncing) syncing = sync().catch(warn);
    await syncing;
    const hit = byUrl && byUrl.get(key);
    if (hit) {
        const cached = await (await caches.open(CACHE)).match(`/__mpack/h/${hit.hash}`);
        if (cached) return cached;
    }
    return fetch(request);
}

self.addEventListener('fetch', (e) => {
    const url = new URL(e.request.url);
    if (url.origin !== self.location.origin || url.pathname.startsWith('/__mpack/')) return;
    if (e.request.mode === 'navigate') {
        syncing = sync().catch(warn);
        return; // the page itself comes from the network
    }
    e.respondWith(respond(e.request, url.pathname + url.search));
});
