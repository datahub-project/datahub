import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';
import zlib from 'node:zlib';

const SW = path.join(path.dirname(fileURLToPath(import.meta.url)), 'sw.js');
const ORIGIN = 'http://localhost:3010';

// An in-memory Cache Storage shared by every worker started in a test (it outlives the worker).
function cacheStorage() {
    const store = new Map();
    const cache = {
        match: async (key) => (store.has(key) ? store.get(key).clone() : undefined),
        put: async (key, response) => {
            store.set(key, response);
        },
    };
    return { store, caches: { open: async () => cache } };
}

// A pack server: chunk bytes are the gzipped module bodies back to back, like mpack's.
function packServer(modules, { failRoot = false } = {}) {
    const state = { requests: [] };
    state.set = (mods) => {
        const chunks = mods.map((group, i) => {
            const parts = group.map(([, body]) => zlib.gzipSync(body));
            return {
                hash: `c${i}-${group.map(([u, b]) => u + b).join()}`,
                bytes: Buffer.concat(parts),
                mods: group.map(([url], j) => [url, `h-${url}-${group[j][1]}`, parts[j].length, 'text/javascript']),
            };
        });
        state.chunks = chunks;
        state.root = chunks.map((c) => c.hash).join('|');
    };
    state.set(modules);
    state.fetch = async (input) => {
        const url = typeof input === 'string' ? input : input.url;
        state.requests.push(url);
        if (url === '/__mpack/root') {
            if (failRoot) throw new Error('offline');
            return new Response(state.root);
        }
        if (url === '/__mpack/index') {
            return Response.json({ root: state.root, chunks: state.chunks.map(({ hash, mods }) => ({ hash, mods })) });
        }
        if (url.startsWith('/__mpack/c/')) return new Response(state.chunks.find((c) => url.endsWith(c.hash)).bytes);
        return new Response(`network:${url}`);
    };
    return state;
}

// Starts a fresh worker (as a browser would) and returns a way to dispatch events to it.
function startWorker(storage, server, warnings = []) {
    const handlers = {};
    const calls = { skipWaiting: 0, claim: 0 };
    const self = {
        location: { origin: ORIGIN },
        addEventListener: (type, fn) => {
            handlers[type] = fn;
        },
        skipWaiting: () => {
            calls.skipWaiting += 1;
        },
        clients: {
            claim: async () => {
                calls.claim += 1;
            },
        },
    };
    const context = vm.createContext({
        self,
        caches: storage.caches,
        fetch: server.fetch,
        Response,
        Blob,
        DecompressionStream,
        URL,
        Map,
        Set,
        JSON,
        Uint8Array,
        Promise,
        Array,
        console: { warn: (...a) => warnings.push(a) },
    });
    new vm.Script(fs.readFileSync(SW, 'utf8'), { filename: SW }).runInContext(context);
    const dispatch = (type, event) => handlers[type](event);
    const request = async (pathname, mode = 'cors') => {
        let responded = null;
        dispatch('fetch', {
            request: { url: `${ORIGIN}${pathname}`, mode },
            respondWith: (p) => {
                responded = p;
            },
        });
        return responded ? (await responded).text() : null;
    };
    return { dispatch, request, calls };
}

const MODULES = [
    [
        ['/src/a.ts', 'A1'],
        ['/src/b.ts', 'B1'],
    ],
    [['/src/c.ts', 'C1']],
];

test('takes control on install and activate', async () => {
    const worker = startWorker(cacheStorage(), packServer(MODULES));
    worker.dispatch('install', {});
    let waited = null;
    worker.dispatch('activate', {
        waitUntil: (p) => {
            waited = p;
        },
    });
    await waited;
    assert.deepEqual(worker.calls, { skipWaiting: 1, claim: 1 });
});

test('leaves other origins, its own endpoints and pages to the network', async () => {
    const worker = startWorker(cacheStorage(), packServer(MODULES));
    let responded = false;
    worker.dispatch('fetch', {
        request: { url: 'https://cdn.example.com/x.js', mode: 'cors' },
        respondWith: () => (responded = true),
    });
    worker.dispatch('fetch', {
        request: { url: `${ORIGIN}/__mpack/root`, mode: 'cors' },
        respondWith: () => (responded = true),
    });
    assert.equal(await worker.request('/login', 'navigate'), null);
    assert.equal(responded, false);
});

test('fills the cache on the first visit and serves modules from it', async () => {
    const storage = cacheStorage();
    const server = packServer(MODULES);
    const worker = startWorker(storage, server);
    await worker.request('/login', 'navigate');
    assert.equal(await worker.request('/src/a.ts'), 'A1');
    assert.equal(await worker.request('/src/c.ts'), 'C1');
    assert.equal(await worker.request('/src/unknown.ts'), 'network:http://localhost:3010/src/unknown.ts');
    assert.deepEqual(
        server.requests.filter((u) => u.startsWith('/__mpack/c/')).length,
        2,
        'both chunks downloaded once',
    );
});

test('a revisit with the same root downloads nothing', async () => {
    const storage = cacheStorage();
    const server = packServer(MODULES);
    const first = startWorker(storage, server);
    await first.request('/login', 'navigate');
    await first.request('/src/a.ts'); // module requests wait for the update to finish
    server.requests.length = 0;
    const worker = startWorker(storage, server);
    await worker.request('/login', 'navigate');
    assert.equal(await worker.request('/src/b.ts'), 'B1');
    assert.deepEqual(server.requests, ['/__mpack/root']);
});

test('after an edit only the changed chunk downloads, and the new version is served', async () => {
    const storage = cacheStorage();
    const server = packServer(MODULES);
    const worker = startWorker(storage, server);
    await worker.request('/login', 'navigate');
    assert.equal(await worker.request('/src/c.ts'), 'C1');
    server.set([MODULES[0], [['/src/c.ts', 'C2']]]);
    server.requests.length = 0;
    await worker.request('/login', 'navigate');
    assert.equal(await worker.request('/src/c.ts'), 'C2', 'never the stale version');
    assert.deepEqual(
        server.requests.filter((u) => u.startsWith('/__mpack/c/')),
        [`/__mpack/c/${server.chunks[1].hash}`],
    );
});

test('falls back to the network when a cached module is missing', async () => {
    const storage = cacheStorage();
    const worker = startWorker(storage, packServer(MODULES));
    await worker.request('/login', 'navigate');
    await worker.request('/src/b.ts'); // wait for the update to finish
    storage.store.delete([...storage.store.keys()].find((k) => k.includes('/src/a.ts')));
    assert.equal(await worker.request('/src/a.ts'), 'network:http://localhost:3010/src/a.ts');
});

test('serves from the network when an update fails', async () => {
    const warnings = [];
    const worker = startWorker(cacheStorage(), packServer(MODULES, { failRoot: true }), warnings);
    await worker.request('/login', 'navigate');
    assert.equal(await worker.request('/src/a.ts'), 'network:http://localhost:3010/src/a.ts');
    assert.equal(warnings.length, 1);
});

test('a worker restarted mid-page updates once before serving', async () => {
    const storage = cacheStorage();
    const server = packServer(MODULES);
    const worker = startWorker(storage, server);
    const [a, b] = await Promise.all([worker.request('/src/a.ts'), worker.request('/src/b.ts')]);
    assert.deepEqual([a, b], ['A1', 'B1']);
    assert.equal(server.requests.filter((u) => u === '/__mpack/root').length, 1);
});

test('an empty pack needs no downloads', async () => {
    const server = packServer([]);
    const worker = startWorker(cacheStorage(), server);
    await worker.request('/login', 'navigate');
    assert.equal(await worker.request('/src/a.ts'), 'network:http://localhost:3010/src/a.ts');
    assert.deepEqual(
        server.requests,
        ['/__mpack/root', '/__mpack/index', '/src/a.ts'].map((u, i) => (i === 2 ? `${ORIGIN}${u}` : u)),
    );
});
