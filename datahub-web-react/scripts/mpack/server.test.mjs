import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import zlib from 'node:zlib';

import { createMpack, httpFetcher } from './server.mjs';

const MAP = '\n//# sourceMappingURL=data:application/json;base64,AAAA';

// A stand-in Vite dev server whose files the tests can change.
async function fakeVite(files) {
    const server = http.createServer((req, res) => {
        if (req.url === '/broken') {
            req.socket.destroy();
            return;
        }
        const f = files[req.url];
        if (!f) {
            res.writeHead(404, { connection: 'close' });
            res.end('nope');
            return;
        }
        res.writeHead(f.status ?? 200, { 'content-type': f.ctype, etag: '"v1"', upgrade: 'h2c' });
        res.end(f.body);
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    return { server, url: `http://127.0.0.1:${server.address().port}` };
}

class FakeSocket {
    static last = null;

    constructor(url, protocol) {
        this.url = url;
        this.protocol = protocol;
        FakeSocket.last = this;
    }

    send(message) {
        this.onmessage({ data: JSON.stringify(message) });
    }
}

const js = (body) => ({ ctype: 'text/javascript', body });

function files() {
    return {
        '/src/a.ts': js(`export const a = 1;${MAP}`),
        '/src/b.ts': js(`import { a } from '/src/a.ts';${MAP}`),
        '/src/style.css': { ctype: 'text/css', body: 'body{}' },
        '/@vite/client': js('const wsToken = "tok";'),
        '/login': { ctype: 'text/html', body: '<html><head></head><body>login</body></html>' },
        '/later.ts': js('export const later = 1;'),
        '/teapot': { ctype: 'text/plain', body: 'short and stout', status: 418 },
    };
}

async function setup(t, extra = {}) {
    const served = files();
    const vite = await fakeVite(served);
    const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mpack-test-'));
    const logs = [];
    const mpack = createMpack({
        viteUrl: vite.url,
        workDir,
        modules: ['/src/a.ts', '/src/b.ts', '/src/style.css', '/src/gone.ts'],
        WebSocketImpl: FakeSocket,
        log: (...a) => logs.push(a.join(' ')),
        learnDelayMs: 5,
        ...extra,
    });
    const front = http.createServer(mpack.handle);
    await new Promise((resolve) => front.listen(0, '127.0.0.1', resolve));
    const base = `http://127.0.0.1:${front.address().port}`;
    t.after(() => {
        mpack.close();
        front.closeAllConnections();
        front.close();
        vite.server.closeAllConnections();
        vite.server.close();
        fs.rmSync(workDir, { recursive: true, force: true });
    });
    return { served, mpack, base, logs, workDir, viteUrl: vite.url };
}

const index = async (base) => (await fetch(`${base}/__mpack/index`)).json(); // fetch undoes the brotli encoding

test('packs the module list, without source maps, and serves chunks by hash', async (t) => {
    const { mpack, base, logs } = await setup(t);
    await mpack.build();
    assert.match(logs[0], /^build: 3 modules in \d+ chunks \(\d+ new\)/); // /src/gone.ts is a 404
    assert.equal(await (await fetch(`${base}/__mpack/root`)).text(), mpack.root);
    const idx = await index(base);
    assert.equal(idx.root, mpack.root);
    const chunk = idx.chunks[0];
    const bytes = Buffer.from(await (await fetch(`${base}/__mpack/c/${chunk.hash}`)).arrayBuffer());
    let off = 0;
    const bodies = chunk.mods.map(([, , len]) => {
        const part = zlib.gunzipSync(bytes.subarray(off, off + len)).toString();
        off += len;
        return part;
    });
    assert.ok(bodies.includes('export const a = 1;'), 'source map stripped from scripts');
    assert.ok(bodies.includes('body{}'), 'non-script modules are kept as they are');
    assert.equal((await fetch(`${base}/__mpack/c/unknown`)).status, 404);
    const sw = await fetch(`${base}/__mpack/sw.js`);
    assert.equal(sw.headers.get('service-worker-allowed'), '/', 'without it the worker cannot control the app');
    assert.match(await sw.text(), /mpack service worker/);
});

test('rebuilds only changed modules and their importers when Vite reports an update', async (t) => {
    const { served, mpack, base, logs, viteUrl } = await setup(t);
    await mpack.build();
    const before = mpack.root;
    const events = await fetch(`${base}/__mpack/events`);
    const reader = events.body.getReader();
    const first = new TextDecoder().decode((await reader.read()).value);
    assert.equal(first, `data: ${before}\n\n`);

    await mpack.watchVite();
    assert.equal(FakeSocket.last.url, `ws://${new URL(viteUrl).host}/?token=tok`);
    assert.equal(FakeSocket.last.protocol, 'vite-hmr');
    served['/src/a.ts'] = js('export const a = 2;');
    FakeSocket.last.send({ type: 'update', updates: [{ path: '/src/a.ts', acceptedPath: '/src/a.ts' }] });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.notEqual(mpack.root, before);
    assert.match(logs.at(-1), /^vite update \(1 path\(s\), 2 module\(s\) re-read\)/);
    assert.equal(new TextDecoder().decode((await reader.read()).value), `data: ${mpack.root}\n\n`);
    reader.cancel();
});

test('a change that does not move the root notifies nobody, and pages that left are forgotten', async (t) => {
    const { mpack, base, logs } = await setup(t);
    await mpack.build();
    const controller = new AbortController();
    const events = await fetch(`${base}/__mpack/events`, { signal: controller.signal });
    await events.body.getReader().read();
    controller.abort();
    await new Promise((resolve) => setTimeout(resolve, 20));
    await mpack.rebuild(new Set(['/src/style.css']), 'test');
    assert.match(logs.at(-1), /\(0 new\)/);
});

test('full reloads rebuild everything; other messages are ignored', async (t) => {
    const { served, mpack } = await setup(t);
    await mpack.build();
    await mpack.watchVite();
    const before = mpack.root;
    FakeSocket.last.send({ type: 'connected' });
    served['/src/b.ts'] = js('export const b = 2;');
    FakeSocket.last.send({ type: 'full-reload', path: '*' });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.notEqual(mpack.root, before);
});

test('batches changes that arrive during a rebuild and learns new paths', async (t) => {
    const { served, mpack } = await setup(t);
    await mpack.build();
    served['/src/new.ts'] = js('export const n = 1;');
    const one = mpack.rebuild(new Set(['/src/a.ts']), 'test');
    const two = mpack.rebuild(new Set(['/src/new.ts']), 'test');
    await Promise.all([one, two]);
    assert.ok(mpack.order.includes('/src/new.ts'));
    await mpack.rebuild(new Set(), 'nothing');
});

test('passes everything else through to Vite, injecting the bootstrap into HTML', async (t) => {
    const { mpack, base } = await setup(t);
    await mpack.build();
    const html = await fetch(`${base}/login`);
    assert.equal(html.headers.get('etag'), null);
    assert.match(await html.text(), /<head><script>.*serviceWorker.*<\/script><\/head>/);
    const teapot = await fetch(`${base}/teapot`);
    assert.equal(teapot.status, 418);
    assert.equal(teapot.headers.get('upgrade'), null);
    assert.equal(teapot.headers.get('content-type'), 'text/plain');
    assert.equal((await fetch(`${base}/missing`)).status, 404);
    const posted = await fetch(`${base}/src/a.ts`, { method: 'POST', body: 'x' });
    assert.equal(posted.status, 200);
});

test('learns modules the page fetches that the list lacks', async (t) => {
    const { mpack, base, logs } = await setup(t);
    await mpack.build();
    await (await fetch(`${base}/later.ts`)).text();
    await (await fetch(`${base}/later.ts`)).text(); // already being learned
    await (await fetch(`${base}/src/a.ts`)).text(); // already packed
    await (await fetch(`${base}/@vite/client`)).text(); // never packed
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.ok(mpack.order.includes('/later.ts'));
    assert.match(logs.at(-1), /^learned 1 module\(s\)/);
});

test('answers 502 when the dev server is unreachable', async (t) => {
    const { mpack, base } = await setup(t, { viteUrl: 'http://127.0.0.1:9' });
    const r = await fetch(`${base}/anything`);
    assert.equal(r.status, 502);
    assert.equal(mpack.root, '');
});

test('fetches modules over HTTP and reports connection failures', async () => {
    const vite = await fakeVite({ '/x.js': js('x') });
    try {
        const get = httpFetcher(vite.url);
        assert.deepEqual(await get('/x.js'), { status: 200, ctype: 'text/javascript', body: Buffer.from('x') });
        await assert.rejects(get('/broken'));
    } finally {
        vite.server.closeAllConnections();
        vite.server.close();
    }
});

test('uses the platform WebSocket and a timestamped console log by default', async (t) => {
    const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mpack-test-'));
    t.after(() => fs.rmSync(workDir, { recursive: true, force: true }));
    const logged = [];
    t.mock.method(console, 'log', (...a) => logged.push(a));
    const mpack = createMpack({ viteUrl: 'http://127.0.0.1:9', workDir, fetchModule: async () => ({ status: 404 }) });
    await mpack.build();
    assert.match(logged[0][0], /^\d\d:\d\d:\d\d\.\d{3}$/);
    assert.equal(typeof globalThis.WebSocket, 'function');
});

test('logs the HMR connection and reconnects after Vite drops it, until closed', async (t) => {
    let failNext = false;
    const { mpack, logs } = await setup(t, {
        fetchModule: async (url) => {
            if (failNext) {
                failNext = false;
                throw new Error('vite restarting');
            }
            return {
                status: url === '/@vite/client' ? 200 : 404,
                ctype: 'text/javascript',
                body: Buffer.from('const wsToken = "t";'),
            };
        },
    });
    await mpack.watchVite({ retryMs: 5 });
    const first = FakeSocket.last;
    first.onopen();
    assert.equal(logs.at(-1), 'following Vite HMR events');
    failNext = true; // the first reconnect fails; it retries
    first.onclose();
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.notEqual(FakeSocket.last, first, 'reconnected with a new socket');
    assert.match(logs.join('\n'), /Vite HMR socket closed; reconnecting in 5 ms/);
    mpack.close();
    const count = logs.length;
    FakeSocket.last.onclose();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(logs.length, count, 'no reconnect after close()');
});
