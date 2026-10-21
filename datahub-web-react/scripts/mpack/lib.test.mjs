import assert from 'node:assert/strict';
import test from 'node:test';

import {
    BOOT_SCRIPT,
    affectedModules,
    barePath,
    changedPaths,
    chunkModules,
    endsChunk,
    injectBoot,
    isPackableUrl,
    isScriptContentType,
    merkleRoot,
    sha256,
    stripInlineSourceMap,
    viteWsToken,
} from './lib.mjs';

const mod = (url, size = 10) => ({ url, hash: sha256(url), ctype: 'text/javascript', gz: Buffer.alloc(size) });

test('strips only inline source maps', () => {
    assert.equal(stripInlineSourceMap('a();\n//# sourceMappingURL=data:application/json;base64,AAAA'), 'a();');
    assert.equal(stripInlineSourceMap('a();\n//# sourceMappingURL=a.js.map'), 'a();\n//# sourceMappingURL=a.js.map');
});

test('recognises script content types', () => {
    assert.equal(isScriptContentType('text/javascript; charset=utf-8'), true);
    assert.equal(isScriptContentType('application/typescript'), true);
    assert.equal(isScriptContentType('text/css'), false);
    assert.equal(isScriptContentType(undefined), false);
});

test('packs app modules but not Vite internals, HMR copies or its own endpoints', () => {
    assert.equal(isPackableUrl('/src/app/App.tsx'), true);
    assert.equal(isPackableUrl('/node_modules/.vite/deps/react.js?v=1234'), true);
    assert.equal(isPackableUrl('/@vite/client'), false);
    assert.equal(isPackableUrl('/__mpack/sw.js'), false);
    assert.equal(isPackableUrl('/src/a.tsx?t=1700000000000'), false);
    assert.equal(barePath('/src/a.tsx?import'), '/src/a.tsx');
});

test('chunk boundaries depend only on the module path', () => {
    const cutting = Array.from({ length: 2000 }, (_, i) => `/src/m${i}.ts`).find((u) => endsChunk(u));
    assert.ok(cutting, 'some path ends a chunk');
    assert.equal(endsChunk(cutting), true);
    assert.equal(endsChunk(cutting, 1), true);
});

test('cuts chunks at content-defined boundaries and at the size cap', () => {
    const modules = [mod('/a'), mod('/b'), mod('/c'), mod('/d', 50)];
    const chunks = chunkModules(modules, { cut: (url) => url === '/b', maxBytes: 40 });
    assert.deepEqual(
        chunks.map((c) => c.mods.map((m) => m[0])),
        [
            ['/a', '/b'],
            ['/c', '/d'],
        ],
    );
    assert.equal(chunks[0].hash, sha256(sha256('/a') + sha256('/b')));
    assert.equal(chunks[1].parts.length, 2);
    assert.deepEqual(chunks[0].mods[0], ['/a', sha256('/a'), 10, 'text/javascript']);
    assert.deepEqual(chunkModules([]), []);
    assert.equal(chunkModules([mod('/x')]).length, 1); // default boundary rule and cap
});

test('editing one module changes only its own chunk', () => {
    const urls = Array.from({ length: 400 }, (_, i) => `/src/m${i}.ts`);
    const before = chunkModules(urls.map((u) => mod(u)));
    const edited = urls.map((u) => (u === '/src/m200.ts' ? { ...mod(u, 99), hash: 'changed' } : mod(u)));
    const after = chunkModules(edited);
    const changed = after.filter((c, i) => c.hash !== before[i].hash);
    assert.equal(before.length, after.length);
    assert.equal(changed.length, 1);
});

test('computes a Merkle root over leaf hashes', () => {
    assert.equal(merkleRoot([]), '');
    assert.equal(merkleRoot(['a']), 'a');
    assert.equal(merkleRoot(['a', 'b']), sha256('ab'));
    assert.equal(merkleRoot(['a', 'b', 'c']), sha256(sha256('ab') + sha256('cc')));
});

test('finds changed modules and their importers', () => {
    const order = ['/src/a.ts', '/src/b.ts?import', '/src/c.ts', '/src/d.ts'];
    const bodies = new Map([
        ['/src/a.ts', 'export const a = 1;'],
        ['/src/c.ts', "import { b } from '/src/b.ts';"],
        ['/src/d.ts', 'nothing'],
    ]);
    assert.deepEqual([...affectedModules(order, bodies, new Set(['/src/b.ts']))], ['/src/b.ts?import', '/src/c.ts']);
});

test('reads changed paths from Vite HMR messages', () => {
    const update = {
        type: 'update',
        updates: [{ path: '/src/a.ts', acceptedPath: '/src/a.ts' }, { path: '/src/b.ts' }],
    };
    assert.deepEqual([...changedPaths(update)], ['/src/a.ts', '/src/b.ts']);
    assert.deepEqual([...changedPaths({ type: 'update' })], []);
    assert.deepEqual([...changedPaths({ type: 'full-reload', path: '/src/c.ts' })], ['/src/c.ts']);
    assert.equal(changedPaths({ type: 'full-reload', path: '*' }), null);
    assert.equal(changedPaths({ type: 'full-reload' }), null);
    assert.equal(changedPaths({ type: 'connected' }), null);
});

test("reads the HMR socket token from Vite's client", () => {
    assert.equal(viteWsToken('const wsToken = "abc_123";'), 'abc_123');
    assert.equal(viteWsToken('no token here'), '');
});

test('injects the service-worker bootstrap into HTML', () => {
    assert.equal(
        injectBoot('<html><head><title>x</title></head></html>'),
        `<html><head>${BOOT_SCRIPT}<title>x</title></head></html>`,
    );
    assert.equal(injectBoot('<p>fragment</p>'), `${BOOT_SCRIPT}<p>fragment</p>`);
});
