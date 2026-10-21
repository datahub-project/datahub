import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { USAGE, main, parseCli } from './cli.mjs';

test('requires the dev server URL', () => {
    assert.throws(() => parseCli([]), { message: USAGE });
});

test('applies defaults and reads an optional starting module list', (t) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mpack-cli-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const list = path.join(dir, 'modules.json');
    fs.writeFileSync(list, JSON.stringify(['/src/index.tsx']));
    assert.deepEqual(parseCli(['--vite', 'http://127.0.0.1:3000']), {
        viteUrl: 'http://127.0.0.1:3000',
        port: 3010,
        workDir: path.join(os.tmpdir(), 'mpack'),
        modules: [],
    });
    assert.deepEqual(parseCli(['--vite', 'http://x:1', '--port', '4000', '--work-dir', dir, '--modules', list]), {
        viteUrl: 'http://x:1',
        port: 4000,
        workDir: dir,
        modules: ['/src/index.tsx'],
    });
});

test('builds, follows Vite and serves', async (t) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mpack-cli-'));
    const printed = [];
    t.mock.method(console, 'log', (...a) => printed.push(a.join(' ')));
    const sockets = [];
    const { server, mpack } = await main(['--vite', 'http://127.0.0.1:9', '--port', '0', '--work-dir', dir], {
        fetchModule: async (url) => ({
            status: url === '/@vite/client' ? 200 : 404,
            ctype: 'text/javascript',
            body: Buffer.from(''),
        }),
        WebSocketImpl: class {
            constructor(url) {
                sockets.push(url);
            }
        },
        log: () => {},
    });
    t.after(() => {
        mpack.close();
        server.close();
        fs.rmSync(dir, { recursive: true, force: true });
    });
    assert.equal(sockets.length, 1);
    const root = await fetch(`http://127.0.0.1:${server.address().port}/__mpack/root`);
    assert.equal(root.status, 200);
    assert.match(printed.at(-1), /^mpack: http:\/\/127\.0\.0\.1:\d+\/ -> http:\/\/127\.0\.0\.1:9$/);
});
