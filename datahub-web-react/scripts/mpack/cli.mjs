#!/usr/bin/env node
// mpack: serve a Vite dev server's modules as content-addressed chunks for slow links. See README.md.
//
//   node scripts/mpack/cli.mjs --vite http://127.0.0.1:3000 [--port 3010] [--work-dir DIR] [--modules FILE]
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { parseArgs } from 'node:util';

import { createMpack } from './server.mjs';

export const USAGE = 'usage: cli.mjs --vite <dev server URL> [--port 3010] [--work-dir DIR] [--modules FILE]';

export function parseCli(argv) {
    const { values } = parseArgs({
        args: argv,
        options: {
            vite: { type: 'string' },
            port: { type: 'string', default: '3010' },
            'work-dir': { type: 'string', default: path.join(os.tmpdir(), 'mpack') },
            modules: { type: 'string' },
        },
    });
    if (!values.vite) throw new Error(USAGE);
    const modules = values.modules ? JSON.parse(fs.readFileSync(values.modules, 'utf8')) : [];
    return { viteUrl: values.vite, port: Number(values.port), workDir: values['work-dir'], modules };
}

// Builds the pack, starts following Vite's HMR events and serves; resolves with the listening server.
export async function main(argv, deps = {}) {
    const options = parseCli(argv);
    const mpack = createMpack({ ...options, ...deps });
    await mpack.build();
    await mpack.watchVite();
    const server = http.createServer(mpack.handle);
    await new Promise((resolve) => {
        server.listen(options.port, '127.0.0.1', resolve);
    });
    console.log(`mpack: http://127.0.0.1:${server.address().port}/ -> ${options.viteUrl}`);
    return { server, mpack };
}

/* node:coverage ignore next 3 */
if (import.meta.url === `file://${process.argv[1]}`) {
    main(process.argv.slice(2)).catch((e) => (console.error(e.message), process.exit(1)));
}
