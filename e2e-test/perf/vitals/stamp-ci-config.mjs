#!/usr/bin/env node
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export function stampCiConfig(cfg, env = process.env) {
    const next = structuredClone(cfg);
    const base = env.BASE_SHA || '';
    const head = env.HEAD_SHA || env.GITHUB_SHA || '';
    if (next.builds?.A) {
        next.builds.A.commit = base || null;
        next.builds.A.label = `base (${base.slice(0, 7) || 'unknown'})`;
    }
    if (next.builds?.B) {
        next.builds.B.commit = head || null;
        next.builds.B.label = `pr (${head.slice(0, 7) || 'unknown'})`;
    }
    return next;
}

export function homeConfigFrom(loginCfg) {
    return {
        ...structuredClone(loginCfg),
        auth: { mode: 'stub', actorUrn: 'urn:li:corpuser:boot' },
        routes: [{ name: 'home', path: '/' }],
        throttling: {
            method: 'devtools',
            cpuSlowdownMultiplier: 4,
            rttMs: 40,
            throughputKbps: 10240,
            requestLatencyMs: 150,
            downloadThroughputKbps: 10240,
            uploadThroughputKbps: 10240,
        },
    };
}

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isDirectRun) {
    const cfg = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.ci.json'), 'utf8'));
    fs.writeFileSync(path.join(__dirname, 'config.ci.run.json'), `${JSON.stringify(stampCiConfig(cfg), null, 2)}\n`);
}
