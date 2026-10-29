#!/usr/bin/env node
// PR and master entry point for the boot web-vitals comment.
// Login stays on Lantern. Home is a second pass: signed-in shell, devtools, CPU 4×.
// A home failure still leaves the login section in the comment.
import { spawnSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { renderBootComment } from './comment.mjs';
import { homeConfigFrom, stampCiConfig } from './stamp-ci-config.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
    const args = {};
    for (let i = 0; i < argv.length; i += 1) {
        if (argv[i].startsWith('--')) {
            const key = argv[i].slice(2);
            const val = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[(i += 1)] : 'true';
            args[key] = val;
        }
    }
    return args;
}

function writeJson(file, value) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

function readJson(file) {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
}

export function pushLoginConfig(env = process.env) {
    const base = readJson(path.join(__dirname, 'config.ci.json'));
    const sha = env.HEAD_SHA || env.GITHUB_SHA || '';
    const short = sha.slice(0, 7) || 'unknown';
    return {
        ...base,
        auth: { mode: 'anonymous' },
        builds: {
            A: {
                label: `master@${short}`,
                dist: 'builds/head/dist',
                port: 8181,
                commit: sha || null,
            },
        },
        routes: [{ name: 'boot', path: '/login' }],
    };
}

function runLh(configPath, { noAuth, out, mdOut }) {
    const args = [path.join(__dirname, 'lh-ab.mjs'), '--config', configPath, '--out', out, '--md-out', mdOut];
    if (noAuth) args.push('--no-auth');
    const result = spawnSync(process.execPath, args, { cwd: __dirname, stdio: 'inherit' });
    return result.status == null ? 1 : result.status;
}

function loadReport(file) {
    if (!fs.existsSync(file)) return null;
    try {
        return readJson(file);
    } catch {
        return null;
    }
}

function main() {
    const args = parseArgs(process.argv.slice(2));
    const mode = args.mode === 'push' ? 'push' : 'pr';
    const results = path.join(__dirname, 'results');
    fs.mkdirSync(results, { recursive: true });

    const loginCfg = mode === 'push' ? pushLoginConfig() : stampCiConfig(readJson(path.join(__dirname, 'config.ci.json')));
    const homeCfg = homeConfigFrom(loginCfg);
    const loginConfigPath = path.join(results, 'ci-login.config.json');
    const homeConfigPath = path.join(results, 'ci-home.config.json');
    writeJson(loginConfigPath, loginCfg);
    writeJson(homeConfigPath, homeCfg);

    const loginOut = path.join(results, 'ci-boot-login.json');
    const homeOut = path.join(results, 'ci-boot-home.json');
    const loginRc = runLh(loginConfigPath, {
        noAuth: true,
        out: loginOut,
        mdOut: path.join(results, 'ci-boot-login.md'),
    });
    const homeRc = runLh(homeConfigPath, {
        noAuth: false,
        out: homeOut,
        mdOut: path.join(results, 'ci-boot-home.md'),
    });

    const login = loadReport(loginOut);
    const home = loadReport(homeOut);
    const loginError = login ? null : `exit ${loginRc}`;
    const homeError = home ? null : `exit ${homeRc}`;
    const sha = process.env.HEAD_SHA || process.env.GITHUB_SHA || '';
    const combined = {
        suite: 'web-vitals-boot',
        release: mode === 'push' ? `master@${sha.slice(0, 7)}` : null,
        login,
        home,
        loginError,
        homeError,
    };
    writeJson(path.join(results, 'ci-boot.json'), combined);

    if (mode === 'pr') {
        const body = renderBootComment({ login, home, loginError, homeError });
        fs.writeFileSync(path.join(results, 'ci-boot-comment.md'), `<!-- web-vitals-boot-ab -->\n${body}\n`);
    }

    if (!login || loginRc !== 0) process.exit(loginRc || 1);
}

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isDirectRun) main();
