#!/usr/bin/env node
// PR and master entry point for the boot web-vitals comment.
// Login stays on Lantern. Home is a second pass: signed-in shell, devtools, CPU 4×.
// A home failure still leaves the login section in the comment.
import { spawn } from 'child_process';
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

// A wedged Lighthouse child must not block the other pass. The login comment
// is written after both attempts, so a timed-out home run still leaves /login.
export const LH_PASS_TIMEOUT_MS = 20 * 60 * 1000;
// Puppeteer traps SIGTERM and closes Chrome from that handler. A wedged browser
// never leaves the handler, so the process group is SIGKILLed after this grace.
export const LH_KILL_GRACE_MS = 15 * 1000;

export function bootOutcome({ loginRc, homeRc, loginReport, homeReport }) {
    const loginError = loginReport ? null : `exit ${loginRc}`;
    const homeError = homeReport ? null : `exit ${homeRc}`;
    const warning = (homeError || homeRc !== 0) ? `home (/) run failed (exit ${homeRc})` : null;
    const exitCode = !loginReport || loginRc !== 0 ? loginRc || 1 : 0;
    return { loginError, homeError, warning, exitCode };
}

function signalProcessGroup(pid, signal) {
    if (!pid) return;
    try {
        // The child is a process-group leader, so this also reaches Chrome.
        process.kill(-pid, signal);
    } catch {
        try {
            process.kill(pid, signal);
        } catch {
            // already exited
        }
    }
}

// spawnSync's timeout only sends SIGTERM and then waits forever if the child
// handles that signal. This starts a new process group, signals it, and SIGKILLs
// the group if it is still alive after the grace period.
export function runBoundedProcess(execPath, args, { cwd, timeoutMs, graceMs, stdio = 'inherit' } = {}) {
    return new Promise((resolve) => {
        const child = spawn(execPath, args, { cwd, stdio, detached: true });
        let settled = false;
        let timedOut = false;
        let timer;
        let killTimer;
        const finish = (code) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            clearTimeout(killTimer);
            resolve({ code: code == null ? 1 : code, timedOut });
        };
        child.on('exit', (code) => finish(code));
        child.on('error', () => finish(1));
        timer = setTimeout(() => {
            timedOut = true;
            signalProcessGroup(child.pid, 'SIGTERM');
            killTimer = setTimeout(() => signalProcessGroup(child.pid, 'SIGKILL'), graceMs);
        }, timeoutMs);
    });
}

async function runLh(configPath, { noAuth, out, mdOut }) {
    const args = [path.join(__dirname, 'lh-ab.mjs'), '--config', configPath, '--out', out, '--md-out', mdOut];
    if (noAuth) args.push('--no-auth');
    const { code, timedOut } = await runBoundedProcess(process.execPath, args, {
        cwd: __dirname,
        timeoutMs: LH_PASS_TIMEOUT_MS,
        graceMs: LH_KILL_GRACE_MS,
    });
    if (timedOut) {
        process.stdout.write(`lighthouse pass timed out after ${LH_PASS_TIMEOUT_MS}ms: ${configPath}\n`);
        return 1;
    }
    return code;
}

function loadReport(file) {
    if (!fs.existsSync(file)) return null;
    try {
        return readJson(file);
    } catch {
        return null;
    }
}

async function main() {
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
    const loginRc = await runLh(loginConfigPath, {
        noAuth: true,
        out: loginOut,
        mdOut: path.join(results, 'ci-boot-login.md'),
    });
    const homeRc = await runLh(homeConfigPath, {
        noAuth: false,
        out: homeOut,
        mdOut: path.join(results, 'ci-boot-home.md'),
    });

    const login = loadReport(loginOut);
    const home = loadReport(homeOut);
    const { loginError, homeError, warning, exitCode } = bootOutcome({
        loginRc,
        homeRc,
        loginReport: login,
        homeReport: home,
    });
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

    // Home does not gate the job, so surface its failure as a workflow annotation
    // rather than only a line in the PR comment.
    if (warning) {
        process.stdout.write(`::warning title=Boot web vitals::${warning}\n`);
    }

    if (exitCode !== 0) process.exit(exitCode);
}

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isDirectRun) {
    main().catch((error) => {
        process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`);
        process.exit(1);
    });
}
