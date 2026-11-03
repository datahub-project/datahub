import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import { bootOutcome, runBoundedProcess } from './run-ci-boot.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));

function isAlive(pid) {
    // An orphan reparented to a subreaper that has not reaped it yet is a zombie:
    // already killed, but kill(pid, 0) still succeeds.
    try {
        const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
        if (stat.slice(stat.lastIndexOf(')') + 2).startsWith('Z')) return false;
    } catch {
        // no /proc entry, fall through to kill(pid, 0)
    }
    try {
        process.kill(pid, 0);
        return true;
    } catch (error) {
        return error.code === 'EPERM';
    }
}

// Child discovery reads /proc/<pid>/task/<pid>/children, which only Linux has.
const procTree = { skip: process.platform !== 'linux' && 'needs Linux /proc for child discovery' };

// The root traps SIGTERM like Puppeteer does, unless rootExitsOnTerm: then it
// exits and leaves its detached grandchild to be reparented to init.
function wedgeScript({ rootExitsOnTerm = false } = {}) {
    return `
import { spawn } from 'node:child_process';
import fs from 'node:fs';
${rootExitsOnTerm ? '' : "process.on('SIGTERM', () => {});"}
const child = spawn(process.execPath, ['-e', 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000);'], {
    detached: true,
    stdio: 'ignore',
});
fs.writeFileSync(process.argv[2], String(child.pid));
setInterval(() => {}, 1000);
`;
}

async function readPidFile(file) {
    const started = Date.now();
    while (Date.now() - started < 5000) {
        if (fs.existsSync(file)) {
            const pid = Number(fs.readFileSync(file, 'utf8'));
            if (pid > 0) return pid;
        }
        await new Promise((resolve) => setTimeout(resolve, 20));
    }
    throw new Error(`pid file was not written: ${file}`);
}

function killQuietly(pid) {
    if (!pid) return;
    try {
        process.kill(pid, 'SIGKILL');
    } catch (error) {
        if (error.code !== 'ESRCH') throw error;
    }
}

function readPidIfWritten(file) {
    try {
        return Number(fs.readFileSync(file, 'utf8')) || null;
    } catch {
        return null;
    }
}

// The deadline is generous so a loaded runner still writes the pid file first;
// the test waits for it so the timeout always lands on a running grandchild.
async function expectGrandchildKilled({ rootExitsOnTerm }) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'vitals-kill-'));
    const script = path.join(dir, 'wedge.mjs');
    const pidFile = path.join(dir, 'grand.pid');
    fs.writeFileSync(script, wedgeScript({ rootExitsOnTerm }));
    try {
        const run = runBoundedProcess(process.execPath, [script, pidFile], {
            timeoutMs: 3000,
            graceMs: 400,
            stdio: 'ignore',
        });
        const grandPid = await readPidFile(pidFile);
        const result = await run;
        assert.equal(result.timedOut, true);
        assert.equal(isAlive(grandPid), false);
    } finally {
        killQuietly(readPidIfWritten(pidFile));
        fs.rmSync(dir, { recursive: true, force: true });
    }
}

describe('bootOutcome', () => {
    it('warns and exits 0 when only the home pass fails', () => {
        const outcome = bootOutcome({ loginRc: 0, homeRc: 1, loginReport: { routes: {} }, homeReport: null });
        assert.equal(outcome.exitCode, 0);
        assert.equal(outcome.loginError, null);
        assert.equal(outcome.homeError, 'exit 1');
        assert.match(outcome.warning, /home \(\/\) run failed \(exit 1\)/);
    });

    it('exits non-zero when the login pass fails', () => {
        const outcome = bootOutcome({ loginRc: 1, homeRc: 0, loginReport: null, homeReport: { routes: {} } });
        assert.equal(outcome.exitCode, 1);
        assert.equal(outcome.loginError, 'exit 1');
        assert.equal(outcome.warning, null);
    });

    it('is quiet when both passes succeed', () => {
        const outcome = bootOutcome({
            loginRc: 0,
            homeRc: 0,
            loginReport: { routes: {} },
            homeReport: { routes: {} },
        });
        assert.equal(outcome.exitCode, 0);
        assert.equal(outcome.warning, null);
        assert.equal(outcome.loginError, null);
        assert.equal(outcome.homeError, null);
    });
});

describe('runBoundedProcess', () => {
    it('returns a normal exit without waiting for the deadline', async () => {
        const started = Date.now();
        const result = await runBoundedProcess(process.execPath, ['-e', 'process.exit(0)'], {
            timeoutMs: 5000,
            graceMs: 1000,
            stdio: 'ignore',
        });
        assert.equal(result.timedOut, false);
        assert.equal(result.code, 0);
        assert.ok(Date.now() - started < 4000);
    });

    it('SIGKILLs a child that ignores SIGTERM', async () => {
        const started = Date.now();
        const result = await runBoundedProcess(
            process.execPath,
            ['-e', 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000);'],
            { timeoutMs: 300, graceMs: 400, stdio: 'ignore' },
        );
        assert.equal(result.timedOut, true);
        assert.equal(result.code, 1);
        const elapsed = Date.now() - started;
        assert.ok(elapsed < 3000, `returned in ${elapsed}ms`);
    });

    it('SIGKILLs a detached grandchild Chrome would leave behind', { ...procTree, timeout: 10_000 }, () =>
        expectGrandchildKilled({ rootExitsOnTerm: false }),
    );

    it('SIGKILLs a detached grandchild after its parent exits on SIGTERM', { ...procTree, timeout: 10_000 }, () =>
        expectGrandchildKilled({ rootExitsOnTerm: true }),
    );

    it('SIGKILLs the detached grandchild when this process is canceled', { ...procTree, timeout: 10_000 }, async () => {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'vitals-cancel-'));
        const script = path.join(dir, 'wedge.mjs');
        const wrapper = path.join(dir, 'wrapper.mjs');
        const pidFile = path.join(dir, 'grand.pid');
        fs.writeFileSync(script, wedgeScript());
        fs.writeFileSync(
            wrapper,
            `import { runBoundedProcess } from ${JSON.stringify(path.join(here, 'run-ci-boot.mjs'))};
await runBoundedProcess(process.execPath, [process.argv[2], process.argv[3]], {
    timeoutMs: 20000,
    graceMs: 1000,
    stdio: 'ignore',
});
`,
        );
        const child = spawn(process.execPath, [wrapper, script, pidFile], { stdio: 'ignore' });
        try {
            const grandPid = await readPidFile(pidFile);
            process.kill(child.pid, 'SIGTERM');
            const exitCode = await new Promise((resolve) => child.once('exit', resolve));
            assert.equal(exitCode, 143);
            assert.equal(isAlive(grandPid), false);
        } finally {
            killQuietly(child.pid);
            killQuietly(readPidIfWritten(pidFile));
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });
});
