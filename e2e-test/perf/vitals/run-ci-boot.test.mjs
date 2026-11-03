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
    try {
        process.kill(pid, 0);
        return true;
    } catch (error) {
        return error.code === 'EPERM';
    }
}

function wedgeScript() {
    return `
import { spawn } from 'node:child_process';
import fs from 'node:fs';
const child = spawn(process.execPath, ['-e', 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000);'], {
    detached: true,
    stdio: 'ignore',
});
fs.writeFileSync(process.argv[2], String(child.pid));
process.on('SIGTERM', () => {});
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

    it('SIGKILLs a detached grandchild Chrome would leave behind', async () => {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'vitals-kill-'));
        const script = path.join(dir, 'wedge.mjs');
        const pidFile = path.join(dir, 'grand.pid');
        fs.writeFileSync(script, wedgeScript());
        try {
            const result = await runBoundedProcess(process.execPath, [script, pidFile], {
                timeoutMs: 400,
                graceMs: 400,
                stdio: 'ignore',
            });
            const grandPid = Number(fs.readFileSync(pidFile, 'utf8'));
            assert.equal(result.timedOut, true);
            assert.equal(isAlive(grandPid), false);
        } finally {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('SIGKILLs the detached grandchild when this process is canceled', { timeout: 10_000 }, async () => {
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
            try {
                process.kill(child.pid, 'SIGKILL');
            } catch {
                // already exited
            }
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });
});
