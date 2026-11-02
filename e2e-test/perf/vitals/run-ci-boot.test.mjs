import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { bootOutcome, runBoundedProcess } from './run-ci-boot.mjs';

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
});
