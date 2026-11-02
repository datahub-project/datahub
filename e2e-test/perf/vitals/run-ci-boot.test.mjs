import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { bootOutcome } from './run-ci-boot.mjs';

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
