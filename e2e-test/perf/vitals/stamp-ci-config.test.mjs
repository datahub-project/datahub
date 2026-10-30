import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { homeConfigFrom, stampCiConfig } from './stamp-ci-config.mjs';

describe('homeConfigFrom', () => {
    it('keeps the login builds and switches the home pass to devtools at CPU 4×', () => {
        const login = stampCiConfig(
            {
                auth: { mode: 'anonymous' },
                builds: {
                    A: { label: 'base', dist: 'builds/base/dist', port: 8181 },
                    B: { label: 'pr', dist: 'builds/pr/dist', port: 8182 },
                },
                routes: [{ name: 'boot', path: '/login' }],
                runsPerBuild: 5,
                warmupRunsPerBuild: 2,
                throttling: { method: 'simulate', cpuSlowdownMultiplier: 1 },
            },
            { BASE_SHA: 'abcdef1234567890', HEAD_SHA: '1234567890abcdef' },
        );
        const home = homeConfigFrom(login);
        assert.equal(home.auth.mode, 'stub');
        assert.equal(home.routes[0].path, '/');
        assert.equal(home.runsPerBuild, 5);
        assert.equal(home.warmupRunsPerBuild, 2);
        assert.equal(home.throttling.method, 'devtools');
        assert.equal(home.throttling.cpuSlowdownMultiplier, 4);
        assert.equal(home.throttling.requestLatencyMs, 150);
        assert.equal(home.builds.A.label, 'base (abcdef1)');
        assert.equal(home.builds.B.dist, 'builds/pr/dist');
        assert.equal(login.routes[0].path, '/login');
        assert.equal(login.throttling.method, 'simulate');
    });
});
