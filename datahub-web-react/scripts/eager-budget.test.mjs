import assert from 'node:assert/strict';
import test from 'node:test';

import { budgetExitCode, evaluateEagerBudget, sizeLimitJson } from './eager-budget.mjs';

const baseline = { totalGzip: 2_500_000, maxPhosphorModules: 80 };

test('accepts a build within the slack', () => {
    const errors = evaluateEagerBudget(
        { totalGzip: 2_505_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 40, muiIconModules: 0 }] },
        baseline,
    );
    assert.deepEqual(errors, []);
});

test('fails when eager gzip grows past the slack', () => {
    const errors = evaluateEagerBudget(
        { totalGzip: 3_400_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 10, muiIconModules: 0 }] },
        baseline,
    );
    assert.equal(errors.length, 1);
    assert.match(errors[0], /3400000/);
});

test('fails when eager gzip drops, so the baseline must ratchet down', () => {
    const errors = evaluateEagerBudget(
        { totalGzip: 2_000_000, chunks: [{ file: 'assets/index.js', phosphorModules: 10, muiIconModules: 0 }] },
        baseline,
    );
    assert.match(errors[0], /Update datahub-web-react\/eager-budget.json totalGzip to 2000000/);
});

test('fails when a named catalog chunk is eager', () => {
    const errors = evaluateEagerBudget(
        {
            totalGzip: 2_500_000,
            chunks: [{ file: 'assets/phosphor-vendor-abc.js', phosphorModules: 10, muiIconModules: 0 }],
        },
        baseline,
    );
    assert.match(errors[0], /phosphor-vendor/);
});

test('fails when the MUI icon catalog is in an eager chunk', () => {
    const errors = evaluateEagerBudget(
        { totalGzip: 2_500_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 0, muiIconModules: 400 }] },
        baseline,
    );
    assert.match(errors[0], /@mui\/icons-material/);
});

test('fails when an eager chunk absorbs the Phosphor catalog', () => {
    const errors = evaluateEagerBudget(
        { totalGzip: 2_500_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 1512, muiIconModules: 0 }] },
        baseline,
    );
    assert.match(errors[0], /1512 Phosphor modules/);
});

test('json output is the size the bundle-size comment reads', () => {
    const payload = sizeLimitJson(2_540_980);
    assert.equal(payload[0].size, 2_540_980);
    assert.equal(JSON.parse(JSON.stringify(payload))[0].size, 2_540_980);
});

test('--json still exits 0 when the budget fails so the comment can post', () => {
    const errors = ['over'];
    assert.equal(budgetExitCode(true, errors), 0);
    assert.equal(budgetExitCode(false, errors), 1);
    assert.equal(budgetExitCode(false, []), 0);
});

test('fails when the baseline omits totalGzip', () => {
    const errors = evaluateEagerBudget(
        { totalGzip: 2_500_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 10, muiIconModules: 0 }] },
        { maxPhosphorModules: 80 },
    );
    assert.match(errors[0], /totalGzip/);
});

test('fails when the Phosphor catalog is split across eager chunks', () => {
    const errors = evaluateEagerBudget(
        {
            totalGzip: 2_500_000,
            chunks: [
                { file: 'assets/vendor.js', phosphorModules: 50, muiIconModules: 0 },
                { file: 'assets/index.js', phosphorModules: 50, muiIconModules: 0 },
            ],
        },
        baseline,
    );
    assert.match(errors[0], /100 Phosphor modules/);
});
