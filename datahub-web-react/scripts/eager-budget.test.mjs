import assert from 'node:assert/strict';
import test from 'node:test';

import { budgetExitCode, evaluateEagerBudget, sizeLimitJson } from './eager-budget.mjs';

const baseline = { totalGzip: 2_500_000, maxPhosphorModules: 80 };

test('accepts a build within the slack', () => {
    const findings = evaluateEagerBudget(
        { totalGzip: 2_505_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 40, muiIconModules: 0 }] },
        baseline,
    );
    assert.deepEqual(findings, { hard: [], soft: [] });
});

test('soft-warns when eager gzip grows past the slack without failing CI', () => {
    // +20 KB: past 10 KB slack, under 10% cliff (250 KB).
    const findings = evaluateEagerBudget(
        { totalGzip: 2_520_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 10, muiIconModules: 0 }] },
        baseline,
    );
    assert.equal(findings.hard.length, 0);
    assert.equal(findings.soft.length, 1);
    assert.match(findings.soft[0], /2520000/);
    assert.equal(budgetExitCode(false, findings), 0);
});

test('fails when eager gzip grows past the 10% cliff', () => {
    const findings = evaluateEagerBudget(
        { totalGzip: 3_400_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 10, muiIconModules: 0 }] },
        baseline,
    );
    assert.equal(findings.soft.length, 0);
    assert.equal(findings.hard.length, 1);
    assert.match(findings.hard[0], /3400000/);
    assert.match(findings.hard[0], /10%/);
    assert.equal(budgetExitCode(false, findings), 1);
});

test('soft-warns when eager gzip drops so the baseline can ratchet down', () => {
    const findings = evaluateEagerBudget(
        { totalGzip: 2_000_000, chunks: [{ file: 'assets/index.js', phosphorModules: 10, muiIconModules: 0 }] },
        baseline,
    );
    assert.equal(findings.hard.length, 0);
    assert.match(findings.soft[0], /Update datahub-web-react\/eager-budget.json totalGzip to 2000000/);
    assert.equal(budgetExitCode(false, findings), 0);
});

test('fails when a named catalog chunk is eager', () => {
    const findings = evaluateEagerBudget(
        {
            totalGzip: 2_500_000,
            chunks: [{ file: 'assets/phosphor-vendor-abc.js', phosphorModules: 10, muiIconModules: 0 }],
        },
        baseline,
    );
    assert.match(findings.hard[0], /phosphor-vendor/);
    assert.equal(budgetExitCode(false, findings), 1);
});

test('fails when the MUI icon catalog is in an eager chunk', () => {
    const findings = evaluateEagerBudget(
        { totalGzip: 2_500_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 0, muiIconModules: 400 }] },
        baseline,
    );
    assert.match(findings.hard[0], /@mui\/icons-material/);
    assert.equal(budgetExitCode(false, findings), 1);
});

test('fails when an eager chunk absorbs the Phosphor catalog', () => {
    const findings = evaluateEagerBudget(
        { totalGzip: 2_500_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 1512, muiIconModules: 0 }] },
        baseline,
    );
    assert.match(findings.hard[0], /1512 Phosphor modules/);
    assert.equal(budgetExitCode(false, findings), 1);
});

test('json output is the size the bundle-size comment reads', () => {
    const payload = sizeLimitJson(2_540_980);
    assert.equal(payload[0].size, 2_540_980);
    assert.equal(JSON.parse(JSON.stringify(payload))[0].size, 2_540_980);
});

test('--json still exits 0 when hard composition fails so the comment can post', () => {
    const findings = { hard: ['over'], soft: [] };
    assert.equal(budgetExitCode(true, findings), 0);
    assert.equal(budgetExitCode(false, findings), 1);
    assert.equal(budgetExitCode(false, { hard: [], soft: ['warn'] }), 0);
});

test('fails when the baseline omits totalGzip', () => {
    const findings = evaluateEagerBudget(
        { totalGzip: 2_500_000, chunks: [{ file: 'assets/vendor.js', phosphorModules: 10, muiIconModules: 0 }] },
        { maxPhosphorModules: 80 },
    );
    assert.match(findings.hard[0], /totalGzip/);
});

test('fails when the Phosphor catalog is split across eager chunks', () => {
    const findings = evaluateEagerBudget(
        {
            totalGzip: 2_500_000,
            chunks: [
                { file: 'assets/vendor.js', phosphorModules: 50, muiIconModules: 0 },
                { file: 'assets/index.js', phosphorModules: 50, muiIconModules: 0 },
            ],
        },
        baseline,
    );
    assert.match(findings.hard[0], /100 Phosphor modules/);
});

test('byte soft warn and composition hard fail can both appear', () => {
    const findings = evaluateEagerBudget(
        {
            totalGzip: 2_520_000,
            chunks: [{ file: 'assets/phosphor-vendor-abc.js', phosphorModules: 10, muiIconModules: 0 }],
        },
        baseline,
    );
    assert.equal(findings.soft.length, 1);
    assert.equal(findings.hard.length, 1);
    assert.equal(budgetExitCode(false, findings), 1);
});
