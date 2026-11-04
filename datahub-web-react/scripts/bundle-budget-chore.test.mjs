import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
    applyRatchet,
    budgetsChanged,
    classify,
    hasUnderBaselineFieldFinding,
    isUnderBaseline,
    measuredFromFindings,
    planRatchet,
} from './bundle-budget-chore.mjs';
import { writeBudgetFindings } from './eager-budget.mjs';

const chorePath = path.join(path.dirname(fileURLToPath(import.meta.url)), 'bundle-budget-chore.mjs');

const eagerShrink = 'Eager JS fell to 280000 bytes gzip, 20000 under the 300000 baseline. Update …';
const shellGrow = 'Logged-in shell JS is 1008000 bytes gzip, 8000 over the 1000000 baseline (allowed +10240).';
const shellShrink = 'Logged-in shell JS fell to 900000 bytes gzip, 100000 under the 1000000 baseline. Update …';
const searchShrink = 'Logged-in search JS fell to 1050000 bytes gzip, 50000 under the 1100000 baseline. Update …';

test('ok when both budgets are clean', () => {
    assert.equal(classify({ soft: [], hard: [] }), 'ok');
});

test('ratchet-down when every soft finding is an under-baseline shrink', () => {
    assert.equal(classify({ soft: [eagerShrink], hard: [] }), 'ratchet-down');
    assert.equal(isUnderBaseline(eagerShrink), true);
});

test('soft-over when any soft finding is growth', () => {
    assert.equal(classify({ soft: [shellGrow], hard: [] }), 'soft-over');
});

test('hard wins over soft', () => {
    assert.equal(
        classify({
            soft: [eagerShrink],
            hard: ['assets/phosphor-vendor-x.js is an eager catalog chunk.'],
        }),
        'hard',
    );
});

test('mixed eager shrink + page shell grew within slack leaves shell unchanged', () => {
    const planned = planRatchet({
        eager: { totalGzip: 300_000, maxPhosphorModules: 100 },
        page: { shell: 1_000_000, search: 1_100_000 },
        eagerMeasured: 280_000,
        pageMeasured: { shell: 1_008_000, search: 1_100_000 },
        eagerSoft: [eagerShrink],
        // No under-baseline finding for shell/search — shell grew within slack → ok.
        pageSoft: [],
    });
    assert.equal(planned.eager.totalGzip, 280_000);
    assert.equal(planned.page.shell, 1_000_000);
    assert.equal(planned.page.search, 1_100_000);
    assert.equal(
        budgetsChanged({ eager: { totalGzip: 300_000 }, page: { shell: 1_000_000, search: 1_100_000 } }, planned),
        true,
    );
});

test('both shrink lowers only fields with under-baseline findings', () => {
    const planned = planRatchet({
        eager: { totalGzip: 300_000, maxPhosphorModules: 100 },
        page: { shell: 1_000_000, search: 1_100_000 },
        eagerMeasured: 280_000,
        pageMeasured: { shell: 900_000, search: 1_050_000 },
        eagerSoft: [eagerShrink],
        pageSoft: [shellShrink, searchShrink],
    });
    assert.equal(planned.eager.totalGzip, 280_000);
    assert.equal(planned.page.shell, 900_000);
    assert.equal(planned.page.search, 1_050_000);
});

test('applyRatchet is pure and never raises a field', () => {
    const eager = { totalGzip: 300_000, maxPhosphorModules: 100 };
    const page = { shell: 1_000_000, search: 1_100_000 };
    const result = applyRatchet({
        eager,
        page,
        eagerMeasured: 280_000,
        pageMeasured: { shell: 1_008_000, search: 1_050_000 },
        eagerSoft: [eagerShrink],
        pageSoft: [searchShrink],
    });
    assert.equal(result.changed, true);
    assert.equal(result.eager.totalGzip, 280_000);
    assert.equal(result.page.shell, 1_000_000);
    assert.equal(result.page.search, 1_050_000);
    // Inputs unchanged.
    assert.equal(eager.totalGzip, 300_000);
    assert.equal(page.shell, 1_000_000);
});

test('field finding matcher is label-scoped', () => {
    assert.equal(hasUnderBaselineFieldFinding([shellShrink], 'shell'), true);
    assert.equal(hasUnderBaselineFieldFinding([shellShrink], 'search'), false);
    assert.equal(hasUnderBaselineFieldFinding([shellGrow], 'shell'), false);
});

test('measuredFromFindings reads persisted gzip and nulls missing fields', () => {
    assert.deepEqual(measuredFromFindings({ measured: { totalGzip: 280_000 } }, { measured: { shell: 900_000 } }), {
        eagerMeasured: 280_000,
        pageMeasured: { shell: 900_000, search: null },
    });
    assert.deepEqual(measuredFromFindings({}, null), {
        eagerMeasured: null,
        pageMeasured: { shell: null, search: null },
    });
});

test('missing measured value never changes that field', () => {
    const planned = planRatchet({
        eager: { totalGzip: 300_000 },
        page: { shell: 1_000_000, search: 1_100_000 },
        eagerMeasured: null,
        pageMeasured: { shell: null, search: null },
        eagerSoft: [eagerShrink],
        pageSoft: [shellShrink, searchShrink],
    });
    assert.equal(planned.eager.totalGzip, 300_000);
    assert.equal(planned.page.shell, 1_000_000);
    assert.equal(planned.page.search, 1_100_000);
});

test('writeBudgetFindings persists measured eager gzip', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'eager-findings-'));
    try {
        writeBudgetFindings(dir, { soft: [eagerShrink], hard: [] }, { totalGzip: 280_000 });
        const written = JSON.parse(fs.readFileSync(path.join(dir, 'eager-budget-findings.json'), 'utf8'));
        assert.equal(written.measured.totalGzip, 280_000);
        assert.equal(written.hasFindings, true);
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

/**
 * Mirrors bundle-budget-reassessment.yml: only the findings JSON from the
 * nightly artifact is staged — no dist/assets, no index.html, no report chunks.
 */
function stageArtifactOnly() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'budget-artifact-'));
    const dist = path.join(root, 'dist');
    fs.mkdirSync(dist);
    writeBudgetFindings(dist, { soft: [eagerShrink], hard: [] }, { totalGzip: 280_000 });
    fs.writeFileSync(
        path.join(dist, 'page-budget-findings.json'),
        JSON.stringify({
            soft: [searchShrink],
            hard: [],
            measured: { shell: 1_008_000, search: 1_050_000 },
        }),
    );
    // Report still lists chunks whose JS is absent: the chore must not touch them.
    fs.writeFileSync(
        path.join(dist, 'eager-budget-report.json'),
        JSON.stringify({ chunks: [{ file: 'assets/index-missing.js', phosphorModules: 0, muiIconModules: 0 }] }),
    );
    const eagerBudget = { totalGzip: 300_000, maxPhosphorModules: 100 };
    const pageBudget = { shell: 1_000_000, search: 1_100_000 };
    fs.writeFileSync(path.join(root, 'eager-budget.json'), `${JSON.stringify(eagerBudget, null, 4)}\n`);
    fs.writeFileSync(path.join(root, 'page-budget.json'), `${JSON.stringify(pageBudget, null, 4)}\n`);
    assert.equal(fs.existsSync(path.join(dist, 'assets')), false);
    return { root, dist };
}

function spawnChore(root, dist, extra = []) {
    return spawnSync(process.execPath, [chorePath, ...extra, '--budget-dir', root, '--dist', dist], {
        encoding: 'utf8',
        env: { ...process.env, BUNDLE_EAGER_BUDGET: '', BUNDLE_PAGE_BUDGET: '', BUNDLE_BUDGET_DIST: '' },
    });
}

function runChore(root, dist, extra = []) {
    const result = spawnChore(root, dist, extra);
    assert.equal(result.status, 0, result.stderr);
    return JSON.parse(result.stdout);
}

test('a hard finding exits non-zero with a parseable decision and --apply leaves budgets untouched', () => {
    const { root, dist } = stageArtifactOnly();
    try {
        writeBudgetFindings(
            dist,
            { soft: [eagerShrink], hard: ['Eager JS grew past the 10% cliff.'] },
            { totalGzip: 280_000 },
        );
        const before = ['eager-budget.json', 'page-budget.json'].map((f) => fs.readFileSync(path.join(root, f)));
        const result = spawnChore(root, dist, ['--apply']);
        assert.notEqual(result.status, 0);
        const summary = JSON.parse(result.stdout);
        assert.equal(summary.decision, 'hard');
        assert.equal(summary.applied, undefined);
        const after = ['eager-budget.json', 'page-budget.json'].map((f) => fs.readFileSync(path.join(root, f)));
        assert.deepEqual(after, before);
    } finally {
        fs.rmSync(root, { recursive: true, force: true });
    }
});

test('applyRatchet reports no change when a shrink finding meets a measurement at or above baseline', () => {
    const eager = { totalGzip: 300_000, maxPhosphorModules: 100 };
    const page = { shell: 1_000_000, search: 1_100_000 };
    const result = applyRatchet({
        eager,
        page,
        eagerMeasured: 310_000,
        pageMeasured: { shell: 1_000_000, search: 1_100_000 },
        eagerSoft: [eagerShrink],
        pageSoft: [shellShrink, searchShrink],
    });
    assert.equal(result.changed, false);
    assert.equal(result.eager.totalGzip, 300_000);
    assert.deepEqual(result.page, page);
});

test('classify and --apply run from artifact JSON alone (no dist/assets)', () => {
    const { root, dist } = stageArtifactOnly();
    try {
        const classified = runChore(root, dist);
        assert.equal(classified.decision, 'ratchet-down');
        assert.equal(classified.eagerMeasured, 280_000);
        assert.deepEqual(classified.pageMeasured, { shell: 1_008_000, search: 1_050_000 });
        assert.equal(classified.applied, undefined);

        const applied = runChore(root, dist, ['--apply']);
        assert.equal(applied.applied.changed, true);
        const eager = JSON.parse(fs.readFileSync(path.join(root, 'eager-budget.json'), 'utf8'));
        const page = JSON.parse(fs.readFileSync(path.join(root, 'page-budget.json'), 'utf8'));
        assert.equal(eager.totalGzip, 280_000);
        assert.equal(eager.maxPhosphorModules, 100);
        // Shell grew within slack: no under-baseline finding, so never raised.
        assert.equal(page.shell, 1_000_000);
        assert.equal(page.search, 1_050_000);
    } finally {
        fs.rmSync(root, { recursive: true, force: true });
    }
});
