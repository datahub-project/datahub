import assert from 'node:assert/strict';
import test from 'node:test';

import {
    applyRatchet,
    budgetsChanged,
    classify,
    hasUnderBaselineFieldFinding,
    isUnderBaseline,
    planRatchet,
} from './bundle-budget-chore.mjs';

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
        budgetsChanged(
            { eager: { totalGzip: 300_000 }, page: { shell: 1_000_000, search: 1_100_000 } },
            planned,
        ),
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
