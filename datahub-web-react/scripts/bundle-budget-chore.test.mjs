import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
    applyRatchet,
    budgetsChanged,
    classify,
    isUnderBaseline,
    planRatchet,
} from './bundle-budget-chore.mjs';

test('ok when both budgets are clean', () => {
    assert.equal(classify({ soft: [], hard: [] }), 'ok');
});

test('ratchet-down when every soft finding is an under-baseline shrink', () => {
    assert.equal(
        classify({
            soft: ['Eager JS fell to 200000 bytes gzip, 100000 under the 300000 baseline. Update …'],
            hard: [],
        }),
        'ratchet-down',
    );
    assert.equal(isUnderBaseline('Eager JS fell to 200000 bytes gzip, 100000 under the 300000 baseline.'), true);
});

test('soft-over when any soft finding is growth', () => {
    assert.equal(
        classify({
            soft: ['Eager JS is 320000 bytes gzip, 20000 over the 300000 baseline (allowed +10240).'],
            hard: [],
        }),
        'soft-over',
    );
});

test('hard wins over soft', () => {
    assert.equal(
        classify({
            soft: ['Eager JS fell to 200000 bytes gzip, 100000 under the 300000 baseline.'],
            hard: ['assets/phosphor-vendor-x.js is an eager catalog chunk.'],
        }),
        'hard',
    );
});

test('planRatchet never raises a field that grew within slack', () => {
    const planned = planRatchet({
        eager: { totalGzip: 300_000, maxPhosphorModules: 100 },
        page: { shell: 1_000_000, search: 1_100_000 },
        // Eager shrank; shell grew 8 KB (within typical slack); search unchanged.
        eagerMeasured: 280_000,
        pageMeasured: { shell: 1_008_000, search: 1_100_000 },
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

test('applyRatchet writes only lowered ceilings', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'budget-chore-'));
    fs.writeFileSync(
        path.join(dir, 'eager-budget.json'),
        `${JSON.stringify({ totalGzip: 300_000, maxPhosphorModules: 100 }, null, 4)}\n`,
    );
    fs.writeFileSync(
        path.join(dir, 'page-budget.json'),
        `${JSON.stringify({ shell: 1_000_000, search: 1_100_000 }, null, 4)}\n`,
    );
    const result = applyRatchet({
        rootDir: dir,
        eagerMeasured: 280_000,
        pageMeasured: { shell: 1_008_000, search: 1_050_000 },
    });
    assert.equal(result.changed, true);
    assert.equal(result.eager.totalGzip, 280_000);
    assert.equal(result.page.shell, 1_000_000);
    assert.equal(result.page.search, 1_050_000);
    const writtenPage = JSON.parse(fs.readFileSync(path.join(dir, 'page-budget.json'), 'utf8'));
    assert.equal(writtenPage.shell, 1_000_000);
    assert.equal(writtenPage.search, 1_050_000);
});
