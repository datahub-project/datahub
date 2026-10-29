import assert from 'node:assert/strict';
import test from 'node:test';

// Inline the classifier used by bundle-budget-chore.mjs so the decision table
// stays covered without spinning up a dist/.
function isUnderBaseline(message) {
    return /fell to .+ under the .+ baseline/.test(message);
}

function classify(findings) {
    const soft = findings?.soft || [];
    const hard = findings?.hard || [];
    if (hard.length) return 'hard';
    if (!soft.length) return 'ok';
    if (soft.every(isUnderBaseline)) return 'ratchet-down';
    return 'soft-over';
}

test('ok when both budgets are clean', () => {
    assert.equal(classify({ soft: [], hard: [] }), 'ok');
});

test('ratchet-down when every soft finding is an under-baseline shrink', () => {
    assert.equal(
        classify({
            soft: [
                'Eager JS fell to 200000 bytes gzip, 100000 under the 300000 baseline. Update …',
            ],
            hard: [],
        }),
        'ratchet-down',
    );
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
