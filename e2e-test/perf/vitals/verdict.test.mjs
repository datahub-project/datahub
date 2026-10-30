import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { judgeMetric, judgeRoute, rangesOverlap } from './verdict.mjs';

function metric(raw) {
    const sorted = [...raw].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    const p50 = sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    return { p50, p95: sorted[sorted.length - 1], raw, n: raw.length };
}

function routeData(aFcp, bFcp, aLcp, bLcp) {
    const data = {
        A: { fcp: metric(aFcp), lcp: metric(aLcp), sampleSize: aFcp.length },
        B: { fcp: metric(bFcp), lcp: metric(bLcp), sampleSize: bFcp.length },
        comparison: {},
    };
    for (const key of ['fcp', 'lcp']) {
        const a = data.A[key].p50;
        const b = data.B[key].p50;
        data.comparison[key] = { delta_pct: ((b - a) / a) * 100 };
    }
    return data;
}

describe('rangesOverlap', () => {
    it('treats a shared endpoint as overlap', () => {
        assert.equal(rangesOverlap({ min: 600, max: 943 }, { min: 943, max: 980 }), true);
    });
});

describe('judgeMetric', () => {
    it('calls a large median gap noise when the ranges overlap', () => {
        const judged = judgeMetric({
            aRaw: [600, 771, 943],
            bRaw: [900, 946, 980],
            aP50: 771,
            bP50: 946,
            deltaPct: 22.7,
            targetN: 3,
        });
        assert.equal(judged.label, 'noise');
    });

    it('calls a separated move inside 5% and 50 ms no change', () => {
        const judged = judgeMetric({
            aRaw: [1000, 1000, 1000],
            bRaw: [1040, 1040, 1040],
            aP50: 1000,
            bP50: 1040,
            deltaPct: 4,
            targetN: 3,
        });
        assert.equal(judged.label, 'no change');
    });

    it('calls a separated slower first paint a regression', () => {
        const judged = judgeMetric({
            aRaw: [1000, 1000, 1000],
            bRaw: [1200, 1200, 1200],
            aP50: 1000,
            bP50: 1200,
            deltaPct: 20,
            targetN: 3,
        });
        assert.equal(judged.label, 'regression');
    });

    it('calls a separated faster first paint an improvement', () => {
        const judged = judgeMetric({
            aRaw: [1800, 1800, 1900],
            bRaw: [800, 860, 900],
            aP50: 1800,
            bP50: 860,
            deltaPct: -52,
            targetN: 3,
        });
        assert.equal(judged.label, 'improvement');
    });

    it('does not label a short sample', () => {
        const judged = judgeMetric({
            aRaw: [100, 100],
            bRaw: [200, 200, 200],
            aP50: 100,
            bP50: 200,
            deltaPct: 100,
            targetN: 3,
        });
        assert.equal(judged.label, 'short');
    });
});

describe('judgeRoute', () => {
    it('stays one line when both metrics are noise', () => {
        const judged = judgeRoute(routeData([600, 771, 943], [900, 946, 980], [1100, 1100, 1300], [1000, 1200, 1400]), 3);
        assert.equal(judged.label, 'noise');
        assert.equal(judged.expand, false);
    });

    it('stays one line when one metric is noise and the other is no change', () => {
        const judged = judgeRoute(
            routeData([600, 771, 943], [900, 946, 980], [1000, 1000, 1000], [1040, 1040, 1040]),
            3,
        );
        assert.equal(judged.label, 'quiet');
        assert.equal(judged.expand, false);
    });

    it('expands when first paint improves and LCP regresses', () => {
        const judged = judgeRoute(
            routeData([1700, 1800, 1900], [800, 860, 900], [2000, 2000, 2100], [2500, 2600, 2700]),
            3,
        );
        assert.equal(judged.label, 'mixed');
        assert.equal(judged.metrics.fcp.label, 'improvement');
        assert.equal(judged.metrics.lcp.label, 'regression');
        assert.equal(judged.expand, true);
    });

    it('expands an improvement when LCP does not move', () => {
        const judged = judgeRoute(
            routeData([1700, 1800, 1900], [800, 860, 900], [1000, 1000, 1000], [1040, 1040, 1040]),
            3,
        );
        assert.equal(judged.label, 'improvement');
        assert.equal(judged.expand, true);
    });

    it('refuses a label when the sample is short', () => {
        const judged = judgeRoute(routeData([100, 100], [400, 400], [100, 100], [400, 400]), 3);
        assert.equal(judged.label, 'short');
        assert.equal(judged.expand, true);
    });
});
