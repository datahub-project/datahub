import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    aggregate,
    extractMetrics,
    medianBenchmarkIndex,
    reportRunner,
    selectReportNote,
} from './report.mjs';

function lhr(audits, environment) {
    return { audits, environment };
}

describe('extractMetrics ttfb', () => {
    it('reads server-response-time as ttfb in milliseconds', () => {
        const metrics = extractMetrics(
            lhr({ 'server-response-time': { numericValue: 182.4 } }, { benchmarkIndex: 1400 }),
        );
        assert.equal(metrics.ttfb, 182.4);
        assert.equal(metrics.benchmarkIndex, 1400);
    });

    it('leaves ttfb and benchmarkIndex null when the audit and environment are missing', () => {
        const metrics = extractMetrics(lhr({}));
        assert.equal(metrics.ttfb, null);
        assert.equal(metrics.benchmarkIndex, null);
        assert.equal(metrics.mainThread, null);
        assert.equal(metrics.bootup, null);
    });

    it('reads main-thread and JS bootup totals', () => {
        const metrics = extractMetrics(
            lhr({
                'mainthread-work-breakdown': { numericValue: 4200 },
                'bootup-time': { numericValue: 3100 },
            }),
        );
        assert.equal(metrics.mainThread, 4200);
        assert.equal(metrics.bootup, 3100);
    });
});

describe('aggregate ttfb', () => {
    it('aggregates ttfb per build the same way as the other metrics', () => {
        const raw = [100, 300, 200].map((ttfb, index) => ({
            route: 'home',
            build: 'A',
            warmup: false,
            error: null,
            metrics: { ttfb, fcp: null, lcp: null, cls: null, tbt: null, tti: null, transferredBytes: null },
        }));
        raw.push({
            route: 'home',
            build: 'A',
            warmup: true,
            error: null,
            metrics: { ttfb: 9999 },
        });
        const routes = aggregate(raw, ['A']);
        assert.equal(routes.home.A.ttfb.p50, 200);
        assert.equal(routes.home.A.ttfb.p95, 300);
        assert.deepEqual(routes.home.A.ttfb.raw, [100, 300, 200]);
        assert.equal(routes.home.A.ttfb.n, 3);
    });
});

describe('medianBenchmarkIndex', () => {
    it('uses measured runs and ignores warm-ups', () => {
        const index = medianBenchmarkIndex([
            { warmup: true, error: null, benchmarkIndex: 1 },
            { warmup: false, error: null, benchmarkIndex: 100 },
            { warmup: false, error: null, benchmarkIndex: 300 },
            { warmup: false, error: 'x', benchmarkIndex: 9000 },
        ]);
        assert.equal(index, 200);
    });

    it('is null when no measured run recorded a benchmark', () => {
        assert.equal(medianBenchmarkIndex([{ warmup: false, error: null, benchmarkIndex: null }]), null);
        assert.equal(medianBenchmarkIndex([]), null);
    });
});

describe('reportRunner', () => {
    it('reads LAB_RUNNER and is null when unset', () => {
        assert.equal(reportRunner({ LAB_RUNNER: 'devops-runner-default' }), 'devops-runner-default');
        assert.equal(reportRunner({}), null);
    });
});

describe('selectReportNote', () => {
    it('describes a remote authenticated capture', () => {
        const note = selectReportNote({ mode: 'remote', authMode: 'authenticated', stubApi: false });
        assert.match(note, /Remote authenticated run/);
        assert.match(note, /do not forecast a fleet p75/);
    });

    it('describes a remote anonymous capture', () => {
        const note = selectReportNote({ mode: 'remote', authMode: 'anonymous', stubApi: true });
        assert.match(note, /Remote unauthenticated run/);
        assert.match(note, /anonymous \/login/);
    });

    it('keeps the boot-only note for a local stub', () => {
        const note = selectReportNote({ mode: 'ab-local', authMode: 'anonymous', stubApi: true });
        assert.match(note, /Boot-only: no GMS/);
    });

    it('describes local builds against a real backend', () => {
        const note = selectReportNote({ mode: 'ab-local', authMode: 'authenticated', stubApi: false });
        assert.match(note, /Local builds served against one backend/);
    });
});
