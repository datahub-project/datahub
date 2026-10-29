import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { renderBootComment, renderReport } from './comment.mjs';
import { attachVerdicts } from './verdict.mjs';

function metric(raw) {
    const sorted = [...raw].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    const p50 = sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    return { p50, p95: sorted[sorted.length - 1], raw, n: raw.length };
}

function fill(raw) {
    const value = metric(raw);
    const metrics = {};
    for (const key of ['fcp', 'lcp', 'cls', 'tbt', 'tti', 'ttfb', 'transferredBytes', 'mainThread', 'bootup']) {
        metrics[key] = value;
    }
    return metrics;
}

function report({ name, path, aFcp, bFcp, aLcp, bLcp, method, cpu, latency }) {
    const data = {
        A: { ...fill(aFcp), fcp: metric(aFcp), lcp: metric(aLcp), sampleSize: aFcp.length, resourceSummary: null },
        B: { ...fill(bFcp), fcp: metric(bFcp), lcp: metric(bLcp), sampleSize: bFcp.length, resourceSummary: null },
        comparison: {},
        lcpElement: { A: null, B: null },
    };
    for (const key of ['fcp', 'lcp', 'cls', 'tbt', 'tti', 'ttfb', 'transferredBytes', 'mainThread', 'bootup']) {
        const a = data.A[key].p50;
        const b = data.B[key].p50;
        data.comparison[key] = { a_p50: a, b_p50: b, delta: b - a, delta_pct: ((b - a) / a) * 100 };
    }
    const built = {
        versions: { A: { label: 'base' }, B: { label: 'pr' } },
        authVerified: true,
        authMode: name === 'home' ? 'stub' : 'anonymous',
        mode: 'ab-local',
        lighthouseVersion: '12',
        config: {
            runsPerBuild: 3,
            warmupRunsPerBuild: 2,
            formFactor: 'desktop',
            cachePolicy: 'cold',
            routes: [{ name, path }],
            throttling: {
                method,
                throttling: {
                    cpuSlowdownMultiplier: cpu,
                    rttMs: method === 'devtools' ? 40 : latency,
                    throughputKbps: 10240,
                    requestLatencyMs: method === 'devtools' ? latency : 150,
                    downloadThroughputKbps: 10240,
                    uploadThroughputKbps: 10240,
                },
            },
        },
        routes: { [name]: data },
        raw_runs: [],
    };
    return attachVerdicts(built);
}

const noiseLogin = () =>
    report({
        name: 'boot',
        path: '/login',
        aFcp: [600, 771, 943],
        bFcp: [900, 946, 980],
        aLcp: [1100, 1100, 1300],
        bLcp: [1000, 1200, 1400],
        method: 'simulate',
        cpu: 1,
        latency: 40,
    });

const mixedHome = () =>
    report({
        name: 'home',
        path: '/',
        aFcp: [1700, 1800, 1900],
        bFcp: [800, 860, 900],
        aLcp: [2000, 2000, 2100],
        bLcp: [2500, 2600, 2700],
        method: 'devtools',
        cpu: 4,
        latency: 150,
    });

describe('renderBootComment', () => {
    it('stays to the verdict lines when both routes are noise', () => {
        const home = report({
            name: 'home',
            path: '/',
            aFcp: [600, 771, 943],
            bFcp: [900, 946, 980],
            aLcp: [1100, 1100, 1300],
            bLcp: [1000, 1200, 1400],
            method: 'devtools',
            cpu: 4,
            latency: 150,
        });
        const body = renderBootComment({ login: noiseLogin(), home });
        assert.match(body, /\/login` — noise/);
        assert.match(body, /ranges overlap/);
        assert.match(body, /Lantern, CPU 1×/);
        assert.match(body, /devtools, CPU 4×/);
        assert.doesNotMatch(body, /\| metric \|/);
        assert.doesNotMatch(body, /First paint and LCP are judged separately/);
    });

    it('names both directions and keeps the table when the metrics disagree', () => {
        const body = renderBootComment({ login: noiseLogin(), home: mixedHome() });
        assert.match(body, /\/login` — noise/);
        assert.match(body, /\/` — mixed/);
        assert.match(body, /FCP improvement/);
        assert.match(body, /LCP regression/);
        assert.match(body, /\| metric \|/);
        assert.match(body, /#### `\/`/);
        assert.doesNotMatch(body, /#### `\/login`/);
    });

    it('keeps the login line when the home run fails', () => {
        const body = renderBootComment({ login: noiseLogin(), home: null, homeError: 'exit 1' });
        assert.match(body, /\/login` — noise/);
        assert.match(body, /home run failed: exit 1/);
        assert.doesNotMatch(body, /\| metric \|/);
        assert.doesNotMatch(body, /First paint and LCP are judged separately/);
    });
});

describe('renderReport', () => {
    it('does not call overlapping first paint a regression', () => {
        const body = renderReport(noiseLogin());
        assert.match(body, /noise/);
        assert.doesNotMatch(body, /\| metric \|/);
    });

    it('skips the table when auth was not verified', () => {
        const unverified = noiseLogin();
        unverified.authVerified = false;
        unverified.authWarning = 'token probe failed';
        const body = renderReport(unverified);
        assert.match(body, /Auth was not verified/);
        assert.match(body, /token probe failed/);
        assert.doesNotMatch(body, /\| metric \|/);
    });
});
