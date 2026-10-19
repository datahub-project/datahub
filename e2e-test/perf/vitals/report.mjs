import { aggregateResourceSummaries, extractResourceSummary } from './resource-summary.mjs';

export const METRIC_KEYS = [
    'fcp',
    'lcp',
    'cls',
    'tbt',
    'tti',
    'ttfb',
    'transferredBytes',
    'mainThread',
    'bootup',
];

export const LOWER_IS_BETTER = {
    fcp: true,
    lcp: true,
    cls: true,
    tbt: true,
    tti: true,
    ttfb: true,
    transferredBytes: true,
    mainThread: true,
    bootup: true,
};

const AUDIT_FOR_METRIC = {
    fcp: 'first-contentful-paint',
    lcp: 'largest-contentful-paint',
    cls: 'cumulative-layout-shift',
    tbt: 'total-blocking-time',
    tti: 'interactive',
    ttfb: 'server-response-time',
    transferredBytes: 'total-byte-weight',
    // Totals cover work before and after FCP, so they separate a TBT shift
    // (same work, later window) from work the build actually added.
    mainThread: 'mainthread-work-breakdown',
    bootup: 'bootup-time',
};

export function median(values) {
    if (values.length === 0) return null;
    const s = [...values].sort((a, b) => a - b);
    const mid = Math.floor(s.length / 2);
    return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}

// Nearest-rank p95 (deterministic for small n; p95 of n=5 → the max run).
export function percentile(values, p) {
    if (values.length === 0) return null;
    const s = [...values].sort((a, b) => a - b);
    const rank = Math.ceil((p / 100) * s.length);
    return s[Math.min(rank, s.length) - 1];
}

export function findLcpNode(auditDetails) {
    if (!auditDetails || !Array.isArray(auditDetails.items)) return null;
    const stack = [...auditDetails.items];
    while (stack.length) {
        const item = stack.shift();
        if (!item || typeof item !== 'object') continue;
        if (item.node && typeof item.node === 'object') {
            const n = item.node;
            return {
                selector: n.selector || null,
                snippet: n.snippet || null,
                nodeLabel: n.nodeLabel || null,
                boundingRect: n.boundingRect || null,
            };
        }
        if (Array.isArray(item.items)) stack.push(...item.items);
    }
    return null;
}

export function extractMetrics(lhr) {
    const val = (id) => {
        const audit = lhr.audits?.[id];
        return audit && typeof audit.numericValue === 'number' ? audit.numericValue : null;
    };
    const metrics = {};
    for (const key of METRIC_KEYS) metrics[key] = val(AUDIT_FOR_METRIC[key]);
    const lcpEl = lhr.audits?.['largest-contentful-paint-element'];
    metrics.lcpElement = lcpEl ? findLcpNode(lcpEl.details) : null;
    metrics.resourceSummary = extractResourceSummary(lhr.audits?.['resource-summary']);
    metrics.benchmarkIndex = lhr.environment?.benchmarkIndex ?? null;
    return metrics;
}

export function aggregate(rawRuns, builds) {
    const routes = [...new Set(rawRuns.map((r) => r.route))];
    const perRoute = {};
    for (const route of routes) {
        perRoute[route] = { comparison: {}, lcpElement: {} };
        for (const build of builds) {
            perRoute[route][build] = {};
            perRoute[route].lcpElement[build] = null;
            const measured = rawRuns.filter(
                (r) => r.route === route && r.build === build && !r.warmup && r.metrics && r.error === null,
            );
            const lastWithEl = [...measured].reverse().find((r) => r.metrics.lcpElement);
            perRoute[route].lcpElement[build] = lastWithEl ? lastWithEl.metrics.lcpElement : null;
            perRoute[route][build].sampleSize = measured.length;
            perRoute[route][build].resourceSummary = aggregateResourceSummaries(
                measured.map((r) => r.metrics.resourceSummary),
            );
            for (const key of METRIC_KEYS) {
                const values = measured.map((r) => r.metrics[key]).filter((v) => typeof v === 'number');
                perRoute[route][build][key] = {
                    p50: median(values),
                    p95: percentile(values, 95),
                    raw: measured.map((r) => (typeof r.metrics[key] === 'number' ? r.metrics[key] : null)),
                    n: values.length,
                };
            }
        }
        if (builds.includes('A') && builds.includes('B')) {
            for (const key of METRIC_KEYS) {
                const a = perRoute[route].A[key].p50;
                const b = perRoute[route].B[key].p50;
                perRoute[route].comparison[key] = {
                    a_p50: a,
                    b_p50: b,
                    delta: a != null && b != null ? b - a : null,
                    delta_pct: a != null && a !== 0 && b != null ? ((b - a) / a) * 100 : null,
                    lower_is_better: LOWER_IS_BETTER[key],
                };
            }
        }
    }
    return perRoute;
}

export function medianBenchmarkIndex(rawRuns) {
    const values = (rawRuns || [])
        .filter((run) => !run.warmup && run.error == null)
        .map((run) => (typeof run.benchmarkIndex === 'number' ? run.benchmarkIndex : run.metrics?.benchmarkIndex))
        .filter((value) => typeof value === 'number');
    return values.length ? median(values) : null;
}

export function reportRunner(env = process.env) {
    return env.LAB_RUNNER ?? null;
}

export function selectReportNote({ mode, authMode, stubApi }) {
    if (mode === 'remote' && authMode === 'authenticated') {
        return (
            'Remote authenticated run: real routes measured against a live backend with a token session. ' +
            'Numbers include live server and data-volume variance; deltas rank/gate against this one ' +
            'environment and do not forecast a fleet p75.'
        );
    }
    if (mode === 'remote' && authMode === 'anonymous') {
        return (
            'Remote unauthenticated run: measures the anonymous /login boot path served by a live backend, ' +
            'not authenticated routes. Deltas do not forecast a fleet p75.'
        );
    }
    if (stubApi) {
        return 'Boot-only: no GMS. Measures the unauthenticated /login eager-bundle path. Deltas do not forecast a fleet p75.';
    }
    return 'Local builds served against one backend. Deltas rank/gate against that backend; they do not sum across PRs and do not forecast a fleet p75.';
}
