// First paint and LCP are judged on their own. One metric always lands in
// exactly one bucket. A route is mixed only when those two move in opposite
// directions. Overlapping ranges are noise even when the medians disagree.

export const JUDGED_METRICS = ['fcp', 'lcp'];

// A separated median inside both bounds is "no change". Either bound failing
// leaves the delta as a regression or an improvement.
export const NO_CHANGE_ABS_MS = 50;
export const NO_CHANGE_PCT = 5;

export function rangeOf(values) {
    const nums = (values || []).filter((value) => typeof value === 'number' && Number.isFinite(value));
    if (nums.length === 0) return null;
    return {
        min: Math.min(...nums),
        max: Math.max(...nums),
        n: nums.length,
    };
}

export function rangesOverlap(a, b) {
    return a.max >= b.min && b.max >= a.min;
}

export function judgeMetric({ aRaw, bRaw, aP50, bP50, deltaPct, targetN }) {
    const a = rangeOf(aRaw);
    const b = rangeOf(bRaw);
    const base = { a, b, aP50: aP50 ?? null, bP50: bP50 ?? null, deltaPct: deltaPct ?? null };
    if (!a || !b || (targetN != null && (a.n < targetN || b.n < targetN))) {
        return { ...base, label: 'short' };
    }
    if (rangesOverlap(a, b)) return { ...base, label: 'noise' };
    const abs = aP50 != null && bP50 != null ? Math.abs(bP50 - aP50) : null;
    const pct = deltaPct == null ? null : Math.abs(deltaPct);
    if (abs != null && pct != null && abs < NO_CHANGE_ABS_MS && pct < NO_CHANGE_PCT) {
        return { ...base, label: 'no change' };
    }
    const worse = bP50 != null && aP50 != null && bP50 > aP50;
    return { ...base, label: worse ? 'regression' : 'improvement' };
}

function metricInputs(data, key) {
    return {
        aRaw: data?.A?.[key]?.raw,
        bRaw: data?.B?.[key]?.raw,
        aP50: data?.A?.[key]?.p50,
        bP50: data?.B?.[key]?.p50,
        deltaPct: data?.comparison?.[key]?.delta_pct,
    };
}

// quiet: a mix of noise and no change. Still a one-line route.
const SHORT_LABEL = 'short';

export function judgeRoute(data, targetN) {
    const metrics = {};
    for (const key of JUDGED_METRICS) {
        metrics[key] = judgeMetric({ ...metricInputs(data, key), targetN });
    }
    const labels = JUDGED_METRICS.map((key) => metrics[key].label);
    const aN = data?.A?.sampleSize ?? metrics.fcp.a?.n ?? 0;
    const bN = data?.B?.sampleSize ?? metrics.fcp.b?.n ?? 0;
    const sampleShort = targetN != null && (aN < targetN || bN < targetN);
    let label;
    if (sampleShort || labels.includes(SHORT_LABEL)) label = SHORT_LABEL;
    else if (labels.includes('improvement') && labels.includes('regression')) label = 'mixed';
    else if (labels.includes('regression')) label = 'regression';
    else if (labels.includes('improvement')) label = 'improvement';
    else if (labels.every((item) => item === 'no change')) label = 'no change';
    else if (labels.every((item) => item === 'noise')) label = 'noise';
    else label = 'quiet';
    return {
        label,
        metrics,
        expand: label === SHORT_LABEL || label === 'mixed' || label === 'regression' || label === 'improvement',
        targetN: targetN ?? null,
        aN,
        bN,
    };
}

export function attachVerdicts(report) {
    const builds = Object.keys(report?.versions || {});
    if (!builds.includes('A') || !builds.includes('B')) return report;
    const targetN = report.config?.runsPerBuild ?? null;
    for (const data of Object.values(report.routes || {})) {
        data.judgment = judgeRoute(data, targetN);
    }
    return report;
}
