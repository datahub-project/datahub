const RESOURCE_TYPES = ['document', 'script', 'stylesheet', 'image', 'media', 'font', 'other'];
const OVERVIEW_TYPES = ['total', ...RESOURCE_TYPES, 'third-party'];

function median(values) {
    if (values.length === 0) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function percentile(values, p) {
    if (values.length === 0) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const rank = Math.ceil((p / 100) * sorted.length);
    return sorted[Math.min(rank, sorted.length) - 1];
}

/**
 * Extract Lighthouse's `resource-summary` table without retaining request URLs.
 * `third-party` overlaps the MIME-like categories and is therefore reported
 * separately rather than included in category totals.
 */
export function extractResourceSummary(audit) {
    const items = audit?.details?.items;
    if (!Array.isArray(items)) return null;

    const summary = {};
    for (const item of items) {
        const type = item?.resourceType;
        if (!OVERVIEW_TYPES.includes(type)) continue;
        if (typeof item.transferSize !== 'number' || typeof item.requestCount !== 'number') continue;
        summary[type] = {
            requestCount: item.requestCount,
            transferSize: item.transferSize,
        };
    }
    return Object.keys(summary).length > 0 ? summary : null;
}

function aggregateField(summaries, type, field) {
    const raw = summaries.map((summary) => summary?.[type]?.[field]).filter((value) => typeof value === 'number');
    return {
        p50: median(raw),
        p95: percentile(raw, 95),
        raw,
        n: raw.length,
    };
}

export function aggregateResourceSummaries(summaries) {
    const present = summaries.filter(Boolean);
    if (present.length === 0) return null;

    const aggregate = {};
    for (const type of OVERVIEW_TYPES) {
        if (!present.some((summary) => summary[type])) continue;
        aggregate[type] = {
            transferSize: aggregateField(present, type, 'transferSize'),
            requestCount: aggregateField(present, type, 'requestCount'),
        };
    }
    return aggregate;
}

export function resourceCategoryTypes() {
    return [...RESOURCE_TYPES];
}
