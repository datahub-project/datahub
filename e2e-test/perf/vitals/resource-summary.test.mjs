import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { aggregateResourceSummaries, extractResourceSummary, resourceCategoryTypes } from './resource-summary.mjs';

describe('extractResourceSummary', () => {
    it('extracts resource types without retaining labels or URLs', () => {
        const result = extractResourceSummary({
            details: {
                items: [
                    { resourceType: 'total', label: 'Total', requestCount: 20, transferSize: 5_000 },
                    { resourceType: 'script', label: 'Script', requestCount: 3, transferSize: 1_500 },
                    { resourceType: 'image', label: 'Image', requestCount: 5, transferSize: 2_000 },
                    { resourceType: 'third-party', label: 'Third-party', requestCount: 2, transferSize: 800 },
                    { resourceType: 'unknown-future-type', requestCount: 1, transferSize: 10 },
                ],
            },
        });

        assert.deepEqual(result, {
            total: { requestCount: 20, transferSize: 5_000 },
            script: { requestCount: 3, transferSize: 1_500 },
            image: { requestCount: 5, transferSize: 2_000 },
            'third-party': { requestCount: 2, transferSize: 800 },
        });
    });

    it('returns null when the audit has no table', () => {
        assert.equal(extractResourceSummary(undefined), null);
        assert.equal(extractResourceSummary({ details: {} }), null);
    });
});

describe('aggregateResourceSummaries', () => {
    it('reports p50, p95, and every raw value per type', () => {
        const result = aggregateResourceSummaries([
            {
                total: { requestCount: 20, transferSize: 5_000 },
                script: { requestCount: 3, transferSize: 1_500 },
            },
            {
                total: { requestCount: 24, transferSize: 7_000 },
                script: { requestCount: 4, transferSize: 2_500 },
            },
            {
                total: { requestCount: 22, transferSize: 6_000 },
                script: { requestCount: 3, transferSize: 2_000 },
            },
        ]);

        assert.deepEqual(result.script.transferSize, {
            p50: 2_000,
            p95: 2_500,
            raw: [1_500, 2_500, 2_000],
            n: 3,
        });
        assert.equal(result.total.requestCount.p50, 22);
    });

    it('keeps third-party separate from resource categories', () => {
        assert.equal(resourceCategoryTypes().includes('third-party'), false);
        assert.equal(resourceCategoryTypes().includes('script'), true);
    });
});
