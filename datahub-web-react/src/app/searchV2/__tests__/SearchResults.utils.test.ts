import {
    applySearchResultLineageCounts,
    getSearchResultLineage,
    lineageCountsByUrn,
} from '@app/searchV2/SearchResults.utils';

import { EntityType } from '@types';

const URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)';
const OTHER_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.other,PROD)';

describe('getSearchResultLineage', () => {
    it('copies relationship totals for a sibling-free search hit', () => {
        expect(
            getSearchResultLineage({
                urn: URN,
                type: EntityType.Dataset,
                upstream: { filtered: 0, total: 2 },
                downstream: { filtered: 1, total: 4 },
            }),
        ).toEqual({
            urn: URN,
            upstream: { filtered: 0, total: 2 },
            downstream: { filtered: 1, total: 4 },
        });
    });

    it('indexes complete counts and settles missing URNs as zero', () => {
        const counts = lineageCountsByUrn(
            [
                { urn: URN, upstream: { filtered: 0, total: 2 }, downstream: { filtered: 1, total: 4 } },
                { urn: 'urn:li:chart:(looker,1)', upstream: null, downstream: null },
                { urn: OTHER_URN, upstream: { filtered: 0, total: 1 }, downstream: null },
                null,
            ],
            [URN, OTHER_URN, 'urn:li:chart:(looker,1)'],
        );

        expect(counts.size).toBe(3);
        expect(counts.get(URN)).toEqual({
            urn: URN,
            upstream: { filtered: 0, total: 2 },
            downstream: { filtered: 1, total: 4 },
        });
        expect(counts.get(OTHER_URN)).toEqual({
            urn: OTHER_URN,
            upstream: { total: 0, filtered: 0 },
            downstream: { total: 0, filtered: 0 },
        });
        expect(counts.get('urn:li:chart:(looker,1)')).toEqual({
            urn: 'urn:li:chart:(looker,1)',
            upstream: { total: 0, filtered: 0 },
            downstream: { total: 0, filtered: 0 },
        });
    });

    it('does not index partial results when no requested URNs are provided', () => {
        const counts = lineageCountsByUrn([
            { urn: URN, upstream: { filtered: 0, total: 2 }, downstream: null },
            { urn: OTHER_URN, upstream: null, downstream: null },
        ]);

        expect(counts.size).toBe(0);
    });

    it('attaches deferred counts to the matching search card', () => {
        const results = [{ entity: { urn: URN, type: EntityType.Dataset }, matchedFields: [] }];
        const counts = lineageCountsByUrn([
            { urn: URN, upstream: { filtered: 0, total: 2 }, downstream: { filtered: 0, total: 1 } },
        ]);

        expect(applySearchResultLineageCounts(results, counts)[0]?.entity).toMatchObject({
            urn: URN,
            upstream: { filtered: 0, total: 2 },
            downstream: { filtered: 0, total: 1 },
        });
    });

    it('returns the same results until counts arrive', () => {
        const results = [{ entity: { urn: URN } }];
        expect(applySearchResultLineageCounts(results, new Map())).toBe(results);
    });

    it('leaves a result unchanged when the counts map has entries for other URNs only', () => {
        const results = [{ entity: { urn: URN, type: EntityType.Dataset }, matchedFields: [] }];
        const counts = lineageCountsByUrn([
            { urn: OTHER_URN, upstream: { filtered: 0, total: 2 }, downstream: { filtered: 0, total: 1 } },
        ]);

        const applied = applySearchResultLineageCounts(results, counts);
        expect(applied[0]).toBe(results[0]);
        expect(applied[0]?.entity).not.toHaveProperty('upstream');
    });

    it('does not reuse lineage from a combined sibling card', () => {
        expect(
            getSearchResultLineage({
                urn: URN,
                type: EntityType.Dataset,
                upstream: { filtered: 0, total: 2 },
                downstream: { filtered: 0, total: 1 },
                siblingsSearch: { total: 1 },
            }),
        ).toBeNull();
    });
});
