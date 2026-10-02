import {
    applySearchResultLineageCounts,
    getSearchResultLineage,
    lineageCountsByUrn,
} from '@app/searchV2/SearchResults.utils';

import { EntityType } from '@types';

const URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)';

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

    it('indexes counts only for entities that actually returned them', () => {
        const counts = lineageCountsByUrn([
            { urn: URN, upstream: { filtered: 0, total: 2 }, downstream: { filtered: 1, total: 4 } },
            { urn: 'urn:li:chart:(looker,1)', upstream: null, downstream: null },
            null,
        ]);

        expect(counts.size).toBe(1);
        expect(counts.get(URN)).toEqual({
            urn: URN,
            upstream: { filtered: 0, total: 2 },
            downstream: { filtered: 1, total: 4 },
        });
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
