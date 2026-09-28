import { getSearchResultLineage } from '@app/searchV2/SearchResults.utils';

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
