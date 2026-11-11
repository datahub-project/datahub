import { describe, expect, it } from 'vitest';

import { getTotalChildren } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/utils';

describe('getTotalChildren', () => {
    it('sums children and related entities when both totals are known', () => {
        expect(getTotalChildren({ totalNumberOfChildren: 2, totalNumberOfRelatedEntities: 3 })).toBe(5);
        expect(getTotalChildren({ totalNumberOfChildren: 0, totalNumberOfRelatedEntities: 0 })).toBe(0);
    });

    it('is unknown when a total is missing because its request failed', () => {
        expect(getTotalChildren({ totalNumberOfChildren: 0 })).toBeUndefined();
        expect(getTotalChildren({ totalNumberOfRelatedEntities: 0 })).toBeUndefined();
    });
});
