import computeOrFilters from '@app/lineageV3/LineageFilterNode/computeOrFilters';

import { EntityType } from '@types';

const DEGREE = { field: 'degree', values: ['1'] };

describe('computeOrFilters', () => {
    it('returns the default filters when nothing is hidden', () => {
        expect(computeOrFilters([DEGREE], false, false)).toEqual([{ and: [DEGREE] }]);
    });

    it('only hides data process instances when transformations are shown', () => {
        expect(computeOrFilters([DEGREE], false, true)).toEqual([
            { and: [DEGREE, { field: '_entityType', values: [EntityType.DataProcessInstance], negated: true }] },
        ]);
    });

    it('hides data jobs and dbt and SQLMesh datasets as transformations', () => {
        const [nonDatasets, datasets] = computeOrFilters([DEGREE]);

        expect(nonDatasets.and).toEqual([
            DEGREE,
            { field: '_entityType', values: [EntityType.Dataset, EntityType.DataJob], negated: true },
        ]);
        expect(datasets.and).toEqual([
            DEGREE,
            { field: '_entityType', values: [EntityType.DataJob], negated: true },
            { field: 'platform', values: ['urn:li:dataPlatform:dbt', 'urn:li:dataPlatform:sqlmesh'], negated: true },
        ]);
    });
});
