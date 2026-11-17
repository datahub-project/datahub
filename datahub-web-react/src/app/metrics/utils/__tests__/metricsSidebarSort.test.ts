import {
    DEFAULT_METRICS_SIDEBAR_SORT,
    METRICS_SIDEBAR_SORT,
    metricsSidebarSortToCriterion,
} from '@app/metrics/utils/metricsSidebarSort';

import { SortOrder } from '@types';

describe('metricsSidebarSort', () => {
    it('maps name and lastModified selections to search sortCriterion', () => {
        expect(metricsSidebarSortToCriterion(METRICS_SIDEBAR_SORT.NAME_ASC)).toEqual({
            field: '_entityName',
            sortOrder: SortOrder.Ascending,
        });
        expect(metricsSidebarSortToCriterion(METRICS_SIDEBAR_SORT.NAME_DESC)).toEqual({
            field: '_entityName',
            sortOrder: SortOrder.Descending,
        });
        expect(metricsSidebarSortToCriterion(METRICS_SIDEBAR_SORT.LAST_MODIFIED_DESC)).toEqual({
            field: 'lastModifiedAt',
            sortOrder: SortOrder.Descending,
        });
    });

    it('defaults to name A–Z', () => {
        expect(DEFAULT_METRICS_SIDEBAR_SORT).toBe(METRICS_SIDEBAR_SORT.NAME_ASC);
    });
});
