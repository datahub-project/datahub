import { getEntitySubtypeFiltersForEntity } from '@app/searchV2/sidebar/browseContextUtils';
import { ENTITY_SUB_TYPE_FILTER_NAME, TAGS_FILTER_NAME } from '@app/searchV2/utils/constants';

describe('browseContextUtils', () => {
    it('should remove any different entity types from the filter and keep anything related to our given entityType', () => {
        const existingFilters = [
            {
                field: ENTITY_SUB_TYPE_FILTER_NAME,
                values: ['DATASET␞table', 'CONTAINER', 'DATASET', 'CHART', 'DATASET␞view'],
            },
            {
                field: TAGS_FILTER_NAME,
                values: ['urn:li:tag:test'],
            },
        ];
        const entitySubtypeFilters = getEntitySubtypeFiltersForEntity('DATASET', existingFilters);

        expect(entitySubtypeFilters).toMatchObject(['DATASET␞table', 'DATASET', 'DATASET␞view']);
    });
});
