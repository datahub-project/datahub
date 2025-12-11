/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { getEntitySubtypeFiltersForEntity } from '@app/search/sidebar/browseContextUtils';
import { ENTITY_SUB_TYPE_FILTER_NAME, TAGS_FILTER_NAME } from '@app/search/utils/constants';

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
