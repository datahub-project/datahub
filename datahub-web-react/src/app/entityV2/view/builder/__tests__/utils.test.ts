/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { convertNestedSubTypeFilter } from '@app/entityV2/view/builder/utils';
import { ENTITY_FILTER_NAME, ENTITY_SUB_TYPE_FILTER_NAME, TYPE_NAMES_FILTER_NAME } from '@app/search/utils/constants';

describe('view builder utils', () => {
    it('should convert the nested subtypes filter properly along with other filters', () => {
        const filters = [
            { field: 'platform', values: ['platform1', 'platform2'] },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATASETS', 'CONTAINERS␞schema'] },
            { field: 'tag', values: ['tag1', 'tag2'] },
        ];

        expect(convertNestedSubTypeFilter(filters)).toMatchObject([
            { field: 'platform', values: ['platform1', 'platform2'] },
            { field: 'tag', values: ['tag1', 'tag2'] },
            { field: ENTITY_FILTER_NAME, values: ['DATASETS'] },
            { field: TYPE_NAMES_FILTER_NAME, values: ['schema'] },
        ]);
    });
});
