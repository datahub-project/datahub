import {
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from '../../../../search/utils/constants';
import { convertNestedSubTypeFilter } from '../utils';

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
