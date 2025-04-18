import { UnionType } from '../../../search/utils/constants';
import { FAILING_ASSERTION_TYPE_FILTER_FIELD, HAS_FAILING_ASSERTIONS_FILTER_FIELD } from './constants';

export const buildAssertionTypeFilters = (selectedAssertionTypes) => {
    if (selectedAssertionTypes) {
        return {
            unionType: UnionType.OR,
            filters: selectedAssertionTypes.map((assertionType) => ({
                field: FAILING_ASSERTION_TYPE_FILTER_FIELD,
                value: assertionType,
            })),
        };
    }
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: HAS_FAILING_ASSERTIONS_FILTER_FIELD,
                value: 'true',
            },
        ],
    };
};
