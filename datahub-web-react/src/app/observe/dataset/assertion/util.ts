import {
    FAILING_ASSERTION_TYPE_FILTER_FIELD,
    HAS_FAILING_ASSERTIONS_FILTER_FIELD,
} from '@app/observe/dataset/assertion/constants';
import { UnionType } from '@app/search/utils/constants';

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

export const compareListItems = (list1: string[], list2: string[]): boolean => {
    if (list1.length !== list2.length) {
        return false;
    }
    return list1.every((item) => list2.includes(item));
};
