import React from 'react';
import { CheckCircleOutlined, PlusCircleOutlined, PlusOutlined, StopOutlined } from '@ant-design/icons';
import { ENTITY_SUB_TYPE_FILTER_NAME, PLATFORM_FILTER_NAME } from '@src/app/search/utils/constants';
import { FilterOperator } from '../../../../types.generated';
import { FieldType, FilterOperatorInfo, FilterOperatorType, FilterPredicate, FrontendFilterOperator } from '../types';
import { getIsDateRangeFilter } from '../utils';

/**
 * This is a flat version of the supported search filtering operations that can be applied
 * for a given field. It dictates what options are shown to the user.
 *
 * We map these into the backend model, which is split into 2 fields:
 *
 * - operator: the operator to apply
 * - negated: whether to negate the operator
 *
 */
export const EQUALS_OPERATOR = {
    type: FilterOperatorType.EQUALS,
    text: 'equals',
    pluralText: 'is any of',
    filter: {
        operator: FilterOperator.Equal,
        negated: false,
    },
    icon: <PlusOutlined />,
};

export const ALL_EQUALS_OPERATOR = {
    type: FilterOperatorType.ALL_EQUALS,
    text: 'equals',
    pluralText: 'is all of',
    filter: {
        operator: FrontendFilterOperator.AllEqual,
        negated: false,
    },
    icon: <PlusCircleOutlined />,
};

export const EXISTS_OPERATOR = {
    type: FilterOperatorType.EXISTS,
    text: 'exists',
    filter: {
        operator: FilterOperator.Exists,
        negated: false,
    },
    icon: <CheckCircleOutlined />,
};

export const NOT_EQUALS_OPERATOR = {
    type: FilterOperatorType.NOT_EQUALS,
    text: 'does not equal',
    pluralText: 'is not any of',
    filter: {
        operator: FilterOperator.Equal,
        negated: true,
    },
    icon: <StopOutlined />,
};

export const NOT_EXISTS_OPERATOR = {
    type: FilterOperatorType.NOT_EXISTS,
    text: 'does not exist',
    filter: {
        operator: FilterOperator.Exists,
        negated: true,
    },
    icon: <StopOutlined />,
};

export const CONTAINS_OPERATOR = {
    type: FilterOperatorType.CONTAINS,
    text: 'contains',
    pluralText: 'contains any of',
    filter: {
        operator: FilterOperator.Contain,
        negated: false,
    },
};

export const NOT_CONTAINS_OPERATOR = {
    type: FilterOperatorType.NOT_CONTAINS,
    text: 'does not contain',
    pluralText: 'does not contain any of',
    filter: {
        operator: FilterOperator.Contain,
        negated: true,
    },
};

export const GREATER_THAN_OPERATOR = {
    type: FilterOperatorType.GREATER_THAN,
    text: 'is greater than',
    filter: {
        operator: FilterOperator.GreaterThan,
        negated: false,
    },
};

export const GREATER_THAN_OR_EQUALS_OPERATOR = {
    type: FilterOperatorType.GREATER_THAN_OR_EQUALS,
    text: 'is greater than or equal to',
    filter: {
        operator: FilterOperator.GreaterThanOrEqualTo,
        negated: false,
    },
};

export const LESS_THAN_OPERATOR = {
    type: FilterOperatorType.LESS_THAN,
    text: 'is less than',
    filter: {
        operator: FilterOperator.LessThan,
        negated: false,
    },
};

export const LESS_THAN_OR_EQUALS_OPERATOR = {
    type: FilterOperatorType.LESS_THAN_OR_EQUALS,
    text: 'is less than or equal to',
    filter: {
        operator: FilterOperator.LessThanOrEqualTo,
        negated: false,
    },
};

export const SUPPORTED_OPERATORS: FilterOperatorInfo[] = [
    EQUALS_OPERATOR,
    ALL_EQUALS_OPERATOR,
    NOT_EQUALS_OPERATOR,
    EXISTS_OPERATOR,
    NOT_EXISTS_OPERATOR,
    CONTAINS_OPERATOR,
    NOT_CONTAINS_OPERATOR,
    GREATER_THAN_OPERATOR,
    GREATER_THAN_OR_EQUALS_OPERATOR,
    LESS_THAN_OPERATOR,
    LESS_THAN_OR_EQUALS_OPERATOR,
];

export const SEARCH_FILTER_CONDITION_TYPE_TO_INFO = new Map<FilterOperatorType, FilterOperatorInfo>(
    SUPPORTED_OPERATORS.map((condition) => [condition.type, condition]),
);

// Fallback used when we don't know the type of the field.
export const BASE_CONDITION_TYPES = [
    FilterOperatorType.EQUALS,
    FilterOperatorType.ALL_EQUALS,
    FilterOperatorType.NOT_EQUALS,
    FilterOperatorType.EXISTS,
    FilterOperatorType.NOT_EXISTS,
];

export const TEXT_CONDITION_TYPES = [
    FilterOperatorType.CONTAINS,
    FilterOperatorType.NOT_CONTAINS,
    ...BASE_CONDITION_TYPES,
];

export const BUCKETED_TIMESTAMP_CONDITION_TYPES = [FilterOperatorType.GREATER_THAN, FilterOperatorType.LESS_THAN];

export const NUMBER_CONDITION_TYPES = [
    ...BASE_CONDITION_TYPES,
    FilterOperatorType.GREATER_THAN,
    FilterOperatorType.GREATER_THAN_OR_EQUALS,
    FilterOperatorType.LESS_THAN,
    FilterOperatorType.LESS_THAN_OR_EQUALS,
    FilterOperatorType.IS_ANY_OF,
    FilterOperatorType.IS_NOT_ANY_OF,
];

export const BROWSE_CONDITION_TYPES = [FilterOperatorType.EQUALS];

export const BOOLEAN_CONDITION_TYPES = [FilterOperatorType.EQUALS, FilterOperatorType.NOT_EQUALS];

// todo
export const DATE_CONDITION_TYPES = [...BASE_CONDITION_TYPES];

export const PLURAL_ONLY_CONDITION_TYPES = [FilterOperatorType.ALL_EQUALS];

export const FIELDS_WITHOUT_ALL_EQUALS_OPERATOR = [PLATFORM_FILTER_NAME, ENTITY_SUB_TYPE_FILTER_NAME];

const applyFiltersToOperatorOptions = (fieldName: string, operatorOptions: FilterOperatorInfo[], isPlural: boolean) => {
    const excludePluralConditions = FIELDS_WITHOUT_ALL_EQUALS_OPERATOR.includes(fieldName);
    return isPlural && !excludePluralConditions
        ? operatorOptions
        : operatorOptions.filter((o) => !PLURAL_ONLY_CONDITION_TYPES.includes(o.type));
};

/**
 * How we determine which condition options to show after a user has selected a specific field to filter on?
 * We do this using the type of the field, which we can determine from the predicate.
 *
 * @param predicate
 * @returns
 */
export const getOperatorOptionsForPredicate = (predicate: FilterPredicate, isPlural: boolean): FilterOperatorInfo[] => {
    const isDateRangeFilter = getIsDateRangeFilter(predicate.field);
    if (isDateRangeFilter) {
        return BUCKETED_TIMESTAMP_CONDITION_TYPES.map((type) => SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(type)!);
    }
    let operatorOptions: FilterOperatorInfo[] = [];
    switch (predicate.field.type) {
        /* eslint-disable @typescript-eslint/no-non-null-assertion */
        case FieldType.TEXT:
            operatorOptions = TEXT_CONDITION_TYPES.map((type) => SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(type)!);
            break;
        case FieldType.BOOLEAN:
            operatorOptions = BOOLEAN_CONDITION_TYPES.map((type) => SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(type)!);
            break;
        case FieldType.BROWSE_PATH:
            operatorOptions = BROWSE_CONDITION_TYPES.map((type) => SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(type)!);
            break;
        case FieldType.BUCKETED_TIMESTAMP:
            operatorOptions = BUCKETED_TIMESTAMP_CONDITION_TYPES.map(
                (type) => SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(type)!,
            );
            break;
        // case FieldType.NUMBER:
        //     return NUMBER_CONDITION_TYPES.map((type) => SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(type)!);
        // case FieldType.DATE:
        //     return DATE_CONDITION_TYPES.map((type) => SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(type)!);
        default:
            operatorOptions = BASE_CONDITION_TYPES.map((type) => SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(type)!);
            break;
        /* eslint-enable @typescript-eslint/no-non-null-assertion */
    }
    return applyFiltersToOperatorOptions(predicate.field.field, operatorOptions, isPlural);
};

export const convertBackendToFrontendOperatorInfo = ({
    operator,
    negated,
}: {
    operator: FilterOperator;
    negated: boolean;
}): FilterOperatorInfo | undefined => {
    return SUPPORTED_OPERATORS.find((info) => {
        return info.filter.operator === operator && info.filter.negated === negated;
    });
};

export const convertBackendToFrontendOperatorType = ({
    operator,
    negated,
}: {
    operator: FilterOperator;
    negated: boolean;
}): FilterOperatorType | undefined => {
    return SUPPORTED_OPERATORS.find((info) => {
        return info.filter.operator === operator && info.filter.negated === negated;
    })?.type;
};

export const convertFrontendToBackendOperatorType = (
    type: FilterOperatorType,
): { operator: FilterOperator; negated: boolean } => {
    return SEARCH_FILTER_CONDITION_TYPE_TO_INFO.get(type)?.filter as any;
};

// Returns true if the operator accepts values.
export const operatorRequiresValues = (type: FilterOperatorType): boolean => {
    return type !== FilterOperatorType.EXISTS && type !== FilterOperatorType.NOT_EXISTS;
};
