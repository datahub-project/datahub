import { CheckCircleOutlined, PlusCircleOutlined, PlusOutlined, StopOutlined } from '@ant-design/icons';
import i18next from 'i18next';
import React from 'react';

import {
    FieldType,
    FilterOperatorInfo,
    FilterOperatorType,
    FilterPredicate,
    FrontendFilterOperator,
} from '@app/searchV2/filters/types';
import { getIsDateRangeFilter } from '@app/searchV2/filters/utils';
import { ENTITY_SUB_TYPE_FILTER_NAME, PLATFORM_FILTER_NAME } from '@src/app/search/utils/constants';

import { FilterOperator } from '@types';

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
    get text() {
        return i18next.t('search:operator.equals');
    },
    get pluralText() {
        return i18next.t('search:operator.isAnyOf');
    },
    filter: {
        operator: FilterOperator.Equal,
        negated: false,
    },
    icon: <PlusOutlined />,
};

export const ALL_EQUALS_OPERATOR = {
    type: FilterOperatorType.ALL_EQUALS,
    get text() {
        return i18next.t('search:operator.equals');
    },
    get pluralText() {
        return i18next.t('search:operator.isAllOf');
    },
    filter: {
        operator: FrontendFilterOperator.AllEqual,
        negated: false,
    },
    icon: <PlusCircleOutlined />,
};

export const EXISTS_OPERATOR = {
    type: FilterOperatorType.EXISTS,
    get text() {
        return i18next.t('search:operator.exists');
    },
    filter: {
        operator: FilterOperator.Exists,
        negated: false,
    },
    icon: <CheckCircleOutlined />,
};

export const NOT_EQUALS_OPERATOR = {
    type: FilterOperatorType.NOT_EQUALS,
    get text() {
        return i18next.t('search:operator.doesNotEqual');
    },
    get pluralText() {
        return i18next.t('search:operator.isNotAnyOf');
    },
    filter: {
        operator: FilterOperator.Equal,
        negated: true,
    },
    icon: <StopOutlined />,
};

export const NOT_EXISTS_OPERATOR = {
    type: FilterOperatorType.NOT_EXISTS,
    get text() {
        return i18next.t('search:operator.doesNotExist');
    },
    filter: {
        operator: FilterOperator.Exists,
        negated: true,
    },
    icon: <StopOutlined />,
};

const CONTAINS_OPERATOR = {
    type: FilterOperatorType.CONTAINS,
    get text() {
        return i18next.t('search:operator.contains');
    },
    get pluralText() {
        return i18next.t('search:operator.containsAnyOf');
    },
    filter: {
        operator: FilterOperator.Contain,
        negated: false,
    },
};

const NOT_CONTAINS_OPERATOR = {
    type: FilterOperatorType.NOT_CONTAINS,
    get text() {
        return i18next.t('search:operator.doesNotContain');
    },
    get pluralText() {
        return i18next.t('search:operator.doesNotContainAnyOf');
    },
    filter: {
        operator: FilterOperator.Contain,
        negated: true,
    },
};

const GREATER_THAN_OPERATOR = {
    type: FilterOperatorType.GREATER_THAN,
    get text() {
        return i18next.t('search:operator.isGreaterThan');
    },
    filter: {
        operator: FilterOperator.GreaterThan,
        negated: false,
    },
};

const GREATER_THAN_OR_EQUALS_OPERATOR = {
    type: FilterOperatorType.GREATER_THAN_OR_EQUALS,
    get text() {
        return i18next.t('search:operator.isGreaterThanOrEqualTo');
    },
    filter: {
        operator: FilterOperator.GreaterThanOrEqualTo,
        negated: false,
    },
};

const LESS_THAN_OPERATOR = {
    type: FilterOperatorType.LESS_THAN,
    get text() {
        return i18next.t('search:operator.isLessThan');
    },
    filter: {
        operator: FilterOperator.LessThan,
        negated: false,
    },
};

const LESS_THAN_OR_EQUALS_OPERATOR = {
    type: FilterOperatorType.LESS_THAN_OR_EQUALS,
    get text() {
        return i18next.t('search:operator.isLessThanOrEqualTo');
    },
    filter: {
        operator: FilterOperator.LessThanOrEqualTo,
        negated: false,
    },
};

const SUPPORTED_OPERATORS: FilterOperatorInfo[] = [
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
const BASE_CONDITION_TYPES = [
    FilterOperatorType.EQUALS,
    FilterOperatorType.ALL_EQUALS,
    FilterOperatorType.NOT_EQUALS,
    FilterOperatorType.EXISTS,
    FilterOperatorType.NOT_EXISTS,
];

const TEXT_CONDITION_TYPES = [FilterOperatorType.CONTAINS, FilterOperatorType.NOT_CONTAINS, ...BASE_CONDITION_TYPES];

const BUCKETED_TIMESTAMP_CONDITION_TYPES = [FilterOperatorType.GREATER_THAN, FilterOperatorType.LESS_THAN];

const BROWSE_CONDITION_TYPES = [FilterOperatorType.EQUALS];

const BOOLEAN_CONDITION_TYPES = [FilterOperatorType.EQUALS, FilterOperatorType.NOT_EQUALS];

const PLURAL_ONLY_CONDITION_TYPES = [FilterOperatorType.ALL_EQUALS];

const FIELDS_WITHOUT_ALL_EQUALS_OPERATOR = [PLATFORM_FILTER_NAME, ENTITY_SUB_TYPE_FILTER_NAME];

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
