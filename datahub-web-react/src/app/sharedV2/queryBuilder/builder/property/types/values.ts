import { OperatorId } from '@app/sharedV2/queryBuilder/builder/property/types/operators';

import { EntityType } from '@types';

/**
 * The value type of a particular well-supported Property.
 */
export enum ValueTypeId {
    /**
     * A reference to a DataHub entity
     */
    URN = 'URN',
    /**
     * String type
     */
    STRING = 'STRING',
    /**
     * Enum (fixed string) type
     */
    ENUM = 'ENUM',
    /**
     * Number type
     */
    NUMBER = 'NUMBER',
    /**
     * Boolean type
     */
    BOOLEAN = 'BOOLEAN',
    /**
     * A date timestamp
     */
    TIMESTAMP = 'TIMESTAMP',
    /**
     * A list of strings
     */
    STRING_LIST = 'STRING_LIST',
    /**
     * A list of numbers
     */
    NUMBER_LIST = 'NUMBER_LIST',
    /**
     * A list of timestamps
     */
    TIMESTAMP_LIST = 'TIMESTAMP_LIST',
    /**
     * A map of string to string
     */
    ENTRY_LIST = 'ENTRY_LIST',
    /**
     * A list of urns
     */
    URN_LIST = 'URN_LIST',
    /**
     * An exists value type
     */
    EXISTS_LIST = 'EXISTS_LIST',
    /**
     * No value type
     */
    NO_VALUE = 'NO_VALUE',
    /**
     * Schema field object type
     */
    SCHEMA_FIELD_LIST = 'SCHEMA_FIELD_LIST',
}

/**
 * A single well-supported operator.
 */
export type Value = {
    id: ValueTypeId;
    displayName: string;
    operators: OperatorId[];
};

/**
 * A list of value types and their corresponding details,
 * including their display names and the operators that they support
 */
export const valueTypes = [
    {
        id: ValueTypeId.BOOLEAN,
        displayName: 'Boolean',
        operators: [OperatorId.IS_TRUE, OperatorId.IS_FALSE],
    },
    {
        id: ValueTypeId.NUMBER,
        displayName: 'Number',
        operators: [OperatorId.GREATER_THAN, OperatorId.LESS_THAN, OperatorId.EQUAL_TO, OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.STRING,
        displayName: 'String',
        operators: [
            OperatorId.EQUAL_TO,
            OperatorId.CONTAINS_STR,
            OperatorId.STARTS_WITH,
            OperatorId.REGEX_MATCH,
            OperatorId.EXISTS,
        ],
    },
    {
        id: ValueTypeId.ENUM,
        displayName: 'String',
        operators: [OperatorId.EQUAL_TO, OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.TIMESTAMP,
        displayName: 'Time',
        operators: [OperatorId.GREATER_THAN, OperatorId.LESS_THAN, OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.URN,
        displayName: 'Ref',
        operators: [OperatorId.EQUAL_TO, OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.URN_LIST,
        displayName: 'List',
        operators: [OperatorId.CONTAINS_ANY, OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.STRING_LIST,
        displayName: 'List',
        operators: [OperatorId.CONTAINS_ANY, OperatorId.REGEX_MATCH, OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.ENTRY_LIST,
        displayName: 'List',
        operators: [OperatorId.CONTAINS_ANY, OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.NUMBER_LIST,
        displayName: 'List',
        operators: [OperatorId.CONTAINS_ANY, OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.TIMESTAMP_LIST,
        displayName: 'List',
        operators: [OperatorId.CONTAINS_ANY, OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.EXISTS_LIST,
        displayName: 'List',
        operators: [OperatorId.EXISTS],
    },
    {
        id: ValueTypeId.SCHEMA_FIELD_LIST,
        displayName: 'List',
        operators: [OperatorId.SCHEMA_FIELDS_HAVE_DESCRIPTIONS],
    },
];

/**
 * A map of an value type id to the details used to render the value type.
 */
export const VALUE_TYPE_ID_TO_DETAILS = new Map();

valueTypes.forEach((valueType) => {
    VALUE_TYPE_ID_TO_DETAILS.set(valueType.id, valueType);
});

/**
 * An input type supported for selecting a property value.
 */
export enum ValueInputType {
    /**
     * Single select input
     */
    SELECT,
    /**
     * Free text input
     */
    TEXT,
    /**
     * Entity search input
     */
    ENTITY_SEARCH,
    /**
     * A time select input
     */
    TIME_SELECT,
    /**
     * Aggregation-based value input that dynamically fetches values from the backend
     */
    AGGREGATION,
    /**
     * No input type
     */
    NONE,
}

export enum SelectInputMode {
    NONE = 'none',
    SINGLE = 'single',
    MULTIPLE = 'multiple',
}

export type SelectOption = {
    id: string;
    displayName: string;
};

export type SelectParams = {
    options: SelectOption[];
};

export type EntitySearchParams = {
    entityTypes: EntityType[];
};

export type AggregationParams = {
    facetField: string;
    mode?: 'multiple' | 'single';
};

/**
 * Options provided to customize the value select experience for
 * well-known properties.
 */
export type ValueOptions = {
    inputType: ValueInputType;
    options: EntitySearchParams | SelectParams | AggregationParams | undefined;
};
