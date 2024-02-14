import { Entity, EntityType, FilterOperator } from '../../../types.generated';

export interface FilterOptionType {
    field: string;
    value: string;
    count?: number;
    entity?: Entity | null;
}

export interface Filter {
    field: string;
    displayName: string;
    options?: FilterOptionType[];
}

export interface FilterValue {
    value: string;
    entity: Entity | null;
    count?: number;
}

export enum FieldType {
    ENUM,
    TEXT,
    BOOLEAN,
    ENTITY,
    ENTITY_TYPE,
    NESTED_ENTITY_TYPE,
    BROWSE_PATH,
    // NUMBER,
    // DATE,
}

export interface FilterValueOption {
    value: string;
    entity?: Entity | null;
    icon?: React.ReactNode;
    count?: number;
}

export interface FilterField {
    field: string;
    displayName: string;
    type?: FieldType; // Ideally we know the field type. If not we will have default handling.
    entityTypes?: EntityType[]; // The entity types that this field is applicable to.
    icon?: any;
}

export interface FilterPredicate {
    field: FilterField;
    operator: FilterOperatorType;
    values: FilterValue[];
    defaultValueOptions: FilterValueOption[];
}

// The final type of conditions that are shown to the user.
export enum FilterOperatorType {
    EQUALS,
    NOT_EQUALS,
    IS_ANY_OF,
    IS_NOT_ANY_OF,
    EXISTS,
    NOT_EXISTS,
    CONTAINS,
    NOT_CONTAINS,
    GREATER_THAN,
    GREATER_THAN_OR_EQUALS,
    LESS_THAN,
    LESS_THAN_OR_EQUALS,
}

export type FilterOperatorInfo = {
    type: FilterOperatorType;
    text: string;
    pluralText?: string; // Optional: Used when multiple values are selected
    filter: {
        negated: boolean;
        operator: FilterOperator;
    };
    icon?: React.ReactNode;
};
