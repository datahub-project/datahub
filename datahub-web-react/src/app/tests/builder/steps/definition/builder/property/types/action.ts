import { SelectInputMode, ValueTypeId } from '@app/tests/builder/steps/definition/builder/property/types/values';

import { EntityType } from '@types';

/**
 * Core entity type definitions for action support
 */
const CORE_DATA_ASSETS = [
    EntityType.Dataset,
    EntityType.Dashboard,
    EntityType.Chart,
    EntityType.DataFlow,
    EntityType.DataJob,
    EntityType.Container,
] as const;

const LOGICAL_ASSETS = [
    EntityType.GlossaryTerm,
    EntityType.GlossaryNode,
    EntityType.Domain,
    EntityType.DataProduct,
] as const;

export const ENTITY_GROUPS = {
    /** Physical data assets */
    DATA_ASSETS: CORE_DATA_ASSETS,
    /** Assets that can be assigned to data products (per DataProductProperties.pdl) */
    DATA_PRODUCT_ASSIGNABLE_ASSETS: CORE_DATA_ASSETS,
    /**
     * Logical assets (organizational/metadata constructs)
     * Note: DataProduct is conceptually logical but may be auto-categorized as a data asset
     * due to inheriting from assetProps in the property definitions
     */
    LOGICAL_ASSETS,
    /** All assets including data assets and logical assets */
    ALL_ASSETS: [...CORE_DATA_ASSETS, ...LOGICAL_ASSETS],
} as const;

/**
 * An action to be taken on failure or success of a passing test
 */
export enum ActionId {
    ADD_TAGS = 'add_tags',
    REMOVE_TAGS = 'remove_tags',
    ADD_OWNERS = 'add_owners',
    REMOVE_OWNERS = 'remove_owners',
    ADD_GLOSSARY_TERMS = 'add_glossary_terms',
    REMOVE_GLOSSARY_TERMS = 'remove_glossary_terms',
    SET_DOMAIN = 'set_domain',
    UNSET_DOMAIN = 'unset_domain',
    SET_DATA_PRODUCT = 'set_data_product',
    UNSET_DATA_PRODUCT = 'unset_data_product',
    SET_STRUCTURED_PROPERTY = 'set_structured_property',
    UNSET_STRUCTURED_PROPERTY = 'unset_structured_property',
    DEPRECATE = 'deprecate',
    UN_DEPRECATE = 'un_deprecate',
}

export type ActionType = {
    id: ActionId;
    displayName: string;
    description: string;
    valueType?: ValueTypeId;
    valueOptions?: any;
    additionalParams?: Record<string, any>;
    /** Entity types that support this action */
    entityTypes?: readonly EntityType[];
};

/**
 * A list of well-supported action types.
 */
export const ACTION_TYPES: ActionType[] = [
    {
        id: ActionId.ADD_TAGS,
        displayName: 'Add Tags',
        description: 'Add specific tags to an asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
        entityTypes: ENTITY_GROUPS.DATA_ASSETS, // LOGICAL_ASSETS excluded - doesn't support tag operations
    },
    {
        id: ActionId.REMOVE_TAGS,
        displayName: 'Remove Tags',
        description: 'Remove specific tags from an asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
        entityTypes: ENTITY_GROUPS.DATA_ASSETS, // LOGICAL_ASSETS excluded - doesn't support tag operations
    },
    {
        id: ActionId.ADD_GLOSSARY_TERMS,
        displayName: 'Add Glossary Terms',
        description: 'Add specific glossary terms to an asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
            mode: SelectInputMode.MULTIPLE,
        },
        entityTypes: ENTITY_GROUPS.DATA_ASSETS, // LOGICAL_ASSETS excluded - doesn't have glossary terms attached to itself
    },
    {
        id: ActionId.REMOVE_GLOSSARY_TERMS,
        displayName: 'Remove Glossary Terms',
        description: 'Remove specific glossary terms from an asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
            mode: SelectInputMode.MULTIPLE,
        },
        entityTypes: ENTITY_GROUPS.DATA_ASSETS, // LOGICAL_ASSETS excluded - doesn't have glossary terms attached to itself
    },
    {
        id: ActionId.ADD_OWNERS,
        displayName: 'Add Owners',
        description: 'Add specific owners to an asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
            mode: SelectInputMode.MULTIPLE,
        },
        additionalParams: {
            ownerType: 'TECHNICAL_OWNER',
        },
        entityTypes: ENTITY_GROUPS.ALL_ASSETS,
    },
    {
        id: ActionId.REMOVE_OWNERS,
        displayName: 'Remove Owners',
        description: 'Remove specific owners from an asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
            mode: SelectInputMode.MULTIPLE,
        },
        entityTypes: ENTITY_GROUPS.ALL_ASSETS,
    },
    {
        id: ActionId.SET_DOMAIN,
        displayName: 'Set Domain',
        description: 'Set specific Domain for an asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.Domain],
            mode: SelectInputMode.SINGLE,
        },
        entityTypes: ENTITY_GROUPS.ALL_ASSETS,
    },
    {
        id: ActionId.UNSET_DOMAIN,
        displayName: 'Remove Domain',
        description: 'Remove Domain assignment from an asset.',
        valueType: ValueTypeId.NO_VALUE,
        valueOptions: {
            mode: SelectInputMode.NONE,
        },
        entityTypes: ENTITY_GROUPS.ALL_ASSETS,
    },
    {
        id: ActionId.SET_DATA_PRODUCT,
        displayName: 'Set Data Product',
        description: 'Assign a specific Data Product to an asset.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.DataProduct],
            mode: SelectInputMode.SINGLE,
        },
        entityTypes: ENTITY_GROUPS.DATA_PRODUCT_ASSIGNABLE_ASSETS, // Only specific assets can be assigned to data products
    },
    {
        id: ActionId.UNSET_DATA_PRODUCT,
        displayName: 'Remove Data Product',
        description: 'Remove Data Product assignment from an asset.',
        valueType: ValueTypeId.NO_VALUE,
        valueOptions: {
            mode: SelectInputMode.NONE,
        },
        entityTypes: ENTITY_GROUPS.DATA_PRODUCT_ASSIGNABLE_ASSETS, // Only specific assets can be assigned to data products
    },
    {
        id: ActionId.SET_STRUCTURED_PROPERTY,
        displayName: 'Set Structured Property',
        description: 'Assign a value to a structured property on an asset.',
        valueType: ValueTypeId.STRUCTURED_PROPERTY_VALUE,
        valueOptions: {
            mode: SelectInputMode.SINGLE,
        },
        entityTypes: ENTITY_GROUPS.ALL_ASSETS, // Structured properties can be applied to all assets
    },
    {
        id: ActionId.UNSET_STRUCTURED_PROPERTY,
        displayName: 'Remove Structured Property',
        description: 'Remove a structured property value from an asset.',
        valueType: ValueTypeId.STRUCTURED_PROPERTY_VALUE,
        valueOptions: {
            mode: SelectInputMode.SINGLE,
        },
        entityTypes: ENTITY_GROUPS.ALL_ASSETS, // Structured properties can be applied to all assets
    },
    {
        id: ActionId.DEPRECATE,
        displayName: 'Deprecate Asset',
        description: 'Mark asset as Deprecated.',
        valueType: ValueTypeId.NO_VALUE,
        valueOptions: {
            mode: SelectInputMode.NONE,
        },
        entityTypes: ENTITY_GROUPS.ALL_ASSETS,
    },
    {
        id: ActionId.UN_DEPRECATE,
        displayName: 'Un-Deprecate Asset',
        description: 'Un-mark asset as Deprecated.',
        valueType: ValueTypeId.NO_VALUE,
        valueOptions: {
            mode: SelectInputMode.NONE,
        },
        entityTypes: ENTITY_GROUPS.ALL_ASSETS,
    },
];
