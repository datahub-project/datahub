import { SelectInputMode, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';

import { EntityType } from '@types';

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
    },
    {
        id: ActionId.UNSET_DOMAIN,
        displayName: 'Remove Domain',
        description: 'Remove Domain for an asset.',
        valueType: ValueTypeId.URN_LIST,
        valueOptions: {
            entityTypes: [EntityType.Domain],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: ActionId.DEPRECATE,
        displayName: 'Deprecate Asset',
        description: 'Mark asset as Deprecated.',
        valueType: ValueTypeId.NO_VALUE,
        valueOptions: {
            mode: SelectInputMode.NONE,
        },
    },
    {
        id: ActionId.UN_DEPRECATE,
        displayName: 'Un-Deprecate Asset',
        description: 'Un-mark asset as Deprecated.',
        valueType: ValueTypeId.NO_VALUE,
        valueOptions: {
            mode: SelectInputMode.NONE,
        },
    },
];
