import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { EntityType, FormPrompt } from '@src/types.generated';

export const ownershipTypeUrn1 = 'urn:li:ownershipType:__system__data_steward';
export const ownershipTypeUrn2 = 'urn:li:ownershipType:__system__technical_owner';
export const ownershipTypeUrn3 = 'urn:li:ownershipType:__system__business_owner';

export const user1 = {
    urn: 'urn:li:corpuser:1',
    type: EntityType.CorpUser,
    username: 'user1',
};

export const user2 = {
    urn: 'urn:li:corpuser:2',
    type: EntityType.CorpUser,
    username: 'user2',
};

export const user3 = {
    urn: 'urn:li:corpuser:3',
    type: EntityType.CorpUser,
    username: 'user3',
};

export const user4 = {
    urn: 'urn:li:corpuser:4',
    type: EntityType.CorpUser,
    username: 'user4',
};

export const owner1 = {
    __typename: 'Owner',
    owner: user1,
    associatedUrn: 'urn:li:dataset:1',
    ownershipType: {
        urn: ownershipTypeUrn1,
        type: 'DATA_STEWARD',
    },
};

export const owner2 = {
    __typename: 'Owner',
    owner: user2,
    associatedUrn: 'urn:li:dataset:1',
    ownershipType: {
        urn: ownershipTypeUrn2,
        type: 'TECHNICAL_OWNER',
    },
};

export const owner3 = {
    __typename: 'Owner',
    owner: user3,
    associatedUrn: 'urn:li:dataset:1',
    ownershipType: {
        urn: ownershipTypeUrn3,
        type: 'BUSINESS_OWNER',
    },
};

export const owner4 = {
    __typename: 'Owner',
    owner: user4,
    associatedUrn: 'urn:li:dataset:1',
    ownershipType: {
        urn: ownershipTypeUrn3,
        type: 'BUSINESS_OWNER',
    },
};

const allowedOwners = [
    {
        urn: user2.urn,
    },
    {
        urn: user3.urn,
    },
];

const allowedOwnershipTypes = [
    {
        urn: ownershipTypeUrn1,
    },
    {
        urn: ownershipTypeUrn2,
    },
];

export const mockEntityData = {
    ownership: {
        owners: [owner1, owner2, owner3],
    },
} as GenericEntityProperties;

export const mockEntityData2 = {
    ownership: {
        owners: [owner1, owner2, owner3, owner4],
    },
} as GenericEntityProperties;

export const entityDataWithNoOwners = {} as GenericEntityProperties;

export const entityDataWithNoAllowedOwners = {
    ownership: {
        owners: [owner1],
    },
} as GenericEntityProperties;

export const entityDataWithNoAllowedTypes = {
    ownership: {
        owners: [owner3],
    },
} as GenericEntityProperties;

const requiredPromptFields = {
    id: 'promptId',
    type: 'OWNERSHIP',
    required: true,
    formUrn: 'formUrn',
    title: 'Prompt title',
};

export const singleUnrestrictedPrompt = {
    ...requiredPromptFields,
    ownershipParams: {
        cardinality: 'SINGLE',
        allowedOwners: null,
        allowedOwnershipTypes: null,
    },
} as FormPrompt;

export const singleRestrictedPrompt = {
    ...requiredPromptFields,
    ownershipParams: {
        cardinality: 'SINGLE',
        allowedOwners,
        allowedOwnershipTypes: null,
    },
} as FormPrompt;

export const multipleUnrestrictedPrompt = {
    ...requiredPromptFields,
    ownershipParams: {
        cardinality: 'MULTIPLE',
        allowedOwners: null,
    },
} as FormPrompt;

export const multipleRestrictedPrompt = {
    ...requiredPromptFields,
    ownershipParams: {
        cardinality: 'MULTIPLE',
        allowedOwners,
        allowedOwnershipTypes: null,
    },
} as FormPrompt;

export const singleUnrestrictedWithTypesPrompt = {
    ...requiredPromptFields,
    ownershipParams: {
        cardinality: 'SINGLE',
        allowedOwners: null,
        allowedOwnershipTypes,
    },
} as FormPrompt;

export const singleRestrictedWithTypesPrompt = {
    ...requiredPromptFields,
    ownershipParams: {
        cardinality: 'SINGLE',
        allowedOwners,
        allowedOwnershipTypes,
    },
} as FormPrompt;

export const multipleUnrestrictedWithTypesPrompt = {
    ...requiredPromptFields,
    ownershipParams: {
        cardinality: 'MULTIPLE',
        allowedOwners: null,
        allowedOwnershipTypes,
    },
} as FormPrompt;

export const multipleRestrictedWithTypesPrompt = {
    ...requiredPromptFields,
    ownershipParams: {
        cardinality: 'MULTIPLE',
        allowedOwners,
        allowedOwnershipTypes,
    },
} as FormPrompt;

export const multipleUnrestrictedWithSingleTypePrompt = {
    ...requiredPromptFields,
    ownershipParams: {
        cardinality: 'MULTIPLE',
        allowedOwners: null,
        allowedOwnershipTypes: [{ urn: ownershipTypeUrn2 }],
    },
} as FormPrompt;
