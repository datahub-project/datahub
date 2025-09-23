/* eslint-disable */
import EntityRegistry from '@src/app/entity/EntityRegistry';
import { GetAutoCompleteResultsDocument } from '@src/graphql/search.generated';
import { Entity, EntityType } from '@src/types.generated';

// Mock data for entities and selected entities
export const defaultTagEntities = [
    {
        urn: 'urn:li:tag:1',
        name: 'Tag 1',
        item: {
            tag: {
                __typename: 'Tag',
                urn: 'urn:li:tag:1',
                type: 'TAG',
                name: 'Usage - C1',
                description: 'Usage - C',
                properties: { __typename: 'TagProperties', name: 'Usage - C1' },
            },
        },
    },
    {
        urn: 'urn:li:tag:2',
        name: 'Tag 2',
        item: {
            tag: {
                __typename: 'Tag',
                urn: 'urn:li:tag:2',
                type: 'TAG',
                name: 'Usage - C2',
                description: 'Usage - C',
                properties: { __typename: 'TagProperties', name: 'Usage - C2' },
            },
        },
    },
    {
        urn: 'urn:li:tag:3',
        name: 'Tag 3',
        item: {
            tag: {
                __typename: 'Tag',
                urn: 'urn:li:tag:3',
                type: 'TAG',
                name: 'Usage - C3',
                description: 'Usage - C',
                properties: { __typename: 'TagProperties', name: 'Usage - C3' },
            },
        },
    },
];

const mockTagEntity: Entity = {
    type: EntityType.Tag, // Add the required 'type' property
    urn: 'urn:li:tag:1',
    name: 'Tag 1',
    icon: () => null, // Mock icon function
    isSearchEnabled: () => true,
    isBrowseEnabled: () => true,
    isLineageEnabled: () => true,
    getCollectionName: () => 'tag',
    getPathName: () => 'tag',
    getGraphName: () => 'TagGraph',
    renderPreview: () => null,
    renderProfile: () => null,
    displayName: (data) => {
        return data.name;
    },
    getEntityName: (type) => {
        return type;
    },
    // Add other methods required by Entity interface
} as Entity;

const mockGlossaryTermEntity: Entity = {
    type: EntityType.GlossaryTerm, // Add the required 'type' property
    urn: 'urn:li:glossaryTerm:1',
    name: 'Tag 1',
    icon: () => null, // Mock icon function
    isSearchEnabled: () => true,
    isBrowseEnabled: () => true,
    isLineageEnabled: () => true,
    getCollectionName: () => 'glossary terms',
    getPathName: () => 'GlossaryTerm',
    getGraphName: () => 'GlossaryTermGraph',
    renderPreview: () => null,
    renderProfile: () => null,
    displayName: (data) => {
        return data.name;
    },
    getEntityName: (type) => {
        return type;
    },
    // Add other methods required by Entity interface
} as Entity;

export const mockTagSelectedEntities = [{ urn: 'urn:li:tag:1', name: 'Tag 1' }];

export const mockTagEntityRegistry = new EntityRegistry();
mockTagEntityRegistry.entityTypeToEntity = new Map();
// @ts-expect-error
mockTagEntityRegistry.entityTypeToEntity.set(EntityType.Tag, mockTagEntity);

export const mockGlossaryTermEntityRegistry = new EntityRegistry();
mockGlossaryTermEntityRegistry.entityTypeToEntity = new Map();
// @ts-expect-error
mockGlossaryTermEntityRegistry.entityTypeToEntity.set(EntityType.GlossaryTerm, mockGlossaryTermEntity);

export const tagMocks = [
    {
        request: {
            query: GetAutoCompleteResultsDocument,
            variables: {
                input: {
                    type: 'TAG',
                    query: 'a',
                    limit: 10,
                },
            },
        },
        result: {
            data: {
                autoComplete: {
                    query: 'a',
                    suggestions: [
                        'Metadata Completeness - A',
                        'Usage - A',
                        'analytics-team-access',
                        'example tag',
                        'ATTRIBUTE',
                    ],
                    entities: [
                        {
                            urn: 'urn:li:tag:Metadata Completeness - A',
                            type: 'TAG',
                            name: 'Metadata Completeness - A',
                            properties: {
                                name: 'Metadata Completeness - A',
                                colorHex: null,
                                __typename: 'Tag',
                            },
                            __typename: 'Tag',
                        },
                        {
                            urn: 'urn:li:tag:Usage - A',
                            type: 'TAG',
                            name: 'Usage - A',
                            properties: {
                                name: 'Usage - A',
                                colorHex: null,
                            },
                            __typename: 'Tag',
                        },
                    ],
                },
            },
        },
    },
];

export const defaultGlossaryTermEntities = [
    {
        urn: 'urn:li:glossaryTerm:Classification.Sensitive',
        type: 'GLOSSARY_TERM',
        name: 'Sensitive',
        hierarchicalName: 'Classification.Sensitive',
        properties: {
            name: 'Sensitive',
            __typename: 'GlossaryTermProperties',
        },
        parentNodes: {
            count: 1,
            nodes: [
                {
                    urn: 'urn:li:glossaryNode:Classification',
                    type: 'GLOSSARY_NODE',
                    properties: {
                        name: 'Classification',
                        __typename: 'GlossaryNodeProperties',
                    },
                    displayProperties: null,
                    __typename: 'GlossaryNode',
                },
            ],
            __typename: 'ParentNodesResult',
        },
        __typename: 'GlossaryTerm',
    },
    {
        urn: 'urn:li:glossaryTerm:PIITerms.SSN',
        type: 'GLOSSARY_TERM',
        name: 'SSN',
        hierarchicalName: 'PIITerms.SSN',
        properties: {
            name: 'SSN',
            __typename: 'GlossaryTermProperties',
        },
        parentNodes: {
            count: 1,
            nodes: [
                {
                    urn: 'urn:li:glossaryNode:PIITerms',
                    type: 'GLOSSARY_NODE',
                    properties: {
                        name: 'Information Types',
                        __typename: 'GlossaryNodeProperties',
                    },
                    displayProperties: null,
                    __typename: 'GlossaryNode',
                },
            ],
            __typename: 'ParentNodesResult',
        },
        __typename: 'GlossaryTerm',
    },
    {
        urn: 'urn:li:glossaryTerm:PIITerms.ShippingAddress',
        type: 'GLOSSARY_TERM',
        name: 'ShippingAddress',
        hierarchicalName: 'PIITerms.ShippingAddress',
        properties: {
            name: 'ShippingAddress',
            __typename: 'GlossaryTermProperties',
        },
        parentNodes: {
            count: 1,
            nodes: [
                {
                    urn: 'urn:li:glossaryNode:PIITerms',
                    type: 'GLOSSARY_NODE',
                    properties: {
                        name: 'Information Types',
                        __typename: 'GlossaryNodeProperties',
                    },
                    displayProperties: null,
                    __typename: 'GlossaryNode',
                },
            ],
            __typename: 'ParentNodesResult',
        },
        __typename: 'GlossaryTerm',
    },
    {
        urn: 'urn:li:glossaryTerm:PersonalInformation.SocialSecurityNumber',
        type: 'GLOSSARY_TERM',
        name: 'SocialSecurityNumber',
        hierarchicalName: 'PersonalInformation.SocialSecurityNumber',
        properties: {
            name: 'SocialSecurityNumber',
            __typename: 'GlossaryTermProperties',
        },
        parentNodes: {
            count: 1,
            nodes: [
                {
                    urn: 'urn:li:glossaryNode:PersonalInformation',
                    type: 'GLOSSARY_NODE',
                    properties: {
                        name: 'Personal Information - Domestic (PII)',
                        __typename: 'GlossaryNodeProperties',
                    },
                    displayProperties: null,
                    __typename: 'GlossaryNode',
                },
            ],
            __typename: 'ParentNodesResult',
        },
        __typename: 'GlossaryTerm',
    },
    {
        urn: 'urn:li:glossaryTerm:PIITerms2.SecurityNumber',
        type: 'GLOSSARY_TERM',
        name: 'SecurityNumber',
        hierarchicalName: 'PIITerms2.SecurityNumber',
        properties: {
            name: 'SocialSecurityNumber',
            __typename: 'GlossaryTermProperties',
        },
        parentNodes: {
            count: 1,
            nodes: [
                {
                    urn: 'urn:li:glossaryNode:PIITerms2',
                    type: 'GLOSSARY_NODE',
                    properties: {
                        name: 'Personal Information - Domestic (PII)',
                        __typename: 'GlossaryNodeProperties',
                    },
                    displayProperties: null,
                    __typename: 'GlossaryNode',
                },
            ],
            __typename: 'ParentNodesResult',
        },
        __typename: 'GlossaryTerm',
    },
];
export const mockGlossaryTermSelectedEntities = [
    { urn: 'urn:li:glossaryTerm:Classification.Sensitive', name: 'Sensitive' },
];

export const glossaryTermMocks = [
    {
        request: {
            query: GetAutoCompleteResultsDocument,
            variables: {
                input: {
                    type: 'GLOSSARY_TERM',
                    query: 'b',
                    limit: 10,
                },
            },
        },
        result: {
            data: {
                autoComplete: {
                    query: 'b',
                    suggestions: [
                        'Classification.Sensitive',
                        'PIITerms.SSN',
                        'PIITerms.ShippingAddress',
                        'PersonalInformation.SocialSecurityNumber',
                        '079618b2-7972-4d5d-bdb0-e55ac54f4bac',
                        '18ff11b1-402d-475a-b79f-c74befc28823',
                        '2458ac44-e484-41b9-aaaa-7a9166451555',
                        '339b7f42-1eec-4af9-ae24-e2064fb4ddf8',
                        '77a6b516562a18ce0f3d3f43fe513547',
                        '86e1d17052ef5cb737d884d6067cd82c',
                    ],
                    entities: [
                        {
                            urn: 'urn:li:glossaryTerm:PIITerms.Age',
                            type: 'GLOSSARY_TERM',
                            name: 'Age',
                            hierarchicalName: 'PIITerms.Age',
                            properties: {
                                name: 'Age',
                            },
                            parentNodes: {
                                count: 1,
                                nodes: [
                                    {
                                        urn: 'urn:li:glossaryNode:PIITerms',
                                        type: 'GLOSSARY_NODE',
                                        properties: {
                                            name: 'Information Types',
                                            __typename: 'GlossaryNodeProperties',
                                        },
                                        displayProperties: null,
                                        __typename: 'GlossaryNode',
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
        },
    },
];
