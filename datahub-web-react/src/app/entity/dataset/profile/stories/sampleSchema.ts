import { dataset3 } from '../../../../../Mocks';
import { EntityType, Schema, SchemaMetadata, SchemaField, SchemaFieldDataType } from '../../../../../types.generated';

// Extending the schema type with an option for tags
export type TaggedSchemaField = {
    tags: Tag[];
} & SchemaField;

export type Tag = {
    name: string;
    value?: string;
    color: string;
    category: string;
    descriptor?: boolean;
};

export const sampleSchema: SchemaMetadata | Schema | null = {
    name: 'MockSchema',
    platformUrn: 'mock:urn',
    version: 1,
    hash: '',
    fields: [
        {
            fieldPath: 'id',
            nullable: false,
            description: 'order id',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        },
        {
            fieldPath: 'name',
            nullable: true,
            description: 'the name of the order',
            type: SchemaFieldDataType.String,
            nativeDataType: 'string',
            recursive: false,
        },
        {
            fieldPath: 'shipping_address',
            nullable: true,
            description: 'the address the order ships to',
            type: SchemaFieldDataType.String,
            nativeDataType: 'string',
            recursive: false,
        },
        {
            fieldPath: 'count',
            nullable: true,
            description: 'the number of items in the order',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        },
        {
            fieldPath: 'cost',
            nullable: true,
            description: 'the dollar value of the order',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        },
        {
            fieldPath: 'was_returned',
            nullable: true,
            description: 'if the order was sent back',
            type: SchemaFieldDataType.Boolean,
            nativeDataType: 'boolean',
            recursive: false,
        },
        {
            fieldPath: 'payload',
            nullable: true,
            description: 'payload attached to the order',
            type: SchemaFieldDataType.Bytes,
            nativeDataType: 'bytes',
            recursive: false,
        },
        {
            fieldPath: 'payment_information',
            nullable: true,
            description: 'struct representing the payment information',
            type: SchemaFieldDataType.Struct,
            nativeDataType: 'struct',
            recursive: false,
        },
    ],
    platformSchema: {
        __typename: 'TableSchema',
        schema: '{ "type": "record", "name": "SampleHdfsSchema", "namespace": "com.linkedin.dataset", "doc": "Sample HDFS dataset", "fields": [ { "name": "field_foo", "type": [ "string" ] }, { "name": "field_bar", "type": [ "boolean" ] } ] }',
    },
};

export const sampleSchemaWithTags: Schema = {
    name: 'MockSchema',
    platformUrn: 'mock:urn',
    version: 1,
    hash: '',
    fields: [
        {
            fieldPath: 'id',
            nullable: false,
            description: 'order id',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
            globalTags: {
                tags: [
                    {
                        tag: {
                            urn: 'urn:li:tag:Legacy',
                            name: 'Legacy',
                            description: 'this is a legacy dataset',
                            type: EntityType.Tag,
                        },
                        associatedUrn: 'mock:urn',
                    },
                ],
            },
            glossaryTerms: {
                terms: [
                    {
                        term: {
                            type: EntityType.GlossaryTerm,
                            name: 'sample-glossary-term',
                            urn: 'urn:li:glossaryTerm:sample-glossary-term',
                            hierarchicalName: 'example.sample-glossary-term',
                            properties: {
                                name: 'sample-glossary-term',
                                description: 'sample definition',
                                definition: 'sample definition',
                                termSource: 'sample term source',
                            },
                        },
                        associatedUrn: 'mock:urn',
                    },
                ],
            },
        },
        {
            fieldPath: 'name',
            nullable: true,
            description: 'the name of the order',
            type: SchemaFieldDataType.String,
            nativeDataType: 'string',
            recursive: false,
        } as SchemaField,
        {
            fieldPath: 'shipping_address',
            nullable: true,
            description: 'the address the order ships to',
            type: SchemaFieldDataType.String,
            nativeDataType: 'string',
            recursive: false,
        } as SchemaField,
        {
            fieldPath: 'count',
            nullable: true,
            description: 'the number of items in the order',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        },
        {
            fieldPath: 'cost',
            nullable: true,
            description: 'the dollar value of the order',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        } as SchemaField,
        {
            fieldPath: 'was_returned',
            nullable: true,
            description: 'if the order was sent back',
            type: SchemaFieldDataType.Boolean,
            nativeDataType: 'boolean',
            recursive: false,
        },
        {
            fieldPath: 'payload',
            nullable: true,
            description: 'payload attached to the order',
            type: SchemaFieldDataType.Bytes,
            nativeDataType: 'bytes',
            recursive: false,
        },
        {
            fieldPath: 'payment_information',
            nullable: true,
            description: 'struct representing the payment information',
            type: SchemaFieldDataType.Struct,
            nativeDataType: 'struct',
            recursive: false,
        } as SchemaField,
    ],
};

export const sampleSchemaWithPkFk: SchemaMetadata = {
    primaryKeys: ['name'],
    foreignKeys: [
        {
            name: 'constraint',
            sourceFields: [
                {
                    urn: 'datasetUrn',
                    parent: 'dataset',
                    fieldPath: 'shipping_address',
                },
            ],
            foreignFields: [
                {
                    urn: dataset3.urn,
                    parent: dataset3.name,
                    fieldPath: 'address',
                },
            ],
            foreignDataset: dataset3,
        },
    ],
    name: 'MockSchema',
    platformUrn: 'mock:urn',
    version: 1,
    hash: '',
    fields: [
        {
            fieldPath: 'id',
            nullable: false,
            description: 'order id',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
            globalTags: {
                tags: [
                    {
                        tag: {
                            urn: 'urn:li:tag:Legacy',
                            name: 'Legacy',
                            description: 'this is a legacy dataset',
                            type: EntityType.Tag,
                        },
                        associatedUrn: 'mock:urn',
                    },
                ],
            },
            glossaryTerms: {
                terms: [
                    {
                        term: {
                            type: EntityType.GlossaryTerm,
                            urn: 'urn:li:glossaryTerm:sample-glossary-term',
                            name: 'sample-glossary-term',
                            hierarchicalName: 'example.sample-glossary-term',
                            properties: {
                                name: 'sample-glossary-term',
                                description: 'sample definition',
                                definition: 'sample definition',
                                termSource: 'sample term source',
                            },
                        },
                        associatedUrn: 'mock:urn',
                    },
                ],
            },
        },
        {
            fieldPath: 'name',
            nullable: true,
            description: 'the name of the order',
            type: SchemaFieldDataType.String,
            nativeDataType: 'string',
            recursive: false,
        } as SchemaField,
        {
            fieldPath: 'shipping_address',
            nullable: true,
            description: 'the address the order ships to',
            type: SchemaFieldDataType.String,
            nativeDataType: 'string',
            recursive: false,
        } as SchemaField,
        {
            fieldPath: 'count',
            nullable: true,
            description: 'the number of items in the order',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        },
        {
            fieldPath: 'cost',
            nullable: true,
            description: 'the dollar value of the order',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        } as SchemaField,
        {
            fieldPath: 'was_returned',
            nullable: true,
            description: 'if the order was sent back',
            type: SchemaFieldDataType.Boolean,
            nativeDataType: 'boolean',
            recursive: false,
        },
        {
            fieldPath: 'payload',
            nullable: true,
            description: 'payload attached to the order',
            type: SchemaFieldDataType.Bytes,
            nativeDataType: 'bytes',
            recursive: false,
        },
        {
            fieldPath: 'payment_information',
            nullable: true,
            description: 'struct representing the payment information',
            type: SchemaFieldDataType.Struct,
            nativeDataType: 'struct',
            recursive: false,
        } as SchemaField,
    ],
};

export const sampleSchemaWithoutFields: SchemaMetadata | Schema | null = {
    name: 'MockSchema',
    platformUrn: 'mock:urn',
    version: 1,
    hash: '',
    fields: [],
};

export const sampleSchemaWithKeyValueFields: SchemaMetadata | Schema | null = {
    name: 'MockSchema',
    platformUrn: 'mock:urn',
    version: 1,
    hash: '',
    fields: [
        {
            fieldPath: '[key=True].[version=2.0].id',
            nullable: true,
            description: 'the number of items in the order',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        },
        {
            fieldPath: 'count',
            nullable: true,
            description: 'the number of items in the order',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        },
        {
            fieldPath: 'cost',
            nullable: true,
            description: 'the dollar value of the order',
            type: SchemaFieldDataType.Number,
            nativeDataType: 'number',
            recursive: false,
        } as SchemaField,
    ],
};
