import { volcano, lime, geekblue, purple, gold, yellow } from '@ant-design/colors';
import { Schema, SchemaField, SchemaFieldDataType } from '../../../../../types.generated';

// Extending the schema type with an option for tags
export type TaggedSchemaField = {
    tags: Tag[];
} & SchemaField;

const TAGS = {
    pii: { name: 'PII', color: volcano[1], category: 'Privacy' },
    financial: { name: 'Financial', color: gold[1], category: 'Privacy' },
    address: { name: 'Address', color: lime[1], category: 'Descriptor', descriptor: true },
    shipping: { name: 'Shipping', color: yellow[1], category: 'Privacy' },
    name: { name: 'Name', color: purple[1], category: 'Descriptor', descriptor: true },
    euro: { name: 'Currency', value: 'Euros', color: geekblue[1], category: 'Descriptor', descriptor: true },
};

export type Tag = {
    name: string;
    value?: string;
    color: string;
    category: string;
    descriptor?: boolean;
};

export const sampleSchema: Schema = {
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
        },
        {
            fieldPath: 'name',
            nullable: true,
            description: 'the name of the order',
            type: SchemaFieldDataType.String,
            nativeDataType: 'string',
            recursive: false,
            tags: [TAGS.name, TAGS.pii],
        } as SchemaField,
        {
            fieldPath: 'shipping_address',
            nullable: true,
            description: 'the address the order ships to',
            type: SchemaFieldDataType.String,
            nativeDataType: 'string',
            recursive: false,
            tags: [TAGS.address, TAGS.pii, TAGS.shipping],
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
            tags: [TAGS.euro],
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
            tags: [TAGS.financial],
        } as SchemaField,
    ],
};
