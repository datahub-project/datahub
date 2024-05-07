import { SchemaAssertionCompatibility, SchemaFieldDataType } from "../../../../../../../../../../types.generated";


export const compatibilityLevels = [
    {
        id: SchemaAssertionCompatibility.Superset,
        name: "Contains All",
        description: "The actual schema must contain all expected columns. It may contain additional columns."
    },
    {
        id: SchemaAssertionCompatibility.ExactMatch,
        name: "Exact Match",
        description: "The columns in the actual schema must exactly match those in the expected schema. It may not contain any additional columns."
    }
]

export const supportedSchemaFieldTypes = [
    {
        type: SchemaFieldDataType.String,
        name: "String"
    },
    {
        type: SchemaFieldDataType.Number,
        name: "Number"
    },
    {
        type: SchemaFieldDataType.Boolean,
        name: "Boolean"
    },
    {
        type: SchemaFieldDataType.Date,
        name: "Date"
    },
    {
        type: SchemaFieldDataType.Time,
        name: "Timestamp"
    },
    {
        type: SchemaFieldDataType.Struct,
        name: "Struct"
    },
    {
        type: SchemaFieldDataType.Array,
        name: "Array"
    },
    {
        type: SchemaFieldDataType.Map,
        name: "Map"
    },
    {
        type: SchemaFieldDataType.Union,
        name: "Union"
    },
    {
        type: SchemaFieldDataType.Bytes,
        name: "Bytes"
    },
    {
        type: SchemaFieldDataType.Enum,
        name: "Enum"
    },
]