import { SchemaAssertionCompatibility, SchemaAssertionField, SchemaFieldDataType } from '@types';

export const compatibilityLevels = [
    {
        id: SchemaAssertionCompatibility.Superset,
        name: 'Contains All',
        description: 'The actual schema must contain all expected columns. It may contain additional columns.',
    },
    {
        id: SchemaAssertionCompatibility.ExactMatch,
        name: 'Exact Match',
        description:
            'The columns in the actual schema must exactly match those in the expected schema. It may not contain any additional columns.',
    },
];

export const supportedSchemaFieldTypes = [
    {
        type: SchemaFieldDataType.String,
        name: 'String',
    },
    {
        type: SchemaFieldDataType.Number,
        name: 'Number',
    },
    {
        type: SchemaFieldDataType.Boolean,
        name: 'Boolean',
    },
    {
        type: SchemaFieldDataType.Date,
        name: 'Date',
    },
    {
        type: SchemaFieldDataType.Time,
        name: 'Timestamp',
    },
    {
        type: SchemaFieldDataType.Struct,
        name: 'Struct',
    },
    {
        type: SchemaFieldDataType.Array,
        name: 'Array',
    },
    {
        type: SchemaFieldDataType.Map,
        name: 'Map',
    },
    {
        type: SchemaFieldDataType.Union,
        name: 'Union',
    },
    {
        type: SchemaFieldDataType.Bytes,
        name: 'Bytes',
    },
    {
        type: SchemaFieldDataType.Enum,
        name: 'Enum',
    },
];

// Checks that
// 1. Columns are completed (path + type)
// 2. Column path types are not conflicting.
export const areExpectedColumnsValid = (fields: Partial<SchemaAssertionField>[]): boolean => {
    const pathTypeMap: Record<string, string> = {};

    // Use `every` to ensure all fields are valid
    return fields.every((field) => {
        // Check if path and type exist
        if (!field.path || !field.type) {
            return false;
        }

        const existingType = pathTypeMap[field.path];
        // If the path is already mapped and the types do not match, return false
        if (existingType && existingType !== field.type) {
            return false;
        }

        // Map this path to the associated type
        pathTypeMap[field.path] = field.type;
        return true;
    });
};
