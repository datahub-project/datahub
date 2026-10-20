import { EditableSchemaMetadata, SchemaField, SchemaMetadata } from '@types';

/**
 * The primary key to badge: the source key (primaryKeys plus fields flagged isPartOfKey) when the
 * source declares one, otherwise the primary key users declared in editableSchemaMetadata.
 */
export function getEffectivePrimaryKeys(
    schemaMetadata: Pick<SchemaMetadata, 'primaryKeys' | 'fields'> | undefined | null,
    editableSchemaMetadata: Pick<EditableSchemaMetadata, 'primaryKeys'> | undefined | null,
): Set<string> {
    const sourceKeys = new Set([
        ...(schemaMetadata?.primaryKeys || []),
        ...(schemaMetadata?.fields || []).filter((field) => field.isPartOfKey).map((field) => field.fieldPath),
    ]);
    return sourceKeys.size > 0 ? sourceKeys : new Set(editableSchemaMetadata?.primaryKeys || []);
}

/**
 * A field's own isPartOfKey flag also counts, for callers that have no schemaMetadata (for example
 * chart input fields). Precedence holds: such a field already makes the source key non-empty.
 */
export function isPrimaryKeyField(primaryKeys: Set<string>, field: Pick<SchemaField, 'fieldPath' | 'isPartOfKey'>) {
    return primaryKeys.has(field.fieldPath) || !!field.isPartOfKey;
}
