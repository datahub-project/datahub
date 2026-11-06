import { downgradeV2FieldPath } from '@app/lineageV3/utils/downgradeV2FieldPath';

import { SchemaField, SchemaMetadata } from '@types';

/**
 * The field paths to badge as primary key: the source key (primaryKeys plus fields flagged isPartOfKey)
 * when the source declares one, otherwise the primary key users declared in the editablePrimaryKeys aspect.
 * Source keys match exactly. An edited key also matches fields by their simple path, so a key declared as
 * `id` marks the v2 field `[version=2.0].[type=string].id`.
 */
export function getEffectivePrimaryKeys(
    schemaMetadata: Pick<SchemaMetadata, 'primaryKeys' | 'fields'> | undefined | null,
    editedPrimaryKeys: string[] | undefined | null,
): Set<string> {
    const fields = schemaMetadata?.fields || [];
    const sourceKeys = [
        ...(schemaMetadata?.primaryKeys || []),
        ...fields.filter((field) => field.isPartOfKey).map((field) => field.fieldPath),
    ];
    if (sourceKeys.length > 0) return new Set(sourceKeys);
    const edited = new Set(editedPrimaryKeys || []);
    const v2Matches = fields.filter((field) => edited.has(downgradeV2FieldPath(field.fieldPath)));
    return new Set([...edited, ...v2Matches.map((field) => field.fieldPath)]);
}

/**
 * A field's own isPartOfKey flag also counts, for callers that have no schemaMetadata (for example
 * chart input fields). Precedence holds: such a field already makes the source key non-empty.
 */
export function isPrimaryKeyField(primaryKeys: Set<string>, field: Pick<SchemaField, 'fieldPath' | 'isPartOfKey'>) {
    return primaryKeys.has(field.fieldPath) || !!field.isPartOfKey;
}
