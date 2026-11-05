import { normalizeFieldPathKey } from '@app/entityV2/dataset/profile/schema/utils/utils';

import { SchemaField, SchemaMetadata } from '@types';

/**
 * The primary key to badge: the source key (primaryKeys plus fields flagged isPartOfKey) when the
 * source declares one, otherwise the primary key users declared in the editablePrimaryKeys aspect.
 * Paths are normalized, so a key declared as `id` matches the v2 path `[version=2.0].[type=string].id`.
 */
export function getEffectivePrimaryKeys(
    schemaMetadata: Pick<SchemaMetadata, 'primaryKeys' | 'fields'> | undefined | null,
    editedPrimaryKeys: string[] | undefined | null,
): Set<string> {
    const sourceKeys = [
        ...(schemaMetadata?.primaryKeys || []),
        ...(schemaMetadata?.fields || []).filter((field) => field.isPartOfKey).map((field) => field.fieldPath),
    ];
    const keys = sourceKeys.length > 0 ? sourceKeys : editedPrimaryKeys || [];
    return new Set(keys.map(normalizeFieldPathKey).filter((key): key is string => !!key));
}

/**
 * A field's own isPartOfKey flag also counts, for callers that have no schemaMetadata (for example
 * chart input fields). Precedence holds: such a field already makes the source key non-empty.
 */
export function isPrimaryKeyField(primaryKeys: Set<string>, field: Pick<SchemaField, 'fieldPath' | 'isPartOfKey'>) {
    const key = normalizeFieldPathKey(field.fieldPath);
    return (!!key && primaryKeys.has(key)) || !!field.isPartOfKey;
}
