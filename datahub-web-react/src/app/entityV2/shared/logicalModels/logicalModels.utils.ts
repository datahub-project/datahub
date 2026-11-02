import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { LogicalModelColumnDraft } from '@app/entityV2/shared/logicalModels/logicalModels.types';

import { EntityType, SchemaFieldDataType } from '@types';

/** Maps a dataset's schema fields to the editable column draft shape (name + type). */
export function schemaFieldsToColumns(
    fields: ReadonlyArray<{ fieldPath: string; type: SchemaFieldDataType }> | null | undefined,
): LogicalModelColumnDraft[] {
    return (fields ?? []).map((field) => ({ fieldPath: field.fieldPath, type: field.type }));
}

/**
 * Like {@link schemaFieldsToColumns} but throws when the schema couldn't be read. The column
 * add/edit/delete flows read the current columns, mutate them, and write the whole list back via
 * updateLogicalModelSchema (a full replacement). A logical model always has >=1 column, so an
 * absent/empty result means the read failed or was stale — proceeding would silently overwrite the
 * entire schema (and unlink every physical child). Callers must abort instead.
 */
export function columnsFromSchemaOrThrow(
    fields: ReadonlyArray<{ fieldPath: string; type: SchemaFieldDataType }> | null | undefined,
): LogicalModelColumnDraft[] {
    if (!fields || fields.length === 0) {
        throw new Error('Logical model schema could not be read');
    }
    return schemaFieldsToColumns(fields);
}

/** A logical model is a dataset on a platform marked as logical (hand-authored, no ingestion source). */
export function isLogicalModel(entityType: EntityType, entityData: GenericEntityProperties | null): boolean {
    return entityType === EntityType.Dataset && !!entityData?.platform?.properties?.logical;
}

/** The DataHub schema field data types selectable for a logical model column. */
export const LOGICAL_MODEL_COLUMN_TYPE_OPTIONS: SchemaFieldDataType[] = [
    SchemaFieldDataType.String,
    SchemaFieldDataType.Number,
    SchemaFieldDataType.Boolean,
    SchemaFieldDataType.Date,
    SchemaFieldDataType.Time,
    SchemaFieldDataType.Bytes,
    SchemaFieldDataType.Enum,
    SchemaFieldDataType.Array,
    SchemaFieldDataType.Map,
    SchemaFieldDataType.Struct,
    SchemaFieldDataType.Union,
    SchemaFieldDataType.Fixed,
    SchemaFieldDataType.Null,
];

/**
 * Adds the Delete action to the header menu for logical models only (datasets on a platform
 * marked as logical), when the feature is enabled. Regular datasets are unaffected.
 */
export function withLogicalModelHeaderItems(
    entityType: EntityType,
    entityData: GenericEntityProperties | null,
    baseItems: Set<EntityMenuItems> | undefined,
    logicalModelsEnabled: boolean,
): Set<EntityMenuItems> | undefined {
    if (logicalModelsEnabled && baseItems && isLogicalModel(entityType, entityData)) {
        return new Set([...baseItems, EntityMenuItems.DELETE]);
    }
    return baseItems;
}
