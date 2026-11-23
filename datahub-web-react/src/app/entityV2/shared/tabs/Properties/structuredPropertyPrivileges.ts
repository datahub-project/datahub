import { EntityType } from '@types';

type StructuredPropertyPrivilegeFlags = {
    canEditProperties?: boolean | null;
    canEditSchemaFieldProperties?: boolean | null;
};

type StructuredPropertyPrivilegeEntity = {
    type?: EntityType;
    privileges?: StructuredPropertyPrivilegeFlags | null;
    parent?: { privileges?: StructuredPropertyPrivilegeFlags | null } | null;
};

/**
 * Entity properties and column properties are different privileges. A schema-field page has no
 * privileges of its own; the dataset parent carries both flags.
 */
export function canEditDisplayedStructuredProperties(
    entityData: StructuredPropertyPrivilegeEntity | null | undefined,
    fieldScope: boolean,
): boolean {
    const columnProperties = fieldScope || entityData?.type === EntityType.SchemaField;
    const privileges =
        entityData?.type === EntityType.SchemaField ? entityData.parent?.privileges : entityData?.privileges;
    return columnProperties ? !!privileges?.canEditSchemaFieldProperties : !!privileges?.canEditProperties;
}
