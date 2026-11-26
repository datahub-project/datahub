import { SchemaMetadataFieldsFragment } from '@graphql/fragments.generated';

export type ForeignKeySchemaMetadata = Pick<SchemaMetadataFieldsFragment, 'foreignKeys'>;
export type FieldForeignKeyConstraint = NonNullable<NonNullable<ForeignKeySchemaMetadata['foreignKeys']>[number]>;

/**
 * Returns the foreign key constraints of the schema in which the given field participates
 * as a source field.
 */
export default function getFieldForeignKeyConstraints(
    schemaMetadata: ForeignKeySchemaMetadata | undefined | null,
    fieldPath: string,
): FieldForeignKeyConstraint[] {
    return (schemaMetadata?.foreignKeys ?? []).filter(
        (constraint): constraint is FieldForeignKeyConstraint =>
            !!constraint &&
            (constraint.sourceFields ?? []).some((sourceField) => sourceField?.fieldPath?.trim() === fieldPath.trim()),
    );
}
