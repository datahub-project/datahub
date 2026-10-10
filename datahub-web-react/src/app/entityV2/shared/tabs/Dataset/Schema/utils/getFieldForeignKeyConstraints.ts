import { ForeignKeyConstraint, SchemaMetadata } from '@types';

/**
 * Returns the foreign key constraints of the schema in which the given field participates
 * as a source field.
 */
export default function getFieldForeignKeyConstraints(
    schemaMetadata: SchemaMetadata | undefined | null,
    fieldPath: string,
): ForeignKeyConstraint[] {
    return (schemaMetadata?.foreignKeys ?? []).filter(
        (constraint): constraint is ForeignKeyConstraint =>
            !!constraint &&
            (constraint.sourceFields ?? []).some((sourceField) => sourceField?.fieldPath?.trim() === fieldPath.trim()),
    );
}
