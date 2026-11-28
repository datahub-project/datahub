import { decodeSchemaField, encodeSchemaField } from '@app/lineageV3/utils/columnLineageUtils';

import { SchemaMetadataFieldsFragment } from '@graphql/fragments.generated';

export type ForeignKeySchemaMetadata = Pick<SchemaMetadataFieldsFragment, 'foreignKeys'>;
export type FieldForeignKeyConstraint = NonNullable<NonNullable<ForeignKeySchemaMetadata['foreignKeys']>[number]>;

// Python ingestion also escapes this separator in foreign key URNs.
const URN_SEPARATOR = '␟';
const ENCODED_URN_SEPARATOR = encodeURIComponent(URN_SEPARATOR);

export function decodeForeignKeyFieldPath(fieldPath: string): string {
    return decodeSchemaField(fieldPath).replaceAll(ENCODED_URN_SEPARATOR, URN_SEPARATOR);
}

/** Matches source fields only. */
export default function getFieldForeignKeyConstraints(
    schemaMetadata: ForeignKeySchemaMetadata | undefined | null,
    fieldPath: string,
): FieldForeignKeyConstraint[] {
    const rawPath = fieldPath.trim();
    const urnPath = encodeSchemaField(rawPath).replaceAll(URN_SEPARATOR, ENCODED_URN_SEPARATOR);
    return (schemaMetadata?.foreignKeys ?? []).filter(
        (constraint): constraint is FieldForeignKeyConstraint =>
            !!constraint &&
            (constraint.sourceFields ?? []).some((sourceField) => {
                const sourcePath = sourceField?.fieldPath?.trim();
                return sourcePath === rawPath || sourcePath === urnPath;
            }),
    );
}
