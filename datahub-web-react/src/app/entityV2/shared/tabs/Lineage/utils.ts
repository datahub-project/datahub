import { encodeSchemaField } from '@app/lineage/utils/columnLineageUtils';

export function generateSchemaFieldUrn(fieldPath: string | undefined | null, resourceUrn: string): string | null {
    if (!fieldPath || !resourceUrn) return null;
    return `urn:li:schemaField:(${resourceUrn},${encodeSchemaField(fieldPath)})`;
}
