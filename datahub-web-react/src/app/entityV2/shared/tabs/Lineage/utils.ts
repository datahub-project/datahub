import { encodeSchemaField } from '../../../../lineage/utils/columnLineageUtils';

export function generateSchemaFieldUrn(fieldPath: string, resourceUrn: string) {
    return `urn:li:schemaField:(${resourceUrn},${encodeSchemaField(fieldPath)})`;
}
