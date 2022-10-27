export function generateSchemaFieldUrn(fieldPath: string | undefined, resourceUrn: string) {
    if (!fieldPath) return null;
    return `urn:li:schemaField:(${resourceUrn},${fieldPath})`;
}
