export function getSourceUrnFromSchemaFieldUrn(schemaFieldUrn: string) {
    return schemaFieldUrn.replace('urn:li:schemaField:(', '').split(')')[0].concat(')');
}

export function getFieldPathFromSchemaFieldUrn(schemaFieldUrn: string) {
    const val = schemaFieldUrn.replace('urn:li:schemaField:(', '').split(')')[1]?.replace(',', '') ?? '';
    try {
        return decodeURI(val);
    } catch (e) {
        return val;
    }
}

/*
 * Returns a link to the schemaField dataset with the field selected
 */
export function getSchemaFieldParentLink(schemaFieldUrn: string) {
    const fieldPath = getFieldPathFromSchemaFieldUrn(schemaFieldUrn);
    const parentUrn = getSourceUrnFromSchemaFieldUrn(schemaFieldUrn);

    return `/dataset/${parentUrn}/Columns?highlightedPath=${fieldPath}`;
}
