/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
