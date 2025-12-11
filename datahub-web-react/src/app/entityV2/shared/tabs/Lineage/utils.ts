/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { encodeSchemaField } from '@app/lineage/utils/columnLineageUtils';

export function generateSchemaFieldUrn(fieldPath: string | undefined | null, resourceUrn: string): string | null {
    if (!fieldPath || !resourceUrn) return null;
    return `urn:li:schemaField:(${resourceUrn},${encodeSchemaField(fieldPath)})`;
}
