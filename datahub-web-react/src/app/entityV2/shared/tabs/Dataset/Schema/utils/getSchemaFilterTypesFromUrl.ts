/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';

import { SchemaFilterType } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';

export default function getSchemaFilterTypesFromUrl(location: any): SchemaFilterType[] {
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const schemaFilterTypeString = decodeURIComponent(
        params.schemaFilterTypes ? (params.schemaFilterTypes as string) : '',
    );
    if (!schemaFilterTypeString || schemaFilterTypeString.length < 1) {
        return [
            SchemaFilterType.Documentation,
            SchemaFilterType.FieldPath,
            SchemaFilterType.Tags,
            SchemaFilterType.Terms,
        ];
    }
    return schemaFilterTypeString.split(',') as SchemaFilterType[];
}
