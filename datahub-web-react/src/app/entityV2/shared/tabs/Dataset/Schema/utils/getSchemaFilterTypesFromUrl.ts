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
