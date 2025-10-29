import * as diff from 'diff';
import { KEY_SCHEMA_PREFIX, UNION_TOKEN, VERSION_PREFIX } from '@app/entity/dataset/profile/schema/utils/constants';
import { ExtendedSchemaFields } from '@app/entity/dataset/profile/schema/utils/types';

import {
    PlatformSchema,
    SchemaField,
} from '@types';

function filterKeyFieldPath(showKeySchema: boolean, field: SchemaField) {
    return field.fieldPath.indexOf(KEY_SCHEMA_PREFIX) > -1 ? showKeySchema : !showKeySchema;
}

export function downgradeV2FieldPath(fieldPath?: string | null) {
    if (!fieldPath) {
        return fieldPath;
    }

    const cleanedFieldPath = fieldPath.replace(KEY_SCHEMA_PREFIX, '').replace(VERSION_PREFIX, '');

    // strip out all annotation segments
    return cleanedFieldPath
        .split('.')
        .map((segment) => (segment.startsWith('[') ? null : segment))
        .filter(Boolean)
        .join('.');
}
