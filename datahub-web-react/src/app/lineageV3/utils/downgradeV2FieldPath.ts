import { KEY_SCHEMA_PREFIX, VERSION_PREFIX } from '@app/entityV2/dataset/profile/schema/utils/constants';

export function downgradeV2FieldPath(fieldPath: string): string;
export function downgradeV2FieldPath(fieldPath?: string | null): string | null | undefined;
export function downgradeV2FieldPath(fieldPath?: string | null) {
    if (!fieldPath) {
        return fieldPath;
    }

    const cleanedFieldPath = fieldPath.replace(KEY_SCHEMA_PREFIX, '').replace(VERSION_PREFIX, '');

    // Remove all bracket annotations (e.g., [0], [*], [key]) from the field path
    return cleanedFieldPath
        .split('.')
        .map((segment) => {
            // Remove segments that are entirely brackets (e.g., "[0]", "[*]")
            if (segment.startsWith('[') && segment.endsWith(']')) {
                return null;
            }
            // Remove bracket suffixes attached to a name (e.g., "addresses[0]" -> "addresses"), but keep
            // brackets after a space, which are part of the column name (e.g., "Temp [C]").
            return segment.replace(/(\S)(?:\[[^\]]*\])+/g, '$1');
        })
        .filter(Boolean)
        .join('.');
}
