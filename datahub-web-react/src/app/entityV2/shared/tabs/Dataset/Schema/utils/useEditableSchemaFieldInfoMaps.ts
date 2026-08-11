import { useMemo } from 'react';

import { downgradeV2FieldPath } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaFieldInfo, EditableSchemaMetadata } from '@src/types.generated';

type EditableFieldInfoMaps = {
    // O(1) lookup by exact fieldPath — first occurrence wins (matches .find() semantics)
    exactMap: Map<string, EditableSchemaFieldInfo>;
    // O(1) lookup by lowercased downgraded path for path-insensitive matching
    v2NormalizedMap: Map<string, EditableSchemaFieldInfo[]>;
};

/**
 * Builds lookup maps from editableSchemaMetadata once per metadata identity.
 * Used by useExtractFieldTagsInfo and useExtractFieldGlossaryTermsInfo to avoid
 * O(n) .find()/.filter() scans on every rendered row.
 */
export default function useEditableSchemaFieldInfoMaps(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
): EditableFieldInfoMaps {
    return useMemo(() => {
        const exactMap = new Map<string, EditableSchemaFieldInfo>();
        const v2NormalizedMap = new Map<string, EditableSchemaFieldInfo[]>();
        (editableSchemaMetadata?.editableSchemaFieldInfo ?? []).forEach((info) => {
            if (!exactMap.has(info.fieldPath)) exactMap.set(info.fieldPath, info);
            // Lowercase to preserve pathMatchesInsensitiveToV2 / ING-2174 semantics.
            const normalizedPath = (downgradeV2FieldPath(info.fieldPath) ?? info.fieldPath).toLowerCase();
            if (!v2NormalizedMap.has(normalizedPath)) v2NormalizedMap.set(normalizedPath, []);
            v2NormalizedMap.get(normalizedPath)!.push(info);
        });
        return { exactMap, v2NormalizedMap };
    }, [editableSchemaMetadata]);
}
