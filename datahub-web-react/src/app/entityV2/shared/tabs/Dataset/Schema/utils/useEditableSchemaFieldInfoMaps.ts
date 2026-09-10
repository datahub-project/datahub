import { useMemo } from 'react';

import { normalizeFieldPathKey } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaFieldInfo, EditableSchemaMetadata } from '@src/types.generated';

export type EditableFieldInfoMaps = {
    // O(1) lookup by exact fieldPath — first occurrence wins (matches .find() semantics)
    exactMap: Map<string, EditableSchemaFieldInfo>;
    // O(1) lookup by lowercased downgraded path for path-insensitive matching
    v2NormalizedMap: Map<string, EditableSchemaFieldInfo[]>;
};

export function buildEditableSchemaFieldInfoMaps(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
): EditableFieldInfoMaps {
    const exactMap = new Map<string, EditableSchemaFieldInfo>();
    const v2NormalizedMap = new Map<string, EditableSchemaFieldInfo[]>();
    (editableSchemaMetadata?.editableSchemaFieldInfo ?? []).forEach((info) => {
        if (!exactMap.has(info.fieldPath)) exactMap.set(info.fieldPath, info);
        const normalizedPath = normalizeFieldPathKey(info.fieldPath);
        if (!normalizedPath) {
            return;
        }
        if (!v2NormalizedMap.has(normalizedPath)) v2NormalizedMap.set(normalizedPath, []);
        v2NormalizedMap.get(normalizedPath)!.push(info);
    });
    return { exactMap, v2NormalizedMap };
}

/**
 * Builds lookup maps from editableSchemaMetadata once per metadata identity.
 */
export default function useEditableSchemaFieldInfoMaps(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
): EditableFieldInfoMaps {
    return useMemo(
        () => buildEditableSchemaFieldInfoMaps(editableSchemaMetadata),
        [editableSchemaMetadata],
    );
}
