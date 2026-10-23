import React, { useMemo } from 'react';

import { InteriorTitleContent } from '@app/entityV2/dataset/profile/schema/components/InteriorTitleContent';
import { getEffectivePrimaryKeys, isPrimaryKeyField } from '@app/entityV2/dataset/profile/schema/utils/primaryKeys';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';

import { EditableSchemaMetadata, SchemaMetadata } from '@types';

export default function useSchemaTitleRenderer(
    parentUrn: string,
    schemaMetadata: SchemaMetadata | undefined | null,
    editableSchemaMetadata: EditableSchemaMetadata | undefined | null,
    filterText: string,
    isCompact?: boolean,
) {
    const primaryKeys = useMemo(
        () => getEffectivePrimaryKeys(schemaMetadata, editableSchemaMetadata),
        [schemaMetadata, editableSchemaMetadata],
    );
    return (fieldPath: string, record: ExtendedSchemaFields): JSX.Element => {
        return (
            <InteriorTitleContent
                parentUrn={parentUrn}
                isCompact={isCompact}
                filterText={filterText}
                fieldPath={fieldPath}
                record={record}
                schemaMetadata={schemaMetadata}
                isPrimaryKey={isPrimaryKeyField(primaryKeys, record)}
            />
        );
    };
}
