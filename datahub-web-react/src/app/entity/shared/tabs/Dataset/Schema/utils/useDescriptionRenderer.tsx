import React from 'react';
import { EditableSchemaMetadata, SchemaField } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';

export default function useDescriptionRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    onUpdateDescription: (update: string, record?: SchemaField) => Promise<any>,
) {
    return (description: string, record: SchemaField): JSX.Element => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        return (
            <DescriptionField
                description={relevantEditableFieldInfo?.description || description}
                original={record.description}
                isEdited={!!relevantEditableFieldInfo?.description}
                onUpdate={(updatedDescription) => onUpdateDescription(updatedDescription, record)}
                editable
            />
        );
    };
}
