import React from 'react';
import DOMPurify from 'dompurify';
import { EditableSchemaMetadata, SchemaField, SubResourceType } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import LabelField from '../../../../../dataset/profile/schema/components/SchemaLabelField';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import {
    useUpdateDescriptionMutation,
    useUpdateDatasetFieldLabelMutation,
} from '../../../../../../../graphql/mutations.generated';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';

export default function useDescriptionRenderer(editableSchemaMetadata: EditableSchemaMetadata | null | undefined) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [updateFieldLabel] = useUpdateDatasetFieldLabelMutation();

    return (description: string, record: SchemaField): JSX.Element => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const displayedDescription = relevantEditableFieldInfo?.description || description;
        const sanitizedDescription = DOMPurify.sanitize(displayedDescription);
        const originalDescription = record.description ? DOMPurify.sanitize(record.description) : undefined;

        const displayedLabel = relevantEditableFieldInfo?.label || '';
        const sanitizedLabel = DOMPurify.sanitize(displayedLabel);
        const originalLabel = record.label ? DOMPurify.sanitize(record.label) : undefined;

        return (
            <>
                <LabelField
                    label={sanitizedLabel}
                    original={originalLabel}
                    isEdited={!!relevantEditableFieldInfo?.label}
                    onUpdate={(updatedLabel) =>
                        updateFieldLabel({
                            variables: {
                                input: {
                                    label: DOMPurify.sanitize(updatedLabel),
                                    resourceUrn: urn,
                                    subResource: record.fieldPath,
                                    subResourceType: SubResourceType.DatasetField,
                                },
                            },
                        }).then(refetch)
                    }
                />
                <br />
                <DescriptionField
                    description={sanitizedDescription}
                    original={originalDescription}
                    isEdited={!!relevantEditableFieldInfo?.description}
                    onUpdate={(updatedDescription) =>
                        updateDescription({
                            variables: {
                                input: {
                                    description: DOMPurify.sanitize(updatedDescription),
                                    resourceUrn: urn,
                                    subResource: record.fieldPath,
                                    subResourceType: SubResourceType.DatasetField,
                                },
                            },
                        }).then(refetch)
                    }
                />
            </>
        );
    };
}
