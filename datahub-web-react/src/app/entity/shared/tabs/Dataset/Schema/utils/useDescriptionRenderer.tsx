import React, { useState } from 'react';
import DOMPurify from 'dompurify';
import { EditableSchemaMetadata, SchemaField, SubResourceType } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import { useUpdateDescriptionMutation } from '../../../../../../../graphql/mutations.generated';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { getFieldDescriptionDetails } from './getFieldDescriptionDetails';

export default function useDescriptionRenderer(editableSchemaMetadata: EditableSchemaMetadata | null | undefined) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [expandedRows, setExpandedRows] = useState({});
    const [expandedBARows, setExpandedBARows] = useState({});

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    return (description: string, record: SchemaField, index: number): JSX.Element => {
        const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { schemaFieldEntity } = record;
        const { displayedDescription, isPropagated, sourceDetail } = getFieldDescriptionDetails({
            schemaFieldEntity,
            editableFieldInfo,
            defaultDescription: description,
        });

        const sanitizedDescription = DOMPurify.sanitize(displayedDescription);
        const original = record.description ? DOMPurify.sanitize(record.description) : undefined;
        const businessAttributeDescription =
            record?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties
                ?.description || '';

        const handleExpandedRows = (expanded) => setExpandedRows((prev) => ({ ...prev, [index]: expanded }));
        const handleBAExpandedRows = (expanded) => setExpandedBARows((prev) => ({ ...prev, [index]: expanded }));

        return (
            <DescriptionField
                businessAttributeDescription={businessAttributeDescription}
                onExpanded={handleExpandedRows}
                onBAExpanded={handleBAExpandedRows}
                expanded={!!expandedRows[index]}
                baExpanded={!!expandedBARows[index]}
                description={sanitizedDescription}
                original={original}
                isEdited={!!editableFieldInfo?.description}
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
                    }).then(refresh)
                }
                isPropagated={isPropagated}
                sourceDetail={sourceDetail}
                isReadOnly
            />
        );
    };
}
