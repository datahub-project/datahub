import React, { useState } from 'react';
import DOMPurify from 'dompurify';
import { EditableSchemaMetadata, SchemaField, SubResourceType } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import { useUpdateDescriptionMutation } from '../../../../../../../graphql/mutations.generated';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useProposeUpdateDescriptionMutation } from '../../../../../../../graphql/proposals.generated';
import useExtractFieldDescriptionInfo from './useExtractFieldDescriptionInfo';

export default function useDescriptionRenderer(editableSchemaMetadata: EditableSchemaMetadata | null | undefined) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [expandedBARows, setExpandedBARows] = useState({});
    const [proposeUpdateDescription] = useProposeUpdateDescriptionMutation();
    const extractFieldDescription = useExtractFieldDescriptionInfo(editableSchemaMetadata);

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    return (description: string, record: SchemaField, index: number): JSX.Element => {
        const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { sanitizedDescription, isPropagated, sourceDetail } = extractFieldDescription(record, description);
        const original = record.description ? DOMPurify.sanitize(record.description) : undefined;
        const businessAttributeDescription =
            record?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties
                ?.description || '';

        const handleBAExpandedRows = (expanded) => setExpandedBARows((prev) => ({ ...prev, [index]: expanded }));

        return (
            <DescriptionField
                businessAttributeDescription={businessAttributeDescription}
                onBAExpanded={handleBAExpandedRows}
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
                onPropose={(updatedDescription) =>
                    proposeUpdateDescription({
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
