import DOMPurify from 'dompurify';
import React, { useState } from 'react';

import DescriptionField from '@app/entity/dataset/profile/schema/components/SchemaDescriptionField';
import { pathMatchesNewPath } from '@app/entity/dataset/profile/schema/utils/utils';
import { useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { useSchemaRefetch } from '@app/entity/shared/tabs/Dataset/Schema/SchemaContext';
import { getFieldDescriptionDetails } from '@app/entity/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { EditableSchemaMetadata, SchemaField, SubResourceType } from '@types';

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
