import React, { useState } from 'react';

import { useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import DescriptionField from '@app/entityV2/dataset/profile/schema/components/SchemaDescriptionField';
import { pathMatchesExact } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { useSchemaRefetch } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext';
import useExtractFieldDescriptionInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldDescriptionInfo';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { sanitizeRichText } from '@app/entityV2/shared/tabs/Documentation/components/editor/utils';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { EditableSchemaMetadata, SchemaField, SubResourceType } from '@types';

export default function useDescriptionRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    isCompact: boolean,
) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [expandedRows, setExpandedRows] = useState({});
    const extractFieldDescription = useExtractFieldDescriptionInfo(editableSchemaMetadata);

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    return (description: string | undefined, record: SchemaField, index: number): JSX.Element => {
        const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { displayedDescription, sanitizedDescription, isPropagated, sourceDetail } = extractFieldDescription(
            record,
            description,
        );
        const original = record.description ? sanitizeRichText(record.description) : undefined;

        const handleExpandedRows = (expanded) => setExpandedRows((prev) => ({ ...prev, [index]: expanded }));

        if (isCompact) {
            return <CompactMarkdownViewer content={displayedDescription} />;
        }

        return (
            <DescriptionField
                onExpanded={handleExpandedRows}
                expanded={!!expandedRows[index]}
                description={sanitizedDescription}
                original={original}
                isEdited={!!editableFieldInfo?.description}
                onUpdate={(updatedDescription) =>
                    updateDescription({
                        variables: {
                            input: {
                                description: sanitizeRichText(updatedDescription),
                                resourceUrn: urn,
                                subResource: record.fieldPath,
                                subResourceType: SubResourceType.DatasetField,
                            },
                        },
                    }).then(refresh)
                }
                isReadOnly
                isPropagated={isPropagated}
                sourceDetail={sourceDetail}
            />
        );
    };
}
