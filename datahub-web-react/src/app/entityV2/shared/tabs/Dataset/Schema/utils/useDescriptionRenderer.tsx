import React, { useState } from 'react';

import { EditableSchemaMetadata, SchemaField, SubResourceType } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import { pathMatchesExact } from '../../../../../dataset/profile/schema/utils/utils';
import { useUpdateDescriptionMutation } from '../../../../../../../graphql/mutations.generated';
import { useMutationUrn, useRefetch } from '../../../../../../entity/shared/EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import { sanitizeRichText } from '../../../Documentation/components/editor/utils';
import CompactMarkdownViewer from '../../../Documentation/components/CompactMarkdownViewer';
import useExtractFieldDescriptionInfo from './useExtractFieldDescriptionInfo';

export default function useDescriptionRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    isCompact: boolean,
    options?: {
        handleShowMore?: (_: string) => void;
    },
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
        const { schemaFieldEntity } = record;
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
                fieldPath={schemaFieldEntity?.fieldPath}
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
                handleShowMore={options?.handleShowMore}
                isReadOnly
                isPropagated={isPropagated}
                sourceDetail={sourceDetail}
            />
        );
    };
}
