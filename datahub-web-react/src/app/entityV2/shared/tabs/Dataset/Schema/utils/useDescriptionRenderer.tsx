import React, { useState } from 'react';

import { useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import DescriptionField from '@app/entityV2/dataset/profile/schema/components/SchemaDescriptionField';
import { pathMatchesExact } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { useSchemaRefetch } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext';
import useExtractFieldDescriptionInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldDescriptionInfo';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { sanitizeRichText } from '@app/entityV2/shared/tabs/Documentation/components/editor/utils';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { useProposeUpdateDescriptionMutation } from '@graphql/proposals.generated';
import { EditableSchemaMetadata, SchemaField, SubResourceType } from '@types';

export default function useDescriptionRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    isCompact: boolean,
<<<<<<< HEAD
    options?: {
        onInferSchemaDescriptions?: () => Promise<void>;
    },
||||||| dbcab5e404
    options?: {
        handleShowMore?: (_: string) => void;
    },
=======
>>>>>>> master
) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [expandedRows, setExpandedRows] = useState({});
    const [proposeUpdateDescription] = useProposeUpdateDescriptionMutation();
    const extractFieldDescription = useExtractFieldDescriptionInfo(editableSchemaMetadata);

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    return (description: string | undefined, record: SchemaField, index: number): JSX.Element => {
        const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { displayedDescription, sanitizedDescription, isPropagated, isInferred, sourceDetail } =
            extractFieldDescription(record, description);
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
<<<<<<< HEAD
                onPropose={(updatedDescription) =>
                    proposeUpdateDescription({
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
                onInferDescription={options?.onInferSchemaDescriptions}
||||||| dbcab5e404
                handleShowMore={options?.handleShowMore}
=======
>>>>>>> master
                isReadOnly
                enableInferenceButton
                isPropagated={isPropagated}
                isInferred={isInferred}
                sourceDetail={sourceDetail}
            />
        );
    };
}
