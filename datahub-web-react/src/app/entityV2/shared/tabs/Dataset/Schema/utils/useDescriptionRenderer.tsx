import React, { useState } from 'react';

import { useIsDocumentationInferenceEnabled } from '@src/app/entityV2/shared/components/inferredDocs/utils';
import { EditableSchemaMetadata, SchemaField, SubResourceType } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useUpdateDescriptionMutation } from '../../../../../../../graphql/mutations.generated';
import { useMutationUrn, useRefetch } from '../../../../../../entity/shared/EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import { useProposeUpdateDescriptionMutation } from '../../../../../../../graphql/proposals.generated';
import { sanitizeRichText } from '../../../Documentation/components/editor/utils';
import { getFieldDescriptionDetails } from './getFieldDescriptionDetails';
import CompactMarkdownViewer from '../../../Documentation/components/CompactMarkdownViewer';

export default function useDescriptionRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    isCompact: boolean,
    options?: {
        onInferSchemaDescriptions?: () => Promise<void>;
        handleShowMore?: (_: string) => void;
    },
) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [expandedRows, setExpandedRows] = useState({});
    const [proposeUpdateDescription] = useProposeUpdateDescriptionMutation();
    const enableInferredDescriptions = useIsDocumentationInferenceEnabled();

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    return (description: string | undefined, record: SchemaField, index: number): JSX.Element => {
        const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find((candidateEditableFieldInfo) =>
            pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { schemaFieldEntity } = record;
        const { displayedDescription, isPropagated, isInferred, sourceDetail } = getFieldDescriptionDetails({
            schemaFieldEntity,
            editableFieldInfo,
            defaultDescription: description,
            enableInferredDescriptions,
        });
        const sanitizedDescription = sanitizeRichText(displayedDescription);
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
                handleShowMore={options?.handleShowMore}
                isReadOnly
                enableInferenceButton
                isPropagated={isPropagated}
                isInferred={isInferred}
                sourceDetail={sourceDetail}
            />
        );
    };
}
