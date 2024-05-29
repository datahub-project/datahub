import React, { useState } from 'react';
import styled from 'styled-components';

import { EditableSchemaMetadata, SchemaField, SubResourceType } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useUpdateDescriptionMutation } from '../../../../../../../graphql/mutations.generated';
import { useMutationUrn, useRefetch } from '../../../../../../entity/shared/EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import { useProposeUpdateDescriptionMutation } from '../../../../../../../graphql/proposals.generated';
import { REDESIGN_COLORS } from '../../../../constants';
import { sanitizeRichText } from '../../../Documentation/components/editor/utils';
import { getFieldDescriptionDetails } from './getFieldDescriptionDetails';

const CompactDescription = styled.div`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    text-wrap: none;
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-weight: 400;
`;

export default function useDescriptionRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    isCompact: boolean,
) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [expandedRows, setExpandedRows] = useState({});
    const [proposeUpdateDescription] = useProposeUpdateDescriptionMutation();

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    return (description: string | undefined, record: SchemaField, index: number): JSX.Element => {
        const editableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find((candidateEditableFieldInfo) =>
            pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { schemaFieldEntity } = record;
        const { displayedDescription, isPropagated, sourceDetail } = getFieldDescriptionDetails({
            schemaFieldEntity,
            editableFieldInfo,
            defaultDescription: description,
        });
        const sanitizedDescription = sanitizeRichText(displayedDescription);
        const original = record.description ? sanitizeRichText(record.description) : undefined;

        const handleExpandedRows = (expanded) => setExpandedRows((prev) => ({ ...prev, [index]: expanded }));

        if (isCompact) {
            return <CompactDescription>{sanitizedDescription}</CompactDescription>;
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
                isReadOnly
                isPropagated={isPropagated}
                sourceDetail={sourceDetail}
            />
        );
    };
}
