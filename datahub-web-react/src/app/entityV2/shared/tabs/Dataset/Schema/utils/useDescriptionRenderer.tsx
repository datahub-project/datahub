import React, { useState } from 'react';
import DOMPurify from 'dompurify';
import styled from 'styled-components';

import { EditableSchemaMetadata, SchemaField, SubResourceType } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useUpdateDescriptionMutation } from '../../../../../../../graphql/mutations.generated';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import { useProposeUpdateDescriptionMutation } from '../../../../../../../graphql/proposals.generated';
import { REDESIGN_COLORS } from '../../../../constants';

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

    return (description: string, record: SchemaField, index: number): JSX.Element => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const displayedDescription = relevantEditableFieldInfo?.description || description;
        const sanitizedDescription = DOMPurify.sanitize(displayedDescription);
        const original = record.description ? DOMPurify.sanitize(record.description) : undefined;

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
                isReadOnly
            />
        );
    };
}
