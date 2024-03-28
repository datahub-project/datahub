import React, { useState } from 'react';
import DOMPurify from 'dompurify';
import { SchemaField, SubResourceType } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import { useUpdateDescriptionMutation } from '../../../../../../../graphql/mutations.generated';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';
import { useSchemaRefetch } from '../SchemaContext';

export default function useDescriptionRenderer() {
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
        const displayedDescription = record?.description || description;
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
                isEdited={!!record.description}
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
                isReadOnly
            />
        );
    };
}
