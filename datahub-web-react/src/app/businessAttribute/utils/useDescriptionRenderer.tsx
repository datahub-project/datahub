import React, { useState } from 'react';
import DOMPurify from 'dompurify';
import { BusinessAttribute } from '../../../types.generated';
import DescriptionField from '../../entity/dataset/profile/schema/components/SchemaDescriptionField';
import { useUpdateDescriptionMutation } from '../../../graphql/mutations.generated';

export default function useDescriptionRenderer(businessAttributeRefetch: () => Promise<any>) {
    const [updateDescription] = useUpdateDescriptionMutation();
    const [expandedRows, setExpandedRows] = useState({});

    const refresh: any = () => {
        businessAttributeRefetch?.();
    };

    return (description: string, record: BusinessAttribute, index: number): JSX.Element => {
        const relevantEditableFieldInfo = record?.properties;
        const displayedDescription = relevantEditableFieldInfo?.description || description;
        const sanitizedDescription = DOMPurify.sanitize(displayedDescription);

        const handleExpandedRows = (expanded) => setExpandedRows((prev) => ({ ...prev, [index]: expanded }));

        return (
            <DescriptionField
                onExpanded={handleExpandedRows}
                expanded={!!expandedRows[index]}
                description={sanitizedDescription}
                onUpdate={(updatedDescription) =>
                    updateDescription({
                        variables: {
                            input: {
                                description: DOMPurify.sanitize(updatedDescription),
                                resourceUrn: record.urn,
                            },
                        },
                    }).then(refresh)
                }
            />
        );
    };
}
//
