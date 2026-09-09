import DOMPurify from 'dompurify';
import React from 'react';

import DescriptionField from '@app/entity/dataset/profile/schema/components/SchemaDescriptionField';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { BusinessAttribute } from '@types';

export default function useDescriptionRenderer(businessAttributeRefetch: () => Promise<any>) {
    const [updateDescription] = useUpdateDescriptionMutation();

    const refresh: any = () => {
        businessAttributeRefetch?.();
    };

    return (description: string, record: BusinessAttribute): JSX.Element => {
        const relevantEditableFieldInfo = record?.properties;
        const displayedDescription = relevantEditableFieldInfo?.description || description;
        const sanitizedDescription = DOMPurify.sanitize(displayedDescription);

        return (
            <DescriptionField
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
