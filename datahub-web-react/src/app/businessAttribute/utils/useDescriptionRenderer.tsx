/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import DOMPurify from 'dompurify';
import React, { useState } from 'react';

import DescriptionField from '@app/entity/dataset/profile/schema/components/SchemaDescriptionField';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { BusinessAttribute } from '@types';

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
