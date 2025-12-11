/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { sanitizeRichText } from '@components/components/Editor/utils';

import { getFieldDescriptionDetails } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';
import { pathMatchesExact } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { EditableSchemaMetadata, SchemaField } from '@src/types.generated';

export default function useExtractFieldDescriptionInfo(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
) {
    return (record: SchemaField, description: string | undefined | null = null) => {
        const editableFieldInfoB = editableSchemaMetadata?.editableSchemaFieldInfo?.find((candidateEditableFieldInfo) =>
            pathMatchesExact(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );
        const { displayedDescription, isPropagated, sourceDetail, attribution } = getFieldDescriptionDetails({
            schemaFieldEntity: record.schemaFieldEntity,
            editableFieldInfo: editableFieldInfoB,
            defaultDescription: description || record?.description,
        });

        const sanitizedDescription = sanitizeRichText(displayedDescription);

        return {
            displayedDescription,
            sanitizedDescription,
            isPropagated,
            sourceDetail,
            attribution,
        };
    };
}
