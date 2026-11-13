import React, { useCallback, useState } from 'react';

import { sanitizeRichText } from '@components/components/Editor/utils';

import DescriptionField from '@app/entityV2/dataset/profile/schema/components/SchemaDescriptionField';
import { useMutationUrn, useRefetch } from '@app/entityV2/shared/EntityContext';
import { useSchemaRefetch } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext';
import useEditableSchemaFieldInfoMaps, {
    EditableFieldInfoMaps,
} from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useEditableSchemaFieldInfoMaps';
import useExtractFieldDescriptionInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldDescriptionInfo';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { EditableSchemaMetadata, SchemaField, SubResourceType } from '@types';

export default function useDescriptionRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    isCompact: boolean,
    fieldInfoMaps?: EditableFieldInfoMaps,
) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
    const fallbackMaps = useEditableSchemaFieldInfoMaps(fieldInfoMaps ? undefined : editableSchemaMetadata);
    const maps = fieldInfoMaps ?? fallbackMaps;
    const extractFieldDescription = useExtractFieldDescriptionInfo(editableSchemaMetadata, maps);

    return useCallback(
        (description: string | undefined, record: SchemaField): JSX.Element => {
            const editableFieldInfo = maps.exactMap.get(record.fieldPath);
            const { displayedDescription, sanitizedDescription, isPropagated, attribution } = extractFieldDescription(
                record,
                description,
            );
            const original = record.description ? sanitizeRichText(record.description) : undefined;

            // Keyed by field path: a row's index shifts when a struct expands or the table is re-sorted.
            const handleExpandedRows = (expanded) =>
                setExpandedRows((prev) => ({ ...prev, [record.fieldPath]: expanded }));

            if (isCompact) {
                return <CompactMarkdownViewer content={displayedDescription} />;
            }

            return (
                <DescriptionField
                    onExpanded={handleExpandedRows}
                    expanded={!!expandedRows[record.fieldPath]}
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
                        }).then(() => {
                            refetch?.();
                            schemaRefetch?.();
                        })
                    }
                    isReadOnly
                    isPropagated={isPropagated}
                    attribution={attribution}
                    dataTestId={`schema-field-${record.fieldPath}-description`}
                />
            );
        },
        [maps, extractFieldDescription, isCompact, expandedRows, updateDescription, urn, refetch, schemaRefetch],
    );
}
