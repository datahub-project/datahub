import React from 'react';
import { EditableSchemaMetadata, EntityType, SchemaField } from '../../../../../../../types.generated';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import BusinessAttributeGroup from '../../../../../../shared/businessAttribute/BusinessAttributeGroup';

export default function useBusinessAttributeRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    attributeHoveredIndex: string | undefined,
    setAttributeHoveredIndex: (index: string | undefined) => void,
    filterText: string,
) {
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    return (businessAttribute: string, record: SchemaField): JSX.Element => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        return (
            <div data-testid={`schema-field-${record.fieldPath}-businessAttribute`}>
                <BusinessAttributeGroup
                    businessAttribute={relevantEditableFieldInfo?.businessAttributes?.businessAttribute}
                    canRemove
                    buttonProps={{ size: 'small' }}
                    canAddAttribute={attributeHoveredIndex === record.fieldPath}
                    onOpenModal={() => setAttributeHoveredIndex(undefined)}
                    entityUrn={urn}
                    entityType={EntityType.Dataset}
                    entitySubresource={record.fieldPath}
                    highlightText={filterText}
                    refetch={refresh}
                />
            </div>
        );
    };
}
