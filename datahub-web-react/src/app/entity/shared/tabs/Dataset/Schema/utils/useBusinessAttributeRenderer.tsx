import React from 'react';
import { EditableSchemaMetadata, EntityType, SchemaField } from '../../../../../../../types.generated';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useMutationUrn, useRefetch } from '../../../../EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import BusinessAttributeGroup from '../../../../../../shared/businessAttribute/BusinessAttributeGroup';

export default function useBusinessAttributeRenderer(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
    filterText: string,
    canEdit: boolean,
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
            <BusinessAttributeGroup
                businessAttribute={relevantEditableFieldInfo?.businessAttributes?.businessAttribute}
                canRemove={canEdit}
                buttonProps={{ size: 'small' }}
                canAddAttribute={canEdit}
                entityUrn={urn}
                entityType={EntityType.Dataset}
                entitySubresource={record.fieldPath}
                highlightText={filterText}
                refetch={refresh}
            />
        );
    };
}
