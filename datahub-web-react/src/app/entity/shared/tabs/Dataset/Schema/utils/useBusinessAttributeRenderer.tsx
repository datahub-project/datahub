import React from 'react';
import { EntityType, SchemaField } from '../../../../../../../types.generated';
import { useRefetch } from '../../../../EntityContext';
import { useSchemaRefetch } from '../SchemaContext';
import BusinessAttributeGroup from '../../../../../../shared/businessAttribute/BusinessAttributeGroup';
import { useBusinessAttributesFlag } from '../../../../../../useAppConfig';

export default function useBusinessAttributeRenderer(filterText: string, canEdit: boolean) {
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();

    const businessAttributesFlag = useBusinessAttributesFlag();

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    return (businessAttribute: string, record: SchemaField): JSX.Element | null => {
        return businessAttributesFlag ? (
            <BusinessAttributeGroup
                businessAttribute={record?.schemaFieldEntity?.businessAttributes?.businessAttribute || undefined}
                canRemove={canEdit}
                buttonProps={{ size: 'small' }}
                canAddAttribute={canEdit}
                entityUrn={record?.schemaFieldEntity?.urn}
                entityType={EntityType.Dataset}
                entitySubresource={record.fieldPath}
                highlightText={filterText}
                refetch={refresh}
            />
        ) : null;
    };
}
