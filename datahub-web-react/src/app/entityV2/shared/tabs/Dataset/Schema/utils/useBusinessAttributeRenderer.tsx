import React from 'react';

import { useRefetch } from '@app/entity/shared/EntityContext';
import BusinessAttributeGroup from '@app/shared/businessAttribute/BusinessAttributeGroup';
import { useBusinessAttributesFlag } from '@app/useAppConfig';

import { EntityType, SchemaField } from '@types';

export default function useBusinessAttributeRenderer(filterText: string, canEdit: boolean) {
    const refetch = useRefetch();

    const businessAttributesFlag = useBusinessAttributesFlag();

    const refresh: any = () => {
        refetch?.();
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
