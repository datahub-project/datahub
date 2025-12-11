/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useRefetch } from '@app/entity/shared/EntityContext';
import { useSchemaRefetch } from '@app/entity/shared/tabs/Dataset/Schema/SchemaContext';
import BusinessAttributeGroup from '@app/shared/businessAttribute/BusinessAttributeGroup';
import { useBusinessAttributesFlag } from '@app/useAppConfig';

import { EntityType, SchemaField } from '@types';

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
