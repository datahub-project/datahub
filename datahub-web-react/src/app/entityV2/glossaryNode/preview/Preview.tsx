/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FolderOutlined } from '@ant-design/icons';
import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Owner, ParentNodesResult } from '@types';

export const Preview = ({
    urn,
    data,
    name,
    description,
    owners,
    parentNodes,
    headerDropdownItems,
    previewType,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    parentNodes?: ParentNodesResult | null;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.GlossaryNode, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={description || ''}
            owners={owners}
            logoComponent={<FolderOutlined style={{ fontSize: '20px' }} />}
            entityType={EntityType.GlossaryNode}
            parentEntities={parentNodes?.nodes}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
        />
    );
};
