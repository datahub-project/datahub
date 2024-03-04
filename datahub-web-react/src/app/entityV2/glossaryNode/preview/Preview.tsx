import React from 'react';
import { FolderOutlined } from '@ant-design/icons';
import { EntityType, Owner, ParentNodesResult } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

export const Preview = ({
    urn,
    name,
    description,
    owners,
    parentNodes,
    headerDropdownItems,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    parentNodes?: ParentNodesResult | null;
    headerDropdownItems?: Set<EntityMenuItems>;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.GlossaryNode, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            owners={owners}
            logoComponent={<FolderOutlined style={{ fontSize: '20px' }} />}
            entityType={EntityType.GlossaryNode}
            parentEntities={parentNodes?.nodes}
            headerDropdownItems={headerDropdownItems}
        />
    );
};
