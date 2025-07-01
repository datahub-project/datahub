import { FolderOutlined } from '@ant-design/icons';
import React from 'react';

import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Owner, ParentNodesResult } from '@types';

export const Preview = ({
    urn,
    name,
    description,
    owners,
    parentNodes,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    parentNodes?: ParentNodesResult | null;
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
            type={entityRegistry.getEntityName(EntityType.GlossaryNode)}
            parentEntities={parentNodes?.nodes}
        />
    );
};
