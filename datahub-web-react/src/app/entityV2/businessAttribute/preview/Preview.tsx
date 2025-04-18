import { GlobalOutlined } from '@ant-design/icons';
import React from 'react';

import { getRelatedEntitiesUrl } from '@app/businessAttribute/businessAttributeUtils';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import UrlButton from '@app/entityV2/shared/UrlButton';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Owner } from '@types';

export const Preview = ({
    urn,
    name,
    description,
    owners,
    previewType,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            previewType={previewType}
            url={entityRegistry.getEntityUrl(EntityType.BusinessAttribute, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            owners={owners}
            logoComponent={<GlobalOutlined style={{ fontSize: '20px' }} />}
            type="Business Attribute"
            typeIcon={entityRegistry.getIcon(EntityType.BusinessAttribute, 14, IconStyleType.ACCENT)}
            entityTitleSuffix={
                <UrlButton href={getRelatedEntitiesUrl(entityRegistry, urn)}>View Related Entities</UrlButton>
            }
        />
    );
};
