import { GlobalOutlined } from '@ant-design/icons';
import React from 'react';

import { getRelatedEntitiesUrl } from '@app/businessAttribute/businessAttributeUtils';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import UrlButton from '@app/entityV2/shared/UrlButton';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Owner } from '@types';

export const Preview = ({
    urn,
    data,
    name,
    description,
    owners,
    previewType,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            entityType={EntityType.BusinessAttribute}
            data={data}
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
