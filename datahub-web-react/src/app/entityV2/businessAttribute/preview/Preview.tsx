import React from 'react';
import { GlobalOutlined } from '@ant-design/icons';
import { EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';
import UrlButton from '../../shared/UrlButton';
import { getRelatedEntitiesUrl } from '../../../businessAttribute/businessAttributeUtils';

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
