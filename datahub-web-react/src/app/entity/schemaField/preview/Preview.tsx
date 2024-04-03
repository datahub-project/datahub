import React from 'react';
import { PicCenterOutlined } from '@ant-design/icons';
import { useLocation } from 'react-router-dom';
import { EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';
import UrlButton from '../../shared/UrlButton';
import { getRelatedEntitiesUrl } from '../../../businessAttribute/businessAttributeUtils';

export const Preview = ({
    datasetUrn,
    businessAttributeUrn,
    name,
    description,
    owners,
    previewType,
}: {
    datasetUrn: string;
    businessAttributeUrn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const relatedEntitiesUrl = getRelatedEntitiesUrl(entityRegistry, businessAttributeUrn);

    const url = `${entityRegistry.getEntityUrl(EntityType.Dataset, datasetUrn)}/${encodeURIComponent('Schema')}?schemaFilter=${encodeURIComponent(name)}`;

    return (
        <DefaultPreviewCard
            previewType={previewType}
            url={url}
            name={name ?? ''}
            urn={datasetUrn}
            description={description ?? ''}
            owners={owners}
            logoComponent={<PicCenterOutlined style={{ fontSize: '20px' }} />}
            type="Column"
            typeIcon={entityRegistry.getIcon(EntityType.SchemaField, 14, IconStyleType.ACCENT)}
            entityTitleSuffix={
                decodeURIComponent(location.pathname) !== decodeURIComponent(relatedEntitiesUrl) && (
                    <UrlButton href={relatedEntitiesUrl}>View Related Entities</UrlButton>
                )
            }
        />
    );
};