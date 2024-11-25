import React from 'react';
import { PicCenterOutlined } from '@ant-design/icons';
import { Dataset, EntityType, Owner, ParentContainersResult } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';

export const Preview = ({
    datasetUrn,
    name,
    description,
    owners,
    previewType,
    parentContainers,
    platformName,
    platformLogo,
    platformInstanceId,
    parentDataset,
}: {
    datasetUrn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    previewType: PreviewType;
    parentContainers?: ParentContainersResult | null;
    platformName?: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    parentDataset?: Dataset;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    const url = `${entityRegistry.getEntityUrl(EntityType.Dataset, datasetUrn)}/${encodeURIComponent(
        'Schema',
    )}?schemaFilter=${encodeURIComponent(name)}`;

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
            logoUrl={platformLogo || ''}
            platform={platformName}
            platformInstanceId={platformInstanceId}
            parentContainers={parentContainers}
            parentDataset={parentDataset}
        />
    );
};
