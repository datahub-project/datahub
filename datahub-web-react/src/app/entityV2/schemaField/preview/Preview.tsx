import React from 'react';
import { PicCenterOutlined } from '@ant-design/icons';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { capitalizeFirstLetterOnly } from '@src/app/shared/textUtil';
import { EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';

export const Preview = ({
    data,
    datasetUrn,
    name,
    description,
    owners,
    previewType,
    parent,
}: {
    data: GenericEntityProperties | null;
    datasetUrn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    previewType: PreviewType;
    parent?: GenericEntityProperties;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    const url = `${entityRegistry.getEntityUrl(EntityType.Dataset, datasetUrn)}/${encodeURIComponent(
        'Columns',
    )}?schemaFilter=${encodeURIComponent(name)}`;

    return (
        <DefaultPreviewCard
            data={data}
            entityType={EntityType.SchemaField}
            platform={parent?.platform?.properties?.displayName || capitalizeFirstLetterOnly(parent?.platform?.name)}
            logoUrl={parent?.platform?.properties?.logoUrl || ''}
            previewType={previewType}
            url={url}
            name={name ?? ''}
            urn={datasetUrn}
            description={description ?? ''}
            owners={owners}
            logoComponent={<PicCenterOutlined style={{ fontSize: '20px' }} />}
            type="Column"
            typeIcon={entityRegistry.getIcon(EntityType.SchemaField, 14, IconStyleType.ACCENT)}
        />
    );
};
