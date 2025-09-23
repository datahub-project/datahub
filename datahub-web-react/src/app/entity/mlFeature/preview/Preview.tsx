import React from 'react';

import { IconStyleType, PreviewType } from '@app/entity/Entity';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform, DataProduct, EntityPath, EntityType, Owner } from '@types';

export const Preview = ({
    urn,
    name,
    platformInstanceId,
    featureNamespace,
    description,
    dataProduct,
    owners,
    platform,
    degree,
    paths,
    previewType,
}: {
    urn: string;
    name: string;
    featureNamespace: string;
    platformInstanceId?: string;
    description?: string | null;
    dataProduct?: DataProduct | null;
    owners?: Array<Owner> | null;
    platform?: DataPlatform | null | undefined;
    degree?: number;
    paths?: EntityPath[];
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlfeature, urn)}
            name={name}
            urn={urn}
            platformInstanceId={platformInstanceId}
            description={description || ''}
            platform={
                platform?.properties?.displayName || capitalizeFirstLetterOnly(platform?.name) || featureNamespace
            }
            logoUrl={platform?.properties?.logoUrl || ''}
            type="ML Feature"
            typeIcon={entityRegistry.getIcon(EntityType.Mlfeature, 14, IconStyleType.ACCENT)}
            owners={owners}
            dataProduct={dataProduct}
            degree={degree}
            paths={paths}
            previewType={previewType}
        />
    );
};
