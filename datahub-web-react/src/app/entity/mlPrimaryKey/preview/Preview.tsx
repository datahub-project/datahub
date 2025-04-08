import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform, DataProduct, EntityPath, EntityType, Owner } from '@types';

export const Preview = ({
    urn,
    name,
    featureNamespace,
    description,
    owners,
    platform,
    dataProduct,
    platformInstanceId,
    degree,
    paths,
}: {
    urn: string;
    name: string;
    featureNamespace: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    platform?: DataPlatform | null | undefined;
    dataProduct?: DataProduct | null;
    platformInstanceId?: string;
    degree?: number;
    paths?: EntityPath[];
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlprimaryKey, urn)}
            name={name}
            urn={urn}
            description={description || ''}
            platform={
                platform?.properties?.displayName || capitalizeFirstLetterOnly(platform?.name) || featureNamespace
            }
            logoUrl={platform?.properties?.logoUrl || ''}
            type="ML Primary Key"
            typeIcon={entityRegistry.getIcon(EntityType.MlprimaryKey, 14, IconStyleType.ACCENT)}
            owners={owners}
            dataProduct={dataProduct}
            platformInstanceId={platformInstanceId}
            degree={degree}
            paths={paths}
        />
    );
};
