import React from 'react';
import { DataPlatform, DataProduct, EntityPath, EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';

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
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const platformName = platform?.properties?.displayName || capitalizeFirstLetterOnly(platform?.name);
    const platformTitle =
        platformName && featureNamespace
            ? `${platformName} > ${featureNamespace}`
            : platformName || featureNamespace || '';

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlfeature, urn)}
            name={name}
            urn={urn}
            platformInstanceId={platformInstanceId}
            description={description || ''}
            platform={platformTitle}
            logoUrl={platform?.properties?.logoUrl || ''}
            type="ML Feature"
            typeIcon={entityRegistry.getIcon(EntityType.Mlfeature, 14, IconStyleType.ACCENT)}
            owners={owners}
            dataProduct={dataProduct}
            degree={degree}
            paths={paths}
        />
    );
};
