import React from 'react';
import { DataPlatform, EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const Preview = ({
    urn,
    name,
    featureNamespace,
    description,
    owners,
    platform,
}: {
    urn: string;
    name: string;
    featureNamespace: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    platform?: DataPlatform | null | undefined;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlfeature, urn)}
            name={name}
            description={description || ''}
            platform={capitalizeFirstLetterOnly(platform?.properties?.displayName) || featureNamespace}
            logoUrl={platform?.properties?.logoUrl || ''}
            type="MLFeature"
            owners={owners}
        />
    );
};
