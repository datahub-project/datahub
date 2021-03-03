import React from 'react';
import { EntityType, FabricType, Owner, GlobalTags } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getLogoFromPlatform } from '../../chart/getLogoFromPlatform';

export const Preview = ({
    urn,
    name,
    origin,
    description,
    platformName,
    owners,
    globalTags,
}: {
    urn: string;
    name: string;
    origin: FabricType;
    description?: string | null;
    platformName: string;
    owners?: Array<Owner> | null;
    globalTags?: GlobalTags | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    return (
        <DefaultPreviewCard
            url={`/${entityRegistry.getPathName(EntityType.Dataset)}/${urn}`}
            name={name || ''}
            description={description || ''}
            type="Dataset"
            logoUrl={getLogoFromPlatform(platformName) || ''}
            platform={platformName}
            qualifier={origin}
            tags={globalTags || undefined}
            owners={
                owners?.map((owner) => {
                    return {
                        urn: owner.owner.urn,
                        name: owner.owner.info?.fullName || '',
                        photoUrl: owner.owner.editableInfo?.pictureLink || '',
                    };
                }) || []
            }
        />
    );
};
