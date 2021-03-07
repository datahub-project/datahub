import React from 'react';
import { EntityType, FabricType, Owner, GlobalTags } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const Preview = ({
    urn,
    name,
    origin,
    description,
    platformName,
    platformLogo,
    owners,
    globalTags,
}: {
    urn: string;
    name: string;
    origin: FabricType;
    description?: string | null;
    platformName: string;
    platformLogo?: string | null;
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
            logoUrl={platformLogo || ''}
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
