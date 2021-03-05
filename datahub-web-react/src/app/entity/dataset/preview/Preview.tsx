import React from 'react';
import { EntityType, FabricType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const Preview = ({
    urn,
    name,
    origin,
    description,
    platformName,
    platformLogo,
    tags,
    owners,
}: {
    urn: string;
    name: string;
    origin: FabricType;
    description?: string | null;
    platformName: string;
    platformLogo?: string | null;
    tags: Array<string>;
    owners?: Array<Owner> | null;
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
            tags={tags}
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
