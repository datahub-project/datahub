import React from 'react';
import { EntityType, GlobalTags, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const Preview = ({
    urn,
    name,
    description,
    platformName,
    platformLogo,
    owners,
    globalTags,
    snippet,
}: {
    urn: string;
    name: string;
    description?: string | null;
    platformName: string;
    platformLogo?: string | null;
    owners?: Array<Owner> | null;
    globalTags?: GlobalTags | null;
    snippet?: React.ReactNode | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    console.log('heres a description');
    return (
        <DefaultPreviewCard
            url={`/${entityRegistry.getPathName(EntityType.DataJob)}/${urn}`}
            name={name}
            description={description || ''}
            type="Data Task"
            platform={platformName.charAt(0).toUpperCase() + platformName.slice(1)}
            logoUrl={platformLogo || ''}
            owners={
                owners?.map((owner) => {
                    return {
                        urn: owner.owner.urn,
                        name: owner.owner.info?.fullName || '',
                        photoUrl: owner.owner.editableInfo?.pictureLink || '',
                    };
                }) || []
            }
            tags={globalTags || undefined}
            snippet={snippet}
        />
    );
};
