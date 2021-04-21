import React from 'react';
import { EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const Preview = ({
    urn,
    name,
    description,
    platformName,
    owners,
    snippet,
}: {
    urn: string;
    name: string;
    description?: string | null;
    platformName: string;
    owners?: Array<Owner> | null;
    snippet?: React.ReactNode | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={`/${entityRegistry.getPathName(EntityType.DataFlow)}/${urn}`}
            name={name || ''}
            description={description || ''}
            type="DataFlow"
            platform={platformName}
            qualifier={origin}
            owners={
                owners?.map((owner) => {
                    return {
                        urn: owner.owner.urn,
                        name: owner.owner.info?.fullName || '',
                        photoUrl: owner.owner.editableInfo?.pictureLink || '',
                    };
                }) || []
            }
            snippet={snippet}
        />
    );
};
