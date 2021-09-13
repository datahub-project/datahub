import React from 'react';
import { EntityType, GlobalTags, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';

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
    const capitalizedPlatform = capitalizeFirstLetter(platformName);
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataFlow, urn)}
            name={name}
            description={description || ''}
            type="Data Pipeline"
            platform={capitalizedPlatform}
            logoUrl={platformLogo || ''}
            owners={owners}
            tags={globalTags || undefined}
            snippet={snippet}
        />
    );
};
