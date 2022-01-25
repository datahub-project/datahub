import React from 'react';
import { AccessLevel, EntityType, GlobalTags, GlossaryTerms, Owner, SearchInsight } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';

export const DashboardPreview = ({
    urn,
    name,
    description,
    platform,
    access,
    owners,
    tags,
    glossaryTerms,
    insights,
    logoUrl,
}: {
    urn: string;
    platform: string;
    name?: string;
    description?: string | null;
    access?: AccessLevel | null;
    owners?: Array<Owner> | null;
    tags?: GlobalTags;
    glossaryTerms?: GlossaryTerms | null;
    insights?: Array<SearchInsight> | null;
    logoUrl?: string | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalizedPlatform = capitalizeFirstLetter(platform);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dashboard, urn)}
            name={name || ''}
            description={description || ''}
            type="Dashboard"
            logoUrl={logoUrl || ''}
            platform={capitalizedPlatform}
            qualifier={access}
            owners={owners}
            tags={tags}
            glossaryTerms={glossaryTerms || undefined}
            insights={insights}
        />
    );
};
