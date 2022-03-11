import React from 'react';
import {
    AccessLevel,
    Domain,
    Container,
    EntityType,
    GlobalTags,
    GlossaryTerms,
    Owner,
    SearchInsight,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/textUtil';

export const DashboardPreview = ({
    urn,
    name,
    description,
    platform,
    access,
    owners,
    tags,
    glossaryTerms,
    domain,
    container,
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
    domain?: Domain | null;
    container?: Container | null;
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
            container={container || undefined}
            glossaryTerms={glossaryTerms || undefined}
            domain={domain}
            insights={insights}
        />
    );
};
