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
    ParentContainersResult,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/textUtil';

export const ChartPreview = ({
    urn,
    name,
    description,
    platform,
    platformInstanceId,
    access,
    owners,
    tags,
    glossaryTerms,
    domain,
    container,
    insights,
    logoUrl,
    parentContainers,
}: {
    urn: string;
    platform: string;
    platformInstanceId?: string;
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
    parentContainers?: ParentContainersResult | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalizedPlatform = capitalizeFirstLetter(platform);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Chart, urn)}
            name={name || ''}
            description={description || ''}
            type="Chart"
            logoUrl={logoUrl || ''}
            platform={capitalizedPlatform}
            platformInstanceId={platformInstanceId}
            qualifier={access}
            tags={tags}
            owners={owners}
            glossaryTerms={glossaryTerms || undefined}
            domain={domain}
            container={container || undefined}
            insights={insights}
            parentContainers={parentContainers}
        />
    );
};
