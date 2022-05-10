import React from 'react';
import {
    EntityType,
    FabricType,
    Owner,
    GlobalTags,
    GlossaryTerms,
    SearchInsight,
    Domain,
    Container,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { IconStyleType } from '../../Entity';

export const Preview = ({
    urn,
    name,
    origin,
    description,
    platformName,
    platformLogo,
    platformInstanceId,
    owners,
    globalTags,
    domain,
    snippet,
    insights,
    glossaryTerms,
    subtype,
    container,
}: {
    urn: string;
    name: string;
    origin: FabricType;
    description?: string | null;
    platformName: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    globalTags?: GlobalTags | null;
    snippet?: React.ReactNode | null;
    insights?: Array<SearchInsight> | null;
    glossaryTerms?: GlossaryTerms | null;
    subtype?: string | null;
    container?: Container | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalPlatformName = capitalizeFirstLetterOnly(platformName);
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dataset, urn)}
            name={name || ''}
            description={description || ''}
            type={capitalizeFirstLetterOnly(subtype) || 'Dataset'}
            logoUrl={platformLogo || ''}
            typeIcon={entityRegistry.getIcon(EntityType.Dataset, 12, IconStyleType.ACCENT)}
            platform={capitalPlatformName}
            platformInstanceId={platformInstanceId}
            qualifier={origin}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            container={container || undefined}
            snippet={snippet}
            glossaryTerms={glossaryTerms || undefined}
            insights={insights}
        />
    );
};
