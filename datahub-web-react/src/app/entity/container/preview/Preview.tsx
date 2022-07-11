import React from 'react';
import {
    Container,
    EntityType,
    Owner,
    SearchInsight,
    SubTypes,
    Domain,
    ParentContainersResult,
    GlobalTags,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';

export const Preview = ({
    urn,
    name,
    platformName,
    platformLogo,
    platformInstanceId,
    description,
    owners,
    tags,
    insights,
    subTypes,
    logoComponent,
    container,
    entityCount,
    domain,
    parentContainers,
}: {
    urn: string;
    name: string;
    platformName: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    tags?: GlobalTags | null;
    insights?: Array<SearchInsight> | null;
    subTypes?: SubTypes | null;
    logoComponent?: JSX.Element;
    container?: Container | null;
    entityCount?: number;
    domain?: Domain | null;
    parentContainers?: ParentContainersResult | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const typeName = (subTypes?.typeNames?.length && subTypes?.typeNames[0]) || 'Container';
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Container, urn)}
            name={name || ''}
            platform={platformName}
            platformInstanceId={platformInstanceId}
            description={description || ''}
            type={typeName}
            owners={owners}
            insights={insights}
            logoUrl={platformLogo || undefined}
            logoComponent={logoComponent}
            container={container || undefined}
            typeIcon={entityRegistry.getIcon(EntityType.Container, 12, IconStyleType.ACCENT)}
            entityCount={entityCount}
            domain={domain || undefined}
            parentContainers={parentContainers}
            tags={tags || undefined}
        />
    );
};
