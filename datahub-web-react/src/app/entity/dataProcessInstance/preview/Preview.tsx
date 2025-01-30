import React from 'react';
import {
    DataProduct,
    Deprecation,
    Domain,
    Entity as GeneratedEntity,
    EntityPath,
    EntityType,
    GlobalTags,
    Health,
    Owner,
    SearchInsight,
    Container,
    ParentContainersResult,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';

export const Preview = ({
    urn,
    name,
    subType,
    description,
    platformName,
    platformLogo,
    platformInstanceId,
    container,
    owners,
    domain,
    dataProduct,
    deprecation,
    globalTags,
    snippet,
    insights,
    externalUrl,
    degree,
    paths,
    health,
    parentEntities,
    parentContainers,
    duration,
    status,
    startTime,
}: {
    urn: string;
    name: string;
    subType?: string | null;
    description?: string | null;
    platformName?: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    container?: Container;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    dataProduct?: DataProduct | null;
    deprecation?: Deprecation | null;
    globalTags?: GlobalTags | null;
    snippet?: React.ReactNode | null;
    insights?: Array<SearchInsight> | null;
    externalUrl?: string | null;
    degree?: number;
    paths?: EntityPath[];
    health?: Health[] | null;
    parentEntities?: Array<GeneratedEntity> | null;
    parentContainers?: ParentContainersResult | null;
    duration?: number | null;
    status?: string | null;
    startTime?: number | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataProcessInstance, urn)}
            name={name}
            urn={urn}
            description={description || ''}
            type={subType || 'Process Instance'}
            typeIcon={entityRegistry.getIcon(EntityType.DataProcessInstance, 14, IconStyleType.ACCENT)}
            platform={platformName || undefined}
            logoUrl={platformLogo || undefined}
            platformInstanceId={platformInstanceId}
            container={container}
            parentContainers={parentContainers}
            owners={owners}
            tags={globalTags || undefined}
            domain={domain}
            dataProduct={dataProduct}
            snippet={snippet}
            deprecation={deprecation}
            dataTestID="process-instance-item-preview"
            insights={insights}
            externalUrl={externalUrl}
            degree={degree}
            paths={paths}
            health={health || undefined}
            parentEntities={parentEntities}
            duration={duration}
            status={status}
            startTime={startTime}
        />
    );
};
