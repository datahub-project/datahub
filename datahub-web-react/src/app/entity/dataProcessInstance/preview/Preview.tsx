import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import {
    Container,
    DataProcessRunEvent,
    DataProduct,
    Deprecation,
    Domain,
    EntityPath,
    EntityType,
    Entity as GeneratedEntity,
    GlobalTags,
    Health,
    Owner,
    ParentContainersResult,
    SearchInsight,
} from '@types';

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
    lastRunEvent,
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
    lastRunEvent?: DataProcessRunEvent | null;
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
            lastRunEvent={lastRunEvent}
        />
    );
};
