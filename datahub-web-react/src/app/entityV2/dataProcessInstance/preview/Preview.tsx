import { GenericEntityProperties } from '@app/entity/shared/types';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
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
} from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';

export default function Preview({
    urn,
    name,
    data,
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
}: {
    urn: string;
    name: string;
    data: GenericEntityProperties | null;
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
}): JSX.Element {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            entityType={EntityType.DataProcessInstance}
            url={entityRegistry.getEntityUrl(EntityType.DataProcessInstance, urn)}
            name={name}
            urn={urn}
            data={data}
            description={description || ''}
            type={subType || 'Process Instance'}
            typeIcon={entityRegistry.getIcon(EntityType.DataProcessInstance, 14, IconStyleType.ACCENT)}
            platform={platformName || undefined}
            logoUrl={platformLogo || undefined}
            platformInstanceId={platformInstanceId}
            container={container}
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
        />
    );
}
